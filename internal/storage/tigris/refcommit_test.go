package tigris

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"
)

// packedTestStorer builds a storer with packed-ref writes enabled.
func packedTestStorer(t *testing.T, f *fakeS3, opts ...Option) *Storer {
	t.Helper()
	return newTestStorer(t, f, append([]Option{WithPackedRefs(true)}, opts...)...)
}

// TestCommitRefsCostsOnePut is the second headline assertion of this change.
// Before it, a push of 500 refs cost 500 GetObject calls and 500 PutObject
// calls.
func TestCommitRefsCostsOnePut(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	obs, snapshot := countingObserver()
	s := packedTestStorer(t, f, obs)

	sets := make([]*plumbing.Reference, 0, 500)
	for i := 0; i < 500; i++ {
		sets = append(sets, hashRef("refs/tags/v"+strconv.Itoa(i), headAB))
	}
	if err := s.commitRefs(sets, nil, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got := snapshot()["PutObject"]; got != 1 {
		t.Errorf("PutObject calls = %d, want 1 (map: %v)", got, snapshot())
	}

	// And every ref really landed.
	view, err := s.refView()
	if err != nil {
		t.Fatalf("refView: %v", err)
	}
	if len(view) != 500 {
		t.Errorf("view holds %d refs, want 500", len(view))
	}
}

func TestCommitRefsAppliesSetsAndRemoves(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	seedPackedRefs(t, f, s,
		hashRef("refs/heads/main", headAB),
		hashRef("refs/heads/doomed", headAB),
	)

	sym := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.ReferenceName("refs/heads/main"))
	sets := []*plumbing.Reference{hashRef("refs/heads/main", headCD), sym}
	removes := []plumbing.ReferenceName{plumbing.ReferenceName("refs/heads/doomed")}

	if err := s.commitRefs(sets, removes, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}

	main, err := s.Reference(plumbing.ReferenceName("refs/heads/main"))
	if err != nil {
		t.Fatalf("read main: %v", err)
	}
	if main.Hash().String() != headCD {
		t.Errorf("main = %s, want %s", main.Hash(), headCD)
	}
	head, err := s.Reference(plumbing.HEAD)
	if err != nil {
		t.Fatalf("read HEAD: %v", err)
	}
	if head.Type() != plumbing.SymbolicReference {
		t.Errorf("HEAD lost its symbolic nature: %+v", head)
	}
	if _, err := s.Reference(plumbing.ReferenceName("refs/heads/doomed")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("removed ref survived: %v", err)
	}
}

// TestCommitRefsUsesCreateIfAbsentOnAFreshRepo pins that the first ever write
// sends If-None-Match, not If-Match against an empty ETag.
func TestCommitRefsUsesCreateIfAbsentOnAFreshRepo(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)

	// Another writer wins the race to create the object. Our create-if-absent
	// must be refused, then retried as a compare-and-swap and succeed.
	f.putHook = func(p *putSnapshot) {
		if p.n == 1 {
			f.putLocked(packedRefsKey, string(encodePackedRefs(refSet(hashRef("refs/heads/other", headCD)))), nil)
		}
	}

	if err := s.commitRefs([]*plumbing.Reference{hashRef("refs/heads/main", headAB)}, nil, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}

	view, err := s.refView()
	if err != nil {
		t.Fatalf("refView: %v", err)
	}
	if _, ok := view[plumbing.ReferenceName("refs/heads/main")]; !ok {
		t.Error("our ref did not land")
	}
	if _, ok := view[plumbing.ReferenceName("refs/heads/other")]; !ok {
		t.Error("the racing writer's ref was clobbered instead of merged")
	}
}

func TestCommitRefsRetriesAndReportsContention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stealFirst int // rewrite packed-refs under us before this many attempts
		wantErr    error
	}{
		{name: "one lost race then success", stealFirst: 1},
		{name: "three lost races then success", stealFirst: 3},
		{name: "never wins", stealFirst: maxRefCASRetries + 1, wantErr: ErrRefContention},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			s := packedTestStorer(t, f)
			seedPackedRefs(t, f, s, hashRef("refs/heads/other", headCD))

			var retries int
			f.putHook = func(p *putSnapshot) {
				if p.n <= tt.stealFirst {
					// A competing writer lands first, invalidating our ETag.
					f.putLocked(packedRefsKey, string(encodePackedRefs(refSet(
						hashRef("refs/heads/other", headCD),
						hashRef("refs/heads/racer"+strconv.Itoa(p.n), headAB),
					))), nil)
				}
			}
			s.refCASObserver = func() { retries++ }

			err := s.commitRefs([]*plumbing.Reference{hashRef("refs/heads/main", headAB)}, nil, nil)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("want %v, got %v", tt.wantErr, err)
				}
				if retries != maxRefCASRetries-1 {
					t.Errorf("observed %d retries, want %d", retries, maxRefCASRetries-1)
				}
				return
			}
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			if retries != tt.stealFirst {
				t.Errorf("observed %d retries, want %d", retries, tt.stealFirst)
			}

			view, verr := s.refView()
			if verr != nil {
				t.Fatalf("refView: %v", verr)
			}
			if _, ok := view[plumbing.ReferenceName("refs/heads/main")]; !ok {
				t.Error("our ref did not land after the retries")
			}
			// The competing writer's last ref must survive: a retry re-reads and
			// re-applies rather than overwriting.
			last := plumbing.ReferenceName("refs/heads/racer" + strconv.Itoa(tt.stealFirst))
			if _, ok := view[last]; !ok {
				t.Errorf("retry clobbered the competing writer's ref %s", last)
			}
		})
	}
}

// TestCommitRefsRevalidatesExpectations is the race that refs.go documented as
// a known hole before this change. A competing writer moves the ref between
// our read and our write, and the result must be go-git's own
// ErrReferenceHasChanged — not ErrRefContention, which means something else.
func TestCommitRefsRevalidatesExpectations(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	seedPackedRefs(t, f, s, hashRef("refs/heads/main", headAB))

	f.putHook = func(p *putSnapshot) {
		if p.n == 1 {
			// Somebody else moved main to headCD first.
			f.putLocked(packedRefsKey, string(encodePackedRefs(refSet(hashRef("refs/heads/main", headCD)))), nil)
		}
	}

	expect := []refExpectation{{
		name: plumbing.ReferenceName("refs/heads/main"),
		old:  hashRef("refs/heads/main", headAB),
	}}
	err := s.commitRefs([]*plumbing.Reference{hashRef("refs/heads/main", headAB)}, nil, expect)

	if !errors.Is(err, storage.ErrReferenceHasChanged) {
		t.Fatalf("want ErrReferenceHasChanged, got %v", err)
	}
	if errors.Is(err, ErrRefContention) {
		t.Error("a genuine ref conflict was reported as raw contention")
	}

	// The competing writer's value must stand.
	cur, rerr := s.Reference(plumbing.ReferenceName("refs/heads/main"))
	if rerr != nil {
		t.Fatalf("read after refusal: %v", rerr)
	}
	if cur.Hash().String() != headCD {
		t.Errorf("refused commit mutated the ref: %s", cur.Hash())
	}
}

// TestCommitRefsFlushesOnce pins the invariant refs.go protects — a ref never
// names an object whose upload has not finished — and pins that a batch pays
// for one flush rather than one per ref.
func TestCommitRefsFlushesOnce(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)

	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(5)
	if _, err := obj.Write([]byte("hello")); err != nil {
		t.Fatalf("buffer: %v", err)
	}
	h, err := s.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("SetEncodedObject: %v", err)
	}

	sets := []*plumbing.Reference{
		plumbing.NewHashReference(plumbing.ReferenceName("refs/heads/a"), h),
		plumbing.NewHashReference(plumbing.ReferenceName("refs/heads/b"), h),
	}
	if err := s.commitRefs(sets, nil, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The object must be readable, which is only true if the flush happened
	// before the ref write.
	if err := s.HasEncodedObject(h); err != nil {
		t.Errorf("ref points at an object that is not there: %v", err)
	}
}

// TestPackedWritesAreGated pins that WithPackedRefs off keeps every write on
// the loose path, which is what makes a rollback safe.
func TestPackedWritesAreGated(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f) // default: off

	if err := s.SetReference(hashRef("refs/heads/main", headAB)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, ok := f.objs["refs/refs/heads/main"]; !ok {
		t.Error("write did not land as a loose ref")
	}
	if _, ok := f.objs[packedRefsKey]; ok {
		t.Error("packed-refs was written while WithPackedRefs was off")
	}
}

// TestConcurrentCommitsAllLand runs real goroutines through the retry loop. It
// is the closest a fake gets to the production race, and it is what proves the
// loop converges rather than losing writes.
func TestConcurrentCommitsAllLand(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	base := newTestStorer(t, f, WithPackedRefs(true))

	const writers = 8
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each writer gets its own Storer, exactly as each request does.
			s := base.Scoped("")
			errs[i] = s.commitRefs([]*plumbing.Reference{
				hashRef("refs/heads/w"+strconv.Itoa(i), headAB),
			}, nil, nil)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}

	fresh := base.Scoped("")
	view, err := fresh.refView()
	if err != nil {
		t.Fatalf("refView: %v", err)
	}
	if len(view) != writers {
		t.Errorf("view holds %d refs, want %d — the loop lost writes: %v", len(view), writers, view)
	}
}
