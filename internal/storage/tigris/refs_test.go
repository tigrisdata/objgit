package tigris

import (
	"errors"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"
)

const (
	headAB = "1111111111111111111111111111111111111111"
	headCD = "2222222222222222222222222222222222222222"
)

func mustHash(hexval string) plumbing.Hash {
	h, ok := plumbing.FromHex(hexval)
	if !ok {
		panic("refs_test bug: bad hex fixture " + hexval)
	}
	return h
}

func hashRef(name, hexval string) *plumbing.Reference {
	return plumbing.NewHashReference(plumbing.ReferenceName(name), mustHash(hexval))
}

func TestReferenceRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	main := hashRef("refs/heads/main", headAB)
	if err := s.SetReference(main); err != nil {
		t.Fatalf("set: %v", err)
	}
	sym := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.ReferenceName("refs/heads/main"))
	if err := s.SetReference(sym); err != nil {
		t.Fatalf("set sym: %v", err)
	}

	t.Run("loose values mirror dotgit encoding", func(t *testing.T) {
		o := f.get(t, "refs/refs/heads/main")
		if got := string(o.body); got != headAB+"\n" {
			t.Errorf("want %q, got %q", headAB+"\n", got)
		}
		osym := f.get(t, "refs/HEAD")
		if got := string(osym.body); got != "ref: refs/heads/main\n" {
			t.Errorf("want symbolic encoding, got %q", got)
		}
	})

	t.Run("reads reconstruct typed references", func(t *testing.T) {
		back, err := s.Reference(plumbing.ReferenceName("refs/heads/main"))
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if back.Hash().String() != main.Hash().String() {
			t.Errorf("hash mismatch: %s vs %s", back.Hash().String(), main.Hash().String())
		}

		head, err := s.Reference(plumbing.HEAD)
		if err != nil {
			t.Fatalf("get HEAD: %v", err)
		}
		if head.Type() != plumbing.SymbolicReference || head.Target().String() != "refs/heads/main" {
			t.Errorf("HEAD lost symbolic nature: %+v", head)
		}
	})

	t.Run("missing reference is the go-git sentinel", func(t *testing.T) {
		if _, err := s.Reference(plumbing.ReferenceName("refs/heads/nope")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
			t.Errorf("want ErrReferenceNotFound, got %v", err)
		}
	})

	t.Run("nil set is tolerated like memory-storage parity", func(t *testing.T) {
		if err := s.SetReference(nil); err != nil {
			t.Errorf("nil SetReference errored: %v", err)
		}
	})
}

func TestCheckAndSetReferenceCas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		currently string // pre-stored hash hex, "" for fresh
		oldVal    *plumbing.Reference
		wantErr   error // nil means swap must succeed
	}{
		{name: "create with nil old"},
		{name: "matching old swaps", currently: headAB, oldVal: hashRef("refs/heads/x", headAB)},
		{name: "stale old refuses", currently: headCD, oldVal: hashRef("refs/heads/x", headAB), wantErr: storage.ErrReferenceHasChanged},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			if tt.currently != "" {
				f.put("refs/refs/heads/x", tt.currently+"\n", nil)
			}
			s := newTestStorer(t, f)

			next := hashRef("refs/heads/x", headCD)
			err := s.CheckAndSetReference(next, tt.oldVal)

			cur, gerr := s.Reference(plumbing.ReferenceName("refs/heads/x"))
			if tt.wantErr == nil {
				if !errors.Is(err, tt.wantErr) && !(err == nil && tt.wantErr == nil) {
					t.Fatalf("want clean swap, got %v", err)
				}
				if gerr != nil {
					t.Fatalf("swap refused unexpectedly: %v", gerr)
				}
				if cur.Hash().String() != headCD {
					t.Errorf("swap landed the wrong value: %s", cur.Hash().String())
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("want ErrReferenceHasChanged, got %v", err)
			}
			if gerr != nil {
				t.Fatalf("failed CAS destroyed readability: %v", gerr)
			}
			if cur.Hash().String() != tt.currently {
				t.Errorf("failed CAS mutated the ref (now %s)", cur.Hash().String())
			}
		})
	}
}

func TestIterReferencesSortedAndComplete(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	// Deliberately out of lexical insertion order.
	for _, n := range []string{"refs/heads/zeta", "refs/tags/v1", "refs/heads/alpha"} {
		if err := s.SetReference(hashRef(n, headAB)); err != nil {
			t.Fatalf("set %s: %v", n, err)
		}
	}

	it, err := s.IterReferences()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	var names []string
	if err := it.ForEach(func(r *plumbing.Reference) error {
		names = append(names, r.Name().String())
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	want := []string{"refs/heads/alpha", "refs/heads/zeta", "refs/tags/v1"} // S3-sorted
	if len(names) != len(want) {
		t.Fatalf("walked %d refs, want %d: %v", len(names), len(want), names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("order mismatch:\nwant %v\ngot  %v", want, names)
		}
	}

	n, cerr := s.CountLooseRefs()
	if cerr != nil {
		t.Fatalf("count: %v", cerr)
	}
	if n != len(want) {
		t.Errorf("count disagrees with walk: %d vs %d", n, len(want))
	}
}

func TestRemoveReferenceDeletesAndToleratesAbsence(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	if err := s.SetReference(hashRef("refs/heads/gone", headAB)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.RemoveReference(plumbing.ReferenceName("refs/heads/gone")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := s.RemoveReference(plumbing.ReferenceName("refs/heads/never-there")); err != nil {
		t.Errorf("absent removal errored: %v", err)
	}
	if _, err := s.Reference(plumbing.ReferenceName("refs/heads/gone")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("ref survived removal: %v", err)
	}
}

func TestMalformedRefEntriesAreSkipped(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.put("refs/refs/heads/fine", headAB+"\n", nil)
	f.put("refs/refs/heads/junk", "definitely not a ref.", nil)
	s := newTestStorer(t, f)

	it, err := s.IterReferences()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	names := map[string]bool{}
	if err := it.ForEach(func(r *plumbing.Reference) error {
		names[r.Name().String()] = true
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	if names["refs/heads/junk"] {
		t.Error("malformed entry leaked into iteration")
	}
	if !names["refs/heads/fine"] {
		t.Error("healthy sibling vanished with the junk")
	}

	n, _ := s.CountLooseRefs()
	if n != 1 {
		t.Errorf("count must agree with the walk, got %d", n)
	}
}

func TestPackRefsIsDeliberateNoOp(t *testing.T) {
	t.Parallel()

	if err := newTestStorer(t, newFakeS3(t)).PackRefs(); err != nil {
		t.Errorf("PackRefs must succeed vacuously, got %v", err)
	}
}

func TestShallowMarks(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	t.Run("fresh bucket reads as unmarked", func(t *testing.T) {
		got, err := s.Shallow()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("want no marks, got %v", got)
		}
	})

	a, _ := plumbing.FromHex(headAB)
	b, _ := plumbing.FromHex(headCD)
	if err := s.SetShallow([]plumbing.Hash{a, b}); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Shallow()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(got) != 2 || got[0].String() != a.String() || got[1].String() != b.String() {
		t.Errorf("marks corrupted across round trip: %v", got)
	}

	putsBefore := f.nputs()
	deletesBefore := f.ndeletes()
	if err := s.SetShallow(nil); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if f.nputs() != putsBefore {
		t.Error("clearing shallow wrote instead of deleting")
	}
	if f.ndeletes() != deletesBefore+1 {
		t.Error("clearing shallow did not delete the mark object")
	}
	if left, err := s.Shallow(); err != nil || len(left) != 0 {
		t.Errorf("cleared marks still readable: %v, %v", left, err)
	}
}
