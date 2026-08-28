package tigris

import (
	"errors"
	"strconv"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

// seedPackedRefs writes a packed-refs object straight into the fake bucket,
// under the given storer's prefix.
func seedPackedRefs(t *testing.T, f *fakeS3, s *Storer, refs ...*plumbing.Reference) {
	t.Helper()
	f.put(s.prefix+packedRefsKey, string(encodePackedRefs(refSet(refs...))), nil)
}

func TestRefViewMergesPackedAndLoose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		packed []*plumbing.Reference
		loose  map[string]string // key suffix under refs/ -> stored body
		want   map[string]string // ref name -> expected hash hex
	}{
		{
			name: "packed only",
			packed: []*plumbing.Reference{
				hashRef("refs/heads/main", headAB),
				hashRef("refs/tags/v1", headCD),
			},
			want: map[string]string{"refs/heads/main": headAB, "refs/tags/v1": headCD},
		},
		{
			name:  "loose only, the state of a bucket before any packed write",
			loose: map[string]string{"refs/heads/main": headAB + "\n"},
			want:  map[string]string{"refs/heads/main": headAB},
		},
		{
			name: "a loose ref supplies a name packed does not hold",
			packed: []*plumbing.Reference{
				hashRef("refs/heads/main", headAB),
			},
			loose: map[string]string{"refs/heads/topic": headCD + "\n"},
			want:  map[string]string{"refs/heads/main": headAB, "refs/heads/topic": headCD},
		},
		{
			// The load-bearing case. A loose key can only exist if something
			// wrote it after the last packed write, so it is newer and must
			// win. Get this backwards and a fleet running two releases at once
			// swallows every push made by the older binaries.
			name: "a loose ref beats a packed ref with the same name",
			packed: []*plumbing.Reference{
				hashRef("refs/heads/main", headAB),
			},
			loose: map[string]string{"refs/heads/main": headCD + "\n"},
			want:  map[string]string{"refs/heads/main": headCD},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			s := newTestStorer(t, f)
			if len(tt.packed) > 0 {
				seedPackedRefs(t, f, s, tt.packed...)
			}
			for name, body := range tt.loose {
				f.put(refPrefix+name, body, nil)
			}

			view, err := s.refView()
			if err != nil {
				t.Fatalf("refView: %v", err)
			}
			if len(view) != len(tt.want) {
				t.Fatalf("view holds %d refs, want %d: %v", len(view), len(tt.want), view)
			}
			for name, hex := range tt.want {
				got, ok := view[plumbing.ReferenceName(name)]
				if !ok {
					t.Fatalf("ref %s missing from the view", name)
				}
				if got.Hash().String() != hex {
					t.Errorf("ref %s = %s, want %s", name, got.Hash(), hex)
				}
			}
		})
	}
}

// TestRefViewCostsTwoCalls is one of the two headline assertions of this whole
// change. Before it, one advertisement cost one ListObjectsV2 page per 1000
// refs plus one GetObject per ref.
func TestRefViewCostsTwoCalls(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	obs, snapshot := countingObserver()
	s := newTestStorer(t, f, obs)

	many := make([]*plumbing.Reference, 0, 500)
	for i := 0; i < 500; i++ {
		many = append(many, hashRef("refs/tags/v"+strconv.Itoa(i), headAB))
	}
	seedPackedRefs(t, f, s, many...)

	it, err := s.IterReferences()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	n := 0
	if err := it.ForEach(func(*plumbing.Reference) error { n++; return nil }); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if n != 500 {
		t.Fatalf("walked %d refs, want 500", n)
	}

	seen := snapshot()
	if seen["GetObject"] != 1 {
		t.Errorf("GetObject calls = %d, want 1 (map: %v)", seen["GetObject"], seen)
	}
	if seen["ListObjectsV2"] != 1 {
		t.Errorf("ListObjectsV2 calls = %d, want 1 (map: %v)", seen["ListObjectsV2"], seen)
	}
}

// TestRefCacheIsMemoizedPerStorer pins that the cache builds once, and that a
// Scoped storer gets its own.
func TestRefCacheIsMemoizedPerStorer(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	obs, snapshot := countingObserver()
	base := newTestStorer(t, f, obs)
	s := base.Scoped("acme/widgets")
	seedPackedRefs(t, f, s, hashRef("refs/heads/main", headAB))

	for i := 0; i < 5; i++ {
		if _, err := s.Reference(plumbing.ReferenceName("refs/heads/main")); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if got := snapshot()["GetObject"]; got != 1 {
		t.Errorf("five reads made %d GetObject calls, want 1", got)
	}

	// A sibling repository must not see this one's refs, and must build its own
	// cache.
	other := base.Scoped("acme/gadgets")
	if _, err := other.Reference(plumbing.ReferenceName("refs/heads/main")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("sibling saw another repository's ref: %v", err)
	}
}

// TestRefCacheIsNotStickyOnError copies ensurePacksBuilt's posture: a
// transient failure is retried on the next call, not remembered forever.
func TestRefCacheIsNotStickyOnError(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	seedPackedRefs(t, f, s, hashRef("refs/heads/main", headAB))

	f.listErr = errors.New("transient list failure")
	if _, err := s.refView(); err == nil {
		t.Fatal("a failing list must surface as an error")
	}

	f.listErr = nil
	view, err := s.refView()
	if err != nil {
		t.Fatalf("the cache stayed poisoned after the failure cleared: %v", err)
	}
	if _, ok := view[plumbing.ReferenceName("refs/heads/main")]; !ok {
		t.Error("retry produced an empty view")
	}
}

// TestLooseWriteInvalidatesTheCache pins read-your-own-write. The cache is
// built on read, so any write that bypasses it — every loose write does — has
// to drop it. Miss this and CheckAndSetReference compares against a stale view
// of a ref it just moved itself.
func TestLooseWriteInvalidatesTheCache(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f) // loose writes: WithPackedRefs defaults off
	name := plumbing.ReferenceName("refs/heads/main")

	if err := s.SetReference(hashRef(name.String(), headAB)); err != nil {
		t.Fatalf("first set: %v", err)
	}
	// Build the cache.
	if _, err := s.Reference(name); err != nil {
		t.Fatalf("read: %v", err)
	}

	if err := s.SetReference(hashRef(name.String(), headCD)); err != nil {
		t.Fatalf("second set: %v", err)
	}
	got, err := s.Reference(name)
	if err != nil {
		t.Fatalf("read after write: %v", err)
	}
	if got.Hash().String() != headCD {
		t.Errorf("read its own write as %s, want %s", got.Hash(), headCD)
	}

	if err := s.RemoveReference(name); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := s.Reference(name); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("removed ref still visible through the cache: %v", err)
	}
}

// TestRefViewSurfacesCorruption pins that a corrupt packed-refs object is an
// error and never an empty ref set. An empty set would make a repository look
// brand new, and git would then happily accept a force-push over the top.
func TestRefViewSurfacesCorruption(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	f.put(packedRefsKey, "this is not a packed-refs object", nil)

	if _, err := s.refView(); !errors.Is(err, errBadPackedRefs) {
		t.Fatalf("want errBadPackedRefs, got %v", err)
	}
}
