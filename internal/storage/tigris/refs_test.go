package tigris

import (
	"errors"
	"slices"
	"strings"
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

	t.Run("writes land in packed-refs, not loose keys", func(t *testing.T) {
		if _, ok := f.objs[packedRefsKey]; !ok {
			t.Error("no packed-refs object was written")
		}
		if _, ok := f.objs["refs/refs/heads/main"]; ok {
			t.Error("a loose key was written with packed refs on")
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

// TestLooseReferenceEncodingWithPackedRefsOff pins the loose write path, which
// -packed-refs=false selects. It stays covered because that flag is the lever
// an operator pulls for one release before rolling back to a binary that
// predates the packed format.
func TestLooseReferenceEncodingWithPackedRefsOff(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f, WithPackedRefs(false))

	if err := s.SetReference(hashRef("refs/heads/main", headAB)); err != nil {
		t.Fatalf("set: %v", err)
	}
	sym := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.ReferenceName("refs/heads/main"))
	if err := s.SetReference(sym); err != nil {
		t.Fatalf("set sym: %v", err)
	}

	// Values mirror dotgit encoding exactly.
	if got := string(f.get(t, "refs/refs/heads/main").body); got != headAB+"\n" {
		t.Errorf("want %q, got %q", headAB+"\n", got)
	}
	if got := string(f.get(t, "refs/HEAD").body); got != "ref: refs/heads/main\n" {
		t.Errorf("want symbolic encoding, got %q", got)
	}
	if _, ok := f.objs[packedRefsKey]; ok {
		t.Error("packed-refs was written with the flag off")
	}

	// And a binary with the flag off still reads what it wrote.
	back, err := s.Reference(plumbing.ReferenceName("refs/heads/main"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if back.Hash().String() != headAB {
		t.Errorf("main = %s, want %s", back.Hash(), headAB)
	}
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

	want := []string{"refs/heads/alpha", "refs/heads/zeta", "refs/tags/v1"} // IterReferences sorts by name
	if len(names) != len(want) {
		t.Fatalf("walked %d refs, want %d: %v", len(names), len(want), names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("order mismatch:\nwant %v\ngot  %v", want, names)
		}
	}

	// CountLooseRefs counts only the legacy layer, so packed writes leave it at
	// zero. That is the assertion worth making here: it proves the writes above
	// left no loose keys behind. TestFoldedRefCountDropsToZero covers the
	// number's other half, a bucket that starts with loose keys.
	n, cerr := s.CountLooseRefs()
	if cerr != nil {
		t.Fatalf("count: %v", cerr)
	}
	if n != 0 {
		t.Errorf("packed writes left %d loose refs behind", n)
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

// TestPackRefsIsANoOpOnAnEmptyRepo keeps the vacuous case pinned. PackRefs is
// no longer a no-op in general — see TestFoldedRefCountDropsToZero and
// TestPackRefsIsGated — but it must still succeed with nothing to fold.
func TestPackRefsIsANoOpOnAnEmptyRepo(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	if err := newTestStorer(t, f, WithPackedRefs(true)).PackRefs(); err != nil {
		t.Errorf("PackRefs must succeed vacuously, got %v", err)
	}
	if _, ok := f.objs[packedRefsKey]; ok {
		t.Error("PackRefs wrote an object with nothing to fold")
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

// shallowBody renders marks the way SetShallow does, so a test can seed the
// key without a write going through the cache first.
func shallowBody(marks []plumbing.Hash) string {
	var b strings.Builder
	for _, h := range marks {
		b.WriteString(h.String())
		b.WriteString("\n")
	}
	return b.String()
}

// hexOf flattens marks for comparison. plumbing.Hash has unexported fields, so
// hex strings are the honest way to diff two slices of them.
func hexOf(marks []plumbing.Hash) []string {
	out := make([]string, 0, len(marks))
	for _, h := range marks {
		out = append(out, h.String())
	}
	return out
}

// TestShallowLookupIsMemoized pins the per-request cache behind Shallow.
//
// go-git's revlist.ObjectsWithRef walks one want at a time, and each walk asks
// the storer for the shallow set, so a clone used to pay one GetObject per
// advertised ref. Absent is the common answer and it is the one that must be
// remembered.
func TestShallowLookupIsMemoized(t *testing.T) {
	t.Parallel()

	a, b := mustHash(headAB), mustHash(headCD)

	tests := []struct {
		name  string
		seed  []plumbing.Hash // marks already in the bucket
		want  []plumbing.Hash
		calls int
	}{
		{name: "absent marker", seed: nil, want: nil, calls: 5},
		{name: "present marker", seed: []plumbing.Hash{a, b}, want: []plumbing.Hash{a, b}, calls: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			if len(tt.seed) > 0 {
				f.put(shallowKey, shallowBody(tt.seed), nil)
			}
			obs, snapshot := countingObserver()
			s := newTestStorer(t, f, obs)

			for i := range tt.calls {
				got, err := s.Shallow()
				if err != nil {
					t.Fatalf("call %d: %v", i, err)
				}
				if !slices.Equal(hexOf(got), hexOf(tt.want)) {
					t.Fatalf("call %d: want marks %v, got %v", i, hexOf(tt.want), hexOf(got))
				}
			}

			if got := snapshot()["GetObject"]; got != 1 {
				t.Errorf("%d Shallow calls cost %d GetObject calls, want 1", tt.calls, got)
			}
		})
	}
}

// TestShallowCacheFollowsWrites pins that a write inside one request is
// visible to the next read in that same request. A cache that only fills and
// never updates would hand back the pre-write answer.
func TestShallowCacheFollowsWrites(t *testing.T) {
	t.Parallel()

	a, b := mustHash(headAB), mustHash(headCD)

	tests := []struct {
		name  string
		seed  []plumbing.Hash
		write []plumbing.Hash
		want  []plumbing.Hash
	}{
		{name: "set over absent", seed: nil, write: []plumbing.Hash{a}, want: []plumbing.Hash{a}},
		{name: "set over present", seed: []plumbing.Hash{a}, write: []plumbing.Hash{a, b}, want: []plumbing.Hash{a, b}},
		{name: "clear present", seed: []plumbing.Hash{a, b}, write: nil, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			if len(tt.seed) > 0 {
				f.put(shallowKey, shallowBody(tt.seed), nil)
			}
			s := newTestStorer(t, f)

			// Warm the cache first. Without this the write has nothing stale to
			// leave behind and the test proves nothing.
			if _, err := s.Shallow(); err != nil {
				t.Fatalf("warm: %v", err)
			}
			if err := s.SetShallow(tt.write); err != nil {
				t.Fatalf("write: %v", err)
			}

			got, err := s.Shallow()
			if err != nil {
				t.Fatalf("read back: %v", err)
			}
			if !slices.Equal(hexOf(got), hexOf(tt.want)) {
				t.Errorf("want marks %v, got %v", hexOf(tt.want), hexOf(got))
			}
		})
	}
}

// TestShallowCacheIsPerScopedPrefix is the isolation guard. Scoped copies the
// Storer value, so a cache held by pointer is shared unless Scoped replaces
// it — and a shared one would give one repository another repository's marks.
func TestShallowCacheIsPerScopedPrefix(t *testing.T) {
	t.Parallel()

	a, b := mustHash(headAB), mustHash(headCD)

	f := newFakeS3(t)
	f.put("acme/widgets/"+shallowKey, shallowBody([]plumbing.Hash{a}), nil)
	f.put("acme/gadgets/"+shallowKey, shallowBody([]plumbing.Hash{b}), nil)

	base := newTestStorer(t, f)

	// Warm the root's cache with the unmarked answer before scoping. A child
	// that inherited the parent's cache would report itself unmarked too.
	if got, err := base.Shallow(); err != nil || len(got) != 0 {
		t.Fatalf("root should read as unmarked, got %v, %v", got, err)
	}

	tests := []struct {
		name   string
		storer *Storer
		want   []plumbing.Hash
	}{
		{name: "widgets", storer: base.Scoped("acme/widgets"), want: []plumbing.Hash{a}},
		{name: "gadgets", storer: base.Scoped("acme/gadgets"), want: []plumbing.Hash{b}},
		{name: "nested unmarked", storer: base.Scoped("acme").Scoped("sprockets"), want: nil},
		{name: "nested marked", storer: base.Scoped("acme").Scoped("widgets"), want: []plumbing.Hash{a}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Read twice: the first fills that Storer's own cache, the second
			// proves the cached answer is still this prefix's answer.
			for i := range 2 {
				got, err := tt.storer.Shallow()
				if err != nil {
					t.Fatalf("call %d: %v", i, err)
				}
				if !slices.Equal(hexOf(got), hexOf(tt.want)) {
					t.Fatalf("call %d: want marks %v, got %v", i, hexOf(tt.want), hexOf(got))
				}
			}
		})
	}

	// The root's own answer must survive its children reading theirs.
	if got, err := base.Shallow(); err != nil || len(got) != 0 {
		t.Errorf("root marks changed under it: %v, %v", got, err)
	}
}

// TestShallowLookupDoesNotCacheErrors pins that only a definitive answer is
// cacheable. A transient GetObject failure must not poison the rest of the
// request.
func TestShallowLookupDoesNotCacheErrors(t *testing.T) {
	t.Parallel()

	a := mustHash(headAB)
	boom := errors.New("injected network failure")

	f := newFakeS3(t)
	f.put(shallowKey, shallowBody([]plumbing.Hash{a}), nil)
	f.getErr = boom

	obs, snapshot := countingObserver()
	s := newTestStorer(t, f, obs)

	if _, err := s.Shallow(); !errors.Is(err, boom) {
		t.Fatalf("want the injected failure, got %v", err)
	}

	f.getErr = nil
	got, err := s.Shallow()
	if err != nil {
		t.Fatalf("retry after a failure: %v", err)
	}
	if !slices.Equal(hexOf(got), hexOf([]plumbing.Hash{a})) {
		t.Errorf("want marks %v, got %v", hexOf([]plumbing.Hash{a}), hexOf(got))
	}
	if n := snapshot()["GetObject"]; n != 2 {
		t.Errorf("want 2 GetObject calls (one failed, one retried), got %d", n)
	}
}

// TestShallowCachedSliceIsNotAliased keeps a caller from editing the cache
// through the slice it was handed.
func TestShallowCachedSliceIsNotAliased(t *testing.T) {
	t.Parallel()

	a, b := mustHash(headAB), mustHash(headCD)

	f := newFakeS3(t)
	f.put(shallowKey, shallowBody([]plumbing.Hash{a, b}), nil)
	s := newTestStorer(t, f)

	first, err := s.Shallow()
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	first[0] = plumbing.ZeroHash

	second, err := s.Shallow()
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if !slices.Equal(hexOf(second), hexOf([]plumbing.Hash{a, b})) {
		t.Errorf("caller edited the cache: want %v, got %v", hexOf([]plumbing.Hash{a, b}), hexOf(second))
	}
}
