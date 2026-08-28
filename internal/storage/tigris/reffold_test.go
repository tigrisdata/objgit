package tigris

import (
	"errors"
	"strconv"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

// TestFoldMovesLooseRefsIntoPacked pins the one-shot migration: the first
// ref-mutating operation on a repository folds every loose ref into its own
// commit, and then deletes the loose keys.
func TestFoldMovesLooseRefsIntoPacked(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	for i := 0; i < 2500; i++ {
		f.put(refPrefix+"refs/tags/v"+strconv.Itoa(i), headAB+"\n", nil)
	}

	if err := s.SetReference(hashRef("refs/heads/main", headCD)); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Every loose key is gone from the bucket.
	for i := 0; i < 2500; i++ {
		if _, ok := f.objs[refPrefix+"refs/tags/v"+strconv.Itoa(i)]; ok {
			t.Fatalf("loose key for v%d survived the fold", i)
		}
	}

	// And every ref is still readable, out of packed-refs.
	fresh := packedTestStorer(t, f)
	view, err := fresh.refView()
	if err != nil {
		t.Fatalf("refView: %v", err)
	}
	if len(view) != 2501 {
		t.Fatalf("view holds %d refs, want 2501", len(view))
	}
	if got := view[plumbing.ReferenceName("refs/tags/v42")]; got == nil || got.Hash().String() != headAB {
		t.Errorf("folded ref v42 = %v", got)
	}

	// 2501 keys is three DeleteObjects calls at 1000 per call.
	if got := f.nbatchDeletes(); got != 3 {
		t.Errorf("batch deletes = %d, want 3", got)
	}
	if got := f.ndeletes(); got != 0 {
		t.Errorf("the fold used %d single-key deletes; it must batch", got)
	}
}

func TestFoldedRefCountDropsToZero(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	f.put(refPrefix+"refs/heads/main", headAB+"\n", nil)

	before, err := s.CountLooseRefs()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if before != 1 {
		t.Fatalf("loose count before the fold = %d, want 1", before)
	}

	if err := s.PackRefs(); err != nil {
		t.Fatalf("PackRefs: %v", err)
	}

	after, err := packedTestStorer(t, f).CountLooseRefs()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if after != 0 {
		t.Errorf("loose count after the fold = %d, want 0", after)
	}
}

// TestPackRefsIsGated pins that PackRefs stays a no-op while writes are off,
// so release 1 never creates a packed-refs object.
func TestPackRefsIsGated(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f) // default: off
	f.put(refPrefix+"refs/heads/main", headAB+"\n", nil)

	if err := s.PackRefs(); err != nil {
		t.Fatalf("PackRefs must succeed vacuously when gated, got %v", err)
	}
	if _, ok := f.objs[packedRefsKey]; ok {
		t.Error("PackRefs wrote packed-refs while WithPackedRefs was off")
	}
	if _, ok := f.objs[refPrefix+"refs/heads/main"]; !ok {
		t.Error("PackRefs deleted a loose ref while gated")
	}
}

// TestFoldStoppedBeforeCleanupKeepsRefsReadable is the crash case. The commit
// PUT lands and the delete fails. The loose keys still win under the merge
// rule, which is the correct result, and the next write retries the delete.
// The order is PUT and then DELETE precisely so that this is the failure mode
// rather than a lost ref.
func TestFoldStoppedBeforeCleanupKeepsRefsReadable(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	f.put(refPrefix+"refs/heads/main", headAB+"\n", nil)
	f.batchDelErr = errors.New("delete failed mid-fold")

	err := s.SetReference(hashRef("refs/heads/topic", headCD))
	if err == nil {
		t.Fatal("a failed loose-key delete must surface, not be swallowed")
	}

	// packed-refs holds both refs, and the loose key is still there.
	if _, ok := f.objs[packedRefsKey]; !ok {
		t.Fatal("the commit PUT did not land before the delete was attempted")
	}
	if _, ok := f.objs[refPrefix+"refs/heads/main"]; !ok {
		t.Fatal("the loose key vanished despite the delete failing")
	}

	// Both refs read correctly from a fresh storer, loose winning for main.
	fresh := packedTestStorer(t, f)
	view, verr := fresh.refView()
	if verr != nil {
		t.Fatalf("refView: %v", verr)
	}
	if got := view[plumbing.ReferenceName("refs/heads/main")]; got == nil || got.Hash().String() != headAB {
		t.Errorf("main = %v, want %s", got, headAB)
	}
	if got := view[plumbing.ReferenceName("refs/heads/topic")]; got == nil || got.Hash().String() != headCD {
		t.Errorf("topic = %v, want %s", got, headCD)
	}

	// The next write, with deletes working again, finishes the job.
	f.batchDelErr = nil
	if err := fresh.SetReference(hashRef("refs/heads/third", headAB)); err != nil {
		t.Fatalf("retry write: %v", err)
	}
	if _, ok := f.objs[refPrefix+"refs/heads/main"]; ok {
		t.Error("the retry did not finish deleting the loose key")
	}
}

// TestRemoveDeletesTheLooseKeyToo pins that a removal cannot resurrect from
// the loose layer.
func TestRemoveDeletesTheLooseKeyToo(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	f.put(refPrefix+"refs/heads/doomed", headAB+"\n", nil)

	if err := s.RemoveReference(plumbing.ReferenceName("refs/heads/doomed")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, ok := f.objs[refPrefix+"refs/heads/doomed"]; ok {
		t.Fatal("the loose key survived the removal and will resurrect the ref")
	}

	fresh := packedTestStorer(t, f)
	if _, err := fresh.Reference(plumbing.ReferenceName("refs/heads/doomed")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("removed ref came back: %v", err)
	}
}
