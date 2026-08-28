package main

import (
	"errors"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"sync"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
)

// batchingStorer is a memory.Storage that also records batch ref updates, so a
// test can see which path updateReferences took and with what.
type batchingStorer struct {
	storage.Storer
	// mu guards the recorded fields. The end-to-end test in this file reads
	// them from the test goroutine while a server goroutine may still be
	// unwinding, which -race notices without it.
	mu      sync.Mutex
	calls   int
	sets    []*plumbing.Reference
	removes []plumbing.ReferenceName
	err     error
}

// record reports what the storer has seen: batch calls, total sets, total
// removes.
func (b *batchingStorer) record() (calls, sets, removes int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls, len(b.sets), len(b.removes)
}

func (b *batchingStorer) UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error {
	b.mu.Lock()
	b.calls++
	b.sets = append(b.sets, sets...)
	b.removes = append(b.removes, removes...)
	failWith := b.err
	b.mu.Unlock()

	if failWith != nil {
		return failWith
	}
	for _, r := range sets {
		if err := b.Storer.SetReference(r); err != nil {
			return err
		}
	}
	for _, n := range removes {
		if err := b.Storer.RemoveReference(n); err != nil {
			return err
		}
	}
	return nil
}

// batchBase is memBase with a batch ref-update surface on every repository, so
// an end-to-end push takes the refUpdater path. memBase itself hands out a bare
// memory.Storage, which has no such surface and would take the fallback.
type batchBase struct {
	mu    sync.Mutex
	repos map[string]*batchingStorer
}

func newBatchBase() *batchBase { return &batchBase{repos: map[string]*batchingStorer{}} }

func (b *batchBase) Scoped(prefix string) storage.Storer {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.repos[prefix]
	if !ok {
		st = &batchingStorer{Storer: memory.NewStorage()}
		b.repos[prefix] = st
	}
	return st
}

// only returns the single storer this base handed out. Asserting on "the one
// repository" keeps the test independent of how RepoRef.Path formats a prefix.
func (b *batchBase) only(t *testing.T) *batchingStorer {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.repos) != 1 {
		t.Fatalf("want exactly one repository, got %d", len(b.repos))
	}
	for _, st := range b.repos {
		return st
	}
	return nil
}

func hashOf(t *testing.T, hex string) plumbing.Hash {
	t.Helper()
	h, ok := plumbing.FromHex(hex)
	if !ok {
		t.Fatalf("bad hex fixture %q", hex)
	}
	return h
}

func TestUpdateReferencesBatchesWhenTheStorerCan(t *testing.T) {
	t.Parallel()

	const (
		hexA = "1111111111111111111111111111111111111111"
		hexB = "2222222222222222222222222222222222222222"
	)

	tests := []struct {
		name        string
		seed        map[string]string // existing refs
		commands    []*packp.Command
		batchErr    error
		wantCalls   int
		wantSets    int
		wantRemoves int
		wantStatus  map[string]error // ref name -> expected status
	}{
		{
			name: "many creates go in one call",
			commands: []*packp.Command{
				{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
				{Name: "refs/tags/v2", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
				{Name: "refs/tags/v3", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
			},
			wantCalls: 1,
			wantSets:  3,
			wantStatus: map[string]error{
				"refs/tags/v1": nil, "refs/tags/v2": nil, "refs/tags/v3": nil,
			},
		},
		{
			name: "creates, updates and deletes share the call",
			seed: map[string]string{"refs/heads/main": hexA, "refs/heads/old": hexA},
			commands: []*packp.Command{
				{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexB)},
				{Name: "refs/heads/main", Old: hashOf(t, hexA), New: hashOf(t, hexB)},
				{Name: "refs/heads/old", Old: hashOf(t, hexA), New: plumbing.ZeroHash},
			},
			wantCalls:   1,
			wantSets:    2,
			wantRemoves: 1,
			wantStatus: map[string]error{
				"refs/tags/v1": nil, "refs/heads/main": nil, "refs/heads/old": nil,
			},
		},
		{
			name: "a rejected command never reaches the batch",
			seed: map[string]string{"refs/heads/main": hexA},
			commands: []*packp.Command{
				// Create over an existing ref: rejected before the batch.
				{Name: "refs/heads/main", Old: plumbing.ZeroHash, New: hashOf(t, hexB)},
				{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
			},
			wantCalls: 1,
			wantSets:  1,
			wantStatus: map[string]error{
				"refs/heads/main": transport.ErrUpdateReference,
				"refs/tags/v1":    nil,
			},
		},
		{
			// The behavior change this task introduces. One PutObject means the
			// batch is all-or-nothing, so a commit failure fails every command
			// in the push rather than the first N succeeding.
			name: "a failed commit fails every command in the batch",
			commands: []*packp.Command{
				{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
				{Name: "refs/tags/v2", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
			},
			batchErr:  errors.New("commit refused"),
			wantCalls: 1,
			wantSets:  2,
			wantStatus: map[string]error{
				"refs/tags/v1": errors.New("commit refused"),
				"refs/tags/v2": errors.New("commit refused"),
			},
		},
		{
			name:       "nothing staged makes no call at all",
			seed:       map[string]string{"refs/heads/main": hexA},
			commands:   []*packp.Command{{Name: "refs/heads/main", Old: plumbing.ZeroHash, New: hashOf(t, hexB)}},
			wantCalls:  0,
			wantStatus: map[string]error{"refs/heads/main": transport.ErrUpdateReference},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mem := memory.NewStorage()
			for name, hex := range tt.seed {
				ref := plumbing.NewHashReference(plumbing.ReferenceName(name), hashOf(t, hex))
				if err := mem.SetReference(ref); err != nil {
					t.Fatalf("seed %s: %v", name, err)
				}
			}
			st := &batchingStorer{Storer: mem, err: tt.batchErr}

			cmdStatus := map[plumbing.ReferenceName]error{}
			var firstErr error
			updateReferences(st, &packp.UpdateRequests{Commands: tt.commands}, cmdStatus, &firstErr)

			calls, sets, removes := st.record()
			if calls != tt.wantCalls {
				t.Errorf("UpdateReferences calls = %d, want %d", calls, tt.wantCalls)
			}
			if sets != tt.wantSets {
				t.Errorf("batched sets = %d, want %d", sets, tt.wantSets)
			}
			if removes != tt.wantRemoves {
				t.Errorf("batched removes = %d, want %d", removes, tt.wantRemoves)
			}
			for name, want := range tt.wantStatus {
				got, ok := cmdStatus[plumbing.ReferenceName(name)]
				if !ok {
					t.Errorf("no status recorded for %s", name)
					continue
				}
				switch {
				case want == nil && got != nil:
					t.Errorf("%s: want success, got %v", name, got)
				case want != nil && got == nil:
					t.Errorf("%s: want %v, got success", name, want)
				case want != nil && got != nil && !errors.Is(got, want) && got.Error() != want.Error():
					t.Errorf("%s: want %v, got %v", name, want, got)
				}
			}
		})
	}
}

// TestUpdateReferencesFallsBackWithoutTheInterface pins that a storer with no
// batch surface still works, one ref at a time.
func TestUpdateReferencesFallsBackWithoutTheInterface(t *testing.T) {
	t.Parallel()

	const hexA = "1111111111111111111111111111111111111111"
	mem := memory.NewStorage()

	cmdStatus := map[plumbing.ReferenceName]error{}
	var firstErr error
	updateReferences(mem, &packp.UpdateRequests{Commands: []*packp.Command{
		{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
	}}, cmdStatus, &firstErr)

	if firstErr != nil {
		t.Fatalf("fallback path errored: %v", firstErr)
	}
	if _, err := mem.Reference(plumbing.ReferenceName("refs/tags/v1")); err != nil {
		t.Errorf("fallback did not write the ref: %v", err)
	}
}

// TestPushOfManyTagsBatchesEndToEnd drives a real git client through Smart
// HTTP and asserts the whole push arrived as one batch.
//
// It deliberately does not assert S3 call counts: these tests run on
// memory.Storage, and the fake bucket is unexported inside
// internal/storage/tigris. Call counts live there, in
// TestUpdateReferencesCostsOnePut and TestRefViewCostsTwoCalls.
func TestPushOfManyTagsBatchesEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	bb := newBatchBase()
	ts := httptest.NewServer((&daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: bb},
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}).httpHandler())
	t.Cleanup(ts.Close)

	const tags = 200
	work := seedRepo(t)
	for i := 0; i < tags; i++ {
		runGit(t, work, "tag", "v"+strconv.Itoa(i))
	}
	runGit(t, work, "remote", "add", "origin", ts.URL+"/acme/widgets.git")
	runGit(t, work, "push", "--tags", "origin", "main")

	calls, sets, removes := bb.only(t).record()
	if calls != 1 {
		t.Errorf("UpdateReferences calls = %d, want 1: git split the push, or the seam was bypassed", calls)
	}
	// 200 tags plus refs/heads/main.
	if sets != tags+1 {
		t.Errorf("batched sets = %d, want %d", sets, tags+1)
	}
	if removes != 0 {
		t.Errorf("batched removes = %d, want 0", removes)
	}

	// And the refs really landed, not just got counted.
	st := bb.only(t)
	for _, name := range []string{"refs/heads/main", "refs/tags/v0", "refs/tags/v199"} {
		if _, err := st.Reference(plumbing.ReferenceName(name)); err != nil {
			t.Errorf("ref %s did not land: %v", name, err)
		}
	}
}
