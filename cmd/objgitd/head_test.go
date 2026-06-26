package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
)

// dummyHash is a stand-in object id for branch refs in unit tests; ensureHEAD
// never dereferences it, it only needs the refs to exist.
var dummyHash = plumbing.NewHash("1111111111111111111111111111111111111111")

// TestEnsureHEAD exercises the dangling-HEAD heal in isolation: objgitd inits
// every repo with HEAD -> refs/heads/main, so a repo populated by pushing a
// project whose default branch differs (golang/go uses master) leaves HEAD
// pointing at a branch that does not exist, and clients cannot check out a
// worktree. ensureHEAD repoints HEAD at an existing branch.
func TestEnsureHEAD(t *testing.T) {
	tests := []struct {
		name       string
		branches   []string            // branch short names to create
		head       *plumbing.Reference // initial HEAD
		wantTarget plumbing.ReferenceName
		wantHash   plumbing.Hash // non-zero ⇒ expect HEAD to stay this detached hash
	}{
		{
			name:       "dangling main heals to master",
			branches:   []string{"master"},
			head:       plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main),
			wantTarget: plumbing.Master,
		},
		{
			name:       "valid head left unchanged",
			branches:   []string{"master"},
			head:       plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Master),
			wantTarget: plumbing.Master,
		},
		{
			name:     "detached head left unchanged",
			branches: []string{"master"},
			head:     plumbing.NewHashReference(plumbing.HEAD, dummyHash),
			wantHash: dummyHash,
		},
		{
			name:       "no branches leaves head as-is",
			branches:   nil,
			head:       plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main),
			wantTarget: plumbing.Main,
		},
		{
			name:       "prefers main when both present",
			branches:   []string{"master", "main"},
			head:       plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("trunk")),
			wantTarget: plumbing.Main,
		},
		{
			name:       "prefers master over other branches when main absent",
			branches:   []string{"zzz", "master"},
			head:       plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main),
			wantTarget: plumbing.Master,
		},
		{
			name:       "falls back to lexicographically smallest branch",
			branches:   []string{"zebra", "alpha", "mango"},
			head:       plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main),
			wantTarget: plumbing.NewBranchReferenceName("alpha"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := filesystem.NewStorage(memfs.New(), cache.NewObjectLRUDefault())
			for _, b := range tt.branches {
				ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName(b), dummyHash)
				if err := st.SetReference(ref); err != nil {
					t.Fatalf("seed branch %q: %v", b, err)
				}
			}
			if err := st.SetReference(tt.head); err != nil {
				t.Fatalf("seed HEAD: %v", err)
			}

			if err := ensureHEAD(st); err != nil {
				t.Fatalf("ensureHEAD: %v", err)
			}

			got, err := st.Reference(plumbing.HEAD)
			if err != nil {
				t.Fatalf("read HEAD back: %v", err)
			}
			if !tt.wantHash.IsZero() {
				if got.Type() != plumbing.HashReference || got.Hash() != tt.wantHash {
					t.Errorf("HEAD = %v %q, want detached hash %s", got.Type(), got.Hash(), tt.wantHash)
				}
				return
			}
			if got.Type() != plumbing.SymbolicReference || got.Target() != tt.wantTarget {
				t.Errorf("HEAD target = %q (type %v), want %q", got.Target(), got.Type(), tt.wantTarget)
			}
		})
	}
}

// TestEnsureHEADIdempotent verifies a second heal is a no-op once HEAD resolves.
func TestEnsureHEADIdempotent(t *testing.T) {
	st := filesystem.NewStorage(memfs.New(), cache.NewObjectLRUDefault())
	if err := st.SetReference(plumbing.NewHashReference(plumbing.Master, dummyHash)); err != nil {
		t.Fatalf("seed branch: %v", err)
	}
	if err := st.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main)); err != nil {
		t.Fatalf("seed HEAD: %v", err)
	}

	for i := range 2 {
		if err := ensureHEAD(st); err != nil {
			t.Fatalf("ensureHEAD pass %d: %v", i, err)
		}
		got, err := st.Reference(plumbing.HEAD)
		if err != nil {
			t.Fatalf("read HEAD pass %d: %v", i, err)
		}
		if got.Target() != plumbing.Master {
			t.Errorf("pass %d: HEAD target = %q, want %q", i, got.Target(), plumbing.Master)
		}
	}
}

// TestSmartHTTPHealsDanglingHEAD reproduces the reported bug end-to-end: a repo
// already sitting in the bucket whose HEAD points at a nonexistent branch (the
// init default refs/heads/main, while only master was pushed — as for a mirror
// of golang/go) must heal on the next clone so git checks out a worktree instead
// of printing "remote HEAD refers to nonexistent ref, unable to checkout".
func TestSmartHTTPHealsDanglingHEAD(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	fs := memfs.New()
	ts := httptest.NewServer((&daemon{
		sysFS:    fs,
		resolver: repofs.BucketResolver{Base: fs},
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}).httpHandler())
	t.Cleanup(ts.Close)

	// Push a single "master" branch (no "main"), like a project whose default
	// branch is master.
	work := t.TempDir()
	runGit(t, work, "init", "-b", "master")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	writeFile(t, filepath.Join(work, "README.md"), "hello\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "initial")
	if out, err := tryGit(work, "push", ts.URL+"/acme/go.git", "master"); err != nil {
		t.Fatalf("push failed: %v\n%s", err, out)
	}

	// Re-break HEAD to simulate a repo created before this fix (post-push heal
	// would otherwise have already fixed it): point HEAD back at the dangling
	// refs/heads/main directly in the backing store. The very next load (this
	// clone) must heal it on the way to serving the advertisement.
	breakHEAD(t, fs, "/acme/go")

	dst := t.TempDir()
	out, err := tryGit(dst, "clone", ts.URL+"/acme/go.git", "cloned")
	if err != nil {
		t.Fatalf("clone failed: %v\n%s", err, out)
	}
	if strings.Contains(out, "nonexistent ref") {
		t.Errorf("clone still warned about nonexistent HEAD ref; output:\n%s", out)
	}

	cloned := filepath.Join(dst, "cloned")
	if _, err := exec.Command("git", "-C", cloned, "rev-parse", "HEAD").Output(); err != nil {
		t.Fatalf("cloned repo has no checked-out HEAD: %v", err)
	}
	if got := strings.TrimSpace(runGit(t, cloned, "rev-parse", "--abbrev-ref", "HEAD")); got != "master" {
		t.Errorf("checked out branch = %q, want master", got)
	}
	if _, err := exec.Command("git", "-C", cloned, "cat-file", "-e", "HEAD:README.md").Output(); err != nil {
		t.Errorf("worktree missing README.md after clone: %v", err)
	}

	// After a load-healed clone, the advertisement now carries the symref.
	if body := getInfoRefs(t, ts.URL+"/acme/go.git"); !strings.Contains(body, "symref=HEAD:refs/heads/master") {
		t.Errorf("expected symref=HEAD:refs/heads/master after heal; advertisement:\n%q", body)
	}
}

// breakHEAD points the bare repo at repoPath's HEAD back at the dangling
// refs/heads/main, simulating a repository created before the heal existed.
func breakHEAD(t *testing.T, fs billy.Filesystem, repoPath string) {
	t.Helper()
	sub, err := fs.Chroot(repoPath)
	if err != nil {
		t.Fatalf("chroot %q: %v", repoPath, err)
	}
	st := filesystem.NewStorage(sub, cache.NewObjectLRUDefault())
	if err := st.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main)); err != nil {
		t.Fatalf("break HEAD: %v", err)
	}
	if _, err := st.Reference(plumbing.Main); err == nil {
		t.Fatalf("test setup invalid: refs/heads/main exists, HEAD would not dangle")
	}
}

// getInfoRefs fetches the smart-HTTP upload-pack advertisement for repoURL.
func getInfoRefs(t *testing.T, repoURL string) string {
	t.Helper()
	resp, err := http.Get(repoURL + "/info/refs?service=git-upload-pack")
	if err != nil {
		t.Fatalf("GET info/refs: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read info/refs: %v", err)
	}
	return string(body)
}
