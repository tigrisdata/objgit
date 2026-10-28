package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/storage"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
)

// TestSmartHTTP drives a real git client against the smart-HTTP handler over an
// in-memory filesystem, covering push (create-on-demand), the write-gate
// enforced by the authorizer, and clone round-trips.
func TestSmartHTTP(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	for _, tt := range []struct {
		name         string
		allowPush    bool
		doPush       bool
		wantPushErr  bool
		wantCloneErr bool
	}{
		{
			name:      "push creates repo and clone round-trips",
			allowPush: true,
			doPush:    true,
		},
		{
			name:         "push rejected when disabled",
			allowPush:    false,
			doPush:       true,
			wantPushErr:  true,
			wantCloneErr: true,
		},
		{
			name:         "clone of missing repo fails",
			allowPush:    true,
			doPush:       false,
			wantCloneErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ts, mb := newHTTPServer(t, tt.allowPush)
			remote := ts.URL + "/acme/test.git"

			var srcHead string
			if tt.doPush {
				work := seedRepo(t)
				srcHead = strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))

				out, err := tryGit(work, "push", remote, "main")
				if tt.wantPushErr {
					if err == nil {
						t.Fatalf("expected push to be rejected, got success:\n%s", out)
					}
				} else if err != nil {
					t.Fatalf("push failed: %v\n%s", err, out)
				}
			}

			// The bare repo must exist iff a push was expected to land.
			pushLanded := tt.doPush && !tt.wantPushErr
			if exists := mb.exists("acme/test"); pushLanded && !exists {
				t.Fatal("expected repo to be created on push, but it does not exist")
			} else if !pushLanded && exists {
				t.Fatal("repository must not exist when push did not land")
			}

			dst := t.TempDir()
			out, err := tryGit(dst, "clone", remote, "cloned")
			if tt.wantCloneErr {
				if err == nil {
					t.Fatalf("expected clone to fail, got success:\n%s", out)
				}
				return
			}
			if err != nil {
				t.Fatalf("clone failed: %v\n%s", err, out)
			}

			gotHead := strings.TrimSpace(runGit(t, filepath.Join(dst, "cloned"), "rev-parse", "HEAD"))
			if gotHead != srcHead {
				t.Logf("want: %s", srcHead)
				t.Logf("got:  %s", gotHead)
				t.Error("cloned HEAD does not match pushed HEAD")
			}
		})
	}
}

// newHTTPServer starts an httptest server backed by a fresh in-memory resolver
// and returns it alongside that resolver's backing store for state assertions.
func newHTTPServer(t *testing.T, allowPush bool) (*httptest.Server, *memBase) {
	t.Helper()
	mb := newMemBase()
	d := &daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: mb},
		authz:    auth.AllowAnonymous{AllowWrite: allowPush},
	}
	ts := httptest.NewServer(d.httpHandler())
	t.Cleanup(ts.Close)
	return ts, mb
}

// TestSmartHTTPAnonymousReadWhilePushDisabled verifies that with push disabled,
// anonymous clone of an existing repo still succeeds — reads are always allowed
// by the default authorizer, only writes are gated.
func TestSmartHTTPAnonymousReadWhilePushDisabled(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	// Seed a repo via a push-enabled server over a shared backing store.
	mb := newMemBase()
	seed := httptest.NewServer((&daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: mb},
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}).httpHandler())
	t.Cleanup(seed.Close)

	work := seedRepo(t)
	srcHead := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
	if out, err := tryGit(work, "push", seed.URL+"/acme/test.git", "main"); err != nil {
		t.Fatalf("seed push failed: %v\n%s", err, out)
	}

	// Serve the same backing store with push disabled and clone from it.
	ro := httptest.NewServer((&daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: mb},
		authz:    auth.AllowAnonymous{AllowWrite: false},
	}).httpHandler())
	t.Cleanup(ro.Close)

	dst := t.TempDir()
	if out, err := tryGit(dst, "clone", ro.URL+"/acme/test.git", "cloned"); err != nil {
		t.Fatalf("anonymous clone should succeed with push disabled: %v\n%s", err, out)
	}
	gotHead := strings.TrimSpace(runGit(t, filepath.Join(dst, "cloned"), "rev-parse", "HEAD"))
	if gotHead != srcHead {
		t.Errorf("cloned HEAD %q != seeded HEAD %q", gotHead, srcHead)
	}
}

// TestSmartHTTPHookStreams pushes a repo carrying .objgit/hooks/receive-pack
// over smart-HTTP and asserts the hook runs synchronously and streams its
// output to the client over the sideband (rendered as "remote:" lines). This
// exercises the flush-on-write wrapper that delivers band-2 progress live
// instead of letting net/http buffer it.
func TestSmartHTTPHookStreams(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	var logBuf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	ts := httptest.NewServer((&daemon{
		sysFS:       memfs.New(),
		resolver:    repofs.BucketResolver{Base: newMemBase()},
		authz:       auth.AllowAnonymous{AllowWrite: true},
		allowHooks:  true,
		hookTimeout: 30 * time.Second,
	}).httpHandler())
	t.Cleanup(ts.Close)

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")

	hook := strings.Join([]string{
		"cat README.md",
		"echo hook_ran",
	}, "\n") + "\n"
	writeFile(t, filepath.Join(work, "README.md"), "hello from http repo\n")
	writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), hook)
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "with hook")

	out, err := tryGit(work, "push", ts.URL+"/acme/hooked.git", "main")
	if err != nil {
		t.Fatalf("push failed: %v\n%s", err, out)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "hook: running") {
		t.Fatalf("hook did not run; logs:\n%s", logs)
	}
	for _, want := range []string{"hello from http repo", "hook_ran"} {
		if !strings.Contains(out, want) {
			t.Errorf("push output missing streamed hook output %q; output:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "remote:") {
		t.Errorf("hook output not streamed as remote progress; output:\n%s", out)
	}
}

// seedRepo creates a local git repository with one commit and returns its path.
func seedRepo(t *testing.T) string {
	t.Helper()
	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "commit", "--allow-empty", "-m", "initial")
	return work
}

// recordingResolver wraps a Resolver and records the last credential it saw, so
// a test can assert the HTTP Basic-auth credential reached filesystem resolution.
type recordingResolver struct {
	inner    repofs.Resolver
	mu       sync.Mutex
	lastCred repofs.Credential
}

func (r *recordingResolver) Resolve(ctx context.Context, ref repofs.RepoRef, cred repofs.Credential, create bool) (storage.Storer, error) {
	r.mu.Lock()
	r.lastCred = cred
	r.mu.Unlock()
	return r.inner.Resolve(ctx, ref, cred, create)
}

func (r *recordingResolver) credential() repofs.Credential {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastCred
}

// TestHTTPRejectsNonOrgRepoPath verifies the {orgID}/{repoName} shape is enforced
// by the router: a single-segment path matches no pattern and is a 404.
func TestHTTPRejectsNonOrgRepoPath(t *testing.T) {
	d := &daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: newMemBase()},
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}
	ts := httptest.NewServer(d.httpHandler())
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/single.git/info/refs?service=git-upload-pack")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("single-segment path: status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

// TestHTTPCredentialReachesResolver verifies the HTTP Basic-auth username and
// password are threaded into the filesystem resolver.
func TestHTTPCredentialReachesResolver(t *testing.T) {
	rec := &recordingResolver{inner: repofs.BucketResolver{Base: newMemBase()}}
	d := &daemon{
		sysFS:    memfs.New(),
		resolver: rec,
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}
	ts := httptest.NewServer(d.httpHandler())
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/acme/widgets.git/info/refs?service=git-upload-pack", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("alice", "s3cret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if got, want := rec.credential(), (repofs.Credential{Username: "alice", Password: "s3cret"}); got != want {
		t.Errorf("resolver credential = %+v, want %+v", got, want)
	}
}
