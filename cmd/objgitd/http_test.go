package main

import (
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"tangled.org/xeiaso.net/objgit/internal/auth"
)

// TestSmartHTTP drives a real git client against the smart-HTTP handler over an
// in-memory filesystem, covering push (create-on-demand), the allowPush gate,
// and clone round-trips.
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
			ts, fs := newHTTPServer(t, tt.allowPush)
			remote := ts.URL + "/test.git"

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

			// The bare repo must exist on disk iff a push was expected to land.
			_, statErr := fs.Stat("/test.git/config")
			pushLanded := tt.doPush && !tt.wantPushErr
			if pushLanded && statErr != nil {
				t.Fatalf("expected repo to be created on push, but config missing: %v", statErr)
			}
			if !pushLanded && statErr == nil {
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

// newHTTPServer starts an httptest server backed by a fresh in-memory filesystem
// and returns it alongside that filesystem for state assertions.
func newHTTPServer(t *testing.T, allowPush bool) (*httptest.Server, billy.Filesystem) {
	t.Helper()
	fs := memfs.New()
	d := &daemon{
		fs:     fs,
		loader: transport.NewFilesystemLoader(fs, false),
		authz:  auth.AllowAnonymous{AllowWrite: allowPush},
	}
	ts := httptest.NewServer(d)
	t.Cleanup(ts.Close)
	return ts, fs
}

// TestSmartHTTPAnonymousReadWhilePushDisabled verifies that with push disabled,
// anonymous clone of an existing repo still succeeds — reads are always allowed
// by the default authorizer, only writes are gated.
func TestSmartHTTPAnonymousReadWhilePushDisabled(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	// Seed a repo via a push-enabled server over a shared filesystem.
	fs := memfs.New()
	seed := httptest.NewServer(&daemon{fs: fs, loader: transport.NewFilesystemLoader(fs, false), authz: auth.AllowAnonymous{AllowWrite: true}})
	defer seed.Close()

	work := seedRepo(t)
	srcHead := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
	if out, err := tryGit(work, "push", seed.URL+"/test.git", "main"); err != nil {
		t.Fatalf("seed push failed: %v\n%s", err, out)
	}

	// Serve the same filesystem with push disabled and clone from it.
	ro := httptest.NewServer(&daemon{fs: fs, loader: transport.NewFilesystemLoader(fs, false), authz: auth.AllowAnonymous{AllowWrite: false}})
	defer ro.Close()

	dst := t.TempDir()
	if out, err := tryGit(dst, "clone", ro.URL+"/test.git", "cloned"); err != nil {
		t.Fatalf("anonymous clone should succeed with push disabled: %v\n%s", err, out)
	}
	gotHead := strings.TrimSpace(runGit(t, filepath.Join(dst, "cloned"), "rev-parse", "HEAD"))
	if gotHead != srcHead {
		t.Errorf("cloned HEAD %q != seeded HEAD %q", gotHead, srcHead)
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
