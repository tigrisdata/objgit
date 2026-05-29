package main

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"tangled.org/xeiaso.net/objgit/internal/auth"
)

func TestGitServiceFor(t *testing.T) {
	tests := []struct {
		name    string
		command string
		service string
		ok      bool
	}{
		{
			name:    "upload-pack",
			command: "git-upload-pack",
			service: transport.UploadPackService,
			ok:      true,
		},
		{
			name:    "upload-archive",
			command: "git-upload-archive",
			service: transport.UploadArchiveService,
			ok:      true,
		},
		{
			name:    "receive-pack",
			command: "git-receive-pack",
			service: transport.ReceivePackService,
			ok:      true,
		},
		{
			name:    "git-shell is unsupported",
			command: "git-shell",
			service: "",
			ok:      false,
		},
		{
			name:    "empty string is unsupported",
			command: "",
			service: "",
			ok:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := gitServiceFor(tt.command)
			if ok != tt.ok {
				t.Errorf("gitServiceFor(%q) ok=%v, want %v", tt.command, ok, tt.ok)
			}
			if got != tt.service {
				t.Errorf("gitServiceFor(%q) service=%q, want %q", tt.command, got, tt.service)
			}
		})
	}
}

func TestLoadOrCreateHostKey(t *testing.T) {
	fs := memfs.New()

	s1, err := loadOrCreateHostKey(fs)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	// The key must have been persisted.
	f, err := fs.Open(hostKeyPath)
	if err != nil {
		t.Fatalf("host key not persisted at %s: %v", hostKeyPath, err)
	}
	first, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}

	// A second call must reuse the same key, not regenerate.
	s2, err := loadOrCreateHostKey(fs)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !bytes.Equal(s1.PublicKey().Marshal(), s2.PublicKey().Marshal()) {
		t.Error("second call returned a different key; expected the persisted one to be reused")
	}

	f2, err := fs.Open(hostKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	second, err := io.ReadAll(f2)
	f2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Error("host key file changed on the second call; it must not be rewritten")
	}
}

// checkSSHBinaries skips t if git, ssh, or ssh-keygen are not on PATH.
func checkSSHBinaries(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"git", "ssh", "ssh-keygen"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not installed", bin)
		}
	}
}

// startSSHServer creates an in-memory daemon and starts the SSH server on an
// ephemeral port. It returns the listening address and the backing filesystem.
func startSSHServer(t *testing.T, allowPush, allowHooks bool) (string, billy.Filesystem) {
	t.Helper()
	fs := memfs.New()
	d := &daemon{
		fs:          fs,
		loader:      transport.NewFilesystemLoader(fs, false),
		authz:       auth.AllowAnonymous{AllowWrite: allowPush},
		allowHooks:  allowHooks,
		hookTimeout: 30 * time.Second,
	}
	srv, err := newSSHServer(d, "")
	if err != nil {
		t.Fatalf("newSSHServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln) //nolint:errcheck // returns when ln closes
	t.Cleanup(func() { srv.Close(); ln.Close() })
	return ln.Addr().String(), fs
}

// gitSSHEnv generates a throw-away ed25519 client key and returns an env slice
// with GIT_SSH_COMMAND pointing at ssh using that key with host-key checking
// disabled. The server accepts any public key.
func gitSSHEnv(t *testing.T) []string {
	t.Helper()
	keyDir := t.TempDir()
	key := filepath.Join(keyDir, "id_ed25519")
	if out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-f", key).CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v\n%s", err, out)
	}
	sshCmd := fmt.Sprintf("ssh -i %q -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null", key)
	return append(os.Environ(),
		"GIT_SSH_COMMAND="+sshCmd,
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
	)
}

// gitWithEnv runs git with a custom environment, returning combined output and
// the error (if any). It does NOT call t.Fatal so callers can inspect failures.
func gitWithEnv(dir string, env []string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestSSH drives a real git client over ssh:// against an in-process SSH server
// backed by memfs, covering push/clone round-trips, create-on-demand, push
// rejection, and clone of a missing repository.
func TestSSH(t *testing.T) {
	checkSSHBinaries(t)

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
			addr, fs := startSSHServer(t, tt.allowPush, false)
			env := gitSSHEnv(t)
			remote := "ssh://git@" + addr + "/test.git"

			// Confirm no repo exists before any push.
			_, preStatErr := fs.Stat("/test.git/config")
			if preStatErr == nil {
				t.Fatal("test.git must not exist before any push")
			}

			var srcHead string
			if tt.doPush {
				work := seedRepo(t)
				srcHead = strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))

				out, err := gitWithEnv(work, env, "push", remote, "main")
				if tt.wantPushErr {
					if err == nil {
						t.Fatalf("expected push to be rejected, got success:\n%s", out)
					}
				} else if err != nil {
					t.Fatalf("push failed: %v\n%s", err, out)
				}
			}

			// The bare repo must exist iff a push was expected to land.
			_, statErr := fs.Stat("/test.git/config")
			pushLanded := tt.doPush && !tt.wantPushErr
			if pushLanded && statErr != nil {
				t.Fatalf("expected repo to be created on push, but config missing: %v", statErr)
			}
			if !pushLanded && statErr == nil {
				t.Fatal("repository must not exist when push did not land")
			}

			dst := t.TempDir()
			out, err := gitWithEnv(dst, env, "clone", remote, "cloned")
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

// TestSSHHookFires pushes a repo carrying .objgit/hooks/receive-pack over SSH
// and asserts the hook runs asynchronously after the push response.
func TestSSHHookFires(t *testing.T) {
	checkSSHBinaries(t)

	var logBuf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	addr, _ := startSSHServer(t, true, true)
	env := gitSSHEnv(t)
	remote := "ssh://git@" + addr + "/hooked.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")

	hook := strings.Join([]string{
		"cat README.md",
		"echo hook_ran",
	}, "\n") + "\n"
	writeFile(t, filepath.Join(work, "README.md"), "hello from ssh repo\n")
	writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), hook)
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "with hook")

	if out, err := gitWithEnv(work, env, "push", remote, "main"); err != nil {
		t.Fatalf("push failed: %v\n%s", err, out)
	}

	// The hook runs asynchronously; wait for the terminal log line.
	waitForLog(t, &logBuf, "hook: finished", 30*time.Second)

	logs := logBuf.String()
	if !strings.Contains(logs, "hook: running") {
		t.Fatalf("hook did not run; logs:\n%s", logs)
	}
	for _, want := range []string{"hello from ssh repo", "hook_ran"} {
		if !strings.Contains(logs, want) {
			t.Errorf("hook output missing %q; logs:\n%s", want, logs)
		}
	}
}
