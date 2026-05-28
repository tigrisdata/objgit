package main

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing/transport"
)

// TestDaemonPushCreatesRepo reproduces "git push git://host/new.git" against a
// path that does not exist yet. The daemon must create the bare repository on
// demand and the result must clone back cleanly.
func TestDaemonPushCreatesRepo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	fs := memfs.New()
	d := &daemon{
		fs:        fs,
		loader:    transport.NewFilesystemLoader(fs, false),
		allowPush: true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srvErr := make(chan error, 1)
	go func() { srvErr <- d.Serve(ctx, ln) }()

	remote := "git://" + ln.Addr().String() + "/test.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "commit", "--allow-empty", "-m", "initial")

	// The repository does not exist yet; the push must create it.
	runGit(t, work, "push", remote, "main")

	if _, err := fs.Stat("/test.git/config"); err != nil {
		t.Fatalf("expected bare repo to be created on push, but %q is missing: %v", "/test.git/config", err)
	}

	// Round-trip: a clone must recover the pushed commit.
	dst := t.TempDir()
	runGit(t, dst, "clone", remote, "cloned")
	head := strings.TrimSpace(runGit(t, filepath.Join(dst, "cloned"), "rev-parse", "HEAD"))
	if head == "" {
		t.Fatal("cloned repository has no HEAD")
	}

	cancel()
	_ = ln.Close()
	select {
	case <-srvErr:
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}

// TestDaemonPushDisabled confirms that pushes are rejected (not silently
// creating repos) when allowPush is false.
func TestDaemonPushDisabled(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	fs := memfs.New()
	d := &daemon{
		fs:        fs,
		loader:    transport.NewFilesystemLoader(fs, false),
		allowPush: false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = d.Serve(ctx, ln) }()

	remote := "git://" + ln.Addr().String() + "/test.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "commit", "--allow-empty", "-m", "initial")

	if out, err := tryGit(work, "push", remote, "main"); err == nil {
		t.Fatalf("expected push to be rejected when allowPush is false, got success:\n%s", out)
	}

	if _, err := fs.Stat("/test.git/config"); err == nil {
		t.Fatal("repository must not be created when push is disabled")
	}
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := tryGit(dir, args...)
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return out
}

func tryGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_TERMINAL_PROMPT=0",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
