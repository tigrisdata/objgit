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

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/tigrisdata/objgit/internal/auth"
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
		fs:     fs,
		loader: transport.NewFilesystemLoader(fs, false),
		authz:  auth.AllowAnonymous{AllowWrite: true},
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
		fs:     fs,
		loader: transport.NewFilesystemLoader(fs, false),
		authz:  auth.AllowAnonymous{AllowWrite: false},
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

// TestDaemonPushKeepsPack verifies the receive-pack path stores the incoming
// pack whole (objects/pack/pack-*.pack + .idx) rather than exploding it into
// loose objects (objects/<2-hex>/...). git:// used to hide the PackfileWriter
// capability and fall back to loose objects (one S3 PUT + one Lstat per object);
// writePack now delimits the pack with a Scanner and feeds the PackfileWriter on
// every transport.
func TestDaemonPushKeepsPack(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	fs := memfs.New()
	d := &daemon{
		fs:     fs,
		loader: transport.NewFilesystemLoader(fs, false),
		authz:  auth.AllowAnonymous{AllowWrite: true},
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
	writeFile(t, filepath.Join(work, "README.md"), "hello\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "initial") // blob + tree + commit
	runGit(t, work, "push", remote, "main")

	assertPackedRepo(t, fs, "/test.git")
}

// assertPackedRepo fails unless repoPath holds at least one packfile and no loose
// object directories (a 2-hex-char dir under objects/ such as "ab/").
func assertPackedRepo(t *testing.T, fs billy.Filesystem, repoPath string) {
	t.Helper()

	packs, err := fs.ReadDir(repoPath + "/objects/pack")
	if err != nil {
		t.Fatalf("ReadDir objects/pack: %v", err)
	}
	var packCount int
	for _, e := range packs {
		if strings.HasSuffix(e.Name(), ".pack") {
			packCount++
		}
	}
	if packCount == 0 {
		t.Errorf("expected a packfile under %s/objects/pack, found none", repoPath)
	}

	entries, err := fs.ReadDir(repoPath + "/objects")
	if err != nil {
		t.Fatalf("ReadDir objects: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() && isLooseObjectDir(e.Name()) {
			t.Errorf("found loose-object dir objects/%s; pack should have been kept whole", e.Name())
		}
	}
}

// isLooseObjectDir reports whether name is a git loose-object fan-out directory
// (two lowercase hex characters), distinguishing it from "pack" and "info".
func isLooseObjectDir(name string) bool {
	if len(name) != 2 {
		return false
	}
	for _, c := range name {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
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
