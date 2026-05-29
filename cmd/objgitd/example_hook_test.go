package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"tangled.org/xeiaso.net/objgit/internal/auth"
)

// TestExampleHookRuns pushes the repository's own example hook
// (.objgit/hooks/receive-pack, with a go.mod present) and asserts it runs to
// completion in the sandbox. This guards the shipped example against bit-rot:
// if the hook ever uses something kefka cannot run, this test fails.
func TestExampleHookRuns(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	example, err := os.ReadFile(filepath.Join("..", "..", ".objgit", "hooks", "receive-pack"))
	if err != nil {
		t.Fatalf("read example hook: %v", err)
	}

	var logBuf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	fs := memfs.New()
	d := &daemon{
		fs:          fs,
		loader:      transport.NewFilesystemLoader(fs, false),
		authz:       auth.AllowAnonymous{AllowWrite: true},
		allowPush:   true,
		allowHooks:  true,
		hookTimeout: 30 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = d.Serve(ctx, ln) }()

	remote := "git://" + ln.Addr().String() + "/example.git"
	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	writeFile(t, filepath.Join(work, "go.mod"), "module example.test/thing\n\ngo 1.26\n")
	writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), string(example))
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "example")
	runGit(t, work, "push", remote, "main")

	waitForLog(t, &logBuf, "hook: finished", 30*time.Second)

	logs := logBuf.String()
	if strings.Contains(logs, "with errors") {
		t.Fatalf("example hook errored; logs:\n%s", logs)
	}
	for _, want := range []string{"go module: example.test/thing", "hook done", "manifest"} {
		if !strings.Contains(logs, want) {
			t.Errorf("example hook output missing %q; logs:\n%s", want, logs)
		}
	}
	t.Logf("logs:\n%s", logs)
}
