package main

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/transport"
)

func TestDiffRefs(t *testing.T) {
	main := plumbing.NewBranchReferenceName("main")
	dev := plumbing.NewBranchReferenceName("dev")
	h1 := plumbing.NewHash("1111111111111111111111111111111111111111")
	h2 := plumbing.NewHash("2222222222222222222222222222222222222222")

	tests := []struct {
		name   string
		before map[plumbing.ReferenceName]plumbing.Hash
		after  map[plumbing.ReferenceName]plumbing.Hash
		want   []refUpdate
	}{
		{
			name:   "created",
			before: map[plumbing.ReferenceName]plumbing.Hash{},
			after:  map[plumbing.ReferenceName]plumbing.Hash{main: h1},
			want:   []refUpdate{{Name: main, Old: plumbing.ZeroHash, New: h1}},
		},
		{
			name:   "updated",
			before: map[plumbing.ReferenceName]plumbing.Hash{main: h1},
			after:  map[plumbing.ReferenceName]plumbing.Hash{main: h2},
			want:   []refUpdate{{Name: main, Old: h1, New: h2}},
		},
		{
			name:   "deleted",
			before: map[plumbing.ReferenceName]plumbing.Hash{main: h1, dev: h2},
			after:  map[plumbing.ReferenceName]plumbing.Hash{main: h1},
			want:   []refUpdate{{Name: dev, Old: h2, New: plumbing.ZeroHash}},
		},
		{
			name:   "unchanged",
			before: map[plumbing.ReferenceName]plumbing.Hash{main: h1},
			after:  map[plumbing.ReferenceName]plumbing.Hash{main: h1},
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := diffRefs(tt.before, tt.after)
			if len(got) != len(tt.want) {
				t.Fatalf("diffRefs = %v, want %v", got, tt.want)
			}
			for i, u := range got {
				if u != tt.want[i] {
					t.Errorf("update[%d] = %+v, want %+v", i, u, tt.want[i])
				}
			}
		})
	}
}

// syncBuffer is a goroutine-safe buffer for capturing slog output while the
// server and an async hook write concurrently.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestReceivePackHook pushes a repo carrying .objgit/hooks/receive-pack and
// asserts the hook runs in the sandbox: it reads /src, writes scratch to /tmp,
// and cannot write to the read-only /src.
func TestReceivePackHook(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	var logBuf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	fs := memfs.New()
	d := &daemon{
		fs:          fs,
		loader:      transport.NewFilesystemLoader(fs, false),
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

	remote := "git://" + ln.Addr().String() + "/hooked.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")

	// The hook reads /src (cwd), writes scratch to /tmp, then attempts a write
	// into the read-only /src. The final write aborts the shell with a
	// read-only error, so WROTE_SRC must never print.
	hook := strings.Join([]string{
		"cat README.md",
		"echo built > /tmp/out",
		"cat /tmp/out",
		"echo nope > /src/nope.txt",
		"echo WROTE_SRC",
	}, "\n") + "\n"
	writeFile(t, filepath.Join(work, "README.md"), "hello from repo\n")
	writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), hook)
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "with hook")

	runGit(t, work, "push", remote, "main")

	// The hook runs asynchronously after the push response; wait for it to
	// finish (it ends with an error because of the /src write attempt).
	waitForLog(t, &logBuf, "hook: finished", 30*time.Second)

	logs := logBuf.String()
	if !strings.Contains(logs, "hook: running") {
		t.Fatalf("hook did not run; logs:\n%s", logs)
	}
	// /src is readable and /tmp is writable.
	for _, want := range []string{"hello from repo", "built"} {
		if !strings.Contains(logs, want) {
			t.Errorf("hook output missing %q; logs:\n%s", want, logs)
		}
	}
	// Writing to /src is rejected and aborts the script.
	if !strings.Contains(logs, "read-only filesystem") {
		t.Errorf("expected read-only error when writing /src; logs:\n%s", logs)
	}
	if strings.Contains(logs, "WROTE_SRC") {
		t.Errorf("hook was able to write to read-only /src; logs:\n%s", logs)
	}
}

// TestReceivePackHookAbsent confirms a push with no hook file is a no-op (push
// still succeeds, nothing logged as a hook run).
func TestReceivePackHookAbsent(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	var logBuf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	fs := memfs.New()
	d := &daemon{
		fs:          fs,
		loader:      transport.NewFilesystemLoader(fs, false),
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

	remote := "git://" + ln.Addr().String() + "/plain.git"
	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "commit", "--allow-empty", "-m", "no hook")
	runGit(t, work, "push", remote, "main")

	// runHook logs this at debug level once it sees there is no hook file.
	waitForLog(t, &logBuf, "no hook file", 10*time.Second)

	if strings.Contains(logBuf.String(), "hook: running") {
		t.Errorf("hook ran for a repo with no hook file; logs:\n%s", logBuf.String())
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %q: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}

// waitForLog blocks until the captured log output contains substr, or fails the
// test after timeout. Hooks run asynchronously, so tests synchronize on a
// terminal log line rather than the daemon's WaitGroup (which the push response
// can outrun, racing Add against Wait).
func waitForLog(t *testing.T, buf *syncBuffer, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), substr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for log %q; logs:\n%s", substr, buf.String())
}
