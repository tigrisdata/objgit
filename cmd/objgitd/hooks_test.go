package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
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
// server handles a push on another goroutine.
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

	d := &daemon{
		sysFS:       memfs.New(),
		resolver:    repofs.BucketResolver{Base: newMemBase()},
		authz:       auth.AllowAnonymous{AllowWrite: true},
		allowHooks:  true,
		hookTimeout: 30 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = d.ServeGitProtocol(ctx, ln) }()

	remote := "git://" + ln.Addr().String() + "/acme/hooked.git"

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

	pushOut := runGit(t, work, "push", remote, "main")

	// Hooks now run synchronously and stream stdout/stderr to the client over
	// the sideband, so by the time push returns the hook has finished and its
	// output rides along in the push output as "remote:" lines.
	logs := logBuf.String()
	if !strings.Contains(logs, "hook: running") {
		t.Fatalf("hook did not run; logs:\n%s", logs)
	}
	// /src is readable and /tmp is writable; their output streamed to the client.
	for _, want := range []string{"hello from repo", "built"} {
		if !strings.Contains(pushOut, want) {
			t.Errorf("push output missing streamed hook output %q; output:\n%s", want, pushOut)
		}
	}
	// Output reaches the client over the sideband, rendered with a "remote:" prefix.
	if !strings.Contains(pushOut, "remote:") {
		t.Errorf("hook output not streamed as remote progress; output:\n%s", pushOut)
	}
	// Writing to /src is rejected and aborts the script (logged, not streamed).
	if !strings.Contains(logs, "read-only filesystem") {
		t.Errorf("expected read-only error when writing /src; logs:\n%s", logs)
	}
	if strings.Contains(pushOut, "WROTE_SRC") {
		t.Errorf("hook was able to write to read-only /src; output:\n%s", pushOut)
	}
}

func TestReceivePackHookFileChanges(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	d := &daemon{
		sysFS:       memfs.New(),
		resolver:    repofs.BucketResolver{Base: newMemBase()},
		authz:       auth.AllowAnonymous{AllowWrite: true},
		allowHooks:  true,
		hookTimeout: 30 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = d.ServeGitProtocol(ctx, ln) }()
	remote := "git://" + ln.Addr().String() + "/acme/changes.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), strings.Join([]string{
		`printf 'ADDED=%s\n' "$OBJGIT_ADDED_FILES_JSON"`,
		`printf 'CHANGED=%s\n' "$OBJGIT_CHANGED_FILES_JSON"`,
		`printf 'DELETED=%s\n' "$OBJGIT_DELETED_FILES_JSON"`,
		`printf 'DOCUMENT=%s\n' "$(cat "$OBJGIT_CHANGES_FILE")"`,
	}, "\n")+"\n")

	// The second push contains two commits. A path added and then deleted
	// within that push must not appear in the net file lists.
	for _, tt := range []struct {
		name        string
		prepare     func(t *testing.T)
		wantAdded   []string
		wantChanged []string
		wantDeleted []string
	}{
		{
			name: "new branch compares against empty tree",
			prepare: func(t *testing.T) {
				writeFile(t, filepath.Join(work, "changed.txt"), "before\n")
				writeFile(t, filepath.Join(work, "deleted.txt"), "before\n")
			},
			wantAdded: []string{".objgit/hooks/receive-pack", "changed.txt", "deleted.txt"},
		},
		{
			name: "updated branch reports escaped paths and net changes",
			prepare: func(t *testing.T) {
				writeFile(t, filepath.Join(work, "changed.txt"), "after\n")
				writeFile(t, filepath.Join(work, "a b\nc.txt"), "new\n")
				writeFile(t, filepath.Join(work, "transient.txt"), "temporary\n")
				runGit(t, work, "add", ".")
				runGit(t, work, "commit", "-m", "add transient")
				if err := os.Remove(filepath.Join(work, "transient.txt")); err != nil {
					t.Fatalf("remove transient: %v", err)
				}
				if err := os.Remove(filepath.Join(work, "deleted.txt")); err != nil {
					t.Fatalf("remove deleted: %v", err)
				}
			},
			wantAdded:   []string{"a b\nc.txt"},
			wantChanged: []string{"changed.txt"},
			wantDeleted: []string{"deleted.txt"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepare(t)
			runGit(t, work, "add", "-A")
			runGit(t, work, "commit", "-m", tt.name)
			pushOut := runGit(t, work, "push", remote, "main")

			want := map[string][]string{
				"ADDED":   tt.wantAdded,
				"CHANGED": tt.wantChanged,
				"DELETED": tt.wantDeleted,
			}
			for key, paths := range want {
				if paths == nil {
					paths = []string{}
				}
				encoded, err := json.Marshal(paths)
				if err != nil {
					t.Fatalf("marshal expected %s: %v", key, err)
				}
				if !strings.Contains(pushOut, key+"="+string(encoded)) {
					t.Errorf("push output lacks %s=%s; output:\n%s", key, encoded, pushOut)
				}
			}

			var document struct {
				Added   []string `json:"added"`
				Changed []string `json:"changed"`
				Deleted []string `json:"deleted"`
			}
			found := false
			for _, line := range strings.Split(pushOut, "\n") {
				if _, raw, ok := strings.Cut(line, "DOCUMENT="); ok {
					found = true
					if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &document); err != nil {
						t.Fatalf("parse changes file %q: %v", raw, err)
					}
				}
			}
			if !found {
				t.Fatalf("hook did not print changes file; output:\n%s", pushOut)
			}
			for key, got := range map[string][]string{"added": document.Added, "changed": document.Changed, "deleted": document.Deleted} {
				if got == nil {
					t.Errorf("changes file %s is missing or null; want a JSON array", key)
				}
				if !slices.Equal(got, want[strings.ToUpper(key)]) {
					t.Errorf("changes file %s = %q, want %q", key, got, want[strings.ToUpper(key)])
				}
			}
		})
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

	d := &daemon{
		sysFS:       memfs.New(),
		resolver:    repofs.BucketResolver{Base: newMemBase()},
		authz:       auth.AllowAnonymous{AllowWrite: true},
		allowHooks:  true,
		hookTimeout: 30 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = d.ServeGitProtocol(ctx, ln) }()

	remote := "git://" + ln.Addr().String() + "/acme/plain.git"
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
// test after timeout.
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
