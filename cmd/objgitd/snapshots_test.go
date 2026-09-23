package main

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
	"github.com/tigrisdata/objgit/internal/snapshot"
)

// snapStorer is an in-memory storer that also holds snapshot images, the way
// *tigris.Storer does.
type snapStorer struct {
	*memory.Storage
	*snapshot.MemStore
}

// snapBase is memBase with a snapshot store for each repository.
type snapBase struct {
	mu    sync.Mutex
	repos map[string]*snapStorer
}

func newSnapBase() *snapBase { return &snapBase{repos: map[string]*snapStorer{}} }

func (b *snapBase) Scoped(prefix string) storage.Storer {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.repos[prefix]
	if !ok {
		st = &snapStorer{Storage: memory.NewStorage(), MemStore: snapshot.NewMemStore()}
		b.repos[prefix] = st
	}
	return st
}

func (b *snapBase) repo(prefix string) *snapStorer {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.repos[prefix]
}

func newSnapshotDaemon(t *testing.T, base repofs.Base, on bool) *httptest.Server {
	t.Helper()
	d := &daemon{
		sysFS:           memfs.New(),
		resolver:        repofs.BucketResolver{Base: base},
		authz:           auth.AllowAnonymous{AllowWrite: true},
		snapshots:       on,
		snapshotTimeout: 30 * time.Second,
		snapshotTmpDir:  t.TempDir(),
	}
	ts := httptest.NewServer(d.httpHandler())
	t.Cleanup(ts.Close)
	return ts
}

func TestSmartHTTPPushSnapshots(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	base := newSnapBase()
	ts := newSnapshotDaemon(t, base, true)

	work := seedRepo(t)
	writeFile(t, filepath.Join(work, "README.md"), "hello snapshot\n")
	writeFile(t, filepath.Join(work, "docs", "a.md"), "nested\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "content")
	runGit(t, work, "tag", "-a", "v1.0.0", "-m", "v1")

	out, err := tryGit(work, "push", ts.URL+"/acme/snap.git", "main", "v1.0.0")
	if err != nil {
		t.Fatalf("push: %v\n%s", err, out)
	}
	treeHex := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD^{tree}"))
	tree := plumbing.NewHash(treeHex)

	repo := base.repo("acme/snap")
	if keys := repo.Keys(); len(keys) != 1 || keys[0] != snapshot.Key(tree) {
		t.Fatalf("snapshot keys = %v, want [%s]", keys, snapshot.Key(tree))
	}
	snap, err := snapshot.Open(context.Background(), repo, tree)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer snap.Close()
	got, err := fs.ReadFile(snap, "docs/a.md")
	if err != nil || string(got) != "nested\n" {
		t.Errorf("docs/a.md = %q, %v", got, err)
	}
	for _, want := range []string{
		"objgit: snapshot refs/heads/main (tree " + treeHex[:7] + "): built",
		"objgit: snapshot refs/tags/v1.0.0 (tree " + treeHex[:7] + "): exists",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("push output missing %q:\n%s", want, out)
		}
	}
}

func TestPushSnapshotsOffByDefault(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	base := newSnapBase()
	ts := newSnapshotDaemon(t, base, false)
	work := seedRepo(t)
	if out, err := tryGit(work, "push", ts.URL+"/acme/off.git", "main"); err != nil {
		t.Fatalf("push: %v\n%s", err, out)
	}
	if keys := base.repo("acme/off").Keys(); len(keys) != 0 {
		t.Errorf("snapshot keys = %v, want none", keys)
	}
}

func TestRunSnapshotsPutFailure(t *testing.T) {
	st := &snapStorer{Storage: memory.NewStorage(), MemStore: snapshot.NewMemStore()}
	st.PutErr = errors.New("bucket down")
	commit := writeCommit(t, st.Storage)

	d := &daemon{snapshots: true, snapshotTimeout: 10 * time.Second, snapshotTmpDir: t.TempDir()}
	var progress bytes.Buffer
	d.runSnapshots("acme/x", st, []refUpdate{{Name: plumbing.NewBranchReferenceName("main"), New: commit}}, &progress)

	if !strings.Contains(progress.String(), "): failed") {
		t.Errorf("progress = %q, want a failed line", progress.String())
	}
	if strings.Contains(progress.String(), "bucket down") {
		t.Errorf("progress leaks the error text: %q", progress.String())
	}
}

func TestRunSnapshotsSkipsStorerWithoutStore(t *testing.T) {
	st := memory.NewStorage()
	commit := writeCommit(t, st)
	d := &daemon{snapshots: true, snapshotTimeout: 10 * time.Second}
	var progress bytes.Buffer
	d.runSnapshots("acme/x", st, []refUpdate{{Name: plumbing.NewBranchReferenceName("main"), New: commit}}, &progress)
	if progress.Len() != 0 {
		t.Errorf("progress = %q, want nothing", progress.String())
	}
}

func TestPeelToTree(t *testing.T) {
	st := memory.NewStorage()
	commit := writeCommit(t, st)
	c, err := object.GetCommit(st, commit)
	if err != nil {
		t.Fatal(err)
	}
	tagOfCommit := writeTag(t, st, "v1", commit, plumbing.CommitObject)
	tagOfTag := writeTag(t, st, "v1-again", tagOfCommit, plumbing.TagObject)
	tagOfTree := writeTag(t, st, "tree-tag", c.TreeHash, plumbing.TreeObject)

	tests := []struct {
		name   string
		h      plumbing.Hash
		want   plumbing.Hash
		wantOK bool
	}{
		{"commit", commit, c.TreeHash, true},
		{"tag of commit", tagOfCommit, c.TreeHash, true},
		{"tag of tag", tagOfTag, c.TreeHash, true},
		{"tag of tree", tagOfTree, plumbing.ZeroHash, false},
		{"tree", c.TreeHash, plumbing.ZeroHash, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := peelToTree(st, tt.h)
			if err != nil {
				t.Fatalf("peelToTree: %v", err)
			}
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("peelToTree = %s, %v; want %s, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{5*1024*1024 + 600*1024, "5.6 MiB"},
		{3 << 30, "3.0 GiB"},
	}
	for _, tt := range tests {
		if got := humanBytes(tt.n); got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

// TestHooksIgnoreTags pins that a pushed tag does not run the hook, now that
// snapshotRefs returns tags as well as branches.
func TestHooksIgnoreTags(t *testing.T) {
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

	work := seedRepo(t)
	writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), "echo hook_ran\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "with hook")
	if out, err := tryGit(work, "push", ts.URL+"/acme/tags.git", "main"); err != nil {
		t.Fatalf("push main: %v\n%s", err, out)
	}
	if n := strings.Count(logBuf.String(), "hook: running"); n != 1 {
		t.Fatalf("hook ran %d times after the branch push, want 1", n)
	}

	runGit(t, work, "tag", "-a", "v1", "-m", "v1")
	if out, err := tryGit(work, "push", ts.URL+"/acme/tags.git", "v1"); err != nil {
		t.Fatalf("push tag: %v\n%s", err, out)
	}
	if n := strings.Count(logBuf.String(), "hook: running"); n != 1 {
		t.Errorf("hook ran %d times after the tag push, want 1", n)
	}
}

// writeCommit stores a commit with a one-file tree and returns its hash.
func writeCommit(t *testing.T, st *memory.Storage) plumbing.Hash {
	t.Helper()
	blob := st.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, err := blob.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hi\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	bh, err := st.SetEncodedObject(blob)
	if err != nil {
		t.Fatal(err)
	}
	tree := &object.Tree{Entries: []object.TreeEntry{{Name: "a", Mode: filemode.Regular, Hash: bh}}}
	to := st.NewEncodedObject()
	if err := tree.Encode(to); err != nil {
		t.Fatal(err)
	}
	th, err := st.SetEncodedObject(to)
	if err != nil {
		t.Fatal(err)
	}
	sig := object.Signature{Name: "T", Email: "t@example.com", When: time.Unix(0, 0)}
	c := &object.Commit{Author: sig, Committer: sig, Message: "m", TreeHash: th}
	co := st.NewEncodedObject()
	if err := c.Encode(co); err != nil {
		t.Fatal(err)
	}
	ch, err := st.SetEncodedObject(co)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func writeTag(t *testing.T, st *memory.Storage, name string, target plumbing.Hash, typ plumbing.ObjectType) plumbing.Hash {
	t.Helper()
	sig := object.Signature{Name: "T", Email: "t@example.com", When: time.Unix(0, 0)}
	tag := &object.Tag{Name: name, Tagger: sig, Message: name, TargetType: typ, Target: target}
	o := st.NewEncodedObject()
	if err := tag.Encode(o); err != nil {
		t.Fatal(err)
	}
	h, err := st.SetEncodedObject(o)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
