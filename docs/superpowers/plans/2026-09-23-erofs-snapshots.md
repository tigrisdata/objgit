# erofs Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After a push, build one Zstandard-compressed EROFS image of the tree at each updated ref tip, store it in the bucket, and give a local-cache-backed `Open` for later readers.

**Architecture:** A new `internal/snapshot` package holds the build (`Ensure`) and the open (`Open`) over a small `Store` interface. `*tigris.Storer` implements `Store`, with a second `PackCache` instance as the local LRU cache for downloads. The daemon calls `Ensure` synchronously in the existing `onUpdated` seam of receive-pack.

**Tech Stack:** Go 1.26, go-git v6, `github.com/Xe/erofs` v0.6.1, AWS SDK v2 S3 client via `internal/storage/tigris`, Prometheus `promauto`.

**Spec:** `docs/superpowers/specs/2026-09-23-erofs-snapshots-design.md`. Read it before any task.

## Global Constraints

- Object key: `snapshots/erofs/v1/<tree-hex>.erofs`, relative to the repository prefix.
- Metadata keys (lowercase, exact): `erofs-format` = `1`, `erofs-sha256` = hex SHA-256 of the image, `git-tree` = hex tree hash, `erofs-files` = count of regular files.
- Builder options (exact): `erofs.WithBlockSize(12)`, `erofs.WithEpoch(time.Unix(0, 0))`, `erofs.WithCompression(erofs.CompressionZstd)`.
- Cache id: `erofs-v1-<tree-hex>`. Cache directory pattern: `objgit-snapshots-*`, in the same parent as the pack cache.
- Flags: `-erofs-snapshots` (false), `-snapshot-timeout` (2m), `-snapshot-cache-bytes` (2 GiB, 0 disables), `-snapshot-cache-max-idle` (1h, 0 disables the sweep). Each flag gets a `flagenv` fallback automatically.
- A snapshot failure never fails a push.
- `slog` uses `"err"` as the error key, never `"error"`.
- Tests are table-driven with `tt`. Tests that shell out to `git` are gated with `exec.LookPath("git")`.
- No `repo` label on any metric.
- Every commit ends with the line `Signed-off-by: Xe Iaso <xe@tigrisdata.com>`. Use Conventional Commits.
- Documentation follows the `simple-english` skill (ASD-STE100 pragmatic mode).
- Run only the tests of the package you change until the final task. Other agents work in the same tree at the same time.
- Commit only the files your task names: `git add <paths>`, never `git add -A`. If `git commit` fails on `index.lock`, wait two seconds and retry.

## Review Focus

1. **Unusual names in a tree** (spaces, UTF-8, a leading dot, a 255-byte name). A person expects each one to round-trip. Pinned in Task 1, `TestEnsureMapsModes`.
2. **A directory with many entries** (more dirents than one 4096-byte block holds). A person expects every entry listed. Pinned in Task 1, `TestEnsureMapsModes` case "large directory".
3. **The snapshot timeout expires during a build.** A person expects no partial object, and the push still succeeds. Pinned in Task 1, `TestEnsureCanceled`, and Task 5, `TestRunSnapshotsPutFailure`.
4. **A bucket error on `PutSnapshot`.** A person expects the push to succeed and the client to see `failed`. Pinned in Task 5, `TestRunSnapshotsPutFailure`.
5. **A tag of a tag, and a tag of a tree.** A person expects the first to resolve to the commit tree, and the second to be skipped with no error. Pinned in Task 5, `TestPeelToTree`.

---

## File Structure

| Path                                                                           | Task | Responsibility                                                            |
| ------------------------------------------------------------------------------ | ---- | ------------------------------------------------------------------------- |
| `internal/snapshot/snapshot.go`                                                | 0    | Package doc, `Store`, `File`, `Key`, `CacheID`, metadata constants.       |
| `internal/snapshot/ensure.go`                                                  | 1    | `Ensure`, `Result`, the tree walk.                                        |
| `internal/snapshot/open.go`                                                    | 1    | `Snapshot`, `Open`.                                                       |
| `internal/snapshot/memstore.go`                                                | 1    | `MemStore`, a map-backed `Store` for tests in any package.                |
| `internal/snapshot/ensure_test.go`, `open_test.go`, `fixture_test.go`          | 1    | Tests and the git fixture helper.                                         |
| `internal/storage/tigris/packcache.go` (+ `_test.go`)                          | 2    | `GetChecked`, `EvictIdle`, observer, suffix, `NewSnapshotCache`.          |
| `internal/metrics/metrics.go`                                                  | 3    | Five series, two helpers.                                                 |
| `docs/architecture/metrics.md`                                                 | 3    | The new series.                                                           |
| `internal/storage/tigris/snapshot.go` (+ `_test.go`)                           | 4    | `*Storer` implements `snapshot.Store`.                                    |
| `internal/storage/tigris/tigris.go`                                            | 4    | `snapCache` field and `WithSnapshotCache`.                                |
| `cmd/objgitd/snapshots.go` (+ `_test.go`)                                      | 5    | `runSnapshots`, `peelToTree`, progress formatting.                        |
| `cmd/objgitd/hooks.go`, `git_protocol.go`, `hooks_test.go`                     | 5    | Gate, tags in `snapshotRefs`, branch filter in `runHooks`, daemon fields. |
| `cmd/objgitd/main.go`                                                          | 6    | Flags, cache, sweep goroutine, cleanup.                                   |
| `docs/architecture/snapshots.md`, `tigris-storer.md`, `README.md`, `AGENTS.md` | 7    | Documentation.                                                            |

Dependency order: Task 0 → (Tasks 1, 2, 3 in parallel) → (Task 4 after 2; Task 5 after 1 and 3) → (Tasks 6 and 7 after 4 and 5).

---

### Task 0: Scaffold (the controller does this before any fan-out)

**Files:**

- Modify: `go.mod`, `go.sum`
- Create: `internal/snapshot/snapshot.go`

**Interfaces:**

- Produces: `snapshot.Store`, `snapshot.File`, `snapshot.Key(plumbing.Hash) string`, `snapshot.CacheID(key string) string`, `snapshot.FormatVersion`, `snapshot.MetaFormat`, `snapshot.MetaSHA256`, `snapshot.MetaTree`, `snapshot.MetaFiles`.

- [ ] **Step 1: Add the dependency**

Run: `go get github.com/Xe/erofs@v0.6.1`

- [ ] **Step 2: Write `internal/snapshot/snapshot.go`**

```go
// Package snapshot builds and opens EROFS images of git trees. An image is
// stored next to the repository it came from, under Key(tree), and a later
// reader (such as a web UI) opens it with Open instead of walking git objects.
//
// The package never imports internal/storage/tigris. Storage reaches it
// through Store, which *tigris.Storer implements.
package snapshot

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"
)

// FormatVersion names the mapping from git to EROFS and the build options.
// Changing either one is a new version, and a new key, so an image written
// under one version is never overwritten by another.
const FormatVersion = 1

// Object metadata keys carried by every image. Lowercase, because S3 returns
// user metadata keys lowercased.
const (
	MetaFormat = "erofs-format"
	MetaSHA256 = "erofs-sha256"
	MetaTree   = "git-tree"
	MetaFiles  = "erofs-files"
)

const keyPrefix = "snapshots/erofs/"

// Key returns the object key of the image of tree, relative to the
// repository prefix.
func Key(tree plumbing.Hash) string {
	return fmt.Sprintf("%sv%d/%s.erofs", keyPrefix, FormatVersion, tree)
}

// CacheID returns the local cache id for key: "erofs-v1-<tree>". It holds no
// repository prefix, so two repositories with one tree share one cached file.
func CacheID(key string) string {
	id := strings.TrimSuffix(strings.TrimPrefix(key, keyPrefix), ".erofs")
	return "erofs-" + strings.ReplaceAll(id, "/", "-")
}

// File is a local copy of an image. *os.File satisfies it.
type File interface {
	io.ReaderAt
	io.Closer
}

// Store holds snapshot images for one repository. Keys are relative to the
// repository prefix.
type Store interface {
	// StatSnapshot returns the size of the image at key, or an error that
	// matches fs.ErrNotExist when key is absent.
	StatSnapshot(ctx context.Context, key string) (int64, error)
	// PutSnapshot stores size bytes from body at key, with meta as user
	// metadata.
	PutSnapshot(ctx context.Context, key string, body io.ReadSeeker, size int64, meta map[string]string) error
	// OpenSnapshot returns a local copy of the image at key. The caller must
	// close it. It returns an error that matches fs.ErrNotExist when key is
	// absent.
	OpenSnapshot(ctx context.Context, key string) (File, error)
}
```

- [ ] **Step 3: Build**

Run: `go build ./... && go mod tidy && go build ./...`
Expected: success. `go mod tidy` can drop the erofs requirement because nothing imports it yet. If it does, keep it with `go get github.com/Xe/erofs@v0.6.1` again after Task 1 imports it, and do not run `go mod tidy` here.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum internal/snapshot/snapshot.go
git commit -m "feat(snapshot): add the Store interface and key layout" -m "Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 1: `Ensure`, `Open`, and `MemStore`

**Files:**

- Create: `internal/snapshot/ensure.go`, `internal/snapshot/open.go`, `internal/snapshot/memstore.go`
- Test: `internal/snapshot/fixture_test.go`, `internal/snapshot/ensure_test.go`, `internal/snapshot/open_test.go`

**Interfaces:**

- Consumes: everything from Task 0.
- Produces:
  - `type Result struct { Key string; Status string; Files int; Bytes int64; Elapsed time.Duration }`
  - `const StatusBuilt = "built"`, `const StatusExists = "exists"`
  - `func Ensure(ctx context.Context, objs storer.EncodedObjectStorer, store Store, tree plumbing.Hash, tmpDir string) (Result, error)`
  - `type Snapshot struct { *erofs.FS; f File }`, `func (s *Snapshot) Close() error`
  - `func Open(ctx context.Context, store Store, tree plumbing.Hash) (*Snapshot, error)`
  - `type MemStore struct { PutErr error; ... }`, `func NewMemStore() *MemStore`, `func (m *MemStore) Keys() []string`, `func (m *MemStore) Object(key string) (body []byte, meta map[string]string, ok bool)`, `func (m *MemStore) PutCount() int`

- [ ] **Step 1: Write the fixture helper `fixture_test.go`**

```go
package snapshot

import (
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/memory"
)

// fixture is one entry of a test tree. For a gitlink, content is ignored and
// hash is the submodule commit. Directories come from the paths.
type fixture struct {
	path    string
	mode    filemode.FileMode
	content string
}

// buildTree writes the blobs and trees for entries into st and returns the
// root tree hash. It sorts entries the way git does, so the hash is stable.
func buildTree(t *testing.T, st *memory.Storage, entries []fixture) plumbing.Hash {
	t.Helper()
	type dir struct {
		entries []object.TreeEntry
		subdirs map[string]*dir
	}
	root := &dir{subdirs: map[string]*dir{}}
	for _, e := range entries {
		parts := strings.Split(e.path, "/")
		d := root
		for _, p := range parts[:len(parts)-1] {
			sub, ok := d.subdirs[p]
			if !ok {
				sub = &dir{subdirs: map[string]*dir{}}
				d.subdirs[p] = sub
			}
			d = sub
		}
		var h plumbing.Hash
		if e.mode == filemode.Submodule {
			h = plumbing.NewHash("1111111111111111111111111111111111111111")
		} else {
			h = writeBlob(t, st, e.content)
		}
		d.entries = append(d.entries, object.TreeEntry{Name: path.Base(e.path), Mode: e.mode, Hash: h})
	}
	var write func(d *dir) plumbing.Hash
	write = func(d *dir) plumbing.Hash {
		for name, sub := range d.subdirs {
			d.entries = append(d.entries, object.TreeEntry{Name: name, Mode: filemode.Dir, Hash: write(sub)})
		}
		sort.Slice(d.entries, func(i, j int) bool {
			return sortName(d.entries[i]) < sortName(d.entries[j])
		})
		tree := &object.Tree{Entries: d.entries}
		obj := st.NewEncodedObject()
		if err := tree.Encode(obj); err != nil {
			t.Fatalf("encode tree: %v", err)
		}
		h, err := st.SetEncodedObject(obj)
		if err != nil {
			t.Fatalf("store tree: %v", err)
		}
		return h
	}
	return write(root)
}

// sortName is the key git sorts tree entries by: a directory sorts as if its
// name ended in "/".
func sortName(e object.TreeEntry) string {
	if e.Mode == filemode.Dir {
		return e.Name + "/"
	}
	return e.Name
}

func writeBlob(t *testing.T, st *memory.Storage, content string) plumbing.Hash {
	t.Helper()
	obj := st.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	w, err := obj.Writer()
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	if _, err := w.Write([]byte(content)); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close blob: %v", err)
	}
	h, err := st.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("store blob: %v", err)
	}
	return h
}
```

- [ ] **Step 2: Write the failing tests `ensure_test.go`**

The test file holds these tests. Write all of them now.

```go
package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/Xe/erofs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/storage/memory"
)

func TestEnsureMapsModes(t *testing.T) {
	long := strings.Repeat("n", 255)
	var many []fixture
	for i := range 500 {
		many = append(many, fixture{fmt.Sprintf("big/file-%04d.txt", i), filemode.Regular, fmt.Sprintf("%d\n", i)})
	}

	tests := []struct {
		name    string
		entries []fixture
		// want maps an image path to its mode, and to its content or
		// symlink target. A directory has no content.
		want map[string]struct {
			mode    fs.FileMode
			content string
		}
	}{
		{
			name: "every mode",
			entries: []fixture{
				{"README.md", filemode.Regular, "hello\n"},
				{"bin/run.sh", filemode.Executable, "#!/bin/sh\necho hi\n"},
				{"link", filemode.Symlink, "README.md"},
				{"old", filemode.Deprecated, "group writable\n"},
				{"vendor/lib", filemode.Submodule, ""},
			},
			want: map[string]struct {
				mode    fs.FileMode
				content string
			}{
				"README.md":  {0o644, "hello\n"},
				"bin":        {fs.ModeDir | 0o755, ""},
				"bin/run.sh": {0o755, "#!/bin/sh\necho hi\n"},
				"link":       {fs.ModeSymlink | 0o777, "README.md"},
				"old":        {0o644, "group writable\n"},
				"vendor":     {fs.ModeDir | 0o755, ""},
				"vendor/lib": {fs.ModeDir | 0o755, ""},
			},
		},
		{
			name: "unusual names",
			entries: []fixture{
				{"with space.txt", filemode.Regular, "a"},
				{"ünïcødé.txt", filemode.Regular, "b"},
				{".hidden", filemode.Regular, "c"},
				{long, filemode.Regular, "d"},
			},
			want: map[string]struct {
				mode    fs.FileMode
				content string
			}{
				"with space.txt": {0o644, "a"},
				"ünïcødé.txt":    {0o644, "b"},
				".hidden":        {0o644, "c"},
				long:             {0o644, "d"},
			},
		},
		{
			name:    "large directory",
			entries: many,
			want: map[string]struct {
				mode    fs.FileMode
				content string
			}{
				"big/file-0000.txt": {0o644, "0\n"},
				"big/file-0499.txt": {0o644, "499\n"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.NewStorage()
			tree := buildTree(t, st, tt.entries)
			store := NewMemStore()

			res, err := Ensure(context.Background(), st, store, tree, t.TempDir())
			if err != nil {
				t.Fatalf("Ensure: %v", err)
			}
			if res.Status != StatusBuilt {
				t.Fatalf("Status = %q, want %q", res.Status, StatusBuilt)
			}

			snap, err := Open(context.Background(), store, tree)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer snap.Close()

			for p, w := range tt.want {
				info, err := snap.Lstat(p)
				if err != nil {
					t.Errorf("Lstat(%q): %v", p, err)
					continue
				}
				if info.Mode() != w.mode {
					t.Errorf("%q mode = %v, want %v", p, info.Mode(), w.mode)
				}
				switch {
				case w.mode&fs.ModeSymlink != 0:
					got, err := snap.ReadLink(p)
					if err != nil || got != w.content {
						t.Errorf("ReadLink(%q) = %q, %v; want %q", p, got, err, w.content)
					}
				case w.mode.IsRegular():
					got, err := fs.ReadFile(snap, p)
					if err != nil || string(got) != w.content {
						t.Errorf("ReadFile(%q) = %q, %v; want %q", p, got, err, w.content)
					}
				}
			}

			if tt.name == "large directory" {
				ents, err := fs.ReadDir(snap, "big")
				if err != nil || len(ents) != 500 {
					t.Errorf("ReadDir(big) = %d entries, %v; want 500", len(ents), err)
				}
			}
		})
	}
}

func TestEnsureValidates(t *testing.T) {
	tests := []struct {
		name    string
		entries []fixture
	}{
		{"flat", []fixture{{"a", filemode.Regular, "a"}}},
		{"nested", []fixture{{"x/y/z", filemode.Regular, strings.Repeat("z", 10000)}}},
		{"gitlink", []fixture{{"sub", filemode.Submodule, ""}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.NewStorage()
			tree := buildTree(t, st, tt.entries)
			store := NewMemStore()
			if _, err := Ensure(context.Background(), st, store, tree, t.TempDir()); err != nil {
				t.Fatalf("Ensure: %v", err)
			}
			body, _, _ := store.Object(Key(tree))
			res, err := erofs.Validate(bytes.NewReader(body), int64(len(body)))
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if len(res.Errors) != 0 {
				t.Errorf("Validate errors: %v", res.Errors)
			}
		})
	}
}

func TestEnsureCompresses(t *testing.T) {
	text := strings.Repeat("all work and no play makes jack a dull boy\n", 8000) // ~350 KiB
	st := memory.NewStorage()
	tree := buildTree(t, st, []fixture{{"story.txt", filemode.Regular, text}})
	store := NewMemStore()
	res, err := Ensure(context.Background(), st, store, tree, t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if res.Bytes >= int64(len(text)) {
		t.Errorf("image is %d bytes, want less than the %d-byte file", res.Bytes, len(text))
	}
	snap, err := Open(context.Background(), store, tree)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer snap.Close()
	got, err := fs.ReadFile(snap, "story.txt")
	if err != nil || string(got) != text {
		t.Errorf("story.txt did not read back unchanged: %v", err)
	}
}

func TestEnsureDeterministic(t *testing.T) {
	entries := []fixture{
		{"a.txt", filemode.Regular, strings.Repeat("a", 9000)},
		{"d/b.txt", filemode.Executable, "b"},
		{"l", filemode.Symlink, "a.txt"},
	}
	var bodies [2][]byte
	var sums [2]string
	for i := range 2 {
		st := memory.NewStorage()
		tree := buildTree(t, st, entries)
		store := NewMemStore()
		if _, err := Ensure(context.Background(), st, store, tree, t.TempDir()); err != nil {
			t.Fatalf("Ensure %d: %v", i, err)
		}
		body, meta, _ := store.Object(Key(tree))
		bodies[i], sums[i] = body, meta[MetaSHA256]
	}
	if !bytes.Equal(bodies[0], bodies[1]) {
		t.Error("two builds of one tree gave different bytes")
	}
	if sums[0] == "" || sums[0] != sums[1] {
		t.Errorf("sha256 metadata = %q and %q, want equal and set", sums[0], sums[1])
	}
}

func TestEnsureMetadata(t *testing.T) {
	st := memory.NewStorage()
	tree := buildTree(t, st, []fixture{
		{"a", filemode.Regular, "a"},
		{"b", filemode.Regular, "b"},
		{"l", filemode.Symlink, "a"},
	})
	store := NewMemStore()
	res, err := Ensure(context.Background(), st, store, tree, t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	_, meta, ok := store.Object(Key(tree))
	if !ok {
		t.Fatal("no object at Key(tree)")
	}
	want := map[string]string{MetaFormat: "1", MetaTree: tree.String(), MetaFiles: "2"}
	for k, v := range want {
		if meta[k] != v {
			t.Errorf("meta[%q] = %q, want %q", k, meta[k], v)
		}
	}
	if res.Files != 2 {
		t.Errorf("Files = %d, want 2", res.Files)
	}
}

func TestEnsureExists(t *testing.T) {
	st := memory.NewStorage()
	tree := buildTree(t, st, []fixture{{"a", filemode.Regular, "a"}})
	store := NewMemStore()
	if _, err := Ensure(context.Background(), st, store, tree, t.TempDir()); err != nil {
		t.Fatalf("first Ensure: %v", err)
	}
	res, err := Ensure(context.Background(), st, store, tree, t.TempDir())
	if err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	if res.Status != StatusExists {
		t.Errorf("Status = %q, want %q", res.Status, StatusExists)
	}
	if n := store.PutCount(); n != 1 {
		t.Errorf("PutCount = %d, want 1", n)
	}
}

func TestEnsureLimits(t *testing.T) {
	tests := []struct {
		name    string
		entries []fixture
		path    string // must appear in the error
	}{
		{"name too long", []fixture{{strings.Repeat("n", 256), filemode.Regular, "x"}}, strings.Repeat("n", 256)},
		{"symlink target too long", []fixture{{"l", filemode.Symlink, strings.Repeat("t", 1021)}}, "l"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.NewStorage()
			tree := buildTree(t, st, tt.entries)
			store := NewMemStore()
			_, err := Ensure(context.Background(), st, store, tree, t.TempDir())
			if err == nil || !strings.Contains(err.Error(), tt.path) {
				t.Fatalf("Ensure error = %v, want one that names %q", err, tt.path)
			}
			if n := store.PutCount(); n != 0 {
				t.Errorf("PutCount = %d, want 0", n)
			}
		})
	}
}

func TestEnsureEmptyTree(t *testing.T) {
	st := memory.NewStorage()
	tree := buildTree(t, st, nil)
	store := NewMemStore()
	if _, err := Ensure(context.Background(), st, store, tree, t.TempDir()); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	snap, err := Open(context.Background(), store, tree)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer snap.Close()
	ents, err := fs.ReadDir(snap, ".")
	if err != nil || len(ents) != 0 {
		t.Errorf("ReadDir(.) = %v, %v; want empty", ents, err)
	}
}

func TestEnsureCanceled(t *testing.T) {
	st := memory.NewStorage()
	tree := buildTree(t, st, []fixture{{"a", filemode.Regular, "a"}})
	store := NewMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Ensure(ctx, st, store, tree, t.TempDir())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Ensure error = %v, want context.Canceled", err)
	}
	if n := store.PutCount(); n != 0 {
		t.Errorf("PutCount = %d, want 0", n)
	}
}

func TestEnsureRemovesTempFile(t *testing.T) {
	st := memory.NewStorage()
	tree := buildTree(t, st, []fixture{{"a", filemode.Regular, "a"}})
	tmp := t.TempDir()
	for _, putErr := range []error{nil, errors.New("bucket down")} {
		store := NewMemStore()
		store.PutErr = putErr
		_, _ = Ensure(context.Background(), st, store, tree, tmp)
		ents, _ := os.ReadDir(tmp)
		if len(ents) != 0 {
			t.Errorf("putErr=%v: %d files left in tmpDir", putErr, len(ents))
		}
	}
}

var _ = plumbing.ZeroHash
```

Add `"os"` to the imports for `TestEnsureRemovesTempFile`.

`TestEnsureCanceled` requires `Ensure` to check `ctx.Err()` before `StatSnapshot`, and between entries.

- [ ] **Step 3: Write `open_test.go`**

```go
package snapshot

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

func TestOpenMissing(t *testing.T) {
	_, err := Open(context.Background(), NewMemStore(), plumbing.NewHash("2222222222222222222222222222222222222222"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open error = %v, want fs.ErrNotExist", err)
	}
}
```

- [ ] **Step 4: Run the tests to make sure they fail**

Run: `go test ./internal/snapshot/`
Expected: FAIL, with `undefined: Ensure`, `undefined: NewMemStore`, and `undefined: Open`.

- [ ] **Step 5: Write `memstore.go`**

```go
package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"sync"
)

// MemStore is a Store in memory. It exists for tests, in this package and in
// others that need a Store without a bucket.
type MemStore struct {
	// PutErr, when set, is returned by every PutSnapshot.
	PutErr error

	mu   sync.Mutex
	objs map[string]memObject
	puts int
}

type memObject struct {
	body []byte
	meta map[string]string
}

// NewMemStore returns an empty MemStore.
func NewMemStore() *MemStore { return &MemStore{objs: map[string]memObject{}} }

func (m *MemStore) StatSnapshot(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	if !ok {
		return 0, fmt.Errorf("snapshot: %s: %w", key, fs.ErrNotExist)
	}
	return int64(len(o.body)), nil
}

func (m *MemStore) PutSnapshot(_ context.Context, key string, body io.ReadSeeker, size int64, meta map[string]string) error {
	m.mu.Lock()
	m.puts++
	putErr := m.PutErr
	m.mu.Unlock()
	if putErr != nil {
		return putErr
	}
	b, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(b)) != size {
		return fmt.Errorf("snapshot: put %s: read %d bytes, want %d", key, len(b), size)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = memObject{body: b, meta: maps.Clone(meta)}
	return nil
}

func (m *MemStore) OpenSnapshot(_ context.Context, key string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("snapshot: %s: %w", key, fs.ErrNotExist)
	}
	return nopCloser{bytes.NewReader(o.body)}, nil
}

// Keys returns every stored key, sorted.
func (m *MemStore) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Sorted(maps.Keys(m.objs))
}

// Object returns the body and metadata stored at key.
func (m *MemStore) Object(key string) ([]byte, map[string]string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	return o.body, o.meta, ok
}

// PutCount returns how many times PutSnapshot ran, failed calls included.
func (m *MemStore) PutCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.puts
}

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }
```

- [ ] **Step 6: Write `open.go`**

```go
package snapshot

import (
	"context"
	"fmt"

	"github.com/Xe/erofs"
	"github.com/go-git/go-git/v6/plumbing"
)

// Snapshot is an open image. Every read is local. Close releases the file.
type Snapshot struct {
	*erofs.FS
	f File
}

// Close releases the local file behind the image.
func (s *Snapshot) Close() error { return s.f.Close() }

// Open returns the image of tree. It does not build a missing image: it
// returns an error that matches fs.ErrNotExist instead. Call Ensure first when
// the image must exist.
func Open(ctx context.Context, store Store, tree plumbing.Hash) (*Snapshot, error) {
	key := Key(tree)
	f, err := store.OpenSnapshot(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open %s: %w", key, err)
	}
	fsys, err := erofs.Open(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("snapshot: read %s: %w", key, err)
	}
	return &Snapshot{FS: fsys, f: f}, nil
}
```

- [ ] **Step 7: Write `ensure.go`**

```go
package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/Xe/erofs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// Result statuses.
const (
	StatusBuilt  = "built"
	StatusExists = "exists"
)

// Two EROFS limits that git does not have. The builder checks neither one,
// and the reader refuses a symlink target longer than maxSymlink.
const (
	maxName    = 255
	maxSymlink = 255 * 4
)

// epoch pins every mtime, so one tree always gives identical bytes.
var epoch = time.Unix(0, 0)

// Result describes one Ensure call.
type Result struct {
	Key     string
	Status  string // StatusBuilt or StatusExists
	Files   int    // regular files in the image; 0 when Status is StatusExists
	Bytes   int64  // image size; 0 when Status is StatusExists
	Elapsed time.Duration
}

// Ensure makes sure that the image of tree exists in store. If the image is
// absent, it builds the image from the git objects in objs, in a temp file in
// tmpDir (the OS temp directory when tmpDir is empty), and puts it.
//
// Ensure is idempotent. Two concurrent calls for one tree both build and put
// identical bytes, which is correct, so it takes no lock.
func Ensure(ctx context.Context, objs storer.EncodedObjectStorer, store Store, tree plumbing.Hash, tmpDir string) (Result, error) {
	start := time.Now()
	key := Key(tree)
	res := Result{Key: key}
	done := func() { res.Elapsed = time.Since(start) }

	if err := ctx.Err(); err != nil {
		return res, err
	}
	if _, err := store.StatSnapshot(ctx, key); err == nil {
		res.Status = StatusExists
		done()
		return res, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return res, fmt.Errorf("snapshot: stat %s: %w", key, err)
	}

	root, err := object.GetTree(objs, tree)
	if err != nil {
		return res, fmt.Errorf("snapshot: load tree %s: %w", tree, err)
	}

	// The name is random, never the tree hash: two pushes of one tree can
	// build at the same time.
	f, err := os.CreateTemp(tmpDir, "objgit-snapshot-*.erofs")
	if err != nil {
		return res, fmt.Errorf("snapshot: create temp file: %w", err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	b := erofs.NewBuilder(f,
		erofs.WithBlockSize(12),
		erofs.WithEpoch(epoch),
		erofs.WithCompression(erofs.CompressionZstd),
	)
	w := &walker{ctx: ctx, objs: objs, b: b}
	if err := w.addTree("/", root); err != nil {
		return res, err
	}
	if err := b.Build(); err != nil {
		return res, fmt.Errorf("snapshot: build %s: %w", key, err)
	}

	sum, size, err := hashFile(f)
	if err != nil {
		return res, err
	}
	meta := map[string]string{
		MetaFormat: strconv.Itoa(FormatVersion),
		MetaSHA256: sum,
		MetaTree:   tree.String(),
		MetaFiles:  strconv.Itoa(w.files),
	}
	if err := store.PutSnapshot(ctx, key, f, size, meta); err != nil {
		return res, fmt.Errorf("snapshot: put %s: %w", key, err)
	}

	res.Status = StatusBuilt
	res.Files = w.files
	res.Bytes = size
	done()
	return res, nil
}

// hashFile returns the hex SHA-256 and size of f, and leaves f at offset 0.
func hashFile(f *os.File) (string, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", 0, fmt.Errorf("snapshot: rewind image: %w", err)
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("snapshot: hash image: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", 0, fmt.Errorf("snapshot: rewind image: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

type walker struct {
	ctx   context.Context
	objs  storer.EncodedObjectStorer
	b     *erofs.Builder
	files int
}

// addTree adds every entry of t under dir, depth first. It walks tree
// entries and not t.Files(), because go-git's FileIter skips gitlinks.
func (w *walker) addTree(dir string, t *object.Tree) error {
	for _, e := range t.Entries {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		p := path.Join(dir, e.Name)
		if len(e.Name) > maxName {
			return fmt.Errorf("snapshot: %s: name is %d bytes, EROFS allows %d", p, len(e.Name), maxName)
		}
		switch e.Mode {
		case filemode.Dir:
			sub, err := object.GetTree(w.objs, e.Hash)
			if err != nil {
				return fmt.Errorf("snapshot: %s: load tree: %w", p, err)
			}
			if err := w.b.AddDir(p, info{e.Name, fs.ModeDir | 0o755}); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p, err)
			}
			if err := w.addTree(p, sub); err != nil {
				return err
			}
		case filemode.Submodule:
			// Same as git checkout of a submodule that is not initialized.
			if err := w.b.AddDir(p, info{e.Name, fs.ModeDir | 0o755}); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p, err)
			}
		case filemode.Regular, filemode.Deprecated, filemode.Executable:
			data, err := w.blob(p, e.Hash)
			if err != nil {
				return err
			}
			perm := fs.FileMode(0o644)
			if e.Mode == filemode.Executable {
				perm = 0o755
			}
			if err := w.b.AddFile(p, info{e.Name, perm}, data); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p, err)
			}
			w.files++
		case filemode.Symlink:
			data, err := w.blob(p, e.Hash)
			if err != nil {
				return err
			}
			if len(data) > maxSymlink {
				return fmt.Errorf("snapshot: %s: symlink target is %d bytes, EROFS allows %d", p, len(data), maxSymlink)
			}
			if err := w.b.AddSymlink(p, string(data), info{e.Name, fs.ModeSymlink | 0o777}); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p, err)
			}
		default:
			return fmt.Errorf("snapshot: %s: unknown git mode %o", p, uint32(e.Mode))
		}
	}
	return nil
}

func (w *walker) blob(p string, h plumbing.Hash) ([]byte, error) {
	blob, err := object.GetBlob(w.objs, h)
	if err != nil {
		return nil, fmt.Errorf("snapshot: %s: load blob: %w", p, err)
	}
	r, err := blob.Reader()
	if err != nil {
		return nil, fmt.Errorf("snapshot: %s: read blob: %w", p, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("snapshot: %s: read blob: %w", p, err)
	}
	return data, nil
}

// info is the fs.FileInfo the builder reads the mode and mtime from.
type info struct {
	name string
	mode fs.FileMode
}

func (i info) Name() string       { return i.name }
func (i info) Size() int64        { return 0 }
func (i info) Mode() fs.FileMode  { return i.mode }
func (i info) ModTime() time.Time { return epoch }
func (i info) IsDir() bool        { return i.mode.IsDir() }
func (i info) Sys() any           { return nil }
```

- [ ] **Step 8: Run the tests**

Run: `go test ./internal/snapshot/ -v`
Expected: PASS for every test.

If `TestEnsureMapsModes` fails on the symlink mode, print `info.Mode()` for `link` and set the expected mode in the table to what the reader reports for a symlink. Do not change `ensure.go` for this. The spec does not fix the permission bits of a symlink.

- [ ] **Step 9: Vet and commit**

```bash
go vet ./internal/snapshot/
git add internal/snapshot/
git commit -m "feat(snapshot): build and open erofs images of git trees" -m "Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 2: `PackCache` additions

**Files:**

- Modify: `internal/storage/tigris/packcache.go`, `internal/storage/tigris/packindex.go` (only the `verifiedCopy` call in `openWholePack`)
- Test: `internal/storage/tigris/packcache_test.go`

**Interfaces:**

- Produces:
  - `func NewSnapshotCache(parent string, maxBytes int64, observe func(event string)) (*PackCache, error)`
  - `func (c *PackCache) GetChecked(id string, fetch func(io.Writer) (wantSHA256 string, err error)) (*os.File, error)`
  - `func (c *PackCache) EvictIdle(maxIdle time.Duration) int`
  - Event strings: `"hit"`, `"miss"`, `"evict_budget"`, `"evict_idle"`.
- `Get` and `NewPackCache` keep their signatures and behavior.

- [ ] **Step 1: Write the failing tests**

Read the existing helpers at the top of `packcache_test.go` (`cachePayload`, `newTestPackCache`, `readAll`, `countCacheFiles`) and reuse them. `countCacheFiles` counts files by suffix. If it hardcodes `.bin`, change it to use `c.suffix`. Add:

```go
func TestPackCacheGetChecked(t *testing.T) {
	body := []byte("snapshot bytes")
	sum := sha256.Sum256(body)
	good := hex.EncodeToString(sum[:])

	tests := []struct {
		name    string
		want    string
		wantErr bool
	}{
		{"digest matches", good, false},
		{"digest differs", strings.Repeat("0", 64), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewSnapshotCache(t.TempDir(), 1<<20, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Cleanup() })
			f, err := c.GetChecked("erofs-v1-abc", func(w io.Writer) (string, error) {
				_, err := w.Write(body)
				return tt.want, err
			})
			if tt.wantErr {
				if err == nil {
					f.Close()
					t.Fatal("GetChecked accepted bytes with the wrong digest")
				}
				if n := countCacheFiles(t, c); n != 0 {
					t.Errorf("cache kept %d files after a bad digest", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetChecked: %v", err)
			}
			defer f.Close()
			if got := readAll(t, f); !bytes.Equal(got, body) {
				t.Errorf("body = %q, want %q", got, body)
			}
			if !strings.HasSuffix(f.Name(), ".erofs") {
				t.Errorf("cached file %q does not end in .erofs", f.Name())
			}
		})
	}
}

func TestPackCacheGetStillChecksID(t *testing.T) {
	c := newTestPackCache(t, 1<<20)
	_, err := c.Get(strings.Repeat("0", 64), func(w io.Writer) error {
		_, err := w.Write([]byte("not the digest"))
		return err
	})
	if err == nil {
		t.Fatal("Get accepted bytes whose SHA-256 is not the id")
	}
}

func TestPackCacheEvictIdle(t *testing.T) {
	now := time.Unix(1000, 0)
	c, err := NewSnapshotCache(t.TempDir(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Cleanup() })
	c.now = func() time.Time { return now }

	put := func(id string) *os.File {
		body := []byte(id)
		sum := sha256.Sum256(body)
		f, err := c.GetChecked(id, func(w io.Writer) (string, error) {
			_, err := w.Write(body)
			return hex.EncodeToString(sum[:]), err
		})
		if err != nil {
			t.Fatalf("GetChecked(%s): %v", id, err)
		}
		return f
	}

	old := put("old")         // used at t=1000
	now = now.Add(time.Hour)  // t=4600
	put("fresh").Close()      // used at t=4600
	now = now.Add(time.Minute)

	if n := c.EvictIdle(30 * time.Minute); n != 1 {
		t.Fatalf("EvictIdle = %d, want 1", n)
	}
	if n := countCacheFiles(t, c); n != 1 {
		t.Errorf("%d files left, want 1", n)
	}
	// The evicted entry's open descriptor still reads.
	if got := readAll(t, old); string(got) != "old" {
		t.Errorf("evicted file read %q, want %q", got, "old")
	}
	old.Close()
}

func TestPackCacheObserver(t *testing.T) {
	var mu sync.Mutex
	var events []string
	observe := func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }

	now := time.Unix(1000, 0)
	c, err := NewSnapshotCache(t.TempDir(), 10, observe) // 10-byte budget
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Cleanup() })
	c.now = func() time.Time { return now }

	get := func(id string, body []byte) {
		sum := sha256.Sum256(body)
		f, err := c.GetChecked(id, func(w io.Writer) (string, error) {
			_, err := w.Write(body)
			return hex.EncodeToString(sum[:]), err
		})
		if err != nil {
			t.Fatalf("GetChecked(%s): %v", id, err)
		}
		f.Close()
	}

	get("a", []byte("123456"))  // miss
	get("a", []byte("123456"))  // hit
	get("b", []byte("abcdef"))  // miss, evicts a: evict_budget
	now = now.Add(time.Hour)
	c.EvictIdle(time.Minute)    // evicts b: evict_idle

	want := []string{"miss", "hit", "miss", "evict_budget", "evict_idle"}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(events, want) {
		t.Errorf("events = %v, want %v", events, want)
	}
}
```

Add imports as needed: `bytes`, `crypto/sha256`, `encoding/hex`, `slices`, `strings`, `sync`, `time`.

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `go test ./internal/storage/tigris/ -run 'TestPackCache'`
Expected: FAIL with `undefined: NewSnapshotCache` and `c.now undefined`.

- [ ] **Step 3: Implement**

In `packcache.go`:

1. Add fields to `PackCache`:

   ```go
   	suffix  string              // file name suffix of a cached entry, binSuffix for packs
   	observe func(event string)  // optional; see NewSnapshotCache
   	now     func() time.Time    // wall clock for EvictIdle; a field so tests can move it
   ```

2. Add `usedAt time.Time` to `cacheEntry`, next to `used`. Comment: `// wall-clock time of the last claim; EvictIdle reads it`.

3. Replace `NewPackCache` with a shared constructor:

   ```go
   func NewPackCache(parent string, maxBytes int64) (*PackCache, error) {
   	return newFileCache(parent, "objgit-packs-*", binSuffix, maxBytes, nil)
   }

   // NewSnapshotCache creates a cache for snapshot images, in a fresh
   // objgit-snapshots-* directory under parent. It is the same cache as
   // NewPackCache, with its own directory and budget. Snapshot ids are not
   // content digests, so callers use GetChecked, not Get. observe, when not
   // nil, receives "hit", "miss", "evict_budget", and "evict_idle".
   func NewSnapshotCache(parent string, maxBytes int64, observe func(event string)) (*PackCache, error) {
   	return newFileCache(parent, "objgit-snapshots-*", ".erofs", maxBytes, observe)
   }

   func newFileCache(parent, pattern, suffix string, maxBytes int64, observe func(string)) (*PackCache, error) {
   	dir, err := os.MkdirTemp(parent, pattern)
   	if err != nil {
   		return nil, fmt.Errorf("tigris: create cache directory: %w", err)
   	}
   	return &PackCache{
   		dir:      dir,
   		maxBytes: maxBytes,
   		entries:  make(map[string]*cacheEntry),
   		suffix:   suffix,
   		observe:  observe,
   		now:      time.Now,
   	}, nil
   }

   func (c *PackCache) emit(event string, n int) {
   	if c.observe == nil {
   		return
   	}
   	for range n {
   		c.observe(event)
   	}
   }
   ```

4. `claim`: use `c.suffix` in the path, and set `e.usedAt = c.now()` on both branches.

5. Make `Get` a wrapper, and move its body to `GetChecked`:

   ```go
   func (c *PackCache) Get(id string, fetch func(io.Writer) error) (*os.File, error) {
   	return c.GetChecked(id, expectID(id, fetch))
   }

   // expectID adapts a pack fetch, whose digest is its id, to GetChecked.
   func expectID(id string, fetch func(io.Writer) error) func(io.Writer) (string, error) {
   	return func(w io.Writer) (string, error) { return id, fetch(w) }
   }

   // GetChecked is Get for ids that are not content digests: fetch writes the
   // body and returns the SHA-256 the body must have. A body that disagrees
   // is refused and not cached.
   func (c *PackCache) GetChecked(id string, fetch func(io.Writer) (wantSHA256 string, err error)) (*os.File, error) {
   	// the old Get body, with fill(e, fetch) unchanged, plus:
   	//   after claim:  if mine { c.emit("miss", 1) } else { c.emit("hit", 1) }
   }
   ```

   `fill` takes `fetch func(io.Writer) (string, error)` and passes it to `verifiedCopy`.

6. `verifiedCopy(w io.Writer, id string, fetch func(io.Writer) (string, error), progress *atomic.Int64)`: compare the computed digest with the string that `fetch` returns, not with `id`. Keep `id` for the error message. In `packindex.go` `openWholePack`, change the call to `verifiedCopy(f, id, expectID(id, s.streamPack(id)), &ps.n)`.

7. `evictLocked` returns the count of entries it removed. In `fill`, keep the count, unlock, then call `c.emit("evict_budget", n)`. The callback must run outside the lock.

8. Add `EvictIdle`:

   ```go
   // EvictIdle unlinks every settled entry that no caller claimed in the last
   // maxIdle, and reports how many it removed. It never touches a download
   // that has not settled. Open descriptors keep working, as with budget
   // eviction.
   func (c *PackCache) EvictIdle(maxIdle time.Duration) int {
   	c.mu.Lock()
   	cutoff := c.now().Add(-maxIdle)
   	n := 0
   	for id, e := range c.entries {
   		if e.size == 0 || !e.usedAt.Before(cutoff) {
   			continue
   		}
   		delete(c.entries, id)
   		c.cur -= e.size
   		os.Remove(e.path)
   		n++
   		slog.Debug("cache entry idle, uncached", "id", id, "bytes", e.size, "idle_since", e.usedAt)
   	}
   	c.mu.Unlock()
   	c.emit("evict_idle", n)
   	return n
   }
   ```

9. Update the `PackCache` doc comment. Add one paragraph: the type also serves snapshot images, through `NewSnapshotCache` and `GetChecked`.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/storage/tigris/ -run 'TestPackCache|Pack' -count=1`
Expected: PASS, including every test that existed before.

Then run the whole package once: `go test ./internal/storage/tigris/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/packcache.go internal/storage/tigris/packcache_test.go internal/storage/tigris/packindex.go
git commit -m "feat(storage/tigris): let the pack cache serve snapshot images" -m "Adds GetChecked for ids that are not digests, EvictIdle, and an event observer." -m "Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 3: Metrics

**Files:**

- Modify: `internal/metrics/metrics.go`, `docs/architecture/metrics.md`
- Test: `internal/metrics/metrics_test.go` (create it if it does not exist)

**Interfaces:**

- Produces:
  - `func ObserveSnapshot(result string, dur time.Duration, bytes int64)`: `result` is `"built"`, `"exists"`, or `"error"`. The two histograms observe only for `"built"`.
  - `func ObserveSnapshotCache(event string)`: `"hit"` and `"miss"` go to `objgit_snapshot_cache_opens_total{result}`. `"evict_budget"` and `"evict_idle"` go to `objgit_snapshot_cache_evictions_total{reason}` with `budget` and `idle`. Other events are ignored.

- [ ] **Step 1: Write the failing test**

```go
package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveSnapshot(t *testing.T) {
	tests := []struct {
		result    string
		wantHist  bool
	}{
		{"built", true},
		{"exists", false},
		{"error", false},
	}
	for _, tt := range tests {
		t.Run(tt.result, func(t *testing.T) {
			before := testutil.ToFloat64(snapshotBuilds.WithLabelValues(tt.result))
			histBefore := testutil.CollectAndCount(snapshotImageBytes)
			ObserveSnapshot(tt.result, time.Second, 1<<20)
			if got := testutil.ToFloat64(snapshotBuilds.WithLabelValues(tt.result)); got != before+1 {
				t.Errorf("builds_total{%s} = %v, want %v", tt.result, got, before+1)
			}
			_ = histBefore
		})
	}
}

func TestObserveSnapshotCache(t *testing.T) {
	tests := []struct {
		event string
		vec   string
		label string
	}{
		{"hit", "opens", "hit"},
		{"miss", "opens", "miss"},
		{"evict_budget", "evictions", "budget"},
		{"evict_idle", "evictions", "idle"},
	}
	for _, tt := range tests {
		t.Run(tt.event, func(t *testing.T) {
			c := snapshotCacheOpens.WithLabelValues(tt.label)
			if tt.vec == "evictions" {
				c = snapshotCacheEvictions.WithLabelValues(tt.label)
			}
			before := testutil.ToFloat64(c)
			ObserveSnapshotCache(tt.event)
			if got := testutil.ToFloat64(c); got != before+1 {
				t.Errorf("%s{%s} = %v, want %v", tt.vec, tt.label, got, before+1)
			}
		})
	}
}
```

Remove the unused `histBefore` lines if `go vet` complains. The histogram gating is simple enough that the code review covers it.

- [ ] **Step 2: Run the test to make sure it fails**

Run: `go test ./internal/metrics/`
Expected: FAIL with `undefined: snapshotBuilds`.

- [ ] **Step 3: Implement**

Add to the `var (...)` block in `metrics.go`, after the hook series:

```go
	snapshotBuilds = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "snapshot",
		Name:      "builds_total",
		Help:      "erofs snapshot Ensure calls by result (built, exists, error).",
	}, []string{"result"})

	snapshotDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "snapshot",
		Name:      "build_duration_seconds",
		Help:      "Time for one erofs snapshot Ensure call that built an image.",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s .. ~205s
	})

	snapshotImageBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "snapshot",
		Name:      "image_bytes",
		Help:      "Size of one built erofs snapshot image, after compression.",
		Buckets:   prometheus.ExponentialBuckets(64<<10, 4, 10), // 64 KiB .. 16 GiB
	})

	snapshotCacheOpens = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "snapshot",
		Name:      "cache_opens_total",
		Help:      "Snapshot cache lookups by result (hit, miss).",
	}, []string{"result"})

	snapshotCacheEvictions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "snapshot",
		Name:      "cache_evictions_total",
		Help:      "Snapshot cache evictions by reason (budget, idle).",
	}, []string{"reason"})
```

Add the helpers after `ObserveHook`:

```go
// ObserveSnapshot records one snapshot Ensure call. The duration and size
// histograms only describe builds, so they observe only when result is
// "built".
func ObserveSnapshot(result string, dur time.Duration, bytes int64) {
	snapshotBuilds.WithLabelValues(result).Inc()
	if result == "built" {
		snapshotDuration.Observe(dur.Seconds())
		snapshotImageBytes.Observe(float64(bytes))
	}
}

// ObserveSnapshotCache records one snapshot cache event. Its signature
// matches the observer tigris.NewSnapshotCache takes.
func ObserveSnapshotCache(event string) {
	switch event {
	case "hit", "miss":
		snapshotCacheOpens.WithLabelValues(event).Inc()
	case "evict_budget":
		snapshotCacheEvictions.WithLabelValues("budget").Inc()
	case "evict_idle":
		snapshotCacheEvictions.WithLabelValues("idle").Inc()
	}
}
```

- [ ] **Step 4: Run the test**

Run: `go test ./internal/metrics/ -count=1`
Expected: PASS.

- [ ] **Step 5: Document**

Invoke the `simple-english` skill. In `docs/architecture/metrics.md`:

- Add `ObserveSnapshot` and `ObserveSnapshotCache` to the helper table.
- Add a section `## Snapshots` with a table of the five series (type, labels, meaning), copied from the spec's Metrics table.
- Add one sentence: the size histogram is the data for a later decision about a size limit.

- [ ] **Step 6: Commit**

```bash
git add internal/metrics/ docs/architecture/metrics.md
git commit -m "feat(metrics): add erofs snapshot series" -m "Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 4: `*tigris.Storer` implements `snapshot.Store`

**Files:**

- Create: `internal/storage/tigris/snapshot.go`, `internal/storage/tigris/snapshot_test.go`
- Modify: `internal/storage/tigris/tigris.go` (one field, one option, one line in the `Scoped` doc comment)

**Interfaces:**

- Consumes: `snapshot.Store`, `snapshot.File`, `snapshot.CacheID`, `snapshot.MetaSHA256` (Task 0); `(*PackCache).GetChecked`, `NewSnapshotCache` (Task 2).
- Produces: `func WithSnapshotCache(c *PackCache) Option`; `*Storer` satisfies `snapshot.Store`.

- [ ] **Step 1: Write the failing tests `snapshot_test.go`**

Read `client_test.go` first. Reuse `newFakeS3`, `newTestStorer`, and `countingObserver` (read its doc comment for the return shape). Tests:

```go
package tigris

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/tigrisdata/objgit/internal/snapshot"
)

func putTestSnapshot(t *testing.T, s *Storer, key string, body []byte, sum string) {
	t.Helper()
	meta := map[string]string{snapshot.MetaFormat: "1"}
	if sum != "" {
		meta[snapshot.MetaSHA256] = sum
	}
	if err := s.PutSnapshot(context.Background(), key, bytes.NewReader(body), int64(len(body)), meta); err != nil {
		t.Fatalf("PutSnapshot: %v", err)
	}
}

func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func TestPutSnapshotKeyAndMetadata(t *testing.T) {
	f := newFakeS3(t)
	s := newTestStorer(t, f).Scoped("acme/widgets")
	body := []byte("image")
	putTestSnapshot(t, s, "snapshots/erofs/v1/abc.erofs", body, digest(body))

	o, ok := f.objs["acme/widgets/snapshots/erofs/v1/abc.erofs"]
	if !ok {
		t.Fatalf("no object under the scoped key; have %v", keysOf(f))
	}
	if !bytes.Equal(o.body, body) || o.meta[snapshot.MetaSHA256] != digest(body) {
		t.Errorf("stored %q with meta %v", o.body, o.meta)
	}
}

func TestStatSnapshot(t *testing.T) {
	tests := []struct {
		name    string
		put     bool
		wantErr error
	}{
		{"present", true, nil},
		{"absent", false, fs.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStorer(t, newFakeS3(t))
			if tt.put {
				putTestSnapshot(t, s, "k.erofs", []byte("12345"), digest([]byte("12345")))
			}
			size, err := s.StatSnapshot(context.Background(), "k.erofs")
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil || size != 5 {
				t.Fatalf("StatSnapshot = %d, %v; want 5, nil", size, err)
			}
		})
	}
}

func TestOpenSnapshot(t *testing.T) {
	body := []byte("an erofs image, pretend")
	tests := []struct {
		name    string
		cache   bool
		sum     string
		put     bool
		wantErr error // nil means success; errAny means any error
	}{
		{"cached", true, digest(body), true, nil},
		{"no cache", false, digest(body), true, nil},
		{"bad digest", true, digest([]byte("other")), true, errAny},
		{"bad digest no cache", false, digest([]byte("other")), true, errAny},
		{"missing digest", true, "", true, errAny},
		{"absent", true, "", false, fs.ErrNotExist},
		{"absent no cache", false, "", false, fs.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []Option
			var c *PackCache
			if tt.cache {
				var err error
				c, err = NewSnapshotCache(t.TempDir(), 1<<20, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { c.Cleanup() })
				opts = append(opts, WithSnapshotCache(c))
			}
			s := newTestStorer(t, newFakeS3(t), opts...)
			if tt.put {
				putTestSnapshot(t, s, "snapshots/erofs/v1/abc.erofs", body, tt.sum)
			}
			f, err := s.OpenSnapshot(context.Background(), "snapshots/erofs/v1/abc.erofs")
			switch {
			case tt.wantErr == errAny:
				if err == nil {
					f.Close()
					t.Fatal("OpenSnapshot succeeded, want an error")
				}
				if c != nil && countCacheFiles(t, c) != 0 {
					t.Error("the cache kept a file after a failed download")
				}
				return
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			case err != nil:
				t.Fatalf("OpenSnapshot: %v", err)
			}
			defer f.Close()
			got := make([]byte, len(body))
			if _, err := f.ReadAt(got, 0); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("ReadAt: %v", err)
			}
			if !bytes.Equal(got, body) {
				t.Errorf("read %q, want %q", got, body)
			}
		})
	}
}

var errAny = errors.New("any error")

func TestOpenSnapshotCachesDownload(t *testing.T) {
	c, err := NewSnapshotCache(t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Cleanup() })
	f := newFakeS3(t)
	obs, counts := countingObserver()
	base := newTestStorer(t, f, WithSnapshotCache(c), obs)
	widgets, gadgets := base.Scoped("acme/widgets"), base.Scoped("acme/gadgets")

	body := []byte("shared tree image")
	putTestSnapshot(t, widgets, "snapshots/erofs/v1/abc.erofs", body, digest(body))
	putTestSnapshot(t, gadgets, "snapshots/erofs/v1/abc.erofs", body, digest(body))

	for _, s := range []*Storer{widgets, widgets, gadgets} {
		sf, err := s.OpenSnapshot(context.Background(), "snapshots/erofs/v1/abc.erofs")
		if err != nil {
			t.Fatalf("OpenSnapshot: %v", err)
		}
		sf.Close()
	}
	if got := counts()["GetObject"]; got != 1 {
		t.Errorf("GetObject calls = %d, want 1", got)
	}
}

func TestSnapshotKeysInvisible(t *testing.T) {
	s := newTestStorer(t, newFakeS3(t))
	body := []byte("x")
	putTestSnapshot(t, s, "snapshots/erofs/v1/abc.erofs", body, digest(body))

	refs, err := s.IterReferences()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	_ = refs.ForEach(func(*plumbing.Reference) error { n++; return nil })
	if n != 0 {
		t.Errorf("IterReferences returned %d refs, want 0", n)
	}
	objs, err := s.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		t.Fatal(err)
	}
	n = 0
	_ = objs.ForEach(func(plumbing.EncodedObject) error { n++; return nil })
	if n != 0 {
		t.Errorf("IterEncodedObjects returned %d objects, want 0", n)
	}
}
```

Adapt `countingObserver` usage to its real return shape. If no `keysOf` helper exists, build the key list inline for the error message. Add the `plumbing` import.

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `go test ./internal/storage/tigris/ -run 'Snapshot'`
Expected: FAIL with `s.PutSnapshot undefined` and `undefined: WithSnapshotCache`.

- [ ] **Step 3: Implement**

In `tigris.go`, add a field to `Storer` after `cache`:

```go
	snapCache  *PackCache // optional process-wide snapshot image cache; see snapshot.go
```

Add the option after `WithPackCache`:

```go
// WithSnapshotCache installs the local cache that OpenSnapshot downloads
// images into. Like WithPackCache, pass one cache built once per process —
// see NewSnapshotCache. Scoped shares it.
func WithSnapshotCache(c *PackCache) Option {
	return func(s *Storer) { s.snapCache = c }
}
```

In the `Scoped` doc comment, change "Two things are shared on purpose" to "Three things are shared on purpose", and add the snapshot cache, for the same reason as the pack cache.

Create `snapshot.go`:

```go
package tigris

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tigrisdata/objgit/internal/snapshot"
)

// Storer holds the snapshot images of its repository, next to the git data.
var _ snapshot.Store = (*Storer)(nil)

// StatSnapshot sends one HeadObject for key under this Storer's prefix.
func (s *Storer) StatSnapshot(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + key),
	})
	s.observe("HeadObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return 0, fmt.Errorf("tigris: snapshot %s: %w", key, fs.ErrNotExist)
	default:
		return 0, fmt.Errorf("tigris: head snapshot %s: %w", key, err)
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

// PutSnapshot sends one PutObject. It does not use the upload queue: no ref
// waits for an image, so there is nothing for a flush to order.
func (s *Storer) PutSnapshot(ctx context.Context, key string, body io.ReadSeeker, size int64, meta map[string]string) error {
	start := time.Now()
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        sp(s.bucket),
		Key:           sp(s.prefix + key),
		Body:          body,
		ContentLength: &size,
		ContentType:   sp("application/octet-stream"),
		Metadata:      meta,
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: put snapshot %s: %w", key, err)
	}
	return nil
}

// OpenSnapshot returns a local copy of the image at key. With a snapshot
// cache, the copy lives in the cache and later calls, from any repository
// that holds the same tree, open it with no request. Without one, the copy is
// a private unlinked temp file, as openWholePack does for packs.
//
// The body must match its erofs-sha256 metadata, so a bad download is never
// served and never cached.
func (s *Storer) OpenSnapshot(ctx context.Context, key string) (snapshot.File, error) {
	fetch := s.snapshotFetch(ctx, key)
	if s.snapCache != nil {
		f, err := s.snapCache.GetChecked(snapshot.CacheID(key), fetch)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	slog.Debug("no snapshot cache installed, downloading to a private temp file", "key", key)
	f, err := os.CreateTemp("", "objgit-snapshot-*")
	if err != nil {
		return nil, fmt.Errorf("tigris: create snapshot temp file: %w", err)
	}
	os.Remove(f.Name()) // the descriptor keeps the data until Close

	h := sha256.New()
	want, err := fetch(io.MultiWriter(f, h))
	if err == nil {
		if got := hex.EncodeToString(h.Sum(nil)); got != want {
			err = fmt.Errorf("tigris: snapshot %s failed checksum (got %s, want %s)", key, got, want)
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// snapshotFetch returns the GetChecked fetch for key: it copies the body and
// returns the digest from the object's erofs-sha256 metadata.
func (s *Storer) snapshotFetch(ctx context.Context, key string) func(io.Writer) (string, error) {
	return func(w io.Writer) (string, error) {
		start := time.Now()
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: sp(s.bucket),
			Key:    sp(s.prefix + key),
		})
		s.observe("GetObject", start, err)
		switch {
		case err == nil:
		case isNotFound(err):
			return "", fmt.Errorf("tigris: snapshot %s: %w", key, fs.ErrNotExist)
		default:
			return "", fmt.Errorf("tigris: get snapshot %s: %w", key, err)
		}
		defer out.Body.Close()
		want := out.Metadata[snapshot.MetaSHA256]
		if want == "" {
			return "", fmt.Errorf("tigris: snapshot %s has no %s metadata", key, snapshot.MetaSHA256)
		}
		if _, err := io.Copy(w, out.Body); err != nil {
			return "", fmt.Errorf("tigris: download snapshot %s: %w", key, err)
		}
		return want, nil
	}
}
```

`sp` is in `tigris.go`. `ip` exists only in `client_test.go`, so production code takes the address of `size` directly.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/storage/tigris/ -count=1`
Expected: PASS for the whole package.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/snapshot.go internal/storage/tigris/snapshot_test.go internal/storage/tigris/tigris.go
git commit -m "feat(storage/tigris): store erofs snapshots next to the repository" -m "Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 5: The push path

**Files:**

- Create: `cmd/objgitd/snapshots.go`, `cmd/objgitd/snapshots_test.go`
- Modify: `cmd/objgitd/hooks.go` (`snapshotRefs`, `receivePack`, `runHooks`), `cmd/objgitd/git_protocol.go` (daemon fields), `cmd/objgitd/hooks_test.go` (one new test)

**Interfaces:**

- Consumes: `snapshot.Ensure`, `snapshot.Open`, `snapshot.Store`, `snapshot.NewMemStore`, `snapshot.Key`, `snapshot.StatusBuilt`, `snapshot.StatusExists` (Task 1); `metrics.ObserveSnapshot` (Task 3).
- Produces: daemon fields `snapshots bool`, `snapshotTimeout time.Duration`, `snapshotTmpDir string`; `func (d *daemon) runSnapshots(repoPath string, st storage.Storer, updates []refUpdate, progress io.Writer)`; `func peelToTree(st storer.EncodedObjectStorer, h plumbing.Hash) (plumbing.Hash, bool, error)`.

- [ ] **Step 1: Write the failing tests `snapshots_test.go`**

Read `hooks_test.go` and `http_test.go` first: `runGit`, `tryGit`, `writeFile`, `memBase`, and how the hook tests capture logs (`syncBuffer`). Then write:

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
	"github.com/tigrisdata/objgit/internal/snapshot"
)

// snapStorer is an in-memory storer that also holds snapshot images, the
// way *tigris.Storer does.
type snapStorer struct {
	*memory.Storage
	*snapshot.MemStore
}

// snapBase is memBase with a snapshot store per repository.
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
		t.Skip("git not on PATH")
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
		t.Skip("git not on PATH")
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

// writeCommit stores a commit with a one-file tree and returns its hash.
func writeCommit(t *testing.T, st *memory.Storage) plumbing.Hash {
	t.Helper()
	blob := st.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, _ := blob.Writer()
	w.Write([]byte("hi\n"))
	w.Close()
	bh, err := st.SetEncodedObject(blob)
	if err != nil {
		t.Fatal(err)
	}
	tree := &object.Tree{Entries: []object.TreeEntry{{Name: "a", Mode: 0o100644, Hash: bh}}}
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
```

Use the real `filemode.Regular` constant in `writeCommit` instead of `0o100644` if the literal does not compile. If `runGit` does not return the output, use `exec.Command("git", "-C", work, "rev-parse", "HEAD^{tree}").Output()`.

Add `TestHooksIgnoreTags` to `hooks_test.go`, next to the existing HTTP hook test. Copy its setup (the log capture, the daemon with `allowHooks: true`, and the hook script). Then:

1. Push `main` and make sure that the logs contain `hook: running` once.
2. Run `git tag -a v1 -m v1`, and push `v1` only.
3. Make sure that the count of `hook: running` in the logs is still 1.

- [ ] **Step 2: Run the tests to make sure they fail**

Run: `go test ./cmd/objgitd/ -run 'Snapshot|PeelToTree|HooksIgnoreTags'`
Expected: FAIL to compile, with `unknown field snapshots` and `undefined: peelToTree`.

- [ ] **Step 3: Add the daemon fields in `git_protocol.go`**

After `hookTimeout`:

```go
	// snapshots gates building an erofs image of each updated ref tip after
	// a push; see snapshots.go.
	snapshots       bool
	snapshotTimeout time.Duration
	snapshotTmpDir  string // parent directory for image temp files; "" is the OS temp dir
```

- [ ] **Step 4: Write `snapshots.go`**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/snapshot"
)

// maxTagDepth bounds how many annotated tags peelToTree follows.
const maxTagDepth = 16

// runSnapshots makes sure that an erofs image exists for the tree at the tip
// of each updated ref. It runs synchronously inside onUpdated, so the push
// waits for it, and it writes one progress line per ref. A failure is logged
// and counted and never fails the push: the refs are already committed.
func (d *daemon) runSnapshots(repoPath string, st storage.Storer, updates []refUpdate, progress io.Writer) {
	store, ok := st.(snapshot.Store)
	if !ok {
		slog.Debug("snapshot: storer holds no snapshot store, skipping", "repo", repoPath)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.snapshotTimeout)
	defer cancel()

	// diffRefs ranges over a map, so its order is random. Branches go first,
	// then tags, each by name, so the ref that builds a shared tree (and the
	// progress output) is the same on every push.
	ordered := slices.Clone(updates)
	slices.SortFunc(ordered, func(a, b refUpdate) int {
		if a.Name.IsBranch() != b.Name.IsBranch() {
			if a.Name.IsBranch() {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name.String(), b.Name.String())
	})

	// A branch and a tag on one commit share one tree, and one build.
	seen := map[plumbing.Hash]string{}
	for _, u := range ordered {
		if u.New.IsZero() {
			continue
		}
		log := slog.With("repo", repoPath, "ref", u.Name.String(), "sha", u.New.String())

		tree, ok, err := peelToTree(st, u.New)
		if err != nil {
			log.Error("snapshot: resolve tree", "err", err)
			metrics.ObserveSnapshot("error", 0, 0)
			continue
		}
		if !ok {
			log.Debug("snapshot: ref does not point at a commit, skipping")
			continue
		}
		if status, dup := seen[tree]; dup {
			if status == snapshot.StatusBuilt {
				status = snapshot.StatusExists
			}
			writeSnapshotLine(progress, u.Name, tree, status)
			continue
		}

		res, err := snapshot.Ensure(ctx, st, store, tree, d.snapshotTmpDir)
		if err != nil {
			log.Error("snapshot: build failed", "tree", tree.String(), "dur", res.Elapsed, "err", err)
			metrics.ObserveSnapshot("error", res.Elapsed, 0)
			seen[tree] = "failed"
			writeSnapshotLine(progress, u.Name, tree, "failed")
			continue
		}
		metrics.ObserveSnapshot(res.Status, res.Elapsed, res.Bytes)
		seen[tree] = res.Status
		if res.Status == snapshot.StatusBuilt {
			log.Info("snapshot: built", "key", res.Key, "files", res.Files, "bytes", res.Bytes, "dur", res.Elapsed)
			writeSnapshotLine(progress, u.Name, tree, fmt.Sprintf("built, %d files, %s, %s",
				res.Files, humanBytes(res.Bytes), res.Elapsed.Round(100*time.Millisecond)))
			continue
		}
		writeSnapshotLine(progress, u.Name, tree, res.Status)
	}
}

// writeSnapshotLine writes one "remote: objgit: snapshot ..." line. It never
// carries error text, which can name bucket internals; the log has that.
func writeSnapshotLine(progress io.Writer, ref plumbing.ReferenceName, tree plumbing.Hash, status string) {
	if progress == nil {
		return
	}
	fmt.Fprintf(progress, "objgit: snapshot %s (tree %s): %s\n", ref, tree.String()[:7], status)
}

// peelToTree returns the tree of the commit that h names, following annotated
// tags. ok is false, with no error, when h does not lead to a commit (a tag of
// a tree or a blob, or a tree itself), since there is then nothing to snapshot.
func peelToTree(st storer.EncodedObjectStorer, h plumbing.Hash) (plumbing.Hash, bool, error) {
	for range maxTagDepth {
		obj, err := st.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			return plumbing.ZeroHash, false, fmt.Errorf("load %s: %w", h, err)
		}
		switch obj.Type() {
		case plumbing.CommitObject:
			c, err := object.DecodeCommit(st, obj)
			if err != nil {
				return plumbing.ZeroHash, false, fmt.Errorf("decode commit %s: %w", h, err)
			}
			return c.TreeHash, true, nil
		case plumbing.TagObject:
			tag, err := object.DecodeTag(st, obj)
			if err != nil {
				return plumbing.ZeroHash, false, fmt.Errorf("decode tag %s: %w", h, err)
			}
			h = tag.Target
		default:
			return plumbing.ZeroHash, false, nil
		}
	}
	return plumbing.ZeroHash, false, errors.New("tag chain is too deep")
}

// humanBytes formats n in binary units with one decimal, e.g. "5.6 MiB".
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
```

- [ ] **Step 5: Change `hooks.go`**

1. `snapshotRefs`: keep branches **and** tags:

   ```go
   		if r.Type() == plumbing.HashReference && (r.Name().IsBranch() || r.Name().IsTag()) {
   ```

   Update its doc comment: it returns every branch and tag ref. `runHooks` filters to branches, and snapshots use both.

2. `receivePack`: take the before snapshot, and set `onUpdated`, when hooks or snapshots are on:

   ```go
   	if !d.allowHooks && !d.snapshots {
   		err := receivePackStreaming(ctx, st, r, w, req, d.pushes.admit, nil)
   		d.healHEADAfterPush(err, st, repoPath)
   		return err
   	}
   ```

   Change the log messages that start with `hook: ref snapshot` to start with `push: ref snapshot`, because they now serve both features. In `onUpdated`, replace the `d.runHooks(...)` call with:

   ```go
   		if d.snapshots {
   			d.runSnapshots(repoPath, st, updates, progress)
   		}
   		if d.allowHooks {
   			d.runHooks(repoPath, "receive-pack", st, updates, progress)
   		}
   ```

   Update the doc comment of `receivePack` to name both features.

3. `runHooks`: skip every update that is not a branch:

   ```go
   		if u.New.IsZero() || !u.Name.IsBranch() {
   			continue // deletion, or a tag: hooks run for branches only
   		}
   ```

- [ ] **Step 6: Run the tests**

Run: `go test ./cmd/objgitd/ -count=1`
Expected: PASS for the whole package, including the hook tests that existed before.

- [ ] **Step 7: Commit**

```bash
git add cmd/objgitd/snapshots.go cmd/objgitd/snapshots_test.go cmd/objgitd/hooks.go cmd/objgitd/hooks_test.go cmd/objgitd/git_protocol.go
git commit -m "feat(objgitd): build erofs snapshots of pushed ref tips" -m "Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 6: `main.go` wiring

**Files:**

- Modify: `cmd/objgitd/main.go`

**Interfaces:**

- Consumes: `tigris.NewSnapshotCache`, `tigris.WithSnapshotCache`, `(*PackCache).EvictIdle`, `(*PackCache).Cleanup` (Tasks 2, 4); `metrics.ObserveSnapshotCache` (Task 3); daemon fields (Task 5).

- [ ] **Step 1: Add the flags**, after `packCacheBytes`:

```go
	erofsSnapshots       = flag.Bool("erofs-snapshots", false, "build a zstd-compressed erofs image of the tree at each updated branch and tag tip after a push, and store it next to the repository")
	snapshotTimeout      = flag.Duration("snapshot-timeout", 2*time.Minute, "wall-clock limit for the erofs snapshots of one push")
	snapshotCacheBytes   = flag.Int64("snapshot-cache-bytes", 2<<30, "disk budget for the local erofs snapshot cache, least-recently-used eviction; 0 disables caching")
	snapshotCacheMaxIdle = flag.Duration("snapshot-cache-max-idle", time.Hour, "evict a cached erofs snapshot that nobody opened for this long; 0 disables the idle sweep")
```

- [ ] **Step 2: Build the cache**, after the pack cache block:

```go
	// The snapshot cache sits next to the pack cache, under the same parent,
	// with its own budget. Open downloads whole images into it.
	var snapCache *tigris.PackCache
	if *snapshotCacheBytes > 0 {
		snapCache, err = tigris.NewSnapshotCache(*packCacheDir, *snapshotCacheBytes, metrics.ObserveSnapshotCache)
		if err != nil {
			slog.Error("can't create snapshot cache", "pack_cache_dir", *packCacheDir, "err", err)
			os.Exit(1)
		}
		storerOpts = append(storerOpts, tigris.WithSnapshotCache(snapCache))
	}
```

- [ ] **Step 3: Set the daemon fields**:

```go
		snapshots:       *erofsSnapshots,
		snapshotTimeout: *snapshotTimeout,
		snapshotTmpDir:  *packCacheDir,
```

Add `"erofs_snapshots", *erofsSnapshots` and `"snapshot_cache_bytes", *snapshotCacheBytes` to the `objgitd listening` log line.

- [ ] **Step 4: Start the idle sweep**, after `g, gCtx := errgroup.WithContext(ctx)`:

```go
	if snapCache != nil && *snapshotCacheMaxIdle > 0 {
		g.Go(func() error {
			tick := time.NewTicker(*snapshotCacheMaxIdle / 4)
			defer tick.Stop()
			for {
				select {
				case <-gCtx.Done():
					return nil
				case <-tick.C:
					if n := snapCache.EvictIdle(*snapshotCacheMaxIdle); n > 0 {
						slog.Debug("evicted idle snapshots", "count", n)
					}
				}
			}
		})
	}
```

- [ ] **Step 5: Clean up at shutdown**, next to the pack cache cleanup:

```go
	if cerr := snapCache.Cleanup(); cerr != nil {
		slog.Warn("can't remove the snapshot cache directory", "err", cerr)
	}
```

`Cleanup` accepts a nil receiver, so this is safe when the cache is off.

- [ ] **Step 6: Build and vet**

Run: `go build ./... && go vet ./cmd/objgitd/`
Expected: success.

Run: `go run ./cmd/objgitd -h 2>&1 | grep -E 'erofs-snapshots|snapshot-'`
Expected: the four new flags are listed.

- [ ] **Step 7: Commit**

```bash
git add cmd/objgitd/main.go
git commit -m "feat(objgitd): wire erofs snapshot flags and cache" -m "Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 7: Documentation

**Files:**

- Create: `docs/architecture/snapshots.md`
- Modify: `docs/architecture/tigris-storer.md`, `docs/architecture/README.md`, `AGENTS.md`

Invoke the `simple-english` skill before you write. Copy facts from the spec. Do not add facts that are not in the spec or the code.

- [ ] **Step 1: Write `docs/architecture/snapshots.md`**

Sections, in this order. Each one is descriptive text, with tables where the spec has tables:

1. `# erofs snapshots (internal/snapshot)`: what the feature does, in three sentences. Name the flag `-erofs-snapshots`, which is off by default.
2. `## Object layout`: the key, why it is the tree hash, the `v1` rule, and the metadata table.
3. `## The mapping from git to EROFS`: the mode table, the gitlink rule, and the two limits table.
4. `## Ensure`: the steps, and why it takes no lock.
5. `## Open and the snapshot cache`: the local download, `GetChecked`, the budget, the idle sweep, the fallback without a cache, and why a shared cache id is not leakage.
6. `## The push path`: `runSnapshots`, the order (snapshots, then hooks), the progress lines, and the rule that a failure never fails a push.
7. `## Flags`: the flag table from the spec.
8. `## Known risks`: copy the list from the spec.
9. `## The path to a message queue`: copy from the spec.

- [ ] **Step 2: Update the other pages**

- `docs/architecture/tigris-storer.md`:
  - Add `snapshots/erofs/v1/<tree>.erofs` to the layout table.
  - In "The pack cache", add a paragraph about the second instance: `NewSnapshotCache`, `GetChecked`, `EvictIdle`, and the observer. Link to `snapshots.md`.
- `docs/architecture/README.md`: add a `snapshots.md` row to the pages table.
- `AGENTS.md`:
  - Add `internal/snapshot` to "Where the code lives".
  - Add `cmd/objgitd/snapshots.go` to the same table.
  - Add a `snapshots.md` row to the architecture table: "Read it before you change snapshot images, the snapshot cache, or `runSnapshots`."

- [ ] **Step 3: Self-check**

Run the self-check of the `simple-english` skill on every changed paragraph: sentence length, no `should`, no semicolons, and conditions before commands.

- [ ] **Step 4: Commit**

```bash
git add docs/architecture/snapshots.md docs/architecture/tigris-storer.md docs/architecture/README.md AGENTS.md
git commit -m "docs: describe erofs snapshots and the snapshot cache" -m "Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 8: Whole-branch check (the controller does this)

- [ ] **Step 1: Full test suite**

Run: `go vet ./... && go test ./... -count=1`
Expected: PASS.

- [ ] **Step 2: Whole-branch review**

Dispatch one reviewer on `git diff origin/main...HEAD` against the spec. Fix the findings, then run Step 1 again.
