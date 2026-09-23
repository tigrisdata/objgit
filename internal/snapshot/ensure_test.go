package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
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

func TestEnsureResolvesLFSPointers(t *testing.T) {
	content := []byte("actual LFS file contents\n")
	digest := sha256.Sum256(content)
	oid := hex.EncodeToString(digest[:])
	pointer := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, len(content))
	tests := []struct {
		name    string
		blob    string
		mode    filemode.FileMode
		opener  bool
		openErr error
		want    string
		wantErr string
	}{
		{name: "regular pointer", blob: pointer, mode: filemode.Regular, opener: true, want: string(content)},
		{name: "executable pointer", blob: pointer, mode: filemode.Executable, opener: true, want: string(content)},
		{name: "ordinary file", blob: "ordinary\n", mode: filemode.Regular, want: "ordinary\n"},
		{name: "missing object store", blob: pointer, mode: filemode.Regular, wantErr: "no object store"},
		{name: "missing LFS object", blob: pointer, mode: filemode.Regular, opener: true, openErr: fs.ErrNotExist, wantErr: "file does not exist"},
		{name: "malformed size", blob: strings.Replace(pointer, fmt.Sprintf("size %d", len(content)), "size -1", 1), mode: filemode.Regular, opener: true, wantErr: "invalid LFS object size"},
		{name: "extension", blob: strings.Replace(pointer, "oid sha256:", "ext-0-foo sha256:"+oid+"\noid sha256:", 1), mode: filemode.Regular, opener: true, wantErr: "unsupported or malformed LFS pointer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.NewStorage()
			tree := buildTree(t, st, []fixture{{"file.bin", tt.mode, tt.blob}})
			store := NewMemStore()
			var open LFSOpener
			if tt.opener {
				open = func(_ context.Context, gotOID string, gotSize int64) (io.ReadCloser, error) {
					if gotOID != oid || gotSize != int64(len(content)) {
						t.Errorf("open LFS %s, %d; want %s, %d", gotOID, gotSize, oid, len(content))
					}
					if tt.openErr != nil {
						return nil, tt.openErr
					}
					return io.NopCloser(bytes.NewReader(content)), nil
				}
			}
			_, err := EnsureWithLFS(context.Background(), st, store, tree, t.TempDir(), open)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("EnsureWithLFS error = %v, want %q", err, tt.wantErr)
				}
				if store.PutCount() != 0 {
					t.Error("stored an incomplete image")
				}
				return
			}
			if err != nil {
				t.Fatalf("EnsureWithLFS: %v", err)
			}
			snap, err := Open(context.Background(), store, tree)
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			got, err := fs.ReadFile(snap, "file.bin")
			if err != nil || string(got) != tt.want {
				t.Errorf("file.bin = %q, %v; want %q", got, err, tt.want)
			}
			if tt.mode == filemode.Executable {
				info, err := fs.Stat(snap, "file.bin")
				if err != nil || info.Mode().Perm() != 0o755 {
					t.Errorf("file.bin mode = %v, %v; want 0755", info, err)
				}
			}
		})
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
	want := map[string]string{MetaFormat: "2", MetaTree: tree.String(), MetaFiles: "2"}
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
