package treefs

import (
	"errors"
	"io"
	"os"
	"sort"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
)

// buildTree writes a small tree (a regular file, a nested directory, and an
// executable script) directly into an in-memory object store and returns its
// root tree. Building objects by hand avoids depending on the host git config
// (e.g. commit.gpgSign) and pins the file modes precisely.
func buildTree(t *testing.T) *object.Tree {
	t.Helper()
	store := memory.NewStorage()

	readme := putBlob(t, store, "hello\n")
	nested := putBlob(t, store, "deep\n")
	run := putBlob(t, store, "#!/bin/sh\necho hi\n")

	dir := putTree(t, store, []object.TreeEntry{
		{Name: "nested.txt", Mode: filemode.Regular, Hash: nested},
	})
	root := putTree(t, store, []object.TreeEntry{
		{Name: "README.md", Mode: filemode.Regular, Hash: readme},
		{Name: "dir", Mode: filemode.Dir, Hash: dir},
		{Name: "run.sh", Mode: filemode.Executable, Hash: run},
	})

	tree, err := object.GetTree(store, root)
	if err != nil {
		t.Fatalf("get tree: %v", err)
	}
	return tree
}

func putBlob(t *testing.T, store storage.Storer, data string) plumbing.Hash {
	t.Helper()
	o := store.NewEncodedObject()
	o.SetType(plumbing.BlobObject)
	w, err := o.Writer()
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	if _, err := io.WriteString(w, data); err != nil {
		t.Fatalf("blob write: %v", err)
	}
	_ = w.Close()
	h, err := store.SetEncodedObject(o)
	if err != nil {
		t.Fatalf("set blob: %v", err)
	}
	return h
}

func putTree(t *testing.T, store storage.Storer, entries []object.TreeEntry) plumbing.Hash {
	t.Helper()
	sort.Sort(object.TreeEntrySorter(entries))
	tree := &object.Tree{Entries: entries}
	o := store.NewEncodedObject()
	if err := tree.Encode(o); err != nil {
		t.Fatalf("encode tree: %v", err)
	}
	h, err := store.SetEncodedObject(o)
	if err != nil {
		t.Fatalf("set tree: %v", err)
	}
	return h
}

func TestOpenReadsFileContents(t *testing.T) {
	fs := New(buildTree(t))

	for _, tc := range []struct {
		path string
		want string
	}{
		{"README.md", "hello\n"},
		{"/README.md", "hello\n"},
		{"dir/nested.txt", "deep\n"},
	} {
		f, err := fs.Open(tc.path)
		if err != nil {
			t.Fatalf("open %q: %v", tc.path, err)
		}
		data, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			t.Fatalf("read %q: %v", tc.path, err)
		}
		if string(data) != tc.want {
			t.Errorf("open %q = %q, want %q", tc.path, data, tc.want)
		}
	}
}

func TestStatModesAndSize(t *testing.T) {
	fs := New(buildTree(t))

	info, err := fs.Stat("README.md")
	if err != nil {
		t.Fatalf("stat README.md: %v", err)
	}
	if info.IsDir() {
		t.Error("README.md reported as dir")
	}
	if info.Size() != int64(len("hello\n")) {
		t.Errorf("README.md size = %d, want %d", info.Size(), len("hello\n"))
	}

	dirInfo, err := fs.Stat("dir")
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !dirInfo.IsDir() {
		t.Error("dir not reported as dir")
	}

	exec, err := fs.Stat("run.sh")
	if err != nil {
		t.Fatalf("stat run.sh: %v", err)
	}
	if exec.Mode()&0o111 == 0 {
		t.Errorf("run.sh mode = %v, want executable bit set", exec.Mode())
	}
}

func TestReadDir(t *testing.T) {
	fs := New(buildTree(t))

	entries, err := fs.ReadDir("/")
	if err != nil {
		t.Fatalf("readdir root: %v", err)
	}
	got := map[string]bool{}
	for _, e := range entries {
		got[e.Name()] = e.IsDir()
	}
	if _, ok := got["README.md"]; !ok {
		t.Error("root listing missing README.md")
	}
	if isDir, ok := got["dir"]; !ok || !isDir {
		t.Errorf("root listing missing dir/ (got %v)", got)
	}

	nested, err := fs.ReadDir("dir")
	if err != nil {
		t.Fatalf("readdir dir: %v", err)
	}
	if len(nested) != 1 || nested[0].Name() != "nested.txt" {
		t.Errorf("dir listing = %v, want [nested.txt]", nested)
	}
}

func TestMissingPaths(t *testing.T) {
	fs := New(buildTree(t))

	if _, err := fs.Open("does-not-exist"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("open missing = %v, want ErrNotExist", err)
	}
	if _, err := fs.Stat("nope/also-nope"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stat missing = %v, want ErrNotExist", err)
	}
}

func TestWritesRejected(t *testing.T) {
	fs := New(buildTree(t))

	if _, err := fs.Create("new.txt"); !errors.Is(err, billy.ErrReadOnly) {
		t.Errorf("Create = %v, want ErrReadOnly", err)
	}
	if _, err := fs.OpenFile("README.md", os.O_WRONLY, 0); !errors.Is(err, billy.ErrReadOnly) {
		t.Errorf("OpenFile(write) = %v, want ErrReadOnly", err)
	}
	if err := fs.Remove("README.md"); !errors.Is(err, billy.ErrReadOnly) {
		t.Errorf("Remove = %v, want ErrReadOnly", err)
	}

	// A read-opened handle must still refuse writes.
	f, err := fs.Open("README.md")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("x")); !errors.Is(err, billy.ErrReadOnly) {
		t.Errorf("file.Write = %v, want ErrReadOnly", err)
	}
}
