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
