// Package treefs exposes a git tree (a commit's contents at a single ref) as a
// read-only billy.Filesystem. Nothing is copied up front: directory listings
// read the in-memory tree object, and a file's bytes are fetched from the
// object store only when it is opened. This lets a hook see a checkout of the
// pushed commit without materializing the whole tree.
//
// Every mutating operation returns billy.ErrReadOnly.
package treefs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// FS is a read-only billy.Filesystem backed by a git tree.
type FS struct {
	tree *object.Tree
}

// New returns a filesystem serving the contents of tree.
func New(tree *object.Tree) *FS {
	return &FS{tree: tree}
}

// rel normalizes a billy path into a tree-relative path. The empty string and
// "." denote the tree root.
func rel(p string) string {
	p = path.Clean("/" + p)
	return p[1:] // strip leading slash; root becomes ""
}

func notExist(op, name string) error {
	return &os.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
}

func (f *FS) Open(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDONLY, 0)
}

func (f *FS) OpenFile(filename string, flag int, _ fs.FileMode) (billy.File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC) != 0 {
		return nil, billy.ErrReadOnly
	}
	r := rel(filename)
	if r == "" {
		return nil, &os.PathError{Op: "open", Path: filename, Err: fmt.Errorf("is a directory")}
	}
	file, err := f.tree.File(r)
	if err != nil {
		return nil, notExist("open", filename)
	}
	rc, err := file.Reader()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return newFile(path.Base(r), data, fileInfo{name: path.Base(r), size: int64(len(data)), mode: osMode(file.Mode)}), nil
}

func (f *FS) Stat(filename string) (fs.FileInfo, error) {
	r := rel(filename)
	if r == "" {
		return fileInfo{name: "/", mode: fs.ModeDir | 0o755}, nil
	}
	entry, err := f.tree.FindEntry(r)
	if err != nil {
		return nil, notExist("stat", filename)
	}
	info := fileInfo{name: path.Base(r), mode: osMode(entry.Mode)}
	if entry.Mode != filemode.Dir {
		if size, err := f.tree.Size(r); err == nil {
			info.size = size
		}
	}
	return info, nil
}

// Lstat behaves like Stat; tree entries already carry their own mode, so there
// is no separate link to resolve.
func (f *FS) Lstat(filename string) (fs.FileInfo, error) { return f.Stat(filename) }

func (f *FS) ReadDir(p string) ([]fs.DirEntry, error) {
	r := rel(p)
	tree := f.tree
	if r != "" {
		sub, err := f.tree.Tree(r)
		if err != nil {
			return nil, notExist("readdir", p)
		}
		tree = sub
	}
	out := make([]fs.DirEntry, 0, len(tree.Entries))
	for _, e := range tree.Entries {
		out = append(out, fileInfo{name: e.Name, mode: osMode(e.Mode)})
	}
	return out, nil
}

func (f *FS) Readlink(link string) (string, error) {
	r := rel(link)
	entry, err := f.tree.FindEntry(r)
	if err != nil || entry.Mode != filemode.Symlink {
		return "", notExist("readlink", link)
	}
	file, err := f.tree.File(r)
	if err != nil {
		return "", notExist("readlink", link)
	}
	target, err := file.Contents()
	if err != nil {
		return "", err
	}
	return target, nil
}

// Chroot returns a filesystem rooted at the subtree under p.
func (f *FS) Chroot(p string) (billy.Filesystem, error) {
	r := rel(p)
	if r == "" {
		return f, nil
	}
	sub, err := f.tree.Tree(r)
	if err != nil {
		return nil, notExist("chroot", p)
	}
	return New(sub), nil
}

func (f *FS) Root() string { return "/" }

func (f *FS) Join(elem ...string) string { return path.Join(elem...) }

// Mutating operations are unsupported on a read-only tree.
func (f *FS) Create(string) (billy.File, error)           { return nil, billy.ErrReadOnly }
func (f *FS) Rename(string, string) error                 { return billy.ErrReadOnly }
func (f *FS) Remove(string) error                         { return billy.ErrReadOnly }
func (f *FS) MkdirAll(string, fs.FileMode) error          { return billy.ErrReadOnly }
func (f *FS) Symlink(string, string) error                { return billy.ErrReadOnly }
func (f *FS) TempFile(string, string) (billy.File, error) { return nil, billy.ErrReadOnly }

// osMode maps a git filemode to an fs.FileMode for stat results.
func osMode(m filemode.FileMode) fs.FileMode {
	switch m {
	case filemode.Dir:
		return fs.ModeDir | 0o755
	case filemode.Symlink:
		return fs.ModeSymlink | 0o777
	case filemode.Executable:
		return 0o755
	default:
		return 0o644
	}
}

// fileInfo implements both fs.FileInfo and fs.DirEntry for tree entries.
type fileInfo struct {
	name string
	size int64
	mode fs.FileMode
}

func (i fileInfo) Name() string               { return i.name }
func (i fileInfo) Size() int64                { return i.size }
func (i fileInfo) Mode() fs.FileMode          { return i.mode }
func (i fileInfo) ModTime() time.Time         { return time.Time{} }
func (i fileInfo) IsDir() bool                { return i.mode.IsDir() }
func (i fileInfo) Sys() any                   { return nil }
func (i fileInfo) Type() fs.FileMode          { return i.mode.Type() }
func (i fileInfo) Info() (fs.FileInfo, error) { return i, nil }

var (
	_ billy.Filesystem = (*FS)(nil)
	_ fs.FileInfo      = fileInfo{}
	_ fs.DirEntry      = fileInfo{}
)
