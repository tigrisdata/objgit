// Package mountfs composes several billy filesystems into one, dispatching by
// the first path component. objgit uses it to give a hook sandbox a read-only
// /src (a git tree) alongside a writable /tmp (an in-memory scratch fs) under a
// single filesystem, since the kefka shell mounts exactly one.
//
// The root directory is virtual: it only lists the configured mount points and
// cannot itself be written to.
package mountfs

import (
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6"
)

// FS routes operations to a mounted filesystem chosen by the leading path
// component.
type FS struct {
	mounts map[string]billy.Filesystem
	names  []string // sorted mount names, for a stable root listing
}

// New builds a composite filesystem. Keys are top-level directory names (e.g.
// "src", "tmp") without slashes.
func New(mounts map[string]billy.Filesystem) *FS {
	names := make([]string, 0, len(mounts))
	for name := range mounts {
		names = append(names, name)
	}
	sort.Strings(names)
	return &FS{mounts: mounts, names: names}
}

// route resolves p to a mount and a path relative to it. isRoot is true when p
// addresses the virtual root itself. isMount is true when p addresses a mount
// point exactly (rel == ".").
func (f *FS) route(p string) (sub billy.Filesystem, rel string, isRoot, isMount bool, err error) {
	clean := path.Clean("/" + p)[1:] // strip leading slash; root -> ""
	if clean == "" {
		return nil, "", true, false, nil
	}
	name, rest, _ := strings.Cut(clean, "/")
	sub, ok := f.mounts[name]
	if !ok {
		return nil, "", false, false, fs.ErrNotExist
	}
	if rest == "" {
		return sub, ".", false, true, nil
	}
	return sub, rest, false, false, nil
}

func pathErr(op, name string, err error) error {
	return &os.PathError{Op: op, Path: name, Err: err}
}

func (f *FS) Open(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDONLY, 0)
}

func (f *FS) OpenFile(filename string, flag int, perm fs.FileMode) (billy.File, error) {
	sub, rel, isRoot, isMount, err := f.route(filename)
	if err != nil {
		return nil, pathErr("open", filename, err)
	}
	if isRoot || isMount {
		return nil, pathErr("open", filename, billy.ErrNotSupported)
	}
	return sub.OpenFile(rel, flag, perm)
}

func (f *FS) Stat(filename string) (fs.FileInfo, error) {
	sub, rel, isRoot, isMount, err := f.route(filename)
	if err != nil {
		return nil, pathErr("stat", filename, err)
	}
	if isRoot {
		return dirInfo{name: "/"}, nil
	}
	if isMount {
		return dirInfo{name: path.Base(path.Clean("/" + filename))}, nil
	}
	return sub.Stat(rel)
}

func (f *FS) Lstat(filename string) (fs.FileInfo, error) {
	sub, rel, isRoot, isMount, err := f.route(filename)
	if err != nil {
		return nil, pathErr("lstat", filename, err)
	}
	if isRoot || isMount {
		return f.Stat(filename)
	}
	if sym, ok := sub.(billy.Symlink); ok {
		return sym.Lstat(rel)
	}
	return sub.Stat(rel)
}

func (f *FS) ReadDir(p string) ([]fs.DirEntry, error) {
	sub, rel, isRoot, _, err := f.route(p)
	if err != nil {
		return nil, pathErr("readdir", p, err)
	}
	if isRoot {
		out := make([]fs.DirEntry, 0, len(f.names))
		for _, name := range f.names {
			out = append(out, dirInfo{name: name})
		}
		return out, nil
	}
	return sub.ReadDir(rel)
}

func (f *FS) Readlink(link string) (string, error) {
	sub, rel, isRoot, isMount, err := f.route(link)
	if err != nil {
		return "", pathErr("readlink", link, err)
	}
	if isRoot || isMount {
		return "", pathErr("readlink", link, billy.ErrNotSupported)
	}
	if sym, ok := sub.(billy.Symlink); ok {
		return sym.Readlink(rel)
	}
	return "", pathErr("readlink", link, billy.ErrNotSupported)
}

func (f *FS) Symlink(target, link string) error {
	sub, rel, isRoot, isMount, err := f.route(link)
	if err != nil {
		return pathErr("symlink", link, err)
	}
	if isRoot || isMount {
		return pathErr("symlink", link, billy.ErrReadOnly)
	}
	if sym, ok := sub.(billy.Symlink); ok {
		return sym.Symlink(target, rel)
	}
	return pathErr("symlink", link, billy.ErrNotSupported)
}

func (f *FS) Create(filename string) (billy.File, error) {
	return f.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

func (f *FS) Remove(filename string) error {
	sub, rel, isRoot, isMount, err := f.route(filename)
	if err != nil {
		return pathErr("remove", filename, err)
	}
	if isRoot || isMount {
		return pathErr("remove", filename, billy.ErrReadOnly)
	}
	return sub.Remove(rel)
}

func (f *FS) MkdirAll(filename string, perm fs.FileMode) error {
	sub, rel, isRoot, isMount, err := f.route(filename)
	if err != nil {
		return pathErr("mkdir", filename, err)
	}
	if isRoot || isMount {
		return pathErr("mkdir", filename, billy.ErrReadOnly)
	}
	return sub.MkdirAll(rel, perm)
}

// Rename only works within a single mount.
func (f *FS) Rename(oldpath, newpath string) error {
	oldSub, oldRel, oldRoot, oldMount, err := f.route(oldpath)
	if err != nil {
		return pathErr("rename", oldpath, err)
	}
	newSub, newRel, newRoot, newMount, err := f.route(newpath)
	if err != nil {
		return pathErr("rename", newpath, err)
	}
	if oldRoot || oldMount || newRoot || newMount || oldSub != newSub {
		return pathErr("rename", oldpath, billy.ErrNotSupported)
	}
	return oldSub.Rename(oldRel, newRel)
}

func (f *FS) TempFile(dir, prefix string) (billy.File, error) {
	sub, rel, isRoot, _, err := f.route(dir)
	if err != nil {
		return nil, pathErr("tempfile", dir, err)
	}
	if isRoot {
		return nil, pathErr("tempfile", dir, billy.ErrReadOnly)
	}
	return sub.TempFile(rel, prefix)
}

// Chroot returns the mounted filesystem (optionally further chrooted) so callers
// that chroot into /src or /tmp keep working.
func (f *FS) Chroot(p string) (billy.Filesystem, error) {
	sub, rel, isRoot, isMount, err := f.route(p)
	if err != nil {
		return nil, pathErr("chroot", p, err)
	}
	if isRoot {
		return f, nil
	}
	if isMount {
		return sub, nil
	}
	return sub.Chroot(rel)
}

func (f *FS) Root() string { return "/" }

func (f *FS) Join(elem ...string) string { return path.Join(elem...) }

// dirInfo describes the virtual root and the mount-point directories.
type dirInfo struct{ name string }

func (d dirInfo) Name() string               { return d.name }
func (d dirInfo) Size() int64                { return 0 }
func (d dirInfo) Mode() fs.FileMode          { return fs.ModeDir | 0o755 }
func (d dirInfo) ModTime() time.Time         { return time.Time{} }
func (d dirInfo) IsDir() bool                { return true }
func (d dirInfo) Sys() any                   { return nil }
func (d dirInfo) Type() fs.FileMode          { return fs.ModeDir }
func (d dirInfo) Info() (fs.FileInfo, error) { return d, nil }

var (
	_ billy.Filesystem = (*FS)(nil)
	_ fs.FileInfo      = dirInfo{}
	_ fs.DirEntry      = dirInfo{}
)
