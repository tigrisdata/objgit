// Package kefkash wires a billy.Filesystem into an mvdan.cc/sh interpreter the
// way the kefka virtual shell does. The handler constructors are vendored from
// kefka's internal/billysh (tangled.org/xeiaso.net/kefka), which is not
// importable because it lives under internal/. They depend only on kefka's
// public command/registry package.
//
// One deliberate deviation from upstream: FsysOpenHandler permits write opens
// and delegates them to the filesystem's OpenFile. objgit hands the sandbox a
// composite filesystem (see internal/mountfs) where /src is read-only and /tmp
// is writable, so the filesystem itself — not this handler — enforces what may
// be written. Upstream billysh rejects all write opens because it mounts a
// single read-only tree.
package kefkash

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/go-git/go-billy/v6"
	"mvdan.cc/sh/v3/interp"
	"tangled.org/xeiaso.net/kefka/command/registry"
)

// FsysStatHandler resolves stat calls against fsys, honouring followSymlinks
// when the filesystem supports Lstat.
func FsysStatHandler(reg *registry.Impl, fsys billy.Filesystem) interp.StatHandlerFunc {
	return func(ctx context.Context, name string, followSymlinks bool) (fs.FileInfo, error) {
		resolved := reg.Resolve(name)
		if !followSymlinks {
			if r, ok := fsys.(billy.Symlink); ok {
				return r.Lstat(resolved)
			}
		}
		return fsys.Stat(resolved)
	}
}

// FsysOpenHandler opens files against fsys for both reading and writing. Read
// opens are wrapped so writes through the returned handle are rejected; write
// opens are delegated to fsys.OpenFile, leaving the filesystem to allow or deny
// the write (e.g. a read-only /src vs a writable /tmp).
func FsysOpenHandler(reg *registry.Impl, fsys billy.Filesystem) interp.OpenHandlerFunc {
	return func(ctx context.Context, name string, flag int, perm os.FileMode) (io.ReadWriteCloser, error) {
		resolved := reg.Resolve(name)
		if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC) != 0 {
			return fsys.OpenFile(resolved, flag, perm)
		}
		f, err := fsys.Open(resolved)
		if err != nil {
			return nil, err
		}
		return readOnlyFile{f}, nil
	}
}

// FsysReadDirHandler lists directories against fsys.
func FsysReadDirHandler(reg *registry.Impl, fsys billy.Filesystem) interp.ReadDirHandlerFunc2 {
	return func(ctx context.Context, name string) ([]fs.DirEntry, error) {
		return fsys.ReadDir(reg.Resolve(name))
	}
}

type readOnlyFile struct{ billy.File }

func (readOnlyFile) Write([]byte) (int, error) { return 0, fs.ErrPermission }

// CallHandler intercepts cd and pwd before interp's builtins handle them, so
// directory state is routed through the registry's fsys-relative pwd instead of
// interp's host-rooted Dir. Intercepted calls are replaced with `:` (no-op) so
// interp's builtin doesn't run.
func CallHandler(reg *registry.Impl, fsys billy.Filesystem, stdout, stderr io.Writer) interp.CallHandlerFunc {
	return func(ctx context.Context, args []string) ([]string, error) {
		if len(args) == 0 {
			return args, nil
		}
		switch args[0] {
		case "cd":
			target := ""
			if len(args) > 1 {
				target = args[1]
			}
			if err := reg.Chdir(fsys, target); err != nil {
				fmt.Fprintln(stderr, err)
				return []string{"false"}, nil
			}
			return []string{":"}, nil
		case "pwd":
			pwd := reg.Pwd()
			if pwd == "." {
				fmt.Fprintln(stdout, "/")
			} else {
				fmt.Fprintln(stdout, "/"+pwd)
			}
			return []string{":"}, nil
		}
		return args, nil
	}
}
