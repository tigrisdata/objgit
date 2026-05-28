// symlink.go implements the interface billy.Symlink

package s3fs

import (
	"errors"
	"os"
)

var (
	ErrSymLinkNotSupported = errors.New("symlink not supported by s3")
)

// Lstat returns a FileInfo describing the named file. S3 has no symlinks,
// so Lstat is equivalent to Stat. We still implement it so callers that
// type-assert billy.Symlink (rm, du, cp, ls, touch, file, billyfs,
// billysh) get a usable FileInfo instead of ErrSymLinkNotSupported.
func (fs3 *S3FS) Lstat(filename string) (os.FileInfo, error) {
	return fs3.Stat(filename)
}

// Symlink creates a symbolic-link from link to target. target may be an
// absolute or relative path, and need not refer to an existing node.
// Parent directories of link are created as necessary.
//
// NOTE: Symlink is not supported by s3. It always returns an error.
func (fs3 *S3FS) Symlink(target, link string) error {
	return ErrSymLinkNotSupported
}

// Readlink returns the target path of link.
//
// NOTE: Readlink is not supported by s3. It always returns an error.
// (This may be revised in the future.)
func (fs3 *S3FS) Readlink(link string) (string, error) {
	return "", ErrSymLinkNotSupported
}
