// chroot.go implements the interface billy.Chroot

package s3fs

import "github.com/go-git/go-billy/v6"

// Chroot returns a new filesystem from the same type where the new root is
// the given path. Files outside of the designated directory tree cannot be
// accessed.
func (fs3 *S3FS) Chroot(path string) (billy.Filesystem, error) {
	// TODO: Check that path is a valid subdirectory of the current root
	// ...

	// Calculate the new root
	p := fs3.Join(fs3.root, path)

	// Create the new S3FS with the new root directory. The separator must be
	// carried over; without it ListObjectsV2 runs with an empty delimiter and
	// ReadDir flattens the tree, breaking directory-structured reads (e.g. git
	// ref enumeration under refs/).
	nfs := &S3FS{
		client:    fs3.client,
		bucket:    fs3.bucket,
		root:      p,
		separator: fs3.separator,
		unixMeta:  fs3.unixMeta,
		temps:     make(map[string]*tempBuffer),
	}
	return nfs, nil
}

// Root returns the root path of the filesystem.
func (fs3 *S3FS) Root() string {
	return fs3.root
}
