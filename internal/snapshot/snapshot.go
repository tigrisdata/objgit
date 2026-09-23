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
const FormatVersion = 2

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

// CacheID returns the local cache id for key: "erofs-v2-<tree>". It holds no
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
