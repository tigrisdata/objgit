package s3fs

import (
	"fmt"
	"path"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/tigrisdata/storage-go"
)

const (
	DefaultSeparator = "/"
)

type S3FS struct {
	client    *storage.Client
	bucket    string
	root      string
	separator string
}

// NewS3FS creates a new S3FS Filesystem.
func NewS3FS(client *storage.Client, bucket string) (billy.Filesystem, error) {
	// Check for a non-nil client
	if client == nil {
		return nil, fmt.Errorf("s3 client cannot be nil")
	}
	return &S3FS{
		client:    client,
		bucket:    bucket,
		root:      "",
		separator: DefaultSeparator,
	}, nil
}

// Capabilities returns the filesystem capabilities.
func (fs3 *S3FS) Capabilities() billy.Capability {
	return billy.ReadCapability | billy.WriteCapability
}

func (fs3 *S3FS) cleanPath(p ...string) string {
	// Join the path elements
	j := path.Join(p...)

	// Clean the path before joining to root
	c := path.Clean(j)

	// Join the root and cleaned path
	f := path.Join(fs3.root, c)

	// Return the full path
	return path.Clean(f)
}

// key turns a root-relative billy path into the canonical S3 object key:
// root-joined, cleaned, and stripped of the leading slash that S3 keys never
// carry. All S3 operations must funnel through here so reads and writes agree
// on the same key regardless of chroot depth.
func (fs3 *S3FS) key(name string) string {
	return strings.TrimPrefix(fs3.cleanPath(name), "/")
}
