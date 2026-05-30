package s3fs

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
)

const (
	DefaultSeparator = "/"
)

// s3Client is the subset of *storage.Client (which embeds *s3.Client) that the
// filesystem uses. Naming it as an interface lets tests substitute a counting
// stub; the concrete Tigris client satisfies it unchanged.
type s3Client interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	RenameObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
}

// unixMetaConfig holds the session defaults used when the optional Unix-metadata
// feature is enabled. A nil *unixMetaConfig means the feature is off and the
// filesystem behaves as if no POSIX attributes exist.
type unixMetaConfig struct {
	uid, gid uint32
	umask    os.FileMode
}

type S3FS struct {
	client    s3Client
	bucket    string
	root      string
	separator string
	unixMeta  *unixMetaConfig

	// cache, when non-nil, memoises directory listings so Stat/Open of a path
	// whose parent folder has been listed can skip the S3 round-trip. It is
	// shared by pointer across this filesystem and all of its Chroot children.
	cache *ListingCache

	// packCache, when non-nil, serves reads of immutable pack-directory files
	// (.pack/.idx/.rev) from a local temp file downloaded once, instead of one
	// S3 round-trip per object access. Shared by pointer across Chroot children.
	packCache *PackCache

	// temps holds TempFile-backed buffers keyed by canonical S3 key, so a
	// subsequent Open of the same path returns a reader over the same bytes
	// the writer is still appending to. See tempfs.go.
	tempMu sync.Mutex
	temps  map[string]*tempBuffer
}

// Option configures an S3FS at construction time.
type Option func(*S3FS)

// WithUnixMetadata enables storing and reading POSIX file attributes as S3 user
// metadata (see the unixmeta package). uid and gid are the numeric owner/group
// recorded on newly written objects; callers resolve any names to numbers
// themselves. umask is applied to the default mode (0666 for files) when
// writing. Without this option the filesystem stores no attributes.
func WithUnixMetadata(uid, gid uint32, umask os.FileMode) Option {
	return func(fs3 *S3FS) {
		fs3.unixMeta = &unixMetaConfig{uid: uid, gid: gid, umask: umask}
	}
}

// WithListingCache attaches a directory-listing cache. The same *ListingCache is
// carried into every Chroot child so the whole tree shares one cache keyed by
// full-canonical prefix. Construct the cache with NewListingCache.
func WithListingCache(c *ListingCache) Option {
	return func(fs3 *S3FS) {
		fs3.cache = c
	}
}

// WithPackCache attaches a local temp-file cache for immutable pack-directory
// files. The same *PackCache is carried into every Chroot child so the whole
// tree shares it. Construct it with NewPackCache and defer its Cleanup.
func WithPackCache(c *PackCache) Option {
	return func(fs3 *S3FS) {
		fs3.packCache = c
	}
}

// NewS3FS creates a new S3FS Filesystem. client is typically a *storage.Client;
// it is accepted as the s3Client interface so tests can substitute a stub.
func NewS3FS(client s3Client, bucket string, opts ...Option) (billy.Filesystem, error) {
	// Check for a non-nil client
	if client == nil {
		return nil, fmt.Errorf("s3 client cannot be nil")
	}
	fs3 := &S3FS{
		client:    client,
		bucket:    bucket,
		root:      "",
		separator: DefaultSeparator,
		temps:     make(map[string]*tempBuffer),
	}
	for _, opt := range opts {
		opt(fs3)
	}
	return fs3, nil
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
