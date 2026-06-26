// Package tigrisfs is the production repofs.Resolver: it treats the caller's
// credential as a Tigris keypair (username = access key ID, password = secret
// access key), builds one storage.Client per keypair, and gives every
// repository its own Tigris bucket — created on the push (write) path.
//
// All S3 calls go through github.com/tigrisdata/storage-go's *storage.Client
// (which embeds *s3.Client); no bare AWS client is ever constructed.
package tigrisfs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/tigrisdata/objgit/internal/repofs"
	"github.com/tigrisdata/objgit/internal/s3fs"
	storage "github.com/tigrisdata/storage-go"
)

// ErrNoCredential is returned when a request carries no Tigris keypair. It is
// repofs.ErrUnauthenticated so transports map it to a 401 challenge without
// importing this package.
var ErrNoCredential = repofs.ErrUnauthenticated

// client is what the resolver needs from a per-keypair Tigris client: the object
// operations s3fs consumes (via the embedded s3fs.S3Client) plus the bucket
// lifecycle calls. *storage.Client satisfies it.
type client interface {
	s3fs.S3Client
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, in *s3.CreateBucketInput, opts ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
}

// cachedClient holds the two views of one keypair's client: the raw client for
// bucket lifecycle calls, and the hardened wrapper handed to s3fs for object
// I/O. The hardened wrapper owns its own HTTP connection pool, so it is built
// once per keypair and reused — rebuilding it per request would defeat
// keep-alive reuse.
type cachedClient struct {
	raw      client
	hardened s3fs.S3Client
}

// Resolver implements repofs.Resolver against Tigris with one bucket per repo.
type Resolver struct {
	// newClient builds a client from a keypair. Overridable in tests; defaults to
	// storage.New(ctx, storage.WithAccessKeypair(id, secret)).
	newClient func(ctx context.Context, cred repofs.Credential) (client, error)
	fsOpts    []s3fs.Option

	mu      sync.Mutex
	clients map[string]*cachedClient // keyed by access key ID (cred.Username)
}

// Option configures a Resolver.
type Option func(*Resolver)

// WithFSOptions passes s3fs.Options to every per-bucket S3FS the resolver builds.
func WithFSOptions(opts ...s3fs.Option) Option {
	return func(r *Resolver) { r.fsOpts = append(r.fsOpts, opts...) }
}

// New returns a Resolver that talks to Tigris using each caller's keypair.
func New(opts ...Option) *Resolver {
	r := &Resolver{
		newClient: defaultNewClient,
		clients:   make(map[string]*cachedClient),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

func defaultNewClient(ctx context.Context, cred repofs.Credential) (client, error) {
	c, err := storage.New(ctx, storage.WithAccessKeypair(cred.Username, cred.Password))
	if err != nil {
		return nil, fmt.Errorf("tigrisfs: building storage client: %w", err)
	}
	return c, nil
}

// Resolve builds the billy.Filesystem for ref backed by its own Tigris bucket.
// On the write path (create) it provisions the bucket; on the read path it
// returns transport.ErrRepositoryNotFound when the bucket is absent.
func (r *Resolver) Resolve(ctx context.Context, ref repofs.RepoRef, cred repofs.Credential, create bool) (billy.Filesystem, error) {
	if cred.Username == "" || cred.Password == "" {
		return nil, ErrNoCredential
	}

	cc, err := r.client(ctx, cred)
	if err != nil {
		return nil, err
	}

	bucket := bucketName(ref)
	if create {
		if err := ensureBucket(ctx, cc.raw, bucket); err != nil {
			return nil, fmt.Errorf("tigrisfs: ensuring bucket %q: %w", bucket, err)
		}
	} else {
		ok, err := bucketExists(ctx, cc.raw, bucket)
		if err != nil {
			return nil, fmt.Errorf("tigrisfs: checking bucket %q: %w", bucket, err)
		}
		if !ok {
			return nil, transport.ErrRepositoryNotFound
		}
	}

	return s3fs.NewS3FS(cc.hardened, bucket, r.fsOpts...)
}

// client returns the cached client for a keypair, building and caching it on
// first use. Keyed by access key ID, which uniquely identifies the keypair.
func (r *Resolver) client(ctx context.Context, cred repofs.Credential) (*cachedClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// NOTE: this map is unbounded — one entry per distinct access key ID. Bound
	// it (LRU) if the keypair population grows large.
	if cc, ok := r.clients[cred.Username]; ok {
		return cc, nil
	}

	raw, err := r.newClient(ctx, cred)
	if err != nil {
		return nil, err
	}
	cc := &cachedClient{raw: raw, hardened: s3fs.Harden(raw)}
	r.clients[cred.Username] = cc
	return cc, nil
}

// ensureBucket creates the bucket, treating an already-owned/already-existing
// bucket as success so repeated pushes are idempotent.
func ensureBucket(ctx context.Context, c client, bucket string) error {
	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*types.BucketAlreadyOwnedByYou](err); ok {
		return nil
	}
	if _, ok := errors.AsType[*types.BucketAlreadyExists](err); ok {
		return nil
	}
	return err
}

// bucketExists reports whether the bucket is present, mapping a not-found
// response to (false, nil) and any other error through.
func bucketExists(ctx context.Context, c client, bucket string) (bool, error) {
	_, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

// isNotFound recognizes the several shapes a "bucket does not exist" error can
// take from the S3 API: the typed NotFound/NoSuchBucket errors, or a generic
// smithy API error whose code says so.
func isNotFound(err error) bool {
	if _, ok := errors.AsType[*types.NotFound](err); ok {
		return true
	}
	if _, ok := errors.AsType[*types.NoSuchBucket](err); ok {
		return true
	}
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchBucket", "404":
			return true
		}
	}
	return false
}

// bucketName derives a deterministic, DNS-valid Tigris bucket name from a repo
// ref: "objgit-" + a base36 (0-9a-z) digest of "orgID/name". The digest is
// left-padded so truncation is stable, giving a fixed 39-character name well
// within the 63-character limit.
func bucketName(ref repofs.RepoRef) string {
	const digestLen = 32
	sum := sha256.Sum256([]byte(ref.Path()))
	b36 := new(big.Int).SetBytes(sum[:]).Text(36)
	if len(b36) < digestLen {
		b36 = strings.Repeat("0", digestLen-len(b36)) + b36
	}
	return "objgit-" + b36[:digestLen]
}
