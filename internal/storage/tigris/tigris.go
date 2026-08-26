// Package tigris implements github.com/go-git/go-git/v6/storage.Storer on top
// of one Tigris bucket per repository. See docs/plans/tigris-gogit-storer.md
// and docs/reference/tigris-backend.md for the design.
//
// Layout in the bucket:
//
//	objects/<hex>   loose object keyed by content hash; user metadata carries
//	                the git type (git-type) and size (git-size)
//	refs/<name>     one loose ref (hash hex, or "ref: target" for symbolics)
//	shallow         newline separated commit hashes
//	index           plumbing/format/index-encoded worktree index
//	config          config.Config.Marshal output
//	packs/          reserved for packfiles (future work)
//
// Methods hold no per-call state on the Storer, so a Storer value is safe for
// concurrent use.
package tigris

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
	tstorage "github.com/tigrisdata/storage-go"
)

const (
	objectPrefix = "objects/"
	refPrefix    = "refs/"
	shallowKey   = "shallow"
	indexKey     = "index"
	configKey    = "config"

	metaType = "git-type"
	metaSize = "git-size"

	symRefPrefix = "ref: "
)

var (
	// ErrHashMismatch guards against a corrupted upstream claim: content
	// hashes differently than the object says, so nothing gets stored.
	ErrHashMismatch = errors.New("tigris: content hash does not match the hash claimed by the object")
	// ErrAlternatesNotSupported marks AddAlternate: borrowing objects across
	// buckets needs remote-prefix support this backend lacks.
	ErrAlternatesNotSupported = errors.New("tigris: alternates are not supported")
	// ErrModulesNotSupported marks Module: submodule storers need their own
	// per-module bucket story first.
	ErrModulesNotSupported = errors.New("tigris: submodule storers are not supported")

	errBadMetadata  = errors.New("tigris: object has invalid user metadata")
	errMalformedRef = errors.New("tigris: malformed loose ref")

	errEmptyBucket = errors.New("tigris: bucket must be set")

	errUnimplemented = errors.New("tigris: method not implemented yet") // TEMPORARY: gone by Task 9
)

// s3API is the subset of *tstorage.Client (which embeds *s3.Client) this
// package needs. Naming the subset lets tests substitute a counting fake; the
// concrete Tigris client satisfies it unchanged. Same trick as s3fs.
type s3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// Storer is one bucket-worth of git data speaking go-git's storage.Storer.
type Storer struct {
	client   s3API
	bucket   string
	ctx      context.Context // request-context slot inherited by every operation
	of       formatcfg.ObjectFormat
	oh       *plumbing.ObjectHasher
	observer func(operation string, dur time.Duration, err error)
}

// Option configures a Storer at construction time.
type Option func(*Storer)

// WithObjectFormat selects the git object format used for hashing. Keys under
// objects/ differ between formats, but readers still must know which one to
// expect; mixing formats within one bucket is unsupported. Defaults to
// formatcfg.DefaultObjectFormat.
func WithObjectFormat(of formatcfg.ObjectFormat) Option {
	return func(s *Storer) { s.of = of }
}

// WithObserver installs a callback fired after every S3 round-trip with the
// operation name (e.g. "GetObject"), duration, and error. Instance-level on
// purpose: unlike s3fs, our constructors hold the Storer itself, so nothing
// needs a process-global setter. Wire metrics.ObserveS3 here from main.
func WithObserver(fn func(operation string, dur time.Duration, err error)) Option {
	return func(s *Storer) { s.observer = fn }
}

func withClient(c s3API) Option {
	return func(s *Storer) { s.client = c }
}

// New returns a Storer owning one whole bucket. ctx bounds every request this
// storer issues.
func New(ctx context.Context, bucket string, opts ...Option) (*Storer, error) {
	s := &Storer{
		ctx: ctx,
		of:  formatcfg.DefaultObjectFormat,
	}
	for _, opt := range opts {
		opt(s)
	}
	if bucket == "" {
		return nil, errEmptyBucket
	}
	s.bucket = bucket
	s.oh = plumbing.FromObjectFormat(s.of)
	if s.client == nil {
		c, err := tstorage.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("tigris: failed to create storage-go client: %w", err)
		}
		s.client = c
	}
	return s, nil
}

// Compile-time proof the storer covers the whole surface.
var _ storer.Storer = (*Storer)(nil)

func (s *Storer) observe(op string, start time.Time, err error) {
	if s.observer != nil {
		s.observer(op, time.Since(start), err)
	}
}

func keyOf(h plumbing.Hash) string {
	return objectPrefix + h.String()
}

func refKey(n plumbing.ReferenceName) string {
	return refPrefix + n.String()
}

// isNotFound reports whether err is an S3/Tigris miss. Tigris reports Head
// misses as NotFound and Get misses as NoSuchKey — the same codes s3fs
// matches today (internal/s3fs/basic.go).
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NotFound", "NoSuchKey":
		return true
	default:
		return false
	}
}

// Pointer shims so call sites stay free of aws-sdk-go-v2/aws imports.
func sp(v string) *string { return &v }
func i32(v int32) *int32  { return &v }

func sv(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func bv(v *bool) bool { return v != nil && *v }

// --- temporary stubs -------------------------------------------------------
// Every remaining interface method funnels through errUnimplemented so the
// compile-time assertion holds from day one. Later tasks DELETE their stubs
// from this block as they land real implementations; Task 9 greps for
// leftovers. Do not reorder beyond appending.

func (s *Storer) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	_ = t
	return nil, errUnimplemented
}

func (s *Storer) NewEncodedObject() plumbing.EncodedObject {
	return &plumbing.MemoryObject{} // temporary stub body
}

func (s *Storer) RawObjectWriter(typ plumbing.ObjectType, sz int64) (io.WriteCloser, error) {
	_, _ = typ, sz
	return nil, errUnimplemented
}

func (s *Storer) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	_ = obj
	return plumbing.ZeroHash, errUnimplemented
}

func (s *Storer) AddAlternate(remote string) error {
	_ = remote
	return errUnimplemented
}

func (s *Storer) SetReference(ref *plumbing.Reference) error {
	_ = ref
	return errUnimplemented
}

func (s *Storer) CheckAndSetReference(newRef, old *plumbing.Reference) error {
	_, _ = newRef, old
	return errUnimplemented
}

func (s *Storer) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	_ = n
	return nil, errUnimplemented
}

func (s *Storer) IterReferences() (storer.ReferenceIter, error) {
	return nil, errUnimplemented
}

func (s *Storer) RemoveReference(n plumbing.ReferenceName) error {
	_ = n
	return errUnimplemented
}

func (s *Storer) CountLooseRefs() (int, error) {
	return 0, errUnimplemented
}

func (s *Storer) PackRefs() error {
	return errUnimplemented
}

func (s *Storer) SetShallow(commits []plumbing.Hash) error {
	_ = commits
	return errUnimplemented
}

func (s *Storer) Shallow() ([]plumbing.Hash, error) {
	return nil, errUnimplemented
}

func (s *Storer) SetIndex(idx *index.Index) error {
	_ = idx
	return errUnimplemented
}

func (s *Storer) Index() (*index.Index, error) {
	return nil, errUnimplemented
}

func (s *Storer) Config() (*config.Config, error) {
	return nil, errUnimplemented
}

func (s *Storer) SetConfig(cfg *config.Config) error {
	_ = cfg
	return errUnimplemented
}

func (s *Storer) Module(name string) (storage.Storer, error) {
	_ = name
	return nil, errUnimplemented
}
