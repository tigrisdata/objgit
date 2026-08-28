// Package tigris implements github.com/go-git/go-git/v6/storage.Storer on top
// of one Tigris bucket, optionally shared by many repositories via a key
// prefix. See docs/plans/tigris-gogit-storer.md and
// docs/reference/tigris-backend.md for the design.
//
// Layout in the bucket (all keys additionally rooted under an optional
// per-Storer prefix — see Scoped):
//
//	objects/<hex>       loose object keyed by content hash; user metadata
//	                    carries the git type (git-type) and size (git-size)
//	refs/<name>         one loose ref (hash hex, or "ref: target" for symbolics)
//	shallow             newline separated commit hashes
//	index               plumbing/format/index-encoded worktree index
//	config              config.Config.Marshal output
//	packs/<id>.bin      up to maxPackBytes bytes of
//	                    concatenated payloads, each raw or one zstd frame
//	                    (one push writes as many containers as it needs)
//	packs/<id>.cue      that pack's index (hash, type, codec, offset, stored
//	                    length and raw size per object) — see packindex.go for
//	                    the binary format and compress.go for the codec policy
//
// Methods hold no per-call state on the Storer, so a Storer value is safe for
// concurrent use.
package tigris

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/storer"
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
	prefix   string          // key prefix scoping this Storer to one repository; see Scoped
	ctx      context.Context // request-context slot inherited by every operation
	of       formatcfg.ObjectFormat
	oh       *plumbing.ObjectHasher
	observer func(operation string, dur time.Duration, err error)
	// payloadObserver reports each pack payload's codec and sizes; see
	// WithPayloadObserver.
	payloadObserver func(codec string, raw, stored int64)
	up              *uploader  // batches loose-object PutObject calls off of Close; see upload.go
	packs           *packIndex // pack containers this Storer knows about; see packindex.go
	cache           *PackCache // optional process-wide local pack cache; see packcache.go
	// fetchSem bounds whole-pack downloads in flight; see maxLivePackFetches.
	// Scoped shares it rather than replacing it, so one root Storer's
	// descendants — every repository, in production — share one budget.
	fetchSem chan struct{}
	// maxPackBytes caps a written container's payload; see maxPackBytes. It is
	// the only bound on a container: nothing caps the object count.
	maxPackBytes int64
	// inMemoryCap is the largest object whose codec is decided exactly rather
	// than by a head probe; see inMemoryCap and compress.go.
	inMemoryCap int64
	// probeWindow is how much of an over-cap object gets compressed to decide
	// the whole object's fate; see probeWindow and compress.go.
	probeWindow int
	// packCompression enables writing zstd payloads. Reading them is
	// unconditional — see WithPackCompression for why the two differ.
	packCompression bool
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
//
// The callback must be safe for concurrent use. It fires on whatever goroutine
// made the call, and a background pack prefetch (see startPackFetch) makes S3
// calls of its own, outside any request. metrics.ObserveS3 is a set of
// Prometheus vectors and already satisfies this.
func WithObserver(fn func(operation string, dur time.Duration, err error)) Option {
	return func(s *Storer) { s.observer = fn }
}

// WithPackCache installs a local pack cache shared by this Storer and every
// Storer that Scoped derives from it. Without one, a Storer that bulk-fetches
// a pack drops the copy when it goes out of scope, so the next request
// downloads it again; with one, the copy survives on disk under a byte budget
// and every later request opens it locally. Pass a single cache built once per
// process — see NewPackCache.
func WithPackCache(c *PackCache) Option {
	return func(s *Storer) { s.cache = c }
}

// WithPackCompression controls whether newly written pack containers store
// zstd payloads. Reading them is never gated: a Storer resolves compressed and
// raw containers alike no matter how this is set.
//
// The asymmetry is deliberate, and it is a rollback story. A cue written by
// this code carries format version 2, which an older binary refuses outright
// rather than misreading — the right direction to fail, but it means rolling a
// deploy back loses access to every container written in the window. Shipping
// the reader first and turning writes on in a later release makes that
// window empty.
func WithPackCompression(enabled bool) Option {
	return func(s *Storer) { s.packCompression = enabled }
}

// WithPayloadObserver installs a callback fired once for every object written
// into a pack container, with the codec it was stored under ("raw" or "zstd")
// and its size before and after. Instance-level for the same reason as
// WithObserver, and a callback rather than a direct metrics call so this
// package stays free of any Prometheus import. Wire metrics.ObservePackPayload
// here from main.
func WithPayloadObserver(fn func(codec string, raw, stored int64)) Option {
	return func(s *Storer) { s.payloadObserver = fn }
}

func withClient(c s3API) Option {
	return func(s *Storer) { s.client = c }
}

// withMaxPackBytes lowers the per-container byte cap from maxPackBytes.
// Test-only: it exists so the write-side split can be exercised without a
// 128 MiB git fixture.
func withMaxPackBytes(n int64) Option {
	return func(s *Storer) { s.maxPackBytes = n }
}

// withInMemoryCap lowers the exact-decision size from inMemoryCap. Test-only,
// for the same reason as the two caps above: reaching the probe path — and the
// rewind behind it — otherwise needs a multi-megabyte git fixture.
func withInMemoryCap(n int64) Option {
	return func(s *Storer) { s.inMemoryCap = n }
}

// withProbeWindow shrinks the head window from probeWindow. Test-only: with
// the production 64 KiB window, an object whose head compresses but whose tail
// does not still comes out smaller overall, because zstd stores incompressible
// data with only ~0.015% overhead while a 64 KiB compressible head saves ~65
// KiB. Reaching the rewind honestly would need a fixture of several hundred
// megabytes; a small window reaches it in a few kilobytes.
func withProbeWindow(n int) Option {
	return func(s *Storer) { s.probeWindow = n }
}

// New returns a Storer owning one whole bucket. ctx bounds every request this
// storer issues.
func New(ctx context.Context, bucket string, opts ...Option) (*Storer, error) {
	s := &Storer{
		ctx:             ctx,
		of:              formatcfg.DefaultObjectFormat,
		maxPackBytes:    maxPackBytes,
		inMemoryCap:     inMemoryCap,
		probeWindow:     probeWindow,
		packCompression: true,
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
	s.up = newUploader(s)
	s.packs = newPackIndex()
	s.fetchSem = make(chan struct{}, maxLivePackFetches)
	return s, nil
}

// Compile-time proof the storer covers the whole surface.
var _ storer.Storer = (*Storer)(nil)
var _ storer.PackfileWriter = (*Storer)(nil)

// Scoped returns a Storer sharing this Storer's client, bucket, object
// format, and observer, but addressing keys under an additional prefix —
// letting one bucket host many repositories, each reached through its own
// Storer value. Prefixes nest: scoping an already-scoped Storer extends its
// existing prefix. Cheap: it copies the Storer value and dials nothing.
//
// The returned Storer gets its own uploader and pack index (see upload.go,
// packindex.go), independent of s's: one repository's push, pending/failed
// uploads, or pack read history can never block or leak into another's.
//
// Two things are shared on purpose. The pack cache, because sharing downloaded
// packs across requests is the whole point of it, and its keys are content
// hashes, so a hit is always the exact bytes the caller named. And fetchSem,
// because the bandwidth it rations is the process's, not one repository's.
func (s *Storer) Scoped(prefix string) *Storer {
	cp := *s
	prefix = strings.Trim(prefix, "/")
	switch {
	case prefix == "":
	case cp.prefix == "":
		cp.prefix = prefix + "/"
	default:
		cp.prefix = strings.TrimSuffix(cp.prefix, "/") + "/" + prefix + "/"
	}
	cp.up = newUploader(&cp)
	cp.packs = newPackIndex()
	return &cp
}

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

func sv(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func bv(v *bool) bool { return v != nil && *v }
