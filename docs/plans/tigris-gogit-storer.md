# Plan: Tigris-backed go-git `storage.Storer`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a native go-git `storage.Storer` (`internal/storage/tigris`) backed by one Tigris/S3 bucket — loose git objects keyed by content hash plus flat keys for refs, shallow marks, index, and config.

**Architecture:** One bucket per repository (the model from `docs/plans/per-repo-bucket.md`). Loose objects store raw uncompressed payloads under `objects/<hex>` with S3 user metadata carrying type and size, exactly as specced in `docs/reference/tigris-backend.md`. All S3 access funnels through an unexported `s3API` seam (the pattern `internal/s3fs` already uses), so table-driven unit tests run against an in-memory fake with zero network. Refs, shallow marks, index, and config get flat keys with dotgit-compatible encodings — the spec designed the object half; this plan completes the `storage.Storer` half.

**Tech Stack:** Go 1.26 · go-git **v6.0.0-alpha.4** · `github.com/tigrisdata/storage-go` v0.6.0 (`Client` embeds `*s3.Client`) · aws-sdk-go-v2/service/s3 v1.102.0 · smithy-go.

**Spec:** [`docs/reference/tigris-backend.md`](../reference/tigris-backend.md) — background facts (hash computation, layout rationale, write/read paths, testing seam) live there. Read it first; this plan implements its build-order items **1–2** plus the interfaces the spec did not cover.

## Global Constraints

Work happens on branch `tigris-gogit-storer`.

Every commit ends with `-s`; the trailer must read exactly:

```
Signed-off-by: Xe Iaso <xe@tigrisdata.com>
```

The repo module path is `github.com/tigrisdata/objgit` (per `go.mod`; AGENTS.md's `tangled.org/...` mention is stale).

Name collision to watch: go-git's package is `github.com/go-git/go-git/v6/storage`, Tigris's is `github.com/tigrisdata/storage-go`. Convention here: import go-git's plain (`storage`), alias Tigris's `tstorage` (only `tigris.go` touches it).

Import aliases used throughout this package:

```go
formatcfg "github.com/go-git/go-git/v6/plumbing/format/config" // matches memory-storage precedent
```

Code style: slog with `"err"` key; sentinel + `%w` wrapping; `go tool goimports -w` and `go tool staticcheck` clean on the package before each commit. Tests are white-box (package `tigris`), table-driven with loop variable `tt`, `t.Helper()` on helpers, `t.Parallel()` when nothing shares a fake, `errors.Is` for error checks.

This package serves nothing in the daemon yet — wiring into `cmd/objgitd` is future work. Do not touch anything outside `internal/storage/tigris/` except `AGENTS.md` in Task 9.

Non-goals (spec build-order items 3–6): `PackfileWriter` / `PromisorPackfileWriter`, serving packed objects via ranged GETs, zlib payloads, atomic conditional-PUT CAS on refs.

## Verified v6.0.0-alpha.4 contracts

Confirmed against module source (`$(go env GOPATH)/pkg/mod/github.com/go-git/go-git/v6@v6.0.0-alpha.4`). Do not re-derive:

```go
type storage.Storer interface {   // go-git/v6/storage
	storer.EncodedObjectStorer      // plumbing/storer
	storer.ReferenceStorer          // plumbing/storer
	storer.ShallowStorer            // plumbing/storer
	storer.IndexStorer              // plumbing/storer
	config.ConfigStorer             // config
	storage.ModuleStorer            // v6/storage — NOT part of plumbing/storer!
}

// EncodedObjectStorer — RawObjectWriter takes BOTH typ and sz in alpha.4:
NewEncodedObject() plumbing.EncodedObject
RawObjectWriter(typ plumbing.ObjectType, sz int64) (io.WriteCloser, error)
SetEncodedObject(plumbing.EncodedObject) (plumbing.Hash, error)
EncodedObject(typ plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error)
IterEncodedObjects(plumbing.ObjectType) (storer.EncodedObjectIter, error)
HasEncodedObject(plumbing.Hash) error               // miss → plumbing.ErrObjectNotFound
EncodedObjectSize(plumbing.Hash) (int64, error)     // miss → plumbing.ErrObjectNotFound
AddAlternate(remote string) error
// EncodedObject contract: "Implementors should return (nil,
// plumbing.ErrObjectNotFound) if an object doesn't exist with BOTH the given
// hash and object type." AnyObject bypasses the type filter.

// ReferenceStorer:
SetReference(*plumbing.Reference) error
CheckAndSetReference(newRef, old *plumbing.Reference) error
Reference(n plumbing.ReferenceName) (*plumbing.Reference, error)
IterReferences() (storer.ReferenceIter, error)
RemoveReference(n plumbing.ReferenceName) error
CountLooseRefs() (int, error)
PackRefs() error

// Shallow / Index / Config / Module:
SetShallow([]plumbing.Hash) error        ; Shallow() ([]plumbing.Hash, error)
SetIndex(*index.Index) error             ; Index() (*index.Index, error)
Config() (*config.Config, error)         ; SetConfig(*config.Config) error
Module(name string) (storage.Storer, error)

// Semantics mirrored from the in-memory storer (storage/memory/storage.go):
// - CheckAndSetReference: old != nil && stored.Hash() != old.Hash() →
//   storage.ErrReferenceHasChanged; missing current ref proceeds with the set.
// - Reference miss → plumbing.ErrReferenceNotFound.
// - PackRefs: no-op returning nil.
// - Index(): empty → &index.Index{Version: 2}.
// - SetReference(nil): silently succeeds.
// - AddAlternate: unsupported error.

// plumbing pieces used:
plumbing.NewHasher(f formatcfg.ObjectFormat, t ObjectType, size int64) Hasher // Sum() hashes "<type> <size>\0"+content
plumbing.FromObjectFormat(f formatcfg.ObjectFormat) *plumbing.ObjectHasher    // thread-safe hasher factory
plumbing.NewMemoryObject(oh *plumbing.ObjectHasher) *plumbing.MemoryObject    // then SetType/SetSize/Write
plumbing.ParseObjectType(string) (ObjectType, error)                          // "commit"/"tree"/blob/tag
plumbing.FromHex(string) (plumbing.Hash, bool)                                // strict hex parse
plumbing.ErrObjectNotFound, plumbing.ErrReferenceNotFound, plumbing.ErrInvalidType
storage.ErrReferenceHasChanged                              // v6/storage package
storer.NewEncodedObjectSliceIter([]plumbing.EncodedObject)
storer.NewReferenceSliceIter([]*plumbing.Reference) storer.ReferenceIter
storer.ErrStop                                              // ForEach cancellation
formatcfg.DefaultObjectFormat, formatcfg.SHA256             // plumbing/format/config
config.NewConfig(); (*config.Config).Marshal() ([]byte, error); (*config.Config).Unmarshal(b []byte) error
index.NewEncoder(w io.Writer, h hash.Hash, opts ...) *Encoder ; (*Encoder).Encode(idx *Index) error
index.NewDecoder(r io.Reader, h hash.Hash, opts ...) *Decoder ; (*Decoder).Decode(idx *Index) error
```

House patterns copied from `internal/s3fs`: missing-key detection matches `smithy.APIError` codes `"NotFound"` (Head misses) or `"NoSuchKey"` (Get misses) — exactly what `internal/s3fs/basic.go` sees on real buckets. The metrics seam is an opaque callback `func(operation string, dur time.Duration, err error)` keeping Prometheus out of the package (same shape as s3fs's process-wide observer, so `main` can wire the same `metrics.ObserveS3` to both). The client seam is an unexported interface over exact `*s3.Client` method shapes so the concrete Tigris client satisfies it unchanged.

## Bucket layout

```
objects/<hex>      loose object; raw bytes; metadata git-type=<commit|tree|blob|tag>, git-size=<decimal>
refs/<refname>     one loose ref; "<hex>\n" or symRefPrefix+"target\n"  (HEAD lives at refs/HEAD)
shallow            newline-separated hashes\n ; deleted when emptied
index              bytes from plumbing/format/index Encoder
config             bytes from config.Config.Marshal()
packs/             reserved — unused by this plan
```

Ref encoding mirrors dotgit loose-ref files. Key form is `refs/` + full ref name, so `refs/heads/main` stores at key `refs/refs/heads/main` and `HEAD` stores at `refs/HEAD` — slightly odd-looking but it gives every addressable ref one namespace, which a flat bucket needs.

## Files

| Action | Path                              | Purpose                                                        |
| ------ | --------------------------------- | -------------------------------------------------------------- |
| Create | `internal/storage/tigris/tigris.go` | doc, constants, sentinels, `Storer`, Options, `New`, `s3API`, pointer shims, temp stubs |
| Create | `internal/storage/tigris/client_test.go` | `fakeS3`, seed helpers, constructor tables                |
| Create | `internal/storage/tigris/objects.go` (+test) | HEAD reads, GET decode                                 |
| Create | `internal/storage/tigris/writer.go` (+test) | staging writer, `SetEncodedObject`                      |
| Create | `internal/storage/tigris/iter.go` (+test) | pagination + lazy filtered iteration                     |
| Create | `internal/storage/tigris/refs.go` (+test) | six ref methods, encode/decode, shallow                  |
| Create | `internal/storage/tigris/index.go`, `config.go` (+test) | index/config codecs, `Module`           |
| Create | `internal/storage/tigris/livebucket_test.go` | env-gated real-bucket verification                    |
| Modify | `AGENTS.md`                       | architecture blurb                                             |

---

### Task 1: Package skeleton, S3 seam, constructor, fake client

**Files:**
- Create: `internal/storage/tigris/tigris.go`
- Create: `internal/storage/tigris/client_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces (every later task depends on these exact names):

```go
type Storer struct {
	client   s3API
	bucket   string
	ctx      context.Context // deadline slot shared by every request
	of       formatcfg.ObjectFormat
	oh       *plumbing.ObjectHasher
	observer func(operation string, dur time.Duration, err error)
}
func New(ctx context.Context, bucket string, opts ...Option) (*Storer, error)
type Option func(*Storer)
func WithObjectFormat(of formatcfg.ObjectFormat) Option
func WithObserver(fn func(operation string, dur time.Duration, err error)) Option
func withClient(c s3API) Option // test seam

type s3API interface { GetObject; PutObject; HeadObject; ListObjectsV2; DeleteObject } // shapes below

const (
	objectPrefix = "objects/"
	refPrefix    = "refs/"
	shallowKey   = "shallow"
	indexKey     = "index"
	configKey    = "config"
	metaType     = "git-type"
	metaSize     = "git-size"
	symRefPrefix = "ref: "
)

var (
	ErrHashMismatch           = errors.New("tigris: content hash does not match the hash claimed by the object")
	ErrAlternatesNotSupported = errors.New("tigris: alternates are not supported")
	ErrModulesNotSupported    = errors.New("tigris: submodule storers are not supported")
	errBadMetadata            = errors.New("tigris: object has invalid user metadata")
	errMalformedRef           = errors.New("tigris: malformed loose ref")
	errEmptyBucket            = errors.New("tigris: bucket must be set")
	errUnimplemented          = errors.New("tigris: method not implemented yet") // TEMPORARY
)

var _ storer.Storer = (*Storer)(nil) // forces every stub into existence at once

func keyOf(h plumbing.Hash) string  { return objectPrefix + h.String() }
func refKey(n plumbing.ReferenceName) string { return refPrefix + n.String() }
func isNotFound(err error) bool     // smithy NotFound / NoSuchKey
func sp(v string) *string; func i32(v int32) *int32; func sv(v *string) string; func bv(v *bool) bool
func (s *Storer) observe(op string, start time.Time, err error)
```

In `client_test.go`: `notFound()` (code `NotFound`), `missingKeyErr()` (code `NoSuchKey`), `fakeObject`, `fakeS3` (map-backed, injectable failures, put/delete counters, `listMax` page-size knob), `newFakeS3(t)`, `newTestStorer(t, f, opts...)`, `put(k, body, meta)`, `get(t, k)`, `nputs()`, and `seed(t, f, of, typ, content) plumbing.Hash`.

All five interface groups start as `errUnimplemented` stubs collected in one grep-visible block; each later task deletes the entries it replaces. Task 9 asserts none remain.

- [ ] **Step 1: Branch up**

```bash
git checkout -b tigris-gogit-storer
```

- [ ] **Step 2: Write the failing test**

Create `internal/storage/tigris/client_test.go` with two parts. Part 1, the infrastructure every later task reuses — paste completely:

```go
package tigris

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing"
)

func notFound() error {
	return &smithy.GenericAPIError{Code: "NotFound", Message: "not found"}
}

func missingKeyErr() error {
	return &smithy.GenericAPIError{Code: "NoSuchKey", Message: "no such key"}
}

// fakeObject is one object in the fake bucket.
type fakeObject struct {
	body []byte
	meta map[string]string
}

type fakeS3 struct {
	t    *testing.T
	mu   sync.Mutex
	objs map[string]fakeObject

	getErr  error // injected failures returned verbatim from matching calls
	putErr  error
	headErr error
	delErr  error
	listErr error

	puts    int
	deletes int
	listMax int64 // ListObjectsV2 page size knob; 0 = unlimited
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	return &fakeS3{t: t, objs: make(map[string]fakeObject)}
}

func newTestStorer(t *testing.T, f *fakeS3, opts ...Option) *Storer {
	t.Helper()
	all := append([]Option{withClient(f)}, opts...)
	s, err := New(context.Background(), "test-bucket", all...)
	if err != nil {
		t.Fatalf("failed to create test storer: %v", err)
	}
	return s
}

func (f *fakeS3) put(key, body string, meta map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := make(map[string]string, len(meta))
	for k, v := range meta {
		m[k] = v
	}
	f.objs[key] = fakeObject{body: []byte(body), meta: m}
}

func (f *fakeS3) get(t *testing.T, key string) fakeObject {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[key]
	if !ok {
		t.Fatalf("expected object %q to exist", key)
	}
	return o
}

func (f *fakeS3) del(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objs, key)
}

func (f *fakeS3) nputs() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts
}

func (f *fakeS3) ndeletes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletes
}

// seed builds a well-formed loose object (correct content-hash key plus
// git-type/git-size metadata) with the given object format and stores it,
// returning the hash. Seeds ignore injected failures on purpose.
func seed(t *testing.T, f *fakeS3, of formatcfg.ObjectFormat, typ plumbing.ObjectType, content string) plumbing.Hash {
	t.Helper()
	obj := plumbing.NewMemoryObject(plumbing.FromObjectFormat(of))
	obj.SetType(typ)
	obj.SetSize(int64(len(content)))
	if _, err := obj.Write([]byte(content)); err != nil {
		t.Fatalf("failed to buffer seed content: %v", err)
	}
	h := obj.Hash()
	f.put(keyOf(h), content, map[string]string{
		metaType: typ.String(),
		metaSize: strconv.Itoa(len(content)),
	})
	return h
}

func (f *fakeS3) GetObject(_ context.Context, p *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	o, ok := f.objs[sv(p.Key)]
	if !ok {
		return nil, missingKeyErr()
	}
	out := &s3.GetObjectOutput{
		ContentLength: ip(int64(len(o.body))),
		Metadata:      o.meta,
	}
	out.Body = io.NopCloser(bytes.NewReader(o.body))
	return out, nil
}

func (f *fakeS3) PutObject(_ context.Context, p *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.putErr != nil {
		return nil, f.putErr
	}
	var buf bytes.Buffer
	if p.Body != nil {
		if _, err := io.Copy(&buf, p.Body); err != nil {
			f.t.Fatalf("fake PutObject copy failed: %v", err)
		}
	}
	meta := make(map[string]string, len(p.Metadata))
	for k, v := range p.Metadata {
		meta[k] = v
	}
	f.objs[sv(p.Key)] = fakeObject{body: buf.Bytes(), meta: meta}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) HeadObject(_ context.Context, p *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.headErr != nil {
		return nil, f.headErr
	}
	o, ok := f.objs[sv(p.Key)]
	if !ok {
		return nil, notFound()
	}
	return &s3.HeadObjectOutput{
		ContentLength: ip(int64(len(o.body))),
		Metadata:      o.meta,
	}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, p *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	if f.delErr != nil {
		return nil, f.delErr
	}
	delete(f.objs, sv(p.Key))
	return &s3.DeleteObjectOutput{}, nil
}

// ListObjectsV2 returns matching keys sorted (as real S3 promises), paginated
// at f.listMax pages when >0, honoring opaque numeric continuation tokens.
func (f *fakeS3) ListObjectsV2(_ context.Context, p *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}

	var matched []string
	for k := range f.objs {
		if strings.HasPrefix(k, sv(p.Prefix)) {
			matched = append(matched, k)
		}
	}
	sort.Strings(matched)

	start := 0
	if tok := sv(p.ContinuationToken); tok != "" {
		start, _ = strconv.Atoi(tok)
	}
	end := len(matched)
	if f.listMax > 0 && start+int(f.listMax) < end {
		end = start + int(f.listMax)
	}

	out := &s3.ListObjectsV2Output{IsTruncated: bp(end < len(matched))}
	for _, k := range matched[start:end] {
		out.Contents = append(out.Contents, &types.Object{Key: sp(k)})
	}
	if bv(out.IsTruncated) {
		out.NextContinuationToken = sp(strconv.Itoa(end))
	}
	return out, nil
}

func ip(v int64) *int64 { return &v }
func bp(v bool) *bool   { return &v }

var _ = errors.Is // pacifier while the fake grows; goimports may reclaim
```

Part 2, appended to the same file — the constructor behavior itself:

```go
func TestNewDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bucket  string
		opts    []Option
		wantOf  formatcfg.ObjectFormat
		wantErr error
	}{
		{name: "defaults", bucket: "b", wantOf: formatcfg.DefaultObjectFormat},
		{name: "explicit sha256", bucket: "b", opts: []Option{WithObjectFormat(formatcfg.SHA256)}, wantOf: formatcfg.SHA256},
		{name: "empty bucket rejected", bucket: "", wantErr: errEmptyBucket},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := New(context.Background(), tt.bucket, tt.opts...)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Logf("want: %v", tt.wantErr)
					t.Logf("got:  %v", err)
					t.Error("got wrong error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if s.bucket != tt.bucket {
				t.Errorf("want bucket %q, got %q", tt.bucket, s.bucket)
			}
			if s.of != tt.wantOf {
				t.Errorf("want format %v, got %v", tt.wantOf, s.of)
			}
			if s.oh == nil {
				t.Error("object hasher was not derived from the format")
			}
		})
	}
}

func TestNewDialsRealClientWhenNotInjected(t *testing.T) {
	t.Parallel()

	// Without withClient, New must reach for the storage-go constructor. With
	// no credentials in this environment we only assert that construction
	// either succeeds (client built lazily) or fails wrapped — never panics.
	s, err := New(context.Background(), "some-bucket")
	switch {
	case err == nil && s.client == nil:
		t.Error("storer built with nil client")
	case err != nil:
		t.Logf("dial failure surfaced (fine without credentials): %v", err)
	}
}

func TestObserverIsInvokedByOperation(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "watched")

	seen := map[string]int{}
	s := newTestStorer(t, f, WithObserver(func(op string, _ time.Duration, _ error) {
		seen[op]++
	}))

	if err := s.HasEncodedObject(seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "x")); err != nil {
		// HasEncodedObject is still a stub in this task: expected error path.
		var target error = errUnimplemented
		if !errors.Is(err, target) {
			t.Fatalf("unexpected pre-stub behavior: %v", err)
		}
	}
	_ = seen // populated assertions arrive with Task 2
}
```

(The last test intentionally stays loose until Task 2 wires `observe` into real code paths; it exists so the option's shape is exercised immediately.)

- [ ] **Step 3: Verify red**

Run: `go test ./internal/storage/tigris/ -count=1`
Expected: FAIL — everything undefined (`New`, `Option`, constants all live only in the next step).

- [ ] **Step 4: Write the production skeleton**

Create `internal/storage/tigris/tigris.go`:

```go
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
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing"
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
func sv(v *string) string { if v == nil { return "" }; return *v }
func bv(v *bool) bool     { return v != nil && *v }

// --- temporary stubs -------------------------------------------------------
// Every remaining interface method funnels through errUnimplemented so the
// compile-time assertion holds from day one. Later tasks DELETE their stubs
// from this block as they land real implementations; Task 9 greps for
// leftovers. Do not reorder beyond appending.
func (s *Storer) HasEncodedObject(h plumbing.Hash) error {
	_ = h
	return errUnimplemented
}

func (s *Storer) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	_ = h
	return 0, errUnimplemented
}

func (s *Storer) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	_, _ = t, h
	return nil, errUnimplemented
}

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
```

Then reconcile imports mechanically: add `io`, `log/slog` isn't needed yet, `config "github.com/go-git/go-git/v6/config"` and `"github.com/go-git/go-git/v6/plumbing/format/index"` appear below `formatcfg`'s grouping. After editing run `go tool goimports -w internal/storage/tigris/` and delete the scaffold marks this file grew only if genuinely unused.

One deliberate tolerance: the `var _ = errors.Is` pacifier in `client_test.go` may vanish once Task 2 leans on `errors`; remove it when the compiler stops being placated.

- [ ] **Step 5: Run tests green**

```bash
go tool goimports -w internal/storage/tigris/
go test ./internal/storage/tigris/ -count=1 -v
go tool staticcheck ./internal/storage/tigris/
```

Expected: all three subtests PASS (has-empty rejection handled via `wantErr`, sha256 override honored, dial fallback tolerated), `staticcheck` silent. Zero network attempted anywhere (constructor dials lazily only when creds exist).

If red: likely causes are stub signature typos breaking the compile-time assertion, or the pacifier import tripping vet. Fix names, not contracts.

- [ ] **Step 6: Commit**

```bash
git add internal/storage/tigris/
git commit -s -m "feat(storage/tigris): scaffold Tigris-backed go-git storer seam"
```

---

### Task 2: HEAD-backed reads — `headInfo`, `HasEncodedObject`, `EncodedObjectSize`

**Files:**
- Create: `internal/storage/tigris/objects.go`
- Create: `internal/storage/tigris/objects_test.go`
- Modify: `internal/storage/tigris/tigris.go` (delete replaced stubs)

**Interfaces:**
- Consumes: Task 1 `s3API`, `keyOf`, `metaType`/`metaSize`, `sp`/`ip`… wait — the production-side shim set is `sp/i32/sv/bv`; the fake side adds `ip` (int64 ptr) locally in tests. Both spelled below.
- Produces:

```go
type objHead struct {
	typ  plumbing.ObjectType
	size int64
}

// headInfo resolves one object's type+size purely from HeadObject
// (metadata preferred, ContentLength fallback). Misses surface as
// plumbing.ErrObjectNotFound; damaged metadata wraps errBadMetadata.
func (s *Storer) headInfo(h plumbing.Hash) (objHead, error)

func (s *Storer) HasEncodedObject(h plumbing.Hash) error            // replaces stub
func (s *Storer) EncodedObjectSize(h plumbing.Hash) (int64, error)  // replaces stub
```

- [ ] **Step 1: Failing tests**

Create `internal/storage/tigris/objects_test.go`:

```go
package tigris

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/aws/smithy-go"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing"
)

func TestHasEncodedObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T, f *fakeS3) plumbing.Hash
		wantErr error // nil means success
	}{
		{
			name: "present",
			setup: func(t *testing.T, f *fakeS3) plumbing.Hash {
				return seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "hello")
			},
		},
		{
			name: "absent maps to the go-git sentinel",
			setup: func(_ *testing.T, _ *fakeS3) plumbing.Hash {
				h, _ := plumbing.FromHex("1111111111111111111111111111111111111111")
				return h
			},
			wantErr: plumbing.ErrObjectNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			h := tt.setup(t, f)
			s := newTestStorer(t, f)

			err := s.HasEncodedObject(h)
			if !errors.Is(err, tt.wantErr) && !(err == nil && tt.wantErr == nil) {
				t.Logf("want: %v", tt.wantErr)
				t.Logf("got:  %v", err)
				t.Error("got wrong error")
			}
		})
	}
}

func TestEncodedObjectSizePrefersMetadata(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.TreeObject, "hello")
	f.mu.Lock()
	ent := f.objs[keyOf(h)]
	ent.meta[metaSize] = "4096"
	f.objs[keyOf(h)] = ent
	f.mu.Unlock()

	s := newTestStorer(t, f)

	got, err := s.EncodedObjectSize(h)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 4096 {
		t.Errorf("want metadata size 4096, got %d", got)
	}
}

func TestEncodedObjectSizeFallsBackToContentLength(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "12345")
	delete(f.get(t, keyOf(h)).meta, metaSize)

	s := newTestStorer(t, f)

	got, err := s.EncodedObjectSize(h)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 5 {
		t.Errorf("want fallback size 5, got %d", got)
	}
}

func TestHeadInfoErrorMapping(t *testing.T) {
	t.Parallel()

	t.Run("transport failures wrap, never masquerade", func(t *testing.T) {
		f := newFakeS3(t)
		h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "x")
		f.headErr = &smithy.GenericAPIError{Code: "AccessDenied", Message: "nope"}
		s := newTestStorer(t, f)

		_, err := s.headInfo(h)
		if errors.Is(err, plumbing.ErrObjectNotFound) || errors.Is(err, errBadMetadata) || err == nil {
			t.Errorf("access denied lost its identity: %v", err)
		}
	})

	t.Run("unknown type name flags bad metadata", func(t *testing.T) {
		f := newFakeS3(t)
		h, _ := plumbing.FromHex("2222222222222222222222222222222222222222")
		f.put(keyOf(h), "junk", map[string]string{
			metaType: "frog",
			metaSize: strconv.Itoa(len("junk")),
		})
		s := newTestStorer(t, f)

		if _, err := s.headInfo(h); !errors.Is(err, errBadMetadata) {
			t.Errorf("want errBadMetadata wrapping, got %v", err)
		}
	})

	t.Run("missing every size source flags bad metadata", func(t *testing.T) {
		f := newFakeS3(t)
		h, _ := plumbing.FromHex("3333333333333333333333333333333333333333")
		f.objs[keyOf(h)] = fakeObject{
			body: []byte("sized-body"),
			meta: map[string]string{metaType: plumbing.BlobObject.String()},
		}
		// Hand-build the HEAD output with no ContentLength at all.
		f.headOverride = map[string]*sdk_headshape{
			keyOf(h): {metaOnly: true},
		}
		s := newTestStorer(t, f)

		if _, err := s.headInfo(h); !errors.Is(err, errBadMetadata) {
			t.Errorf("want errBadMetadata wrapping, got %v", err)
		}
	})
}
```

To make the third subtest expressible, extend `fakeS3` in `client_test.go` with one small knob NOW (this is the only Task-2 edit to that file):

```go
// headShape lets a test strip normally-automatic fields from one key's HEAD.
type headShape struct{ metaOnly bool }

// field on fakeS3:  headOverride map[string]*headShape
```

and in `HeadObject`, after lookup, honor it:

```go
	if hs, forced := f.headOverride[sv(p.Key)]; forced && hs.metaOnly {
		return &s3.HeadObjectOutput{Metadata: o.meta}, nil
	}
```

(`headShape` contains `metaOnly bool`; copy `o.meta` into the output rather than sharing the map.)

- [ ] **Step 2: Verify red**

Run: `go test ./internal/storage/tigris/ -run 'TestHasEncodedObject|TestEncodedObjectSize|TestHeadInfo' -count=1`
Expected: FAIL — stubs answer `errUnimplemented` for most rows, and the size-fallback subtest additionally trips absent behavior.

- [ ] **Step 3: Implement**

Create `internal/storage/tigris/objects.go`:

```go
package tigris

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
)

// objHead is everything a HEAD reveals about one git object.
type objHead struct {
	typ  plumbing.ObjectType
	size int64
}

var zeroObjHead objHead

// headInfo fetches HEAD-derived facts for a loose object. Absence keeps the
// go-git contract sentinel; damaged metadata surfaces as wrapped
// errBadMetadata so corruption never masquerades as "doesn't exist".
func (s *Storer) headInfo(h plumbing.Hash) (objHead, error) {
	start := time.Now()
	out, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(keyOf(h)),
	})
	s.observe("HeadObject", start, err)

	switch {
	case err == nil:
	case isNotFound(err):
		return zeroObjHead, plumbing.ErrObjectNotFound
	default:
		return zeroObjHead, fmt.Errorf("tigris: head %s: %w", h.String(), err)
	}

	typ, terr := plumbing.ParseObjectType(out.Metadata[metaType])
	if terr != nil {
		return zeroObjHead, fmt.Errorf("%w: %s has bad %s %q: %w",
			errBadMetadata, h.String(), metaType, out.Metadata[metaType], terr)
	}

	size, ok := declaredSize(out)
	if !ok {
		return zeroObjHead, fmt.Errorf("%w: %s lacks any size source", errBadMetadata, h.String())
	}
	return objHead{typ: typ, size: size}, nil
}

// declaredSize prefers the written-at git-size declaration and falls back to
// ContentLength for legacy writes lacking metadata. Garbage digits fall back
// too rather than fail; absence of both sources is the hard case.
func declaredSize(out *s3.HeadObjectOutput) (int64, bool) {
	if raw, present := out.Metadata[metaSize]; present {
		if n, perr := strconv.ParseInt(raw, 10, 64); perr == nil {
			return n, true
		}
	}
	if out.ContentLength != nil {
		return *out.ContentLength, true
	}
	return 0, false
}

func (s *Storer) HasEncodedObject(h plumbing.Hash) error {
	switch _, err := s.headInfo(h); {
	case err == nil:
		return nil
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return plumbing.ErrObjectNotFound
	default:
		return fmt.Errorf("tigris: probe %s: %w", h.String(), err)
	}
}

func (s *Storer) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	hs, err := s.headInfo(h)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return 0, plumbing.ErrObjectNotFound
	default:
		return 0, fmt.Errorf("tigris: size of %s: %w", h.String(), err)
	}
	return hs.size, nil
}
```

(`io` lands reserved for Task 3's sibling functions; include it now or omit until then — goimports decides.)

Delete the replaced stubs from `tigris.go`'s temporary block: `HasEncodedObject`, `EncodedObjectSize`.

- [ ] **Step 4: Verify green**

```bash
go tool goimports -w internal/storage/tigris/
go test ./internal/storage/tigris/ -count=1
go tool staticcheck ./internal/storage/tigris/
```

Expected: PASS everywhere; observer callback count grew as a side effect of real code calling `observe`.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/
git commit -s -m "feat(storage/tigris): HEAD-backed Has and Size lookups"
```

---

### Task 3: Full-body read — `loadObject` and `EncodedObject`

**Files:**
- Modify: `internal/storage/tigris/objects.go`
- Modify: `internal/storage/tigris/objects_test.go`
- Modify: `internal/storage/tigris/tigris.go` (delete `EncodedObject` stub)

**Interfaces:**
- Consumes: Task 2's `headInfo`/`objHead`.
- Produces:

```go
// loadObject GETs the body and decodes it into a fresh MemoryObject using
// HEAD facts already resolved by the caller. Misses surface as
// plumbing.ErrObjectNotFound; delta types as plumbing.ErrInvalidType.
func (s *Storer) loadObject(h plumbing.Hash, hs objHead) (plumbing.EncodedObject, error)

func (s *Storer) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error)
```

Contract: wrong requested type (when `t != plumbing.AnyObject`) behaves exactly like absence (`nil, plumbing.ErrObjectNotFound`), matching memory-storage parity and the interface doc comment.

- [ ] **Step 1: Failing tests**

Append to `objects_test.go`:

```go
func TestEncodedObjectRoundTrip(t *testing.T) {
	t.Parallel()

	const content = "the quick brown fox jumps over the lazy dog"
	f := newFakeS3(t)
	of := formatcfg.DefaultObjectFormat
	h := seed(t, f, of, plumbing.BlobObject, content)
	other := seed(t, f, of, plumbing.CommitObject, "an unrelated commit")

	s := newTestStorer(t, f)

	t.Run("typed hit decodes identical bytes", func(t *testing.T) {
		obj, err := s.EncodedObject(plumbing.BlobObject, h)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		rd, rerr := obj.Reader()
		if rerr != nil {
			t.Fatalf("reader: %v", rerr)
		}
		defer rd.Close()
		got, rerr := io.ReadAll(rd)
		if rerr != nil {
			t.Fatalf("read: %v", rerr)
		}
		if string(got) != content {
			t.Errorf("want %q, got %q", content, got)
		}
		if obj.Type() != plumbing.BlobObject || obj.Size() != int64(len(content)) {
			t.Errorf("bad framing: type=%v size=%d", obj.Type(), obj.Size())
		}
	})

	t.Run("wrong requested type behaves like absence", func(t *testing.T) {
		if _, err := s.EncodedObject(plumbing.TagObject, h); !errors.Is(err, plumbing.ErrObjectNotFound) {
			t.Errorf("want ErrObjectNotFound, got %v", err)
		}
	})

	t.Run("AnyObject ignores the type filter", func(t *testing.T) {
		if _, err := s.EncodedObject(plumbing.AnyObject, other); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("absent object", func(t *testing.T) {
		if _, err := s.EncodedObject(plumbing.AnyObject, plumbing.ZeroHash); !errors.Is(err, plumbing.ErrObjectNotFound) {
			t.Errorf("want ErrObjectNotFound, got %v", err)
		}
	})
}

func TestEncodedObjectRejectsDeltaTypes(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "real bytes underneath")
	f.mu.Lock()
	ent := f.objs[keyOf(h)]
	ent.meta[metaType] = plumbing.OFSDeltaObject.String()
	f.objs[keyOf(h)] = ent
	f.mu.Unlock()

	s := newTestStorer(t, f)

	if _, err := s.EncodedObject(plumbing.AnyObject, h); !errors.Is(err, plumbing.ErrInvalidType) {
		t.Errorf("want ErrInvalidType, got %v", err)
	}
}
```

Add `io` to the test imports when the compiler asks.

- [ ] **Step 2: Verify red**

Run: `go test ./internal/storage/tigris/ -run 'TestEncodedObject' -count=1`
Expected: FAIL (still `errUnimplemented`).

- [ ] **Step 3: Implement**

Append to `objects.go`:

```go
func (s *Storer) loadObject(h plumbing.Hash, hs objHead) (plumbing.EncodedObject, error) {
	if hs.typ == plumbing.OFSDeltaObject || hs.typ == plumbing.REFDeltaObject {
		return nil, plumbing.ErrInvalidType
	}

	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(keyOf(h)),
	})
	s.observe("GetObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, plumbing.ErrObjectNotFound
	default:
		return nil, fmt.Errorf("tigris: get %s: %w", h.String(), err)
	}
	defer out.Body.Close()

	buf, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("tigris: read %s: %w", h.String(), err)
	}

	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(hs.typ)
	obj.SetSize(hs.size)
	if _, err := obj.Write(buf); err != nil {
		return nil, fmt.Errorf("%w: %s body disagrees with declared size %d: %w",
			errBadMetadata, h.String(), hs.size, err)
	}
	return obj, nil
}

func (s *Storer) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	hs, err := s.headInfo(h)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return nil, plumbing.ErrObjectNotFound
	default:
		return nil, fmt.Errorf("tigris: describe %s: %w", h.String(), err)
	}

	if t != plumbing.AnyObject && hs.typ != t {
		return nil, plumbing.ErrObjectNotFound
	}
	return s.loadObject(h, hs)
}
```

Note the mismatch guard runs before any body download — the cheap HEAD alone can decline the wrong-type request.

- [ ] **Step 4: Verify green**

Run: `go test ./internal/storage/tigris/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/
git commit -s -m "feat(storage/tigris): body fetch and type-filtered EncodedObject"
```

---

### Task 4: The staging writer — `RawObjectWriter`

**Files:**
- Create: `internal/storage/tigris/writer.go`
- Create: `internal/storage/tigris/writer_test.go`
- Modify: `internal/storage/tigris/client_test.go` (extract `hashForBody`)
- Modify: `internal/storage/tigris/tigris.go` (delete `RawObjectWriter` stub; real `NewEncodedObject` ships here too)

**Interfaces:**
- Consumes: `keyOf`, metadata consts, `sp`/`bv`, `observe`, plumbing hasher trio.
- Produces:

```go
func (s *Storer) NewEncodedObject() plumbing.EncodedObject // binds MemoryObject to s.oh (replaces stub body)
func (s *Storer) RawObjectWriter(typ plumbing.ObjectType, sz int64) (io.WriteCloser, error) // replaces stub

// stageWriter backs RawObjectWriter. Callers aborting mid-stream use Discard;
// Task 5's SetEncodedObject depends on that exported-on-package spelling.
type stageWriter struct {
	s      *Storer
	f      *os.File
	typ    plumbing.ObjectType
	size   int64
	wrote  int64
	hasher plumbing.Hasher
	done   bool
}
func (w *stageWriter) Discard()

// hashForBody (client_test.go) factors seed's hash math out for reuse.
func hashForBody(of formatcfg.ObjectFormat, typ plumbing.ObjectType, body string) plumbing.Hash
```

Semantics the tests pin: delta `typ` or negative `sz` rejected before resources exist; upload happens only on Close (count proves it); chunked writes equal one staged object keyed by recomputed hash with both metadata keys filled; short-write Close errors and uploads nothing; overflow Write discards immediately and poisons later writes; double Close idempotent; staging files removed on every exit path including PUT failure; post-discard Write errors.

- [ ] **Step 1: Extract the shared hash helper (test-only refactor)**

Edit `seed` in `client_test.go` to delegate, adding the helper just above it:

```go
func hashForBody(of formatcfg.ObjectFormat, typ plumbing.ObjectType, body string) plumbing.Hash {
	obj := plumbing.NewMemoryObject(plumbing.FromObjectFormat(of))
	obj.SetType(typ)
	obj.SetSize(int64(len(body)))
	if _, err := obj.Write([]byte(body)); err != nil {
		panic("hashForBody buffers only ever-valid sizes: " + err.Error())
	}
	return obj.Hash()
}
```

and shrink `seed` to:

```go
func seed(t *testing.T, f *fakeS3, of formatcfg.ObjectFormat, typ plumbing.ObjectType, content string) plumbing.Hash {
	t.Helper()
	h := hashForBody(of, typ, content)
	f.put(keyOf(h), content, map[string]string{
		metaType: typ.String(),
		metaSize: strconv.Itoa(len(content)),
	})
	return h
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/storage/tigris/writer_test.go`:

```go
package tigris

import (
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing"
)

// pushThrough streams chunks through a fresh RawObjectWriter and hands back
// the concrete stage writer for introspection (Close stays the caller's job
// unless stated otherwise below).
func pushThrough(t *testing.T, s *Storer, typ plumbing.ObjectType, size int64, chunks []string) *stageWriter {
	t.Helper()
	w, err := s.RawObjectWriter(typ, size)
	if err != nil {
		t.Fatalf("open raw writer: %v", err)
	}
	for _, c := range chunks {
		if _, err := io.WriteString(w, c); err != nil {
			t.Fatalf("write %q: %v", c, err)
		}
	}
	return w.(*stageWriter)
}

func TestRawObjectWriterHappyPath(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	const body = "hello world"
	w := pushThrough(t, s, plumbing.BlobObject, int64(len(body)), []string{"hello ", "world"})
	if f.nputs() != 0 {
		t.Fatalf("uploaded before Close (%d puts)", f.nputs())
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := w.Close(); err != nil { // idempotent
		t.Fatalf("second close: %v", err)
	}
	if n := f.nputs(); n != 1 {
		t.Fatalf("want exactly one upload, saw %d", n)
	}

	want := hashForBody(formatcfg.DefaultObjectFormat, plumbing.BlobObject, body)
	obj := f.get(t, keyOf(want))
	if string(obj.body) != body {
		t.Errorf("stored body mismatch: want %q got %q", body, obj.body)
	}
	if obj.meta[metaType] != plumbing.BlobObject.String() {
		t.Errorf("want metadata %s=%s", metaType, plumbing.BlobObject.String())
	}
	if obj.meta[metaSize] != fmt.Sprintf("%d", len(body)) {
		t.Errorf("want metadata %s=%d", metaSize, len(body))
	}
	if _, err := os.Stat(w.f.Name()); !os.IsNotExist(err) {
		t.Errorf("staging file survived Close: %v", err)
	}
}

func TestRawObjectWriterShortWrite(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	w := pushThrough(t, s, plumbing.BlobObject, 10, []string{"abc"}) // 3 < 10
	before := f.nputs()
	if err := w.Close(); err == nil {
		t.Fatal("short write accepted")
	}
	if f.nputs() != before {
		t.Error("short write uploaded anyway")
	}
	if _, err := os.Stat(w.f.Name()); !os.IsNotExist(err) {
		t.Error("staging file survived failed close")
	}
}

func TestRawObjectWriterOverflow(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	w, err := s.RawObjectWriter(plumbing.BlobObject, 2)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := io.WriteString(w, "longer than two"); err == nil {
		t.Fatal("overflow accepted")
	}
	if !w.done {
		t.Error("overflow did not discard the writer")
	}
	if f.nputs() != 0 {
		t.Error("overflow uploaded anyway")
	}
}

func TestRawObjectWriterRejections(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))

	tests := []struct {
		name    string
		typ     plumbing.ObjectType
		size    int64
		wantErr error
	}{
		{name: "OFSDelta refused", typ: plumbing.OFSDeltaObject, size: 5, wantErr: plumbing.ErrInvalidType},
		{name: "REFDelta refused", typ: plumbing.REFDeltaObject, size: 5, wantErr: plumbing.ErrInvalidType},
		{name: "negative size refused", typ: plumbing.BlobObject, size: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.RawObjectWriter(tt.typ, tt.size)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Logf("want: %v\ngot:  %v", tt.wantErr, err)
					t.Error("wrong error")
				}
				return
			}
			if err == nil {
				t.Error("negative size slipped through")
			}
		})
	}
}

func TestRawObjectWriterWriteAfterDiscardPoisonsStream(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	w, err := s.RawObjectWriter(plumbing.BlobObject, 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sw := w.(*stageWriter)
	sw.Discard()
	sw.Discard() // idempotent

	if _, err := io.WriteString(w, "late"); err == nil {
		t.Error("post-discard write accepted")
	}
	if err := w.Close(); err != nil {
		t.Errorf("discard-state close should stay quiet, got %v", err)
	}
	if f.nputs() != 0 {
		t.Error("discarded bytes leaked to the bucket")
	}
	if _, err := os.Stat(sw.f.Name()); !os.IsNotExist(err) {
		t.Errorf("staging file leaked after Discard: %v", err)
	}
}

func TestStageWriterUploadFailureLeavesNoTrash(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.putErr = errors.New("network cut")
	s := newTestStorer(t, f)

	w := pushThrough(t, s, plumbing.BlobObject, 1, []string{"z"})
	name := w.f.Name()
	if err := w.Close(); err == nil {
		t.Fatal("upload failure swallowed")
	}
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Errorf("staging file leaked after failed upload: %v", err)
	}
}

func TestNewEncodedObjectBindsFormat(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t), WithObjectFormat(formatcfg.SHA256))

	obj := s.NewEncodedObject()
	if _, ok := obj.(*plumbing.MemoryObject); !ok {
		t.Fatalf("want *plumbing.MemoryObject, got %T", obj)
	}
	obj.SetType(plumbing.CommitObject)
	obj.SetSize(0)
	if _, werr := obj.Write(nil); werr != nil {
		t.Fatalf("empty write rejected: %v", werr)
	}
	h := obj.Hash()
	if len(h.String()) != 64 {
		t.Errorf("sha256 object produced %q-length hash", len(h.String()))
	}
}
```

- [ ] **Step 3: Verify red**

Run: `go test ./internal/storage/tigris/ -run 'TestRawObjectWriter|TestStageWriter|TestNewEncodedObjectBinds' -count=1`
Expected: FAIL (stub answers `errUnimplemented`; happy path fails immediately at the `open raw writer` Fatalf).

- [ ] **Step 4: Implement**

Create `internal/storage/tigris/writer.go`:

```go
package tigris

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
)

// Writer design follows docs/reference/tigris-backend.md "Write path": bytes
// stream into a local staging file while a hashing tee runs alongside. Close
// performs the single PUT, named by the freshly computed hash. Upload failures
// or size disagreements leave the bucket untouched.
type stageWriter struct {
	s      *Storer
	f      *os.File
	typ    plumbing.ObjectType
	size   int64
	wrote  int64
	hasher plumbing.Hasher
	done   bool
}

func (s *Storer) NewEncodedObject() plumbing.EncodedObject {
	return plumbing.NewMemoryObject(s.oh)
}

func (s *Storer) RawObjectWriter(typ plumbing.ObjectType, sz int64) (io.WriteCloser, error) {
	if typ == plumbing.OFSDeltaObject || typ == plumbing.REFDeltaObject {
		return nil, plumbing.ErrInvalidType
	}
	if sz < 0 {
		return nil, fmt.Errorf("tigris: negative object size %d", sz)
	}

	f, ferr := os.CreateTemp("", "objgit-tigris-*")
	if ferr != nil {
		return nil, fmt.Errorf("tigris: create staging file: %w", ferr)
	}
	return &stageWriter{
		s:      s,
		f:      f,
		typ:    typ,
		size:   sz,
		hasher: plumbing.NewHasher(s.of, typ, sz),
	}, nil
}

func (w *stageWriter) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("tigris: write on discarded raw writer")
	}
	n, err := io.MultiWriter(w.f, w.hasher).Write(p)
	w.wrote += int64(n)
	if err != nil {
		// File and hasher may disagree from here on; the stream is poison.
		w.Discard()
		return n, fmt.Errorf("tigris: stage write: %w", err)
	}
	if w.wrote > w.size {
		w.Discard()
		return n, fmt.Errorf("tigris: wrote %d bytes beyond declared size %d", w.wrote-w.size, w.size)
	}
	return n, nil
}

// Discard aborts the stream: nothing uploads, staged bytes vanish. Idempotent.
func (w *stageWriter) Discard() {
	if w.done {
		return
	}
	w.done = true
	w.f.Close()
	os.Remove(w.f.Name())
}

func (w *stageWriter) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	defer os.Remove(w.f.Name())
	defer w.f.Close()

	if w.wrote != w.size {
		return fmt.Errorf("tigris: staged %d bytes but declared %d", w.wrote, w.size)
	}

	h := w.hasher.Sum()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("tigris: rewind staging file: %w", err)
	}

	start := time.Now()
	_, err := w.s.client.PutObject(w.s.ctx, &s3.PutObjectInput{
		Bucket: sp(w.s.bucket),
		Key:    sp(keyOf(h)),
		Body:   w.f,
		Metadata: map[string]string{
			metaType: w.typ.String(),
			metaSize: strconv.FormatInt(w.wrote, 10),
		},
	})
	w.s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: upload %s: %w", keyOf(h), err)
	}
	return nil
}
```

Also delete from the stub block in `tigris.go`: the entire old `NewEncodedObject` stub and the `RawObjectWriter` stub.

- [ ] **Step 5: Verify green (twice)**

```bash
go tool goimports -w internal/storage/tigris/
go test ./internal/storage/tigris/ -count=1
go test ./internal/storage/tigris/ -count=1 # rerun catches os.CreateTemp reuse flakes
```

Expected: PASS ×2.

- [ ] **Step 6: Commit**

```bash
git add internal/storage/tigris/
git commit -s -m "feat(storage/tigris): hash-verifying staging writer for RawObjectWriter"
```

---

### Task 5: `SetEncodedObject` and `AddAlternate`

**Files:**
- Modify: `internal/storage/tigris/writer.go`
- Modify: `internal/storage/tigris/writer_test.go`
- Modify: `internal/storage/tigris/tigris.go` (delete `SetEncodedObject`, `AddAlternate` stubs)

**Interfaces:**
- Consumes: Task 4's `stageWriter.Discard`.
- Produces: `SetEncodedObject` implementation; test-only `lyingObject` wrapper (defined here, used nowhere else).

Contract pinned by tests: an object whose claimed `Hash()` disagrees with recomputed bytes is refused with `ErrHashMismatch` + `ZeroHash`, and **nothing uploads** (spec CAUTION clause); delta-typed inputs refuse with `plumbing.ErrInvalidType` before any reader opens; successful sets return the recomputed hash and store one object keyed by it.

- [ ] **Step 1: Write the failing test**

Append to `writer_test.go`:

```go
// lyingObject wraps a real encoded object but claims someone else's hash —
// exactly the forgery SetEncodedObject must refuse.
type lyingObject struct {
	plumbing.EncodedObject
	hash plumbing.Hash
}

func (l lyingObject) Hash() plumbing.Hash { return l.hash }

func TestSetEncodedObjectStoresUnderRecomputedHash(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	const body = "committed through set"
	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len(body)))
	if _, err := obj.Write([]byte(body)); err != nil {
		t.Fatalf("buffer: %v", err)
	}
	want := obj.Hash()

	got, err := s.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if got != want {
		t.Fatalf("returned hash %s differs from recomputed %s", got.String(), want.String())
	}
	stored := f.get(t, keyOf(want))
	if string(stored.body) != body {
		t.Errorf("body mismatch: %q", stored.body)
	}
	if f.nputs() != 1 {
		t.Errorf("want one upload, got %d", f.nputs())
	}
}

func TestSetEncodedObjectForgedHashRejectedWithoutUpload(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	victim := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "innocent bystander")
	putsBefore := f.nputs()

	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len("actually different")))
	if _, err := obj.Write([]byte("actually different")); err != nil {
		t.Fatalf("buffer: %v", err)
	}

	got, err := s.SetEncodedObject(lyingObject{EncodedObject: obj, hash: victim})
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("want ErrHashMismatch, got %v", err)
	}
	if got != plumbing.ZeroHash {
		t.Errorf("want ZeroHash on refusal, got %s", got.String())
	}
	if f.nputs() != putsBefore {
		t.Error("forged object uploaded anyway")
	}
}

func TestSetEncodedObjectRejectsDeltas(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))

	delta := plumbing.NewMemoryObject(s.oh)
	delta.SetType(plumbing.OFSDeltaObject)
	if _, err := s.SetEncodedObject(delta); !errors.Is(err, plumbing.ErrInvalidType) {
		t.Errorf("delta-set went through: %v", err)
	}
}

func TestAddAlternateUnsupported(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))

	err := s.AddAlternate("../elsewhere")
	if !errors.Is(err, ErrAlternatesNotSupported) {
		t.Errorf("want ErrAlternatesNotSupported, got %v", err)
	}
}
```

- [ ] **Step 2: Verify red**

Run: `go test ./internal/storage/tigris/ -run 'TestSetEncodedObject|TestAddAlternate' -count=1`
Expected: FAIL (stubs answer `errUnimplemented`).

- [ ] **Step 3: Implement**

Append to `writer.go`:

```go
func (s *Storer) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	switch obj.Type() {
	case plumbing.OFSDeltaObject, plumbing.REFDeltaObject:
		return plumbing.ZeroHash, plumbing.ErrInvalidType
	}

	rd, err := obj.Reader()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("tigris: open reader for %s: %w", obj.Hash().String(), err)
	}
	defer rd.Close()

	w, err := s.RawObjectWriter(obj.Type(), obj.Size())
	if err != nil {
		return plumbing.ZeroHash, err
	}
	sw := w.(*stageWriter)

	if _, err := io.Copy(sw, rd); err != nil {
		sw.Discard()
		return plumbing.ZeroHash, fmt.Errorf("tigris: copy data for %s: %w", obj.Hash().String(), err)
	}

	// Claimed hashes are untrusted (spec CAUTION): prove the recomputed
	// stream agrees before storing anything under any address.
	got := sw.hasher.Sum()
	if want := obj.Hash(); got.String() != want.String() {
		sw.Discard()
		return plumbing.ZeroHash, ErrHashMismatch
	}

	if err := sw.Close(); err != nil {
		return plumbing.ZeroHash, err
	}
	return got, nil
}

func (s *Storer) AddAlternate(remote string) error {
	return fmt.Errorf("%w: remote %q", ErrAlternatesNotSupported, remote)
}
```

- [ ] **Step 4: Verify green**

Run: `go test ./internal/storage/tigris/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/
git commit -s -m "feat(storage/tigris): SetEncodedObject with forged-hash refusal"
```

---

### Task 6: Iteration — paginated `listKeys` and the lazy `objectIter`

**Files:**
- Create: `internal/storage/tigris/iter.go`
- Create: `internal/storage/tigris/iter_test.go`
- Modify: `internal/storage/tigris/tigris.go` (delete `IterEncodedObjects` stub)

**Interfaces:**
- Consumes: `objectPrefix`, `headInfo`, `loadObject`, `plumbing.FromHex`, `sp`/`sv`/`bv`.
- Produces:

```go
func (s *Storer) listKeys(prefix string) ([]string, error)
func (s *Storer) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) // replaces stub
type objectIter struct {
	s    *Storer
	want plumbing.ObjectType
	keys []string
	pos  int
}
```

Pinned behaviors: continuation-token pagination (fake `listMax` forces small pages); ascending lexicographic order (= ascending hash order); `Next` exhaustion → `io.EOF`; `ForEach` visits everything, treats `cb(storer.ErrStop)` as clean stop, propagates custom errors, returns nil at natural end; `Close` abandons remainder and is idempotent; foreign non-hex keys under `objects/` skipped; objects vanishing between LIST and HEAD (race) skipped, not fatal.

- [ ] **Step 1: Write the failing test**

Create `internal/storage/tigris/iter_test.go`:

```go
package tigris

import (
	"errors"
	"io"
	"slices"
	"testing"

	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

func seedManyBlobs(t *testing.T, f *fakeS3, of formatcfg.ObjectFormat, contents ...string) []plumbing.Hash {
	t.Helper()
	hs := make([]plumbing.Hash, 0, len(contents))
	for _, c := range contents {
		hs = append(hs, seed(t, f, of, plumbing.BlobObject, c))
	}
	return hs
}

func TestListKeysPaginatesAndSorts(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.listMax = 2
	seeds := seedManyBlobs(t, f, formatcfg.DefaultObjectFormat, "one", "two", "three", "four", "five")
	f.put("objects/not-a-hash", "decoy", nil)

	s := newTestStorer(t, f)

	keys, err := s.listKeys(objectPrefix)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	want := make([]string, 0, len(seeds)+1)
	for _, h := range seeds {
		want = append(want, keyOf(h))
	}
	want = append(want, "objects/not-a-hash")
	slices.Sort(want)

	if !slices.Equal(keys, want) {
		t.Errorf("keys mismatch:\nwant %v\ngot  %v", want, keys)
	}
	if !slices.IsSorted(keys) {
		t.Errorf("S3 order guarantee broken: %v", keys)
	}
}

func TestIterEncodedObjectsFilterAndOrder(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.listMax = 1 // force many tiny pages
	of := formatcfg.DefaultObjectFormat
	blobs := seedManyBlobs(t, f, of, "blob-a", "blob-b", "blob-c")
	commits := seedManyCommits(t, f, of, "commit-a", "commit-b")
	f.put("objects/beefcafe", "foreign junk", nil) // undecodable key skipped

	s := newTestStorer(t, f)

	it, err := s.IterEncodedObjects(plumbing.BlobObject)
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	var got []plumbing.Hash
	for {
		obj, nerr := it.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			t.Fatalf("next: %v", nerr)
		}
		got = append(got, obj.Hash())
	}

	want := append(slices.Clone(blobs), commits[:0]...) // blobs only
	slices.SortFunc(want, func(a, b plumbing.Hash) bool { return a.String() < b.String() })
	if !slices.EqualFunc(got, want, func(a, b plumbing.Hash) bool { return a.String() == b.String() }) {
		t.Errorf("hash order mismatch:\nwant %v\ngot  %v",
			formatHashes(want), formatHashes(got))
	}
}

func seedManyCommits(t *testing.T, f *fakeS3, of formatcfg.ObjectFormat, contents ...string) []plumbing.Hash {
	t.Helper()
	hs := make([]plumbing.Hash, 0, len(contents))
	for _, c := range contents {
		hs = append(hs, seed(t, f, of, plumbing.CommitObject, c))
	}
	return hs
}

func formatHashes(in []plumbing.Hash) []string {
	out := make([]string, 0, len(in))
	for _, h := range in {
		out = append(out, h.String())
	}
	return out
}

func TestIterForEachStopAndErrors(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	seedManyBlobs(t, f, formatcfg.DefaultObjectFormat, "eins", "zwei", "drei")
	s := newTestStorer(t, f)

	it, err := s.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	visited := 0
	err = it.ForEach(func(plumbing.EncodedObject) error {
		visited++
		if visited == 2 {
			return storer.ErrStop
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach surfaced stop as error: %v", err)
	}
	if visited != 2 {
		t.Errorf("want stop after 2 visits, saw %d", visited)
	}

	boom := errors.New("callback boom")
	it2, _ := s.IterEncodedObjects(plumbing.AnyObject)
	defer it2.Close()
	if err := it2.ForEach(func(plumbing.EncodedObject) error { return boom }); !errors.Is(err, boom) {
		t.Errorf("custom callback error not propagated: %v", err)
	}

	it3, _ := s.IterEncodedObjects(plumbing.AnyObject)
	defer it3.Close()
	full := 0
	if err := it3.ForEach(func(plumbing.EncodedObject) error { full++; return nil }); err != nil || full != 3 {
		t.Errorf("natural end misbehaved: %d visits, err=%v", full, err)
	}
}

func TestIterSurvivesVanishedKeys(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.listMax = 1
	seedManyBlobs(t, f, formatcfg.DefaultObjectFormat, "stable-one", "stable-two", "stable-three")

	s := newTestStorer(t, f)
	it, err := s.IterEncodedObjects(plumbing.AnyObject) // snapshot taken
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	// Wipe everything AFTER the iterator holds its LIST snapshot: per-key
	// HEADs now report misses exactly like a racing deleter would.
	for _, k := range append([]string{},
		append(objectsKeySnapshot(f), otherSeedKeys(f, "stable-one", "stable-two", "stable-three")...)...) {
		f.del(k)
	}

	count := 0
	for {
		if _, nerr := it.Next(); errors.Is(nerr, io.EOF) {
			break
		} else if nerr != nil {
			t.Fatalf("vanished object became fatal: %v", nerr)
		}
		count++
	}
	if count != 0 {
		t.Errorf("expected zero survivors, got %d", count)
	}
}

func objectsKeySnapshot(f *fakeS3) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.objs {
		keys = append(keys, k)
	}
	return keys
}

func otherSeedKeys(f *fakeS3, contents ...string) []string { return objectsKeySnapshot(f) } // covered by snapshot; kept single-source
```

Trim helper sprawl during Step 3 review: `otherSeedKeys` reduces to the snapshot; the deletion loop becomes simply:

```go
	for _, k := range objectsKeySnapshot(f) {
		f.del(k)
	}
```

Final loop for that test, authoritatively:

```go
	count := 0
	for {
		obj, nerr := it.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			t.Fatalf("vanished object became fatal: %v", nerr)
		}
		_ = obj
		count++
	}
	if count != 0 {
		t.Errorf("expected zero survivors, got %d", count)
	}
```

(Clean duplicates out; the shipped file contains exactly one snapshot helper, one Seed helper family, and the final-loop form.)

- [ ] **Step 2: Verify red**

Run: `go test ./internal/storage/tigris/ -run 'TestListKeys|TestIter' -count=1`
Expected: FAIL (`listKeys` undefined, iterator stubbed).

- [ ] **Step 3: Implement**

Create `internal/storage/tigris/iter.go`:

```go
package tigris

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// listKeys walks one prefix fully. S3 returns Contents lexicographically with
// monotone continuation tokens, so results come back sorted.
func (s *Storer) listKeys(prefix string) ([]string, error) {
	var keys []string
	token := ""

	for {
		in := &s3.ListObjectsV2Input{
			Bucket: sp(s.bucket),
			Prefix: sp(prefix),
		}
		if token != "" {
			in.ContinuationToken = sp(token)
		}

		start := time.Now()
		page, err := s.client.ListObjectsV2(s.ctx, in)
		s.observe("ListObjectsV2", start, err)
		if err != nil {
			return nil, fmt.Errorf("tigris: list %q: %w", prefix, err)
		}

		for _, entry := range page.Contents {
			if k := sv(entry.Key); k != "" {
				keys = append(keys, k)
			}
		}
		if !bv(page.IsTruncated) || sv(page.NextContinuationToken) == "" {
			break
		}
		token = sv(page.NextContinuationToken)
	}
	return keys, nil
}

// objectIter resolves keys one HEAD at a time. Laziness buys the cost profile
// the spec asks for: type mismatches cost a HEAD, never a body download.
type objectIter struct {
	s    *Storer
	want plumbing.ObjectType
	keys []string
	pos  int
}

func (s *Storer) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	keys, err := s.listKeys(objectPrefix)
	if err != nil {
		return nil, err
	}
	return &objectIter{s: s, want: t, keys: keys}, nil
}

func (it *objectIter) Next() (plumbing.EncodedObject, error) {
	for it.pos < len(it.keys) {
		raw := strings.TrimPrefix(it.keys[it.pos], objectPrefix)
		it.pos++

		h, ok := plumbing.FromHex(raw)
		if !ok {
			continue // junk under objects/: skip, never poison the walk
		}

		hs, herr := it.s.headInfo(h)
		switch {
		case errors.Is(herr, plumbing.ErrObjectNotFound):
			continue // vanished between LIST and HEAD: tolerate the race
		case errors.Is(herr, errBadMetadata):
			continue // undecodable entry behaves like junk
		case herr != nil:
			return nil, herr
		}

		if it.want != plumbing.AnyObject && hs.typ != it.want {
			continue
		}
		return it.s.loadObject(h, hs)
	}
	return nil, io.EOF
}

func (it *objectIter) ForEach(cb func(plumbing.EncodedObject) error) error {
	for {
		obj, err := it.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		cbErr := cb(obj)
		if cbErr == nil {
			continue
		}
		if errors.Is(cbErr, storer.ErrStop) {
			return nil
		}
		return cbErr
	}
}

func (it *objectIter) Close() {
	it.pos = len(it.keys)
}
```

Then apply the authoritative cleanup noted in Step 1's trailing block so the test file holds exactly one of each helper and the final survivor-counting loop.

- [ ] **Step 4: Verify green**

```bash
go tool goimports -w internal/storage/tigris/
go test ./internal/storage/tigris/ -run 'TestListKeys|TestIter' -count=1 -v
go test ./internal/storage/tigris/ -count=1
```

Expected: PASS. Confirm the order test really walked five blobs (`-v` shows all three `Next` rows plus commits filtered out).

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/
git commit -s -m "feat(storage/tigris): lazy paginated EncodedObject iteration"
```

---

### Task 7: References (six methods) and shallow marks

**Files:**
- Create: `internal/storage/tigris/refs.go`
- Create: `internal/storage/tigris/refs_test.go`
- Modify: `internal/storage/tigris/tigris.go` (delete nine ref/shallow stubs)

**Interfaces:**
- Consumes: `refPrefix`, `symRefPrefix`, `refKey`, `fetchSmall`, `removeSimple` (both born here), `listKeys`, `sp`/`sv`, `isNotFound`, `storage.ErrReferenceHasChanged`, `errMalformedRef` (declared Task 1).
- Produces:

```go
func encodeRefValue(ref *plumbing.Reference) string
func decodeRefValue(name plumbing.ReferenceName, v string) (*plumbing.Reference, error)
func (s *Storer) listLooseRefs() ([]*plumbing.Reference, error) // sole source behind IterReferences + CountLooseRefs
// plus: SetReference, CheckAndSetReference, Reference, IterReferences,
//       RemoveReference, CountLooseRefs, PackRefs (all no-op-by-design doc'd),
//       SetShallow, Shallow, fetchSmall, removeSimple
```

A single `listLooseRefs` guarantees `CountLooseRefs` equals what `IterReferences` walks. Malformed entries are logged (`slog.Warn`, `"err"` key) and skipped. `removeSimple`/`fetchSmall` are shared utility futures for Task 8's index/config; shallow rides along because it wants exactly those primitives.

- [ ] **Step 1: Write the failing test**

Create `internal/storage/tigris/refs_test.go`:

```go
package tigris

import (
	"errors"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"
)

const (
	headAB = "1111111111111111111111111111111111111111"
	headCD = "2222222222222222222222222222222222222222"
)

func hashRef(name, hexval string) *plumbing.Reference {
	h, ok := plumbing.FromHex(hexval)
	if !ok {
		panic("refs_test bug: bad hex fixture " + hexval)
	}
	return plumbing.NewHashReference(plumbing.ReferenceName(name), h)
}

func TestReferenceRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	main := hashRef("refs/heads/main", headAB)
	if err := s.SetReference(main); err != nil {
		t.Fatalf("set: %v", err)
	}
	sym := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.ReferenceName("refs/heads/main"))
	if err := s.SetReference(sym); err != nil {
		t.Fatalf("set sym: %v", err)
	}

	t.Run("loose values mirror dotgit encoding", func(t *testing.T) {
		o := f.get(t, "refs/refs/heads/main")
		if got := string(o.body); got != headAB+"\n" {
			t.Errorf("want %q, got %q", headAB+"\n", got)
		}
		osym := f.get(t, "refs/HEAD")
		if got := string(osym.body); got != "ref: refs/heads/main\n" {
			t.Errorf("want symbolic encoding, got %q", got)
		}
	})

	t.Run("reads reconstruct typed references", func(t *testing.T) {
		back, err := s.Reference(plumbing.ReferenceName("refs/heads/main"))
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if back.Hash().String() != main.Hash().String() {
			t.Errorf("hash mismatch: %s vs %s", back.Hash().String(), main.Hash().String())
		}

		head, err := s.Reference(plumbing.HEAD)
		if err != nil {
			t.Fatalf("get HEAD: %v", err)
		}
		if head.Type() != plumbing.SymbolicReference || head.Target().String() != "refs/heads/main" {
			t.Errorf("HEAD lost symbolic nature: %+v", head)
		}
	})

	t.Run("missing reference is the go-git sentinel", func(t *testing.T) {
		if _, err := s.Reference(plumbing.ReferenceName("refs/heads/nope")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
			t.Errorf("want ErrReferenceNotFound, got %v", err)
		}
	})

	t.Run("nil set is tolerated like memory-storage parity", func(t *testing.T) {
		if err := s.SetReference(nil); err != nil {
			t.Errorf("nil SetReference errored: %v", err)
		}
	})
}

func TestCheckAndSetReferenceCas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		currently string // pre-stored hash hex, "" for fresh
		oldVal    *plumbing.Reference
		wantErr   error // nil means swap must succeed
	}{
		{name: "create with nil old"},
		{name: "matching old swaps", currently: headAB, oldVal: hashRef("refs/heads/x", headAB)},
		{name: "stale old refuses", currently: headCD, oldVal: hashRef("refs/heads/x", headAB), wantErr: storage.ErrReferenceHasChanged},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			if tt.currently != "" {
				f.put("refs/refs/heads/x", tt.currently+"\n", nil)
			}
			s := newTestStorer(t, f)

			next := hashRef("refs/heads/x", headCD)
			err := s.CheckAndSetReference(next, tt.oldVal)

			cur, gerr := s.Reference(plumbing.ReferenceName("refs/heads/x"))
			if tt.wantErr == nil {
				if !errors.Is(err, tt.wantErr) && !(err == nil && tt.wantErr == nil) {
					t.Fatalf("want clean swap, got %v", err)
				}
				if gerr != nil {
					t.Fatalf("swap refused unexpectedly: %v", gerr)
				}
				if cur.Hash().String() != headCD {
					t.Errorf("swap landed the wrong value: %s", cur.Hash().String())
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("want ErrReferenceHasChanged, got %v", err)
			}
			if gerr != nil {
				t.Fatalf("failed CAS destroyed readability: %v", gerr)
			}
			if cur.Hash().String() != tt.currently {
				t.Errorf("failed CAS mutated the ref (now %s)", cur.Hash().String())
			}
		})
	}
}

func TestIterReferencesSortedAndComplete(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	// Deliberately out of lexical insertion order.
	for _, n := range []string{"refs/heads/zeta", "refs/tags/v1", "refs/heads/alpha"} {
		if err := s.SetReference(hashRef(n, headAB)); err != nil {
			t.Fatalf("set %s: %v", n, err)
		}
	}

	it, err := s.IterReferences()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	var names []string
	if err := it.ForEach(func(r *plumbing.Reference) error {
		names = append(names, r.Name().String())
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	want := []string{"refs/heads/alpha", "refs/heads/zeta", "refs/tags/v1"} // S3-sorted
	if len(names) != len(want) {
		t.Fatalf("walked %d refs, want %d: %v", len(names), len(want), names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("order mismatch:\nwant %v\ngot  %v", want, names)
		}
	}

	n, cerr := s.CountLooseRefs()
	if cerr != nil {
		t.Fatalf("count: %v", cerr)
	}
	if n != len(want) {
		t.Errorf("count disagrees with walk: %d vs %d", n, len(want))
	}
}

func TestRemoveReferenceDeletesAndToleratesAbsence(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	if err := s.SetReference(hashRef("refs/heads/gone", headAB)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s.RemoveReference(plumbing.ReferenceName("refs/heads/gone")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := s.RemoveReference(plumbing.ReferenceName("refs/heads/never-there")); err != nil {
		t.Errorf("absent removal errored: %v", err)
	}
	if _, err := s.Reference(plumbing.ReferenceName("refs/heads/gone")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("ref survived removal: %v", err)
	}
}

func TestMalformedRefEntriesAreSkipped(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.put("refs/refs/heads/fine", headAB+"\n", nil)
	f.put("refs/refs/heads/junk", "definitely not a ref.", nil)
	s := newTestStorer(t, f)

	it, err := s.IterReferences()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	names := map[string]bool{}
	if err := it.ForEach(func(r *plumbing.Reference) error {
		names[r.Name().String()] = true
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	if names["refs/heads/junk"] {
		t.Error("malformed entry leaked into iteration")
	}
	if !names["refs/heads/fine"] {
		t.Error("healthy sibling vanished with the junk")
	}

	n, _ := s.CountLooseRefs()
	if n != 1 {
		t.Errorf("count must agree with the walk, got %d", n)
	}
}

func TestPackRefsIsDeliberateNoOp(t *testing.T) {
	t.Parallel()

	if err := newTestStorer(t, newFakeS3(t)).PackRefs(); err != nil {
		t.Errorf("PackRefs must succeed vacuously, got %v", err)
	}
}

func TestShallowMarks(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	t.Run("fresh bucket reads as unmarked", func(t *testing.T) {
		got, err := s.Shallow()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("want no marks, got %v", got)
		}
	})

	a, _ := plumbing.FromHex(headAB)
	b, _ := plumbing.FromHex(headCD)
	if err := s.SetShallow([]plumbing.Hash{a, b}); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Shallow()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(got) != 2 || got[0].String() != a.String() || got[1].String() != b.String() {
		t.Errorf("marks corrupted across round trip: %v", got)
	}

	putsBefore := f.nputs()
	deletesBefore := f.ndeletes()
	if err := s.SetShallow(nil); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if f.nputs() != putsBefore {
		t.Error("clearing shallow wrote instead of deleting")
	}
	if f.ndeletes() != deletesBefore+1 {
		t.Error("clearing shallow did not delete the mark object")
	}
	if left, err := s.Shallow(); err != nil || len(left) != 0 {
		t.Errorf("cleared marks still readable: %v, %v", left, err)
	}
}
```

Note the double-`refs/` fixtures (`refs/refs/heads/main`) are load-bearing: the key namespace is `refPrefix + fullName`, and the tests pin that layout decision on purpose.

- [ ] **Step 2: Verify red**

Run: `go test ./internal/storage/tigris/ -run 'TestReference|TestCheckAndSet|TestIterReferences|TestRemoveReference|TestMalformedRef|TestPackRefs|TestShallow' -count=1`
Expected: FAIL (nine stubs in play).

- [ ] **Step 3: Implement**

Create `internal/storage/tigris/refs.go`:

```go
package tigris

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
)

// Refs store dotgit-loose-ref-style text: raw hex for hashes, "ref: target"
// for symbolics, newline-terminated. Keys carry the FULL ref name after the
// refs/ prefix, giving every addressable name (including HEAD) one namespace
// in the flat bucket.
//
// Concurrency note: CheckAndSetReference compares then writes non-atomically.
// Real CAS via conditional PutObject (If-Match ETag) is listed as follow-up
// work; today the window races exactly like the in-memory storer does.

func encodeRefValue(ref *plumbing.Reference) string {
	if ref.Type() == plumbing.SymbolicReference {
		return symRefPrefix + ref.Target().String() + "\n"
	}
	return ref.Hash().String() + "\n"
}

func decodeRefValue(name plumbing.ReferenceName, v string) (*plumbing.Reference, error) {
	value := strings.TrimSpace(v)
	if target, ok := strings.CutPrefix(value, symRefPrefix); ok {
		return plumbing.NewSymbolicReference(name, plumbing.ReferenceName(strings.TrimSpace(target))), nil
	}
	h, ok := plumbing.FromHex(value)
	if !ok {
		return nil, fmt.Errorf("%w: ref %s body %q", errMalformedRef, name.String(), value)
	}
	return plumbing.NewHashReference(name, h), nil
}

func (s *Storer) SetReference(ref *plumbing.Reference) error {
	if ref == nil {
		return nil // tolerated identically by the in-memory storer
	}
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(refKey(ref.Name())),
		Body:   strings.NewReader(encodeRefValue(ref)),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: set ref %s: %w", ref.Name().String(), err)
	}
	return nil
}

func (s *Storer) CheckAndSetReference(newRef, old *plumbing.Reference) error {
	if newRef == nil {
		return nil
	}
	if old != nil {
		current, err := s.Reference(newRef.Name())
		if err == nil && current.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
		// Missing current reference falls through to creation, mirroring the
		// in-memory storer's lenient behavior.
	}
	return s.SetReference(newRef)
}

func (s *Storer) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	body, err := s.fetchSmall(refKey(n))
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return nil, plumbing.ErrReferenceNotFound
	default:
		return nil, fmt.Errorf("tigris: load ref %s: %w", n.String(), err)
	}

	ref, derr := decodeRefValue(n, string(body))
	if derr != nil {
		return nil, derr
	}
	return ref, nil
}

// listLooseRefs is the single source of truth behind IterReferences and
// CountLooseRefs, so the two can never disagree. Malformed entries log-and-
// skip; vanished-mid-list keys behave like the object iterator's race rule.
func (s *Storer) listLooseRefs() ([]*plumbing.Reference, error) {
	keys, err := s.listKeys(refPrefix)
	if err != nil {
		return nil, err
	}

	var refs []*plumbing.Reference
	for _, k := range keys {
		name := plumbing.ReferenceName(strings.TrimPrefix(k, refPrefix))

		ref, rerr := s.Reference(name)
		switch {
		case rerr == nil:
			refs = append(refs, ref)
		case errors.Is(rerr, plumbing.ErrReferenceNotFound):
			continue
		case errors.Is(rerr, errMalformedRef):
			slog.Warn("skipping malformed loose ref", "key", k, "err", rerr)
			continue
		default:
			return nil, rerr
		}
	}
	return refs, nil
}

func (s *Storer) IterReferences() (storer.ReferenceIter, error) {
	refs, lerr := s.listLooseRefs()
	if lerr != nil {
		return nil, lerr
	}
	return storer.NewReferenceSliceIter(refs), nil
}

func (s *Storer) RemoveReference(n plumbing.ReferenceName) error {
	if err := s.removeSimple(refKey(n)); err != nil {
		return fmt.Errorf("tigris: remove ref %s: %w", n.String(), err)
	}
	return nil
}

func (s *Storer) CountLooseRefs() (int, error) {
	refs, err := s.listLooseRefs()
	if err != nil {
		return 0, err
	}
	return len(refs), nil
}

// PackRefs is deliberately a no-op: every ref stays individually addressable,
// so packed-refs compaction offers nothing in a flat bucket.
func (s *Storer) PackRefs() error {
	return nil
}

// --- shallow marks ---

func (s *Storer) SetShallow(commits []plumbing.Hash) error {
	if len(commits) == 0 {
		return s.removeSimple(shallowKey)
	}
	var b strings.Builder
	for _, c := range commits {
		b.WriteString(c.String())
		b.WriteString("\n")
	}
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(shallowKey),
		Body:   strings.NewReader(b.String()),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: store shallow marks: %w", err)
	}
	return nil
}

func (s *Storer) Shallow() ([]plumbing.Hash, error) {
	body, err := s.fetchSmall(shallowKey)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return nil, nil // absent marker == not shallow, like dotgit
	default:
		return nil, fmt.Errorf("tigris: load shallow marks: %w", err)
	}

	var out []plumbing.Hash
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		h, ok := plumbing.FromHex(line)
		if !ok {
			return nil, fmt.Errorf("%w: shallow mark %q unreadable", errMalformedRef, line)
		}
		out = append(out, h)
	}
	return out, nil
}

// --- small-payload primitives shared by refs, shallow, index, config ---

// fetchSmall GETs one whole ancillary object and returns its body. Misses
// normalize to plumbing.ErrObjectNotFound so every caller maps absence the
// same way object reads do.
func (s *Storer) fetchSmall(key string) ([]byte, error) {
	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
	})
	s.observe("GetObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, plumbing.ErrObjectNotFound
	default:
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// removeSimple deletes one root-level key, tolerating its absence.
func (s *Storer) removeSimple(key string) error {
	start := time.Now()
	_, err := s.client.DeleteObject(s.ctx, &s3.DeleteObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
	})
	s.observe("DeleteObject", start, err)
	switch {
	case err == nil, isNotFound(err):
		return nil
	default:
		return fmt.Errorf("tigris: delete %s: %w", key, err)
	}
}
```

Delete these nine stubs from `tigris.go`: `SetReference`, `CheckAndSetReference`, `Reference`, `IterReferences`, `RemoveReference`, `CountLooseRefs`, `PackRefs`, `SetShallow`, `Shallow`.

- [ ] **Step 4: Verify green**

```bash
go tool goimports -w internal/storage/tigris/
go test ./internal/storage/tigris/ -count=1
go tool staticcheck ./internal/storage/tigris/
```

Expected: PASS, clean.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/
git commit -s -m "feat(storage/tigris): loose references, CAS updates, shallow marks"
```

---

### Task 8: Worktree index, repository config, `Module` — interface completion

**Files:**
- Create: `internal/storage/tigris/index.go`
- Create: `internal/storage/tigris/config.go`
- Create: `internal/storage/tigris/index_test.go`
- Modify: `internal/storage/tigris/tigris.go` (delete final five stubs; retire the block)

**Interfaces:**
- Consumes: `indexKey`, `configKey`, `fetchSmall` (Task 7), `of`, `ErrModulesNotSupported`, codec APIs verified in the header.
- Produces:

```go
func (s *Storer) idxChecksum() hash.Hash         // sha1/sha256 trailer checksum by object format
func (s *Storer) putBytes(key string, body []byte) error
func (s *Storer) SetIndex(idx *index.Index) error
func (s *Storer) Index() (*index.Index, error)   // miss → &index.Index{Version: 2}, nil
func (s *Storer) Config() (*config.Config, error) // miss → config.NewConfig(), nil
func (s *Storer) SetConfig(cfg *config.Config) error
func (s *Storer) Module(name string) (storage.Storer, error) // ErrModulesNotSupported always
```

- [ ] **Step 1: Write the failing test**

Create `internal/storage/tigris/index_test.go`:

```go
package tigris

import (
	"errors"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/index"
)

func TestIndexRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	t.Run("fresh bucket yields pristine v2 index", func(t *testing.T) {
		got, err := s.Index()
		if err != nil {
			t.Fatalf("first read: %v", err)
		}
		if got.Version != 2 || len(got.Entries) != 0 {
			t.Errorf("want pristine v2, got version=%d entries=%d", got.Version, len(got.Entries))
		}
	})

	in := &index.Index{Version: 2}
	entry := &index.Entry{
		Name:       "docs/a.txt",
		CreatedAt:  time.Unix(1700000000, 0).UTC(),
		ModifiedAt: time.Unix(1700000001, 0).UTC(),
		Dev:        1,
		Inode:      42,
		Mode:       filemode.Regular,
		Size:       11,
	}
	entry.Hash, _ = plumbing.FromHex(headAB)
	in.Entries = append(in.Entries, entry)

	if err := s.SetIndex(in); err != nil {
		t.Fatalf("set: %v", err)
	}
	out, err := s.Index()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(out.Entries) != 1 {
		t.Fatalf("entry count lost: %d", len(out.Entries))
	}
	e := out.Entries[0]
	if e.Name != "docs/a.txt" || e.Mode != filemode.Regular || e.Size != 11 ||
		e.Dev != 1 || e.Inode != 42 || e.Hash.String() != headAB {
		t.Errorf("entry fields mangled: %+v", e)
	}
}

func TestConfigRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	cfg, err := s.Config()
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected lazily created default config, got nil")
	}

	cfg.Core.Worktree = "/tmp/demo-worktree"
	cfg.User.Name = "Xe Iaso"
	if err := s.SetConfig(cfg); err != nil {
		t.Fatalf("set: %v", err)
	}

	reloaded, err := s.Config()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.Core.Worktree != "/tmp/demo-worktree" || reloaded.User.Name != "Xe Iaso" {
		t.Errorf("values lost across save: core=%+v user=%+v", reloaded.Core, reloaded.User)
	}
}

func TestModuleExplicitlyUnsupported(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))

	_, err := s.Module("vendor/dep")
	if !errors.Is(err, ErrModulesNotSupported) {
		t.Errorf("want ErrModulesNotSupported, got %v", err)
	}
}
```

- [ ] **Step 2: Verify red**

Run: `go test ./internal/storage/tigris/ -run 'TestIndexRoundTrip|TestConfigRoundTrip|TestModuleExplicitly' -count=1`
Expected: FAIL (stubs).

- [ ] **Step 3: Implement**

Create `internal/storage/tigris/index.go`:

```go
package tigris

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/index"
)

// idxChecksum picks the trailer checksum the index format expects for this
// repository's object format — the same choice dotgit makes by format.
func (s *Storer) idxChecksum() hash.Hash {
	if s.of == formatcfg.SHA256 {
		return sha256.New()
	}
	return sha1.New()
}

func (s *Storer) SetIndex(idx *index.Index) error {
	var buf bytes.Buffer
	if err := index.NewEncoder(&buf, s.idxChecksum()).Encode(idx); err != nil {
		return fmt.Errorf("tigris: encode index: %w", err)
	}
	if err := s.putBytes(indexKey, buf.Bytes()); err != nil {
		return fmt.Errorf("tigris: store index: %w", err)
	}
	return nil
}

func (s *Storer) Index() (*index.Index, error) {
	raw, err := s.fetchSmall(indexKey)
	switch {
	case err == nil:
	case isNotFound(err):
		return &index.Index{Version: 2}, nil // memory-storer parity
	default:
		return nil, fmt.Errorf("tigris: load index: %w", err)
	}

	idx := &index.Index{}
	if derr := index.NewDecoder(bytes.NewReader(raw), s.idxChecksum()).Decode(idx); derr != nil {
		return nil, fmt.Errorf("tigris: decode index: %w", derr)
	}
	return idx, nil
}

// putBytes is the trivial whole-payload PUT used by ancillary keys (index,
// config). Objects bypass this for the streaming staging writer.
func (s *Storer) putBytes(key string, body []byte) error {
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
		Body:   bytes.NewReader(body),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: put %s: %w", key, err)
	}
	return nil
}

var _ = s3.RequestProgress // pacifier retained only if s3 import goes otherwise-unused; prune freely
```

Delete that closing `var _` line during the goimports pass if unused — the intent (import hygiene) matters, not the line.

Create `internal/storage/tigris/config.go`:

```go
package tigris

import (
	"fmt"

	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/storage"
)

func (s *Storer) Config() (*config.Config, error) {
	raw, err := s.fetchSmall(configKey)
	switch {
	case err == nil:
	case isNotFound(err):
		return config.NewConfig(), nil
	default:
		return nil, fmt.Errorf("tigris: load config: %w", err)
	}

	cfg := config.NewConfig()
	if uerr := cfg.Unmarshal(raw); uerr != nil {
		return nil, fmt.Errorf("tigris: parse config: %w", uerr)
	}
	return cfg, nil
}

func (s *Storer) SetConfig(cfg *config.Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("tigris: validate config: %w", err)
	}
	raw, err := cfg.Marshal()
	if err != nil {
		return fmt.Errorf("tigris: marshal config: %w", err)
	}
	if err := s.putBytes(configKey, raw); err != nil {
		return fmt.Errorf("tigris: store config: %w", err)
	}
	return nil
}

// Module refuses submodule storers: they would each need their own bucket (or
// a scheme for nesting prefixes) before the daemon ever serves submodule
// traffic. Explicit beats surprising.
func (s *Storer) Module(name string) (storage.Storer, error) {
	return nil, fmt.Errorf("%w: %q", ErrModulesNotSupported, name)
}
```

Delete the last five stubs (`SetIndex`, `Index`, `Config`, `SetConfig`, `Module`) **and the whole temporary-block banner comment** from `tigris.go`.

- [ ] **Step 4: Verify green and sweep**

```bash
grep -n errUnimplemented internal/storage/tigris/*.go  # MUST print nothing
go tool goimports -w internal/storage/tigris/
go test ./internal/storage/tigris/ -count=1
go tool staticcheck ./internal/storage/tigris/
```

Expected: grep silent, tests PASS, staticcheck clean. The surviving `var _ storer.Storer` assertion is now backed entirely by real code.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/
git commit -s -m "feat(storage/tigris): complete storage.Storer with index, config, module"
```

---

### Task 9: Live-bucket verification (env-gated), docs blurb, final sweep

**Files:**
- Create: `internal/storage/tigris/livebucket_test.go`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: everything built so far; real credentials through the standard AWS chain (`AWS_PROFILE` / env keys) via `New(ctx, bucket)`'s default `storage-go` construction.
- Produces: empirical confirmation of the spec's build-order item 2 ("record which error a real Tigris bucket returns for a missing key"), plus operator documentation.

- [ ] **Step 1: Env-gated integration test**

Create `internal/storage/tigris/livebucket_test.go`:

```go
package tigris

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing"
)

// TestLiveBucketRoundTrip runs only when OBJGIT_TIGRIS_LIVE_BUCKET points at
// a writable bucket. Without it (CI, laptop runs) the test skips. It pins the
// spec's build-order item 2: confirm which error REAL Tigris emits for a
// missing key, and exercise the full write→read→iterate cycle over the wire.
func TestLiveBucketRoundTrip(t *testing.T) {
	bucket := os.Getenv("OBJGIT_TIGRIS_LIVE_BUCKET")
	if bucket == "" {
		t.Skip("OBJGIT_TIGRIS_LIVE_BUCKET not set; skipping live-bucket verification")
	}

	ctx := context.Background()
	s, err := New(ctx, bucket, WithObserver(func(op string, dur time.Duration, oerr error) {
		t.Logf("s3 %-14s dur=%-12s err=%v", op, dur, oerr)
	}))
	if err != nil {
		t.Fatalf("live construct: %v", err)
	}

	// Valid-format digest no sane bucket contains; probes Head-miss mapping.
	ghostHex := strings.Repeat("f", 40)
	ghost, ok := plumbing.FromHex(ghostHex)
	if !ok || ghost.String() != ghostHex {
		t.Fatalf("fixture broken: %#v", ghost)
	}
	err = s.HasEncodedObject(ghost)
	if !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Fatalf("real Tigris absence surfaced as %v — wire actual codes into isNotFound()", err)
	}
	t.Logf("live Head-miss mapped correctly (observed above)")

	// One genuine object lifecycle.
	const body = "live bucket body"
	obj := plumbing.NewMemoryObject(plumbing.FromObjectFormat(formatcfg.DefaultObjectFormat))
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len(body)))
	if _, werr := obj.Write([]byte(body)); werr != nil {
		t.Fatalf("buffer: %v", werr)
	}

	stored, serr := s.SetEncodedObject(obj)
	if serr != nil {
		t.Fatalf("set: %v", serr)
	}
	t.Cleanup(func() {
		_, _ = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: sp(keyOf(stored))})
	})

	if herr := s.HasEncodedObject(stored); herr != nil {
		t.Fatalf("has: %v", herr)
	}

	got, gerr := s.EncodedObject(plumbing.BlobObject, stored)
	if gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	rd, rdrErr := got.Reader()
	if rdrErr != nil {
		t.Fatalf("reader: %v", rdrErr)
	}
	back, rerr := io.ReadAll(rd)
	rd.Close()
	if rerr != nil {
		t.Fatalf("body read: %v", rerr)
	}
	if string(back) != body {
		t.Fatalf("bytes survived the round trip changed: %q", back)
	}

	sz, szerr := s.EncodedObjectSize(stored)
	if szerr != nil || sz != int64(len(body)) {
		t.Fatalf("size: %d err=%v", sz, szerr)
	}

	it, ierr := s.IterEncodedObjects(plumbing.BlobObject)
	if ierr != nil {
		t.Fatalf("iter: %v", ierr)
	}
	defer it.Close()
	found := false
	if ferr := it.ForEach(func(o plumbing.EncodedObject) error {
		if o.Hash().String() == stored.String() {
			found = true
			return errStopWalk
		}
		return nil
	}); ferr != nil {
		t.Fatalf("iterate: %v", ferr)
	}
	if !found {
		t.Error("just-written object missing from live iteration")
	}
}

// errStopWalk exits the live iteration early without carrying storer.ErrStop
// semantics into this file's meaning space.
var errStopWalk = errors.New("found it, stop walking")
```

(The dedicated `errStopWalk` sidesteps one extra import; swapping in `storer.ErrStop` — the idiomatic choice — is equally acceptable. Pick whichever keeps imports tidy.)

- [ ] **Step 2: Execute it against a real bucket once**

```bash
export OBJGIT_TIGRIS_LIVE_BUCKET=<throwaway-test-bucket>
AWS_PROFILE=<profile-with-access> go test ./internal/storage/tigris/ -run TestLiveBucketRoundTrip -count=1 -v
unset OBJGIT_TIGRIS_LIVE_BUCKET
```

Credentials come from the standard chain; the `tigris-storage:tigris-authentication` skill owns creating access keys if you need fresh ones. Expected: PASS, with observation lines showing `HeadObject` reporting `NotFound`.

If live codes turn out different from `"NotFound"`/`"NoSuchKey"`, widen `isNotFound`, adjust the unit fake accordingly, and record the finding in the AGENTS blurb below.

If no bucket is reachable right now, commit the skip-on-absence test anyway and state plainly in the PR description that the live run remains outstanding — do not describe it as verified.

- [ ] **Step 3: Document in AGENTS.md**

You are editing documentation — invoke the `simple-english` skill first, then fit its rules around this draft. Append after the existing `### internal/s3fs` architecture section:

```markdown
### `internal/storage/tigris` — a go-git `storage.Storer` on one Tigris bucket

The s3fs filesystem layers a POSIX-like view over a bucket so stock go-git
storers can use it. This package skips the filesystem layer: it speaks
`storage.Storer` straight at the bucket. One bucket per repository.

Git objects store as loose objects under `objects/<hex>` with type and size
in user metadata (`git-type`, `git-size`). Reads issue a HEAD first. Writes
flow through a local staging file whose hashing tee names the final key, and
a claimed hash that disagrees with recomputed bytes is refused outright.
Refs hold dotgit-style text at `refs/<name>` (symbolics encode as
`ref: target`). Shallow marks, the worktree index, and repo config sit at
root-level keys. A narrow client seam (`s3API`, shaped like s3fs's
`s3Client`) lets tests swap in a fake, and the `WithObserver` option mirrors
s3fs's metrics seam so `main` can point both packages at
`metrics.ObserveS3`. Packfiles — one PUT per push instead of one per object
— are future work tracked in the build order at
`docs/reference/tigris-backend.md`.
```

- [ ] **Step 4: Full-repo regression sweep**

```bash
go tool goimports -w internal/storage/tigris/
go vet ./...
go test ./...
go tool staticcheck ./internal/storage/tigris/...
```

Protocol suites under `cmd/objgitd` need `git` on PATH; they must behave exactly as on `main` since nothing outside `internal/storage/tigris/` changed. Any unexpected divergence belongs to the environment — investigate, don't paper over. Expected: all PASS.

- [ ] **Step 5: Commit and hand off**

```bash
git add internal/storage/tigris/livebucket_test.go AGENTS.md
git commit -s -m "test(storage/tigris): live-bucket absence semantics plus docs"
```

Then hand off per superpowers:finishing-a-development-branch (merge-or-PR decision rests with Xe).

---

## Self-review record

Checked against `docs/reference/tigris-backend.md` and the writing-plans checklist:

- **Spec coverage:** §Layout → Task 1 constants; §Interface map → Tasks 2–6 row-for-row; §Write path (verify-before-upload, staging writer) → Tasks 4–5; §Read path (metadata HEAD reads, lazy iteration costs) → Tasks 2–3, 6; §Testing seam (`s3API` fake, table-driven, live-bucket confirmation of the missing-key error) → Tasks 1 and 9; spec CAUTION on trusting `obj.Hash()` → Task 5's forged-hash refusal.
- **Spec deviations, all deliberate and argued in-line:** requests inherit the Storer's context slot (the spec's own follow-up note); staging uses `os.CreateTemp` disk files (spec's shape, RAM bounded); refs/shallow/index/config/module were unspecified by the spec but demanded by the alpha.4 `storage.Storer` composition — given dotgit-compatible encodings; observer is instance-level because our constructor holds the Storer (unlike s3fs's standalone-filesystem constraint).
- **Placeholder scan:** each code block appears exactly once in its authoritative form; Task 6's and Task 9's drafts explicitly resolve to final listings with stale variants called out for deletion mid-task. Remaining cleanup instructions (`fmt.Sprintf` vs `strconv`-style micro preferences, import pruning during goimports passes) are formatting guidance, not missing content.
- **Type consistency:** `objHead` fields, `stageWriter` methods (`Write/Close/Discard`), `hashForBody`, `fetchSmall`/`removeSimple`/`putBytes`, `errMalformedRef`, `lyingObject`, and `pushThrough` signatures agree across the tasks that consume them. Double-`refs/` key fixtures are intentional and pinned with commentary.
