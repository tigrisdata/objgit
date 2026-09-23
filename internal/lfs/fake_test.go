package lfs

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// fakeObject is one object in the fake bucket.
type fakeObject struct {
	body     []byte
	size     int64
	checksum string
	meta     map[string]string
	etag     string
}

// fakeS3 is an in-memory bucket standing in for Tigris. CI runs with no
// credentials, so every store test goes through this. It mirrors the fakeS3 in
// internal/storage/tigris: an opaque counter ETag, and injectable failures per
// operation.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]fakeObject
	etagSeq int

	headErr   error
	getErr    error
	renameErr error
	putErr    error
	delErr    error

	// putHook runs before a Put is applied, so a test can land a competing
	// write in the middle of a compare-and-swap loop.
	putHook func(key string)

	// headHook runs before a Head is answered. A returned error fails that one
	// call, which is how a test makes the bucket fail for one key and answer
	// another normally.
	headHook func(key string) error

	// renameHook runs before a Rename is applied, so a test can land a
	// competing verify between this call's head and its promotion.
	renameHook func(src string)
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string]fakeObject{}}
}

// errPreconditionFailed is what a bucket answers to a refused If-Match or
// If-None-Match, carrying the code isPreconditionFailed matches.
var errPreconditionFailed = &smithy.GenericAPIError{
	Code:    "PreconditionFailed",
	Message: "At least one of the pre-conditions you specified did not hold",
}

// errBucketUnavailable is a transient bucket fault. It carries a code
// isNotFound does not match, because a fault is never absence.
var errBucketUnavailable = &smithy.GenericAPIError{
	Code:    "InternalError",
	Message: "We encountered an internal error. Please try again.",
}

// readAll drains a request body, which is nil for a bodyless PUT.
func readAll(r io.Reader) []byte {
	if r == nil {
		return nil
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return nil
	}
	return b
}

func (f *fakeS3) nextETag() string {
	f.etagSeq++
	return `"etag-` + strconv.Itoa(f.etagSeq) + `"`
}

// set stores an object directly, bypassing the API. For seeding only.
func (f *fakeS3) set(key string, o fakeObject) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o.etag = f.nextETag()
	f.objects[key] = o
}

func (f *fakeS3) putBlob(oid string, size int64, checksum string) {
	f.set(BlobKey(oid), fakeObject{size: size, checksum: checksum})
}

// putStaged seeds an upload that has landed but not yet been verified.
func (f *fakeS3) putStaged(repo, oid string, size int64, checksum string) {
	f.set(StagingKey(repo, oid), fakeObject{size: size, checksum: checksum})
}

func (f *fakeS3) putMarker(repo, oid string, size int64) {
	f.set(MarkerKey(repo, oid), fakeObject{
		meta: map[string]string{metaSize: strconv.FormatInt(size, 10)},
	})
}

// drop removes an object directly, bypassing the API. For seeding only.
func (f *fakeS3) drop(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
}

func (f *fakeS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if f.headErr != nil {
		return nil, f.headErr
	}
	if f.headHook != nil {
		if err := f.headHook(aws.ToString(in.Key)); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NotFound{}
	}
	out := &s3.HeadObjectOutput{
		ContentLength: aws.Int64(o.size),
		Metadata:      o.meta,
		ETag:          aws.String(o.etag),
	}
	// Only report a checksum when the caller asked for one, the way S3 does.
	if o.checksum != "" && in.ChecksumMode == types.ChecksumModeEnabled {
		out.ChecksumSHA256 = aws.String(o.checksum)
	}
	return out, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(o.body)),
		ContentLength: aws.Int64(int64(len(o.body))),
		ETag:          aws.String(o.etag),
	}, nil
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	key := aws.ToString(in.Key)
	if f.putHook != nil {
		f.putHook(key)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	cur, exists := f.objects[key]
	// Conditional writes, the same compare-and-swap the ref cache uses.
	if aws.ToString(in.IfNoneMatch) == "*" && exists {
		return nil, errPreconditionFailed
	}
	if m := aws.ToString(in.IfMatch); m != "" && (!exists || cur.etag != m) {
		return nil, errPreconditionFailed
	}

	body := readAll(in.Body)
	f.objects[key] = fakeObject{
		body: body,
		size: int64(len(body)),
		meta: in.Metadata,
		etag: f.nextETag(),
	}
	return &s3.PutObjectOutput{ETag: aws.String(f.objects[key].etag)}, nil
}

// RenameObject models the Tigris extension: the source key is gone afterwards.
func (f *fakeS3) RenameObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	if f.renameErr != nil {
		return nil, f.renameErr
	}
	// CopySource is "<bucket>/<key>"; the fake has one bucket, so drop it.
	_, src, _ := strings.Cut(aws.ToString(in.CopySource), "/")
	if f.renameHook != nil {
		f.renameHook(src)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objects[src]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	o.etag = f.nextETag()
	f.objects[aws.ToString(in.Key)] = o
	delete(f.objects, src)
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if f.delErr != nil {
		return nil, f.delErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, aws.ToString(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func newTestStore(t *testing.T, f *fakeS3) *Store {
	t.Helper()
	return NewStore(f, newTestPresigner(t), "test-bucket")
}
