package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// rangeClient is a content-aware s3Client stub for the lazy read path. Unlike
// stubClient (size-only), it stores real bytes and honours the Range header on
// GetObject: it slices the object, fills ContentRange/ContentLength, and returns
// a 416 InvalidRange error for a start at or past the object's end. It also
// reports, per object, how many body bytes the consumer has actually read, so
// tests can assert that Open does not drain the body.
type rangeClient struct {
	objects map[string][]byte

	gets  atomic.Int64
	heads atomic.Int64
	read  atomic.Int64 // body bytes the consumer has read across all GetObject responses
}

func newRangeClient(objects map[string][]byte) *rangeClient {
	return &rangeClient{objects: objects}
}

// countingBody wraps a reader and tallies consumed bytes onto the client, so a
// test can prove the body is streamed lazily rather than buffered at Open.
type countingBody struct {
	r io.Reader
	c *rangeClient
}

func (b countingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.c.read.Add(int64(n))
	return n, err
}

func (b countingBody) Close() error { return nil }

func (c *rangeClient) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	c.heads.Add(1)
	data, ok := c.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, notFound()
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(data))),
		LastModified:  aws.Time(time.Unix(0, 0)),
	}, nil
}

func (c *rangeClient) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.gets.Add(1)
	data, ok := c.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, notFound()
	}
	full := int64(len(data))

	if in.Range == nil {
		return &s3.GetObjectOutput{
			Body:          countingBody{r: bytes.NewReader(data), c: c},
			ContentLength: aws.Int64(full),
			LastModified:  aws.Time(time.Unix(0, 0)),
		}, nil
	}

	start, end, err := parseRange(aws.ToString(in.Range), full)
	if err != nil {
		return nil, err
	}
	if start >= full {
		return nil, &smithy.GenericAPIError{Code: "InvalidRange", Message: "range start past end"}
	}
	if end > full-1 {
		end = full - 1
	}
	slice := data[start : end+1]
	return &s3.GetObjectOutput{
		Body:          countingBody{r: bytes.NewReader(slice), c: c},
		ContentLength: aws.Int64(int64(len(slice))),
		ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", start, end, full)),
		LastModified:  aws.Time(time.Unix(0, 0)),
	}, nil
}

// parseRange parses "bytes=start-" and "bytes=start-end" headers.
func parseRange(h string, full int64) (start, end int64, err error) {
	spec, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0, 0, fmt.Errorf("bad range %q", h)
	}
	lo, hi, _ := strings.Cut(spec, "-")
	start, err = strconv.ParseInt(lo, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if hi == "" {
		return start, full - 1, nil
	}
	end, err = strconv.ParseInt(hi, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

// the remaining s3Client methods are unused by these tests.
func (c *rangeClient) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return &s3.PutObjectOutput{}, nil
}
func (c *rangeClient) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}
func (c *rangeClient) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}
func (c *rangeClient) RenameObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return &s3.CopyObjectOutput{}, nil
}
func (c *rangeClient) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("u")}, nil
}
func (c *rangeClient) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{}, nil
}
func (c *rangeClient) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

// TestS3ReadFileLazyOpen covers the lazy/range read path: Open must not drain
// the body, the uncached path issues exactly one GetObject and no HeadObject,
// and the cache-supplied-head path issues no I/O until the first Read.
func TestS3ReadFileLazyOpen(t *testing.T) {
	const key = "objects/pack/p.pack"
	body := []byte("the quick brown fox jumps over the lazy dog")

	tests := []struct {
		name        string
		head        *s3.HeadObjectOutput // nil = uncached path
		wantOpenGet int64
	}{
		{
			name:        "uncached open issues one GetObject",
			head:        nil,
			wantOpenGet: 1,
		},
		{
			name:        "cache-supplied head issues no IO at open",
			head:        &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(body))), LastModified: aws.Time(time.Unix(0, 0))},
			wantOpenGet: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newRangeClient(map[string][]byte{key: body})
			f, err := newS3ReadFile(c, "bucket", key, key, tt.head)
			if err != nil {
				t.Fatalf("newS3ReadFile: %v", err)
			}
			t.Cleanup(func() { f.Close() })

			if got := c.gets.Load(); got != tt.wantOpenGet {
				t.Fatalf("open GetObject = %d, want %d", got, tt.wantOpenGet)
			}
			if got := c.heads.Load(); got != 0 {
				t.Fatalf("open HeadObject = %d, want 0", got)
			}
			if got := c.read.Load(); got != 0 {
				t.Fatalf("open drained %d body bytes, want 0 (body must be lazy)", got)
			}

			// Reading streams the body and yields the full contents.
			got, err := io.ReadAll(f)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("Read = %q, want %q", got, body)
			}

			// Stat reports the size derived from the response, no HeadObject.
			fi, err := f.Stat()
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if fi.Size() != int64(len(body)) {
				t.Fatalf("Stat size = %d, want %d", fi.Size(), len(body))
			}
			if got := c.heads.Load(); got != 0 {
				t.Fatalf("HeadObject = %d after Read+Stat, want 0", got)
			}
		})
	}
}

// TestS3ReadFileReadAt covers the random-access path: ReadAt learns the size
// from Content-Range, serves a nearby second offset from the read-ahead window
// without a second GetObject, and returns io.EOF past the end.
func TestS3ReadFileReadAt(t *testing.T) {
	const key = "objects/pack/p.idx"
	body := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	c := newRangeClient(map[string][]byte{key: body})

	// Cache-supplied head so Open does no I/O; ReadAt drives all GETs.
	f, err := newS3ReadFile(c, "bucket", key, key,
		&s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(body))), LastModified: aws.Time(time.Unix(0, 0))})
	if err != nil {
		t.Fatalf("newS3ReadFile: %v", err)
	}
	t.Cleanup(func() { f.Close() })

	// First ReadAt fetches a chunk and serves from it.
	buf := make([]byte, 4)
	if n, err := f.ReadAt(buf, 2); err != nil || n != 4 {
		t.Fatalf("ReadAt(_, 2) = (%d, %v), want (4, nil)", n, err)
	}
	if string(buf) != "2345" {
		t.Fatalf("ReadAt(_, 2) = %q, want %q", buf, "2345")
	}
	gets := c.gets.Load()
	if gets != 1 {
		t.Fatalf("first ReadAt GetObject = %d, want 1", gets)
	}

	// A nearby second offset is covered by the window: no new GetObject.
	if n, err := f.ReadAt(buf, 10); err != nil || n != 4 {
		t.Fatalf("ReadAt(_, 10) = (%d, %v), want (4, nil)", n, err)
	}
	if string(buf) != "abcd" {
		t.Fatalf("ReadAt(_, 10) = %q, want %q", buf, "abcd")
	}
	if c.gets.Load() != gets {
		t.Fatalf("windowed ReadAt issued a GetObject: %d -> %d", gets, c.gets.Load())
	}

	// Reading past the end returns io.EOF.
	if n, err := f.ReadAt(make([]byte, 4), int64(len(body))); !errors.Is(err, io.EOF) || n != 0 {
		t.Fatalf("ReadAt past end = (%d, %v), want (0, io.EOF)", n, err)
	}
}

// TestS3ReadFileReadAtUnknownSize exercises the head==nil-with-416 corner: when
// the size is not yet known and ReadAt starts past the end, the stub's 416
// InvalidRange must surface as io.EOF.
func TestS3ReadFileReadAtUnknownSize(t *testing.T) {
	const key = "objects/small"
	body := []byte("tiny")
	c := newRangeClient(map[string][]byte{key: body})

	// Uncached open issues a GetObject and adopts the (full) size, so force the
	// unknown-size 416 path directly on a fresh handle.
	f := &s3ReadFile{client: c, bucket: "bucket", key: key, name: key, size: -1}
	t.Cleanup(func() { f.Close() })

	if n, err := f.ReadAt(make([]byte, 2), 100); !errors.Is(err, io.EOF) || n != 0 {
		t.Fatalf("ReadAt past end (unknown size) = (%d, %v), want (0, io.EOF)", n, err)
	}
}

// TestS3ReadFileMissingKey confirms the uncached Open path still surfaces a
// NoSuchKey/NotFound error (basic.go relies on it for the directory probe).
func TestS3ReadFileMissingKey(t *testing.T) {
	c := newRangeClient(map[string][]byte{})
	if _, err := newS3ReadFile(c, "bucket", "absent", "absent", nil); err == nil {
		t.Fatal("newS3ReadFile of an absent key: want error, got nil")
	}
}

// TestS3ReadFileSeek covers Seek semantics: a Seek that moves the cursor drops
// the open body so the next Read reopens with a Range at the new position.
func TestS3ReadFileSeek(t *testing.T) {
	const key = "blob"
	body := []byte("HEADERthen the payload")
	c := newRangeClient(map[string][]byte{key: body})

	f, err := newS3ReadFile(c, "bucket", key, key, nil)
	if err != nil {
		t.Fatalf("newS3ReadFile: %v", err)
	}
	t.Cleanup(func() { f.Close() })

	if _, err := f.Seek(6, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll after seek: %v", err)
	}
	if want := body[6:]; !bytes.Equal(got, want) {
		t.Fatalf("Read after Seek = %q, want %q", got, want)
	}

	// SeekEnd needs the size; for the uncached handle it was learned at open.
	if pos, err := f.Seek(0, io.SeekEnd); err != nil || pos != int64(len(body)) {
		t.Fatalf("Seek(0, SeekEnd) = (%d, %v), want (%d, nil)", pos, err, len(body))
	}
	if n, err := f.Read(make([]byte, 1)); !errors.Is(err, io.EOF) || n != 0 {
		t.Fatalf("Read at EOF = (%d, %v), want (0, io.EOF)", n, err)
	}
}
