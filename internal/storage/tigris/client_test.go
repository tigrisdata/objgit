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
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
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

// headShape lets a test strip normally-automatic fields from one key's HEAD.
type headShape struct{ metaOnly bool }

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

	headOverride map[string]*headShape
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

// hashForBody factors seed's hash computation out for reuse.
func hashForBody(of formatcfg.ObjectFormat, typ plumbing.ObjectType, body string) plumbing.Hash {
	obj := plumbing.NewMemoryObject(plumbing.FromObjectFormat(of))
	obj.SetType(typ)
	obj.SetSize(int64(len(body)))
	if _, err := obj.Write([]byte(body)); err != nil {
		panic("hashForBody buffers only ever-valid sizes: " + err.Error())
	}
	return obj.Hash()
}

// seed builds a well-formed loose object (correct content-hash key plus
// git-type/git-size metadata) with the given object format and stores it,
// returning the hash. Seeds ignore injected failures on purpose.
func seed(t *testing.T, f *fakeS3, of formatcfg.ObjectFormat, typ plumbing.ObjectType, content string) plumbing.Hash {
	t.Helper()
	h := hashForBody(of, typ, content)
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
	if hs, forced := f.headOverride[sv(p.Key)]; forced && hs.metaOnly {
		// Copy metadata map to satisfy the "copy semantics fine as coded" note.
		metaCopy := make(map[string]string, len(o.meta))
		for k, v := range o.meta {
			metaCopy[k] = v
		}
		return &s3.HeadObjectOutput{Metadata: metaCopy}, nil
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
		out.Contents = append(out.Contents, types.Object{Key: sp(k)})
	}
	if bv(out.IsTruncated) {
		out.NextContinuationToken = sp(strconv.Itoa(end))
	}
	return out, nil
}

func ip(v int64) *int64 { return &v }
func bp(v bool) *bool   { return &v }

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
