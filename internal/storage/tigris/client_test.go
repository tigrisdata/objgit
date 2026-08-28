package tigris

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

	putDelay time.Duration // artificial latency before PutObject lands, for async-upload tests

	livePuts    int // PutObject calls inside putDelay right now
	maxLivePuts int // high-water mark of livePuts, for upload-concurrency tests

	puts    int
	deletes int
	listMax int64 // ListObjectsV2 page size knob; 0 = unlimited

	headOverride map[string]*headShape

	rangedGets  int // GetObject calls that carried a Range header
	fullBinGets int // full (non-ranged) GetObject calls on a pack .bin — i.e. bulk pack downloads

	binGate        *binGate // when set, paces every full .bin body; see holdBinBodies
	liveBinBodies  int      // full .bin bodies being read right now
	maxLiveBinGets int      // high-water mark of liveBinBodies, for prefetch-concurrency tests
}

// binGate paces the bodies of full .bin GETs so a test can park a background
// pack download at a known number of bytes and inspect what reads do while it
// sits there. Without one, a fake body is delivered in a single unpaced read
// and the download is over before a test can look at it.
//
// A gated body hands out prefix bytes and then blocks until release is closed.
// prefix of 0 means it blocks before yielding anything at all, which is how a
// test pins a download open indefinitely.
type binGate struct {
	prefix  int
	release chan struct{}
	started chan struct{} // closed by the first gated body to reach the block
	once    sync.Once
}

// holdBinBodies makes every full .bin body stop after prefix bytes. The
// returned release lets them all finish; waitStarted blocks until at least one
// body has reached the stop.
func (f *fakeS3) holdBinBodies(t *testing.T, prefix int) (release func(), waitStarted func()) {
	t.Helper()
	g := &binGate{prefix: prefix, release: make(chan struct{}), started: make(chan struct{})}

	f.mu.Lock()
	f.binGate = g
	f.mu.Unlock()

	var closeOnce sync.Once
	release = func() { closeOnce.Do(func() { close(g.release) }) }
	t.Cleanup(release) // never leave a download goroutine parked past the test

	waitStarted = func() {
		t.Helper()
		select {
		case <-g.started:
		case <-time.After(10 * time.Second):
			t.Fatal("no gated .bin body ever started")
		}
	}
	return release, waitStarted
}

// peakLiveBinGets is the most full .bin bodies the fake ever had open at one
// time. Meaningful only with a gate installed.
func (f *fakeS3) peakLiveBinGets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxLiveBinGets
}

// gatedBody is a full .bin body that stops after g.prefix bytes until the gate
// releases. It also maintains the fake's live-body high-water mark, which is
// what the prefetch concurrency cap is measured against.
type gatedBody struct {
	f    *fakeS3
	g    *binGate
	body []byte
	pos  int
	held bool // this body is counted in liveBinBodies
}

func (b *gatedBody) Read(p []byte) (int, error) {
	if b.pos >= len(b.body) {
		return 0, io.EOF
	}

	limit := min(b.g.prefix, len(b.body))
	if b.pos >= limit {
		// At the stop line. Register as parked — which is what the live-body
		// high-water mark counts — and wait to be let through.
		if !b.held {
			b.held = true
			b.f.mu.Lock()
			b.f.liveBinBodies++
			if b.f.liveBinBodies > b.f.maxLiveBinGets {
				b.f.maxLiveBinGets = b.f.liveBinBodies
			}
			b.f.mu.Unlock()
			b.g.once.Do(func() { close(b.g.started) })
		}
		<-b.g.release
		limit = len(b.body)
	}

	n := copy(p, b.body[b.pos:limit])
	b.pos += n
	return n, nil
}

func (b *gatedBody) Close() error {
	if b.held {
		b.f.mu.Lock()
		b.f.liveBinBodies--
		b.f.mu.Unlock()
		b.held = false
	}
	return nil
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

// countingObserver returns a WithObserver option that tallies operations, plus
// a snapshot function.
//
// The mutex is not decoration. A Storer's observer fires from whatever
// goroutine made the call, and background pack prefetches (see startPackFetch)
// make S3 calls of their own — so an observer over a bare map races with the
// test that reads it. Production wires metrics.ObserveS3, which is a set of
// Prometheus vectors and already safe.
func countingObserver() (Option, func() map[string]int) {
	var mu sync.Mutex
	seen := map[string]int{}

	opt := WithObserver(func(op string, _ time.Duration, _ error) {
		mu.Lock()
		defer mu.Unlock()
		seen[op]++
	})
	snapshot := func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]int, len(seen))
		for k, v := range seen {
			out[k] = v
		}
		return out
	}
	return opt, snapshot
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

// peakLivePuts is the most PutObject calls the fake ever had in flight at one
// time. Meaningful only with putDelay set.
func (f *fakeS3) peakLivePuts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxLivePuts
}

func (f *fakeS3) ndeletes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletes
}

func (f *fakeS3) nrangedGets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rangedGets
}

// nfullBinGets counts bulk pack downloads: full (non-ranged) GETs of a .bin.
// Counting all full GETs would also sweep in the cold index build's .cue
// fetches, which say nothing about the bulk-download threshold.
func (f *fakeS3) nfullBinGets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fullBinGets
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

	body := o.body
	gated := false
	if rng := sv(p.Range); rng != "" {
		f.rangedGets++
		start, end, ok := parseByteRange(rng, len(o.body))
		if !ok {
			return nil, fmt.Errorf("fake GetObject: bad Range %q", rng)
		}
		body = o.body[start : end+1]
	} else if strings.HasSuffix(sv(p.Key), binSuffix) {
		f.fullBinGets++
		gated = f.binGate != nil
	}

	out := &s3.GetObjectOutput{
		ContentLength: ip(int64(len(body))),
		Metadata:      o.meta,
	}
	if gated {
		out.Body = &gatedBody{f: f, g: f.binGate, body: body}
	} else {
		out.Body = io.NopCloser(bytes.NewReader(body))
	}
	return out, nil
}

// parseByteRange parses a single-range "bytes=start-end" header (as
// internal/storage/tigris always sends) into inclusive start/end offsets
// clamped to size.
func parseByteRange(header string, size int) (start, end int, ok bool) {
	rest, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, false
	}
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	start, err1 := strconv.Atoi(parts[0])
	end, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || start < 0 || end < start || end >= size {
		return 0, 0, false
	}
	return start, end, true
}

func (f *fakeS3) PutObject(_ context.Context, p *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if f.putDelay > 0 {
		// Held outside the lock, so overlapping uploads really do overlap and
		// the high-water mark means something.
		f.mu.Lock()
		f.livePuts++
		if f.livePuts > f.maxLivePuts {
			f.maxLivePuts = f.livePuts
		}
		f.mu.Unlock()

		time.Sleep(f.putDelay)

		f.mu.Lock()
		f.livePuts--
		f.mu.Unlock()
	}
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

// TestScopedIsolatesRepositoriesInOneBucket verifies two Scoped storers over
// the same client/bucket read and write independent data, keyed by prefix.
func TestScopedIsolatesRepositoriesInOneBucket(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	base := newTestStorer(t, f)

	widgets := base.Scoped("acme/widgets")
	gadgets := base.Scoped("acme/gadgets")

	h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "hello")
	// seed wrote at the unprefixed key; write the same content through each
	// scoped storer instead, so each lands under its own prefix.
	wobj := plumbing.NewMemoryObject(plumbing.FromObjectFormat(formatcfg.DefaultObjectFormat))
	wobj.SetType(plumbing.BlobObject)
	wobj.SetSize(5)
	if _, err := wobj.Write([]byte("hello")); err != nil {
		t.Fatalf("buffer object: %v", err)
	}
	if _, err := widgets.SetEncodedObject(wobj); err != nil {
		t.Fatalf("widgets SetEncodedObject: %v", err)
	}
	if err := widgets.up.flush(); err != nil {
		t.Fatalf("widgets flush: %v", err)
	}

	if err := widgets.HasEncodedObject(h); err != nil {
		t.Errorf("widgets should see its own object: %v", err)
	}
	if err := gadgets.HasEncodedObject(h); !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Errorf("gadgets should not see widgets' object, got: %v", err)
	}

	if _, ok := f.objs["acme/widgets/"+keyOf(h)]; !ok {
		t.Errorf("expected key %q in the fake bucket, got keys %v", "acme/widgets/"+keyOf(h), f.objs)
	}
}

// TestScopedNestsPrefixes verifies scoping an already-scoped Storer extends
// its prefix rather than replacing it.
func TestScopedNestsPrefixes(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	base := newTestStorer(t, f)

	nested := base.Scoped("acme").Scoped("widgets")
	if nested.prefix != "acme/widgets/" {
		t.Errorf("nested prefix = %q, want %q", nested.prefix, "acme/widgets/")
	}
}

func TestObserverIsInvokedByOperation(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	// Seed a blob so we can test HasEncodedObject finds it via observer.
	h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "x")

	obs, snapshot := countingObserver()
	s := newTestStorer(t, f, obs)

	// HasEncodedObject should succeed (object exists) and observer records the op.
	if err := s.HasEncodedObject(h); err != nil {
		t.Fatalf("HasEncodedObject failed for existing object: %v", err)
	}
	seen := snapshot()
	if got := seen["HeadObject"]; got != 1 {
		t.Errorf("observer recorded HeadObject calls: got %d, want 1 (map: %v)", got, seen)
	}
}
