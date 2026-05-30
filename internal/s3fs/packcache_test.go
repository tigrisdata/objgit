package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
)

// packStub serves fixed object bytes and counts GetObject calls per key, so a
// test can assert the cache downloads each object exactly once. The embedded nil
// s3Client is never used: PackCache only calls GetObject.
type packStub struct {
	s3Client
	mu   sync.Mutex
	objs map[string][]byte
	gets map[string]int
}

func newPackStub(objs map[string][]byte) *packStub {
	return &packStub{objs: objs, gets: map[string]int{}}
}

func (s *packStub) getCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key]
}

func (s *packStub) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := aws.ToString(in.Key)
	b, ok := s.objs[key]
	if !ok {
		return nil, notFound()
	}
	s.gets[key]++
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(b)),
		ContentLength: aws.Int64(int64(len(b))),
	}, nil
}

// gateReader is an io.ReadCloser whose bytes are released to readers only when
// the test calls release/releaseAll, so a test can drive the streaming pump byte
// by byte and observe reads that race the download. Optionally it injects an
// error once the reader reaches a given offset.
type gateReader struct {
	mu       sync.Mutex
	cond     *sync.Cond
	data     []byte
	released int
	pos      int
	errAt    int // -1 disables; otherwise return failErr once pos reaches it
	failErr  error
	closed   bool
}

func newGateReader(data []byte) *gateReader {
	g := &gateReader{data: data, errAt: -1}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *gateReader) release(n int) {
	g.mu.Lock()
	g.released += n
	if g.released > len(g.data) {
		g.released = len(g.data)
	}
	g.cond.Broadcast()
	g.mu.Unlock()
}

func (g *gateReader) releaseAll() { g.release(len(g.data)) }

func (g *gateReader) Read(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if g.errAt >= 0 && g.pos >= g.errAt {
			return 0, g.failErr
		}
		if g.pos < g.released {
			n := copy(p, g.data[g.pos:g.released])
			g.pos += n
			return n, nil
		}
		if g.pos >= len(g.data) {
			return 0, io.EOF
		}
		if g.closed {
			return 0, io.ErrClosedPipe
		}
		g.cond.Wait()
	}
}

func (g *gateReader) Close() error {
	g.mu.Lock()
	g.closed = true
	g.cond.Broadcast()
	g.mu.Unlock()
	return nil
}

// gateStub serves a gateReader body per key so streaming behaviour can be
// driven deterministically.
type gateStub struct {
	s3Client
	mu    sync.Mutex
	gates map[string]*gateReader
	gets  map[string]int
}

func newGateStub() *gateStub {
	return &gateStub{gates: map[string]*gateReader{}, gets: map[string]int{}}
}

func (s *gateStub) put(key string, g *gateReader) {
	s.mu.Lock()
	s.gates[key] = g
	s.mu.Unlock()
}

func (s *gateStub) getCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key]
}

func (s *gateStub) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := aws.ToString(in.Key)
	g, ok := s.gates[key]
	if !ok {
		return nil, notFound()
	}
	s.gets[key]++
	return &s3.GetObjectOutput{
		Body:          g,
		ContentLength: aws.Int64(int64(len(g.data))),
	}, nil
}

// newTestPackCache builds a PackCache under t.TempDir with the given budget and
// registers Cleanup.
func newTestPackCache(t *testing.T, maxBytes int64) *PackCache {
	t.Helper()
	pc, err := NewPackCache(t.TempDir(), maxBytes)
	if err != nil {
		t.Fatalf("NewPackCache: %v", err)
	}
	t.Cleanup(func() { pc.Cleanup() })
	return pc
}

func mustReadAll(t *testing.T, f billy.File) []byte {
	t.Helper()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return b
}

// readAtChan runs f.ReadAt in a goroutine and returns a channel that yields the
// result, so a test can assert whether a read is pending or has completed
// without hanging the test on a buggy block.
type readAtResult struct {
	n   int
	err error
	p   []byte
}

func readAtChan(f billy.File, length int, off int64) <-chan readAtResult {
	ch := make(chan readAtResult, 1)
	go func() {
		p := make([]byte, length)
		n, err := f.ReadAt(p, off)
		ch <- readAtResult{n: n, err: err, p: p}
	}()
	return ch
}

func TestPackCacheReads(t *testing.T) {
	ctx := context.Background()
	const key = "repo.git/objects/pack/pack-abc.pack"
	const name = "objects/pack/pack-abc.pack"
	want := bytes.Repeat([]byte("PACKDATA"), 4096) // 32 KiB

	tests := []struct {
		name string
		// check exercises one read path and returns (got, wantSlice) to compare.
		check func(t *testing.T, f billy.File) (got, exp []byte)
	}{
		{
			name: "sequential Read",
			check: func(t *testing.T, f billy.File) ([]byte, []byte) {
				return mustReadAll(t, f), want
			},
		},
		{
			name: "ReadAt mid-object",
			check: func(t *testing.T, f billy.File) ([]byte, []byte) {
				p := make([]byte, 100)
				if _, err := f.ReadAt(p, 8000); err != nil && err != io.EOF {
					t.Fatalf("ReadAt: %v", err)
				}
				return p, want[8000:8100]
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newPackStub(map[string][]byte{key: want})
			pc := newTestPackCache(t, 0)

			f, err := pc.open(ctx, stub, "bucket", key, name)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer f.Close()

			got, exp := tt.check(t, f)
			if !bytes.Equal(got, exp) {
				t.Errorf("read bytes mismatch: got %d bytes, want %d", len(got), len(exp))
			}
			if f.Name() != name {
				t.Errorf("Name = %q, want %q", f.Name(), name)
			}
		})
	}
}

// TestPackCacheDownloadsOnce confirms repeated opens of the same key hit the
// cache: exactly one GetObject regardless of how many readers open it. This is
// the whole point — replacing thousands of S3 round-trips with one.
func TestPackCacheDownloadsOnce(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/pack-x.pack"
	data := bytes.Repeat([]byte{0xAB}, 1024)
	stub := newPackStub(map[string][]byte{key: data})
	pc := newTestPackCache(t, 0)

	const opens = 25
	for i := range opens {
		f, err := pc.open(ctx, stub, "bucket", key, "name")
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if got := mustReadAll(t, f); !bytes.Equal(got, data) {
			t.Fatalf("open %d: bytes mismatch", i)
		}
		f.Close()
	}
	if n := stub.getCount(key); n != 1 {
		t.Errorf("GetObject called %d times across %d opens, want 1", n, opens)
	}
}

// TestPackCacheIndependentCursors verifies two open handles over the same cached
// file have independent seek positions.
func TestPackCacheIndependentCursors(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/pack-y.idx"
	data := []byte("0123456789abcdef")
	stub := newPackStub(map[string][]byte{key: data})
	pc := newTestPackCache(t, 0)

	a, err := pc.open(ctx, stub, "bucket", key, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := pc.open(ctx, stub, "bucket", key, "b")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if _, err := a.Seek(10, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	pa := make([]byte, 3)
	if _, err := io.ReadFull(a, pa); err != nil {
		t.Fatal(err)
	}
	if string(pa) != "abc" {
		t.Errorf("a read %q, want abc", pa)
	}
	// b's cursor is untouched, still at 0.
	pb := make([]byte, 3)
	if _, err := io.ReadFull(b, pb); err != nil {
		t.Fatal(err)
	}
	if string(pb) != "012" {
		t.Errorf("b read %q, want 012 (independent cursor)", pb)
	}
}

func TestPackCacheClosedReadFails(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/pack-z.pack"
	stub := newPackStub(map[string][]byte{key: []byte("data")})
	pc := newTestPackCache(t, 0)

	f, err := pc.open(ctx, stub, "bucket", key, "n")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := f.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Errorf("read after close: err = %v, want os.ErrClosed", err)
	}
}

func TestPackCacheMissingObject(t *testing.T) {
	stub := newPackStub(map[string][]byte{})
	pc := newTestPackCache(t, 0)

	_, err := pc.open(context.Background(), stub, "bucket", "r.git/objects/pack/absent.pack", "n")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("open absent: err = %v, want fs.ErrNotExist", err)
	}
}

// TestPackCacheEviction checks that exceeding the byte budget evicts the
// least-recently-opened entry (re-download on next open) while a reader holding
// the evicted file open still reads correct bytes (unlinked-while-open).
func TestPackCacheEviction(t *testing.T) {
	ctx := context.Background()
	const keyA = "r.git/objects/pack/A.pack"
	const keyB = "r.git/objects/pack/B.pack"
	dataA := bytes.Repeat([]byte("A"), 1000)
	dataB := bytes.Repeat([]byte("B"), 1000)
	stub := newPackStub(map[string][]byte{keyA: dataA, keyB: dataB})

	// Budget holds only one object; opening B evicts A.
	pc := newTestPackCache(t, 1500)

	fa, err := pc.open(ctx, stub, "bucket", keyA, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer fa.Close()
	// Touch A so its download completes and it is fully accounted in the budget
	// before B arrives.
	if got := mustReadAll(t, fa); !bytes.Equal(got, dataA) {
		t.Fatalf("A bytes mismatch")
	}

	// Open B: total (2000) exceeds 1500, so A is evicted from the cache.
	fb, err := pc.open(ctx, stub, "bucket", keyB, "b")
	if err != nil {
		t.Fatal(err)
	}
	if got := mustReadAll(t, fb); !bytes.Equal(got, dataB) {
		t.Fatalf("B bytes mismatch")
	}
	fb.Close()

	// fa was opened before eviction; its fd still reads A's bytes correctly.
	if got := mustReadAll(t, fa); !bytes.Equal(got, dataA) {
		t.Errorf("evicted-but-open reader: bytes mismatch")
	}

	// Re-opening A must re-download (its cache entry was dropped).
	fa2, err := pc.open(ctx, stub, "bucket", keyA, "a2")
	if err != nil {
		t.Fatal(err)
	}
	if got := mustReadAll(t, fa2); !bytes.Equal(got, dataA) {
		t.Fatalf("A re-download bytes mismatch")
	}
	fa2.Close()
	if n := stub.getCount(keyA); n != 2 {
		t.Errorf("A GetObject count = %d, want 2 (re-downloaded after eviction)", n)
	}
}

// TestPackCacheServeWhileDownloading confirms a read of an already-arrived
// prefix returns without waiting for the rest of the object to download.
func TestPackCacheServeWhileDownloading(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/stream.pack"
	data := bytes.Repeat([]byte("STREAMDATA"), 1000) // 10 KiB
	g := newGateReader(data)
	stub := newGateStub()
	stub.put(key, g)
	pc := newTestPackCache(t, 0)

	f, err := pc.open(ctx, stub, "bucket", key, "n")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	// Release only the first 200 bytes; the rest of the object is still
	// "downloading" (gated). A read of the prefix must succeed promptly.
	g.release(200)
	ch := readAtChan(f, 100, 0)
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("ReadAt prefix: %v", res.err)
		}
		if !bytes.Equal(res.p, data[:100]) {
			t.Fatalf("ReadAt prefix bytes mismatch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadAt of arrived prefix blocked waiting for full download")
	}

	// Let the rest finish so Close/Cleanup don't race a stuck pump.
	g.releaseAll()
}

// TestPackCacheReadAheadBlocks confirms a read past the current watermark blocks
// until those bytes arrive, then returns them.
func TestPackCacheReadAheadBlocks(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/ahead.pack"
	data := bytes.Repeat([]byte("0123456789"), 1000) // 10 KiB
	g := newGateReader(data)
	stub := newGateStub()
	stub.put(key, g)
	pc := newTestPackCache(t, 0)

	f, err := pc.open(ctx, stub, "bucket", key, "n")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	g.release(100)
	// Read at offset 5000, far past the released watermark: must stay pending.
	ch := readAtChan(f, 64, 5000)
	select {
	case res := <-ch:
		t.Fatalf("ReadAt past watermark returned early: n=%d err=%v", res.n, res.err)
	case <-time.After(200 * time.Millisecond):
		// good, still blocked
	}

	// Release enough to cover the requested range; the read now completes.
	g.releaseAll()
	select {
	case res := <-ch:
		if res.err != nil && res.err != io.EOF {
			t.Fatalf("ReadAt after release: %v", res.err)
		}
		if !bytes.Equal(res.p, data[5000:5064]) {
			t.Fatalf("ReadAt bytes mismatch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadAt stayed blocked after data arrived")
	}
}

// TestPackCacheBelowWindowReadsDisk confirms that once the RAM window has
// scrolled past an offset, reads of that offset are served from disk correctly.
func TestPackCacheBelowWindowReadsDisk(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/big.pack"
	// Larger than ringCap so the front of the object is dropped from RAM.
	data := bytes.Repeat([]byte("Z"), ringCap+(1<<20))
	for i := range data { // make bytes position-dependent to catch offset bugs
		data[i] = byte(i % 251)
	}
	stub := newPackStub(map[string][]byte{key: data})
	pc := newTestPackCache(t, 0)

	f, err := pc.open(ctx, stub, "bucket", key, "n")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	// Force the whole download (synchronous body), so the window is fully
	// trimmed and the entry is done; offset 0 must come from disk.
	if got := mustReadAll(t, f); !bytes.Equal(got, data) {
		t.Fatalf("full read mismatch")
	}
	p := make([]byte, 128)
	if _, err := f.ReadAt(p, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadAt offset 0 from disk: %v", err)
	}
	if !bytes.Equal(p, data[:128]) {
		t.Fatalf("disk-served bytes mismatch at offset 0")
	}
	// A mid-object offset below the (now empty) window, too.
	off := int64(ringCap / 2)
	if _, err := f.ReadAt(p, off); err != nil && err != io.EOF {
		t.Fatalf("ReadAt mid offset from disk: %v", err)
	}
	if !bytes.Equal(p, data[off:off+128]) {
		t.Fatalf("disk-served bytes mismatch at offset %d", off)
	}
}

// TestPackCacheStreamError confirms a mid-stream download error surfaces to
// readers and the entry is dropped so a later open re-downloads.
func TestPackCacheStreamError(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/err.pack"
	data := bytes.Repeat([]byte("E"), 4096)
	g := newGateReader(data)
	boom := errors.New("boom")
	g.errAt = 1024
	g.failErr = boom
	stub := newGateStub()
	stub.put(key, g)
	pc := newTestPackCache(t, 0)

	f, err := pc.open(ctx, stub, "bucket", key, "n")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Release past the error point; the pump reads 1024 bytes then errors.
	g.release(4096)

	// A read past the failed watermark eventually returns the error.
	deadline := time.Now().Add(2 * time.Second)
	var readErr error
	for time.Now().Before(deadline) {
		p := make([]byte, 64)
		_, readErr = f.ReadAt(p, 2048)
		if readErr != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !errors.Is(readErr, boom) {
		t.Fatalf("read after stream error: err = %v, want %v", readErr, boom)
	}
	f.Close()

	// The failed entry was dropped: a later open re-fetches. Use a fresh,
	// fully-releasable gate this time.
	g2 := newGateReader(data)
	g2.releaseAll()
	stub.put(key, g2)
	f2, err := pc.open(ctx, stub, "bucket", key, "n2")
	if err != nil {
		t.Fatalf("reopen after error: %v", err)
	}
	if got := mustReadAll(t, f2); !bytes.Equal(got, data) {
		t.Fatalf("reopen bytes mismatch")
	}
	f2.Close()
	if n := stub.getCount(key); n != 2 {
		t.Errorf("GetObject count = %d, want 2 (re-fetched after stream error)", n)
	}
}

// TestPackCacheConcurrentStreamingReaders confirms many readers opened while a
// pack is still downloading all observe the full, correct bytes, with a single
// GetObject.
func TestPackCacheConcurrentStreamingReaders(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/concurrent.pack"
	data := bytes.Repeat([]byte("CONCURRENT"), 2000) // 20 KiB
	g := newGateReader(data)
	stub := newGateStub()
	stub.put(key, g)
	pc := newTestPackCache(t, 0)

	const readers = 16
	files := make([]billy.File, readers)
	for i := range files {
		f, err := pc.open(ctx, stub, "bucket", key, "n")
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		files[i] = f
	}

	// Drip the body out in chunks while readers race to read it.
	go func() {
		for released := 0; released < len(data); released += 1024 {
			g.release(1024)
			time.Sleep(time.Millisecond)
		}
		g.releaseAll()
	}()

	var wg sync.WaitGroup
	errs := make([]error, readers)
	for i := range files {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got := make([]byte, len(data))
			if _, err := io.ReadFull(io.NewSectionReader(asReaderAt(files[i]), 0, int64(len(data))), got); err != nil {
				errs[i] = err
				return
			}
			if !bytes.Equal(got, data) {
				errs[i] = errors.New("bytes mismatch")
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("reader %d: %v", i, err)
		}
		files[i].Close()
	}
	if n := stub.getCount(key); n != 1 {
		t.Errorf("GetObject count = %d, want 1", n)
	}
}

// TestPackCacheClosedDuringStreaming confirms a handle closed mid-download
// returns os.ErrClosed and does not wedge the pump.
func TestPackCacheClosedDuringStreaming(t *testing.T) {
	ctx := context.Background()
	const key = "r.git/objects/pack/closing.pack"
	data := bytes.Repeat([]byte("X"), 8192)
	g := newGateReader(data)
	stub := newGateStub()
	stub.put(key, g)
	pc := newTestPackCache(t, 0)

	f, err := pc.open(ctx, stub, "bucket", key, "n")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	g.release(100)
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := f.ReadAt(make([]byte, 1), 0); !errors.Is(err, os.ErrClosed) {
		t.Errorf("ReadAt after close: err = %v, want os.ErrClosed", err)
	}
	g.releaseAll()
}

func TestIsPackCacheable(t *testing.T) {
	tests := map[string]bool{
		"r.git/objects/pack/pack-1.pack": true,
		"r.git/objects/pack/pack-1.idx":  true,
		"r.git/objects/pack/pack-1.rev":  true,
		"r.git/refs/heads/main":          false,
		"r.git/HEAD":                     false,
		"r.git/config":                   false,
	}
	for key, want := range tests {
		if got := isPackCacheable(key); got != want {
			t.Errorf("isPackCacheable(%q) = %v, want %v", key, got, want)
		}
	}
}

// asReaderAt adapts a billy.File to io.ReaderAt for use with io.SectionReader.
func asReaderAt(f billy.File) io.ReaderAt { return f }
