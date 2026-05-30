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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func mustReadAll(t *testing.T, f *packCachedFile) []byte {
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

func TestPackCacheReads(t *testing.T) {
	ctx := context.Background()
	const key = "repo.git/objects/pack/pack-abc.pack"
	const name = "objects/pack/pack-abc.pack"
	want := bytes.Repeat([]byte("PACKDATA"), 4096) // 32 KiB

	tests := []struct {
		name string
		// check exercises one read path and returns (got, wantSlice) to compare.
		check func(t *testing.T, f *packCachedFile) (got, exp []byte)
	}{
		{
			name: "sequential Read",
			check: func(t *testing.T, f *packCachedFile) ([]byte, []byte) {
				return mustReadAll(t, f), want
			},
		},
		{
			name: "ReadAt mid-object",
			check: func(t *testing.T, f *packCachedFile) ([]byte, []byte) {
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
// file have independent seek positions (each gets its own *os.File).
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

	// Open B: total (2000) exceeds 1500, so A is evicted from the cache.
	fb, err := pc.open(ctx, stub, "bucket", keyB, "b")
	if err != nil {
		t.Fatal(err)
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
	fa2.Close()
	if n := stub.getCount(keyA); n != 2 {
		t.Errorf("A GetObject count = %d, want 2 (re-downloaded after eviction)", n)
	}
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
