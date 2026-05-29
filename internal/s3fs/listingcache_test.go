package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/go-git/go-billy/v6"
)

// stubClient is a counting, in-memory s3Client for cache tests. It honours
// Prefix+Delimiter listing so ReadDir/Stat resolution behaves like S3.
type stubClient struct {
	mu   sync.Mutex
	keys map[string]int64 // object key -> size

	heads atomic.Int64
	lists atomic.Int64
	puts  atomic.Int64
}

func newStub(keys ...string) *stubClient {
	s := &stubClient{keys: map[string]int64{}}
	for _, k := range keys {
		s.keys[k] = 0
	}
	return s
}

func notFound() error { return &smithy.GenericAPIError{Code: "NotFound", Message: "not found"} }

func (s *stubClient) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	s.heads.Add(1)
	s.mu.Lock()
	size, ok := s.keys[aws.ToString(in.Key)]
	s.mu.Unlock()
	if !ok {
		return nil, notFound()
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(size), LastModified: aws.Time(time.Unix(0, 0))}, nil
}

func (s *stubClient) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	s.lists.Add(1)
	prefix := aws.ToString(in.Prefix)
	delim := aws.ToString(in.Delimiter)

	s.mu.Lock()
	ks := make([]string, 0, len(s.keys))
	for k := range s.keys {
		ks = append(ks, k)
	}
	sizes := make(map[string]int64, len(s.keys))
	for k, v := range s.keys {
		sizes[k] = v
	}
	s.mu.Unlock()
	sort.Strings(ks)

	seenCP := map[string]bool{}
	var cps []types.CommonPrefix
	var contents []types.Object
	for _, k := range ks {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k[len(prefix):]
		if delim != "" {
			if i := strings.Index(rest, delim); i >= 0 {
				cp := prefix + rest[:i+1]
				if !seenCP[cp] {
					seenCP[cp] = true
					cps = append(cps, types.CommonPrefix{Prefix: aws.String(cp)})
				}
				continue
			}
		}
		contents = append(contents, types.Object{
			Key:          aws.String(k),
			Size:         aws.Int64(sizes[k]),
			LastModified: aws.Time(time.Unix(0, 0)),
		})
	}
	return &s3.ListObjectsV2Output{Contents: contents, CommonPrefixes: cps, IsTruncated: aws.Bool(false)}, nil
}

func (s *stubClient) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	s.puts.Add(1)
	s.mu.Lock()
	s.keys[aws.ToString(in.Key)] = 0
	s.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}

func (s *stubClient) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(nil))}, nil
}

func (s *stubClient) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	s.mu.Lock()
	delete(s.keys, aws.ToString(in.Key))
	s.mu.Unlock()
	return &s3.DeleteObjectOutput{}, nil
}

func (s *stubClient) RenameObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return &s3.CopyObjectOutput{}, nil
}

func (s *stubClient) CreateMultipartUpload(_ context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("u")}, nil
}

func (s *stubClient) UploadPart(_ context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{}, nil
}

func (s *stubClient) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

// newTestCache disables listing-driven head seeding so S3-call counts are
// deterministic; TestListingCacheHeadSeed exercises it explicitly.
func newTestCache(stub *stubClient, ttl time.Duration) *ListingCache {
	return NewListingCache(CacheConfig{TTL: ttl, DisableHeadPrefetch: true}, stub, "bucket", "/")
}

func newCachedFS(t *testing.T, stub *stubClient, cache *ListingCache) billy.Filesystem {
	t.Helper()
	fsys, err := NewS3FS(stub, "bucket", WithListingCache(cache))
	if err != nil {
		t.Fatalf("NewS3FS: %v", err)
	}
	return fsys
}

func TestListingCachePopulateOnMiss(t *testing.T) {
	stub := newStub("objects/ab/file1")
	cache := newTestCache(stub, time.Hour)
	fsys := newCachedFS(t, stub, cache)

	// First Stat of an absent key lists the parent once, no HeadObject.
	if _, err := fsys.Stat("objects/ab/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat absent: want ErrNotExist, got %v", err)
	}
	if got := stub.lists.Load(); got != 1 {
		t.Fatalf("first miss: lists = %d, want 1", got)
	}
	if got := stub.heads.Load(); got != 0 {
		t.Fatalf("first miss: heads = %d, want 0", got)
	}

	// A second absent sibling is a pure negative hit: no S3 at all.
	l0, h0 := stub.lists.Load(), stub.heads.Load()
	if _, err := fsys.Stat("objects/ab/nope2"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat absent2: want ErrNotExist, got %v", err)
	}
	if stub.lists.Load() != l0 || stub.heads.Load() != h0 {
		t.Fatalf("negative hit did S3: lists %d->%d heads %d->%d", l0, stub.lists.Load(), h0, stub.heads.Load())
	}

	// A present file resolves from cache but still HeadObjects for metadata.
	l0, h0 = stub.lists.Load(), stub.heads.Load()
	fi, err := fsys.Stat("objects/ab/file1")
	if err != nil {
		t.Fatalf("Stat present: %v", err)
	}
	if fi.IsDir() {
		t.Fatalf("Stat present: reported a directory")
	}
	if stub.lists.Load() != l0 {
		t.Fatalf("present file re-listed: %d->%d", l0, stub.lists.Load())
	}
	if stub.heads.Load() != h0+1 {
		t.Fatalf("present file heads = %d, want %d", stub.heads.Load(), h0+1)
	}
}

func TestListingCacheReadDirThenStat(t *testing.T) {
	stub := newStub("objects/ab/file1", "objects/ab/sub/deep")
	cache := newTestCache(stub, time.Hour)
	fsys := newCachedFS(t, stub, cache)

	entries, err := fsys.ReadDir("objects/ab")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("ReadDir entries = %d, want 2", len(entries))
	}
	if stub.lists.Load() != 1 {
		t.Fatalf("ReadDir lists = %d, want 1", stub.lists.Load())
	}

	// The sub-directory resolves from the cached listing with no S3 calls.
	l0, h0 := stub.lists.Load(), stub.heads.Load()
	fi, err := fsys.Stat("objects/ab/sub")
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if !fi.IsDir() {
		t.Fatalf("Stat dir: not a directory")
	}
	if stub.lists.Load() != l0 || stub.heads.Load() != h0 {
		t.Fatalf("dir resolve did S3: lists %d->%d heads %d->%d", l0, stub.lists.Load(), h0, stub.heads.Load())
	}

	// And an absent sibling is a negative hit.
	if _, err := fsys.Stat("objects/ab/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat missing: want ErrNotExist, got %v", err)
	}
	if stub.lists.Load() != l0 || stub.heads.Load() != h0 {
		t.Fatalf("negative hit did S3 after ReadDir")
	}
}

func TestListingCacheInvalidatesOnWrite(t *testing.T) {
	stub := newStub("refs/heads/main")
	cache := newTestCache(stub, time.Hour)
	fsys := newCachedFS(t, stub, cache)

	// Warm the cache and confirm the new ref reads as absent.
	if _, err := fsys.Stat("refs/heads/new"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat before write: want ErrNotExist, got %v", err)
	}

	// Write the ref; Close must invalidate the parent prefix.
	f, err := fsys.Create("refs/heads/new")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.Write([]byte("deadbeef")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The next Stat must re-list (generation bumped) and now see the ref.
	fi, err := fsys.Stat("refs/heads/new")
	if err != nil {
		t.Fatalf("Stat after write: %v", err)
	}
	if fi.IsDir() {
		t.Fatalf("Stat after write: reported a directory")
	}
}

func TestListingCacheWindowExpiry(t *testing.T) {
	stub := newStub("objects/ab/file1")
	cache := newTestCache(stub, time.Minute)
	now := time.Unix(1_000_000, 0)
	cache.clock = func() time.Time { return now }
	fsys := newCachedFS(t, stub, cache)

	if _, err := fsys.Stat("objects/ab/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("Stat 1")
	}
	if stub.lists.Load() != 1 {
		t.Fatalf("lists = %d, want 1", stub.lists.Load())
	}
	// Same window: served from cache.
	if _, err := fsys.Stat("objects/ab/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("Stat 2")
	}
	if stub.lists.Load() != 1 {
		t.Fatalf("same window re-listed: lists = %d, want 1", stub.lists.Load())
	}
	// Advance past the TTL window: the key changes and we re-list.
	now = now.Add(2 * time.Minute)
	if _, err := fsys.Stat("objects/ab/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("Stat 3")
	}
	if stub.lists.Load() != 2 {
		t.Fatalf("new window lists = %d, want 2", stub.lists.Load())
	}
}

func TestListingCacheWarmer(t *testing.T) {
	stub := newStub("objects/ab/file1")
	cache := newTestCache(stub, time.Hour)
	cache.cfg.RefreshInterval = time.Millisecond // enable warming
	cache.cfg.IdleTTL = time.Minute
	now := time.Unix(1_000_000, 0)
	cache.clock = func() time.Time { return now }

	// Touch a prefix, then warm: the warmer fills it (one list).
	cache.touch("objects/ab/")
	cache.warmOnce(context.Background())
	if stub.lists.Load() != 1 {
		t.Fatalf("warmer lists = %d, want 1", stub.lists.Load())
	}

	// After the idle window the warmer evicts the prefix from its working set.
	now = now.Add(2 * time.Minute)
	cache.warmOnce(context.Background())
	cache.mu.Lock()
	n := len(cache.seen)
	cache.mu.Unlock()
	if n != 0 {
		t.Fatalf("idle prefix not evicted: seen = %d", n)
	}
}

func TestListingCacheHeadSeed(t *testing.T) {
	stub := newStub("objects/ab/f1", "objects/ab/f2")
	cache := NewListingCache(CacheConfig{TTL: time.Hour}, stub, "bucket", "/")
	fsys := newCachedFS(t, stub, cache)

	// A single listing fill (here triggered by an absent lookup) seeds the head
	// cache for every file in the folder straight from the ListObjectsV2 data —
	// no HeadObject round-trips at all.
	if _, err := fsys.Stat("objects/ab/nope"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat: %v", err)
	}
	if got := stub.heads.Load(); got != 0 {
		t.Fatalf("seeding issued HeadObjects: got %d, want 0", got)
	}

	// Stat of a seeded file is served from the head cache: still no HeadObject,
	// and it reports a file with the listed size.
	fi, err := fsys.Stat("objects/ab/f1")
	if err != nil {
		t.Fatalf("Stat f1: %v", err)
	}
	if fi.IsDir() {
		t.Fatalf("Stat f1: reported a directory")
	}
	if got := stub.heads.Load(); got != 0 {
		t.Fatalf("seeded Stat did a HeadObject: got %d, want 0", got)
	}
}

func TestListingCacheSubtreeCollapsesFolders(t *testing.T) {
	stub := newStub("refs/heads/main", "refs/heads/dev", "refs/tags/v1")
	cache := newTestCache(stub, time.Hour) // default RecursivePrefixes = {"refs/"}
	fsys := newCachedFS(t, stub, cache)

	// The first touch of any refs/ folder scans the whole refs/ subtree once.
	entries, err := fsys.ReadDir("refs/heads")
	if err != nil {
		t.Fatalf("ReadDir refs/heads: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("refs/heads entries = %d, want 2", len(entries))
	}
	if stub.lists.Load() != 1 {
		t.Fatalf("first refs read lists = %d, want 1", stub.lists.Load())
	}

	// A different refs/ folder is served from that same subtree: no new S3.
	l0 := stub.lists.Load()
	tags, err := fsys.ReadDir("refs/tags")
	if err != nil {
		t.Fatalf("ReadDir refs/tags: %v", err)
	}
	if len(tags) != 1 {
		t.Fatalf("refs/tags entries = %d, want 1", len(tags))
	}
	if stub.lists.Load() != l0 {
		t.Fatalf("second refs folder re-listed: %d->%d", l0, stub.lists.Load())
	}

	// refs/ itself synthesises its child directories from the subtree.
	top, err := fsys.ReadDir("refs")
	if err != nil {
		t.Fatalf("ReadDir refs: %v", err)
	}
	if len(top) != 2 {
		t.Fatalf("refs entries = %d, want 2 (heads, tags)", len(top))
	}
	for _, e := range top {
		if !e.IsDir() {
			t.Fatalf("refs child %q not a directory", e.Name())
		}
	}

	// And a negative lookup anywhere under refs/ is free.
	if _, err := fsys.Stat("refs/heads/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat missing: want ErrNotExist, got %v", err)
	}
	if stub.lists.Load() != l0 {
		t.Fatalf("negative lookup did S3: %d->%d", l0, stub.lists.Load())
	}
}

func TestListingCacheSubtreeInvalidate(t *testing.T) {
	stub := newStub("refs/heads/main")
	cache := newTestCache(stub, time.Hour)
	fsys := newCachedFS(t, stub, cache)

	// Warm the whole refs/ subtree by reading one folder.
	if _, err := fsys.ReadDir("refs/heads"); err != nil {
		t.Fatalf("ReadDir refs/heads: %v", err)
	}
	if stub.lists.Load() != 1 {
		t.Fatalf("warm lists = %d, want 1", stub.lists.Load())
	}

	// Write a ref into a *sibling* folder. Ancestor invalidation must move the
	// refs/ subtree key so the next read re-scans and sees it.
	f, err := fsys.Create("refs/tags/v1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.Write([]byte("deadbeef")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tags, err := fsys.ReadDir("refs/tags")
	if err != nil {
		t.Fatalf("ReadDir refs/tags after write: %v", err)
	}
	if len(tags) != 1 {
		t.Fatalf("refs/tags entries = %d, want 1", len(tags))
	}
	if stub.lists.Load() < 2 {
		t.Fatalf("subtree not re-scanned after sibling write: lists = %d", stub.lists.Load())
	}
}

func TestListingCacheSubtreeTruncationFallback(t *testing.T) {
	stub := newStub("refs/heads/a", "refs/heads/b", "refs/heads/c")
	cache := NewListingCache(CacheConfig{
		TTL:                 time.Hour,
		DisableHeadPrefetch: true,
		MaxSubtreeKeys:      2, // 3 refs exceed the cap → subtree abandoned
	}, stub, "bucket", "/")
	fsys := newCachedFS(t, stub, cache)

	// The oversized subtree scan is abandoned; the folder is still served
	// correctly, by falling back to a delimited per-folder listing.
	entries, err := fsys.ReadDir("refs/heads")
	if err != nil {
		t.Fatalf("ReadDir refs/heads: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}
	// One truncated subtree scan + one delimited fallback listing.
	if stub.lists.Load() != 2 {
		t.Fatalf("fallback lists = %d, want 2", stub.lists.Load())
	}
}

func TestListingCacheChrootShares(t *testing.T) {
	stub := newStub("repo/objects/ab/file1")
	cache := newTestCache(stub, time.Hour)
	rootfs := newCachedFS(t, stub, cache)

	// Populate the listing for repo/objects/ab/ via the root view.
	if _, err := rootfs.Stat("repo/objects/ab/missing1"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("root Stat: want ErrNotExist, got %v", err)
	}
	if stub.lists.Load() != 1 {
		t.Fatalf("root Stat lists = %d, want 1", stub.lists.Load())
	}

	// A chroot view shares the same cache keyed by canonical prefix, so a Stat
	// under it hits the cached listing with no further S3.
	sub, err := rootfs.Chroot("repo")
	if err != nil {
		t.Fatalf("Chroot: %v", err)
	}
	l0 := stub.lists.Load()
	if _, err := sub.Stat("objects/ab/missing2"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("chroot Stat: want ErrNotExist, got %v", err)
	}
	if stub.lists.Load() != l0 {
		t.Fatalf("chroot Stat re-listed: %d->%d", l0, stub.lists.Load())
	}
}
