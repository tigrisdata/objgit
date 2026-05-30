package s3fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// isPackCacheable reports whether key names an immutable pack-directory file
// that benefits from the local temp-file cache. These files are content-
// addressed (pack-<sha>.{pack,idx,rev}) and re-read with random access many
// times while serving a clone, so a single download served from local disk
// replaces thousands of S3 round-trips. See docs/plans/pack-temp-file-cache.md.
func isPackCacheable(key string) bool {
	return strings.HasSuffix(key, ".pack") ||
		strings.HasSuffix(key, ".idx") ||
		strings.HasSuffix(key, ".rev")
}

// PackCache materialises immutable pack-directory objects to local temp files
// and serves their reads from disk. go-git's upload-pack re-reads pack objects
// thousands of times during delta compression; without this each access is a
// fresh S3 GetObject and a clone never completes. The cache is shared by pointer
// across an S3FS and all of its Chroot children.
//
// Entries are keyed by S3 object key. Each downloads its object once and is
// reused across opens; a total-bytes budget evicts the least-recently-opened
// entries. Eviction unlinks the temp file, which on Linux leaves already-open
// readers working until they close, so eviction never corrupts an in-flight
// read.
type PackCache struct {
	dir      string
	maxBytes int64

	mu       sync.Mutex
	entries  map[string]*packEntry
	curBytes int64
	seq      uint64 // monotonic open counter; entry.used orders the LRU
}

// packEntry is one cached object. once guards the single download; path/size/err
// are set by it. used is the seq of the most recent open, for LRU ordering.
type packEntry struct {
	key  string
	once sync.Once
	path string
	size int64
	err  error
	used uint64
}

// NewPackCache creates a pack cache writing temp files under a fresh directory
// inside parent (os.TempDir() when empty). maxBytes bounds the total size of
// cached files; opens past the budget evict the least-recently-opened entries.
// A maxBytes <= 0 disables the budget (no eviction). Call Cleanup to remove the
// temp directory.
func NewPackCache(parent string, maxBytes int64) (*PackCache, error) {
	if parent == "" {
		parent = os.TempDir()
	}
	dir, err := os.MkdirTemp(parent, "objgit-packs-")
	if err != nil {
		return nil, fmt.Errorf("s3fs: create pack cache dir: %w", err)
	}
	return &PackCache{
		dir:      dir,
		maxBytes: maxBytes,
		entries:  make(map[string]*packEntry),
	}, nil
}

// Cleanup removes the cache's temp directory and all files in it. Already-open
// readers keep working (unlinked-while-open); new opens after Cleanup fail.
func (c *PackCache) Cleanup() error {
	c.mu.Lock()
	dir := c.dir
	c.entries = map[string]*packEntry{}
	c.curBytes = 0
	c.mu.Unlock()
	return os.RemoveAll(dir)
}

// open returns a billy.File for key, downloading the object to a temp file on
// first use and serving from that file thereafter. Each call returns an
// independent *os.File handle so concurrent readers have their own seek cursor.
func (c *PackCache) open(ctx context.Context, client s3Client, bucket, key, name string) (*packCachedFile, error) {
	c.mu.Lock()
	e := c.entries[key]
	if e == nil {
		e = &packEntry{key: key}
		c.entries[key] = e
	}
	c.mu.Unlock()

	e.once.Do(func() {
		path, size, err := c.download(ctx, client, bucket, key)
		if err != nil {
			e.err = err
			// Drop the failed entry so a later open retries the download.
			c.mu.Lock()
			if c.entries[key] == e {
				delete(c.entries, key)
			}
			c.mu.Unlock()
			return
		}
		e.path, e.size = path, size
		c.mu.Lock()
		c.curBytes += size
		c.evictLocked(key)
		c.mu.Unlock()
	})
	if e.err != nil {
		return nil, e.err
	}

	f, err := os.Open(e.path)
	if err != nil {
		// The cached file was evicted/cleaned between download and open; retry
		// through a fresh entry so the object is fetched again.
		c.mu.Lock()
		if c.entries[key] == e {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return nil, err
	}

	c.mu.Lock()
	c.seq++
	e.used = c.seq
	c.mu.Unlock()

	return &packCachedFile{File: f, name: name}, nil
}

// download streams the full object to a temp file and returns its path and size.
func (c *PackCache) download(ctx context.Context, client s3Client, bucket, key string) (string, int64, error) {
	start := time.Now()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	observeS3("GetObject", start, err)
	if err != nil {
		if isNotFound(err) {
			return "", 0, &os.PathError{Op: "open", Path: key, Err: fs.ErrNotExist}
		}
		return "", 0, fmt.Errorf("pack cache GetObject %q: %w", key, err)
	}
	defer out.Body.Close()

	tmp, err := os.CreateTemp(c.dir, "obj-")
	if err != nil {
		return "", 0, fmt.Errorf("pack cache temp file: %w", err)
	}
	n, err := io.Copy(tmp, out.Body)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp.Name())
		return "", 0, fmt.Errorf("pack cache download %q: %w", key, err)
	}
	return tmp.Name(), n, nil
}

// evictLocked removes least-recently-opened entries until the cache is within
// budget. keep is never evicted (it is the entry the caller just populated).
// Callers hold c.mu. A non-positive maxBytes disables eviction.
func (c *PackCache) evictLocked(keep string) {
	if c.maxBytes <= 0 {
		return
	}
	for c.curBytes > c.maxBytes {
		var victim *packEntry
		for _, e := range c.entries {
			if e.key == keep || e.path == "" {
				continue
			}
			if victim == nil || e.used < victim.used {
				victim = e
			}
		}
		if victim == nil {
			return // nothing evictable
		}
		os.Remove(victim.path) // open readers survive on Linux (unlinked fd)
		c.curBytes -= victim.size
		delete(c.entries, victim.key)
	}
}

// packCachedFile is a read-only billy.File backed by a local temp file. It
// embeds *os.File for Read/ReadAt/Seek/Close/Stat and supplies the billy-only
// Lock/Unlock; writes are rejected.
type packCachedFile struct {
	*os.File
	name string
}

func (f *packCachedFile) Name() string { return f.name }

func (f *packCachedFile) Write(p []byte) (int, error)              { return 0, ErrCantWriteToReadOnly }
func (f *packCachedFile) WriteAt(p []byte, off int64) (int, error) { return 0, ErrCantWriteToReadOnly }
func (f *packCachedFile) Truncate(size int64) error                { return ErrTruncateNotSupported }
func (f *packCachedFile) Lock() error                              { return ErrLockNotSupported }
func (f *packCachedFile) Unlock() error                            { return ErrLockNotSupported }
