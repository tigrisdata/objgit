package s3fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6"
)

// ringCap bounds the in-RAM trailing window held for an in-flight download.
// Every byte below the watermark is already written to the temp file, so the
// window is only a hot cache for the most-recently-downloaded bytes; reads that
// fall below it are served from the disk fd. Peak RAM is therefore roughly
// (concurrent in-flight packs) * ringCap regardless of pack size.
const ringCap = 4 << 20 // 4 MiB

var errNegativeOffset = errors.New("s3fs: negative offset")

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
// Downloads stream: open launches a background pump that writes the S3 body to a
// temp file while advancing a watermark, and returns a reader immediately.
// Reads of already-downloaded ranges return at once; reads ahead of the
// watermark block only until that range arrives (the body is one sequential
// stream, so the watermark is monotonic). This overlaps the download with the
// clone instead of blocking it until the whole pack lands.
//
// Entries are keyed by S3 object key. Each downloads its object once and is
// reused across opens; a total-bytes budget evicts the least-recently-opened
// entries. Eviction unlinks the temp file, which on Linux leaves the in-flight
// writer and already-open readers working on the unlinked inode, so eviction
// never corrupts an in-flight read.
type PackCache struct {
	dir      string
	maxBytes int64

	mu       sync.Mutex
	entries  map[string]*packEntry
	curBytes int64
	seq      uint64 // monotonic open counter; entry.used orders the LRU
}

// packEntry is one cached object, possibly still downloading. once guards the
// header GetObject and the launch of the pump goroutine; the pump then fills the
// temp file and the RAM window concurrently with readers.
type packEntry struct {
	cache *PackCache
	key   string
	once  sync.Once

	mu   sync.Mutex
	cond *sync.Cond

	wfd  *os.File // write side: the pump appends sequentially
	rfd  *os.File // shared read side: serves offsets < n; survives unlink
	path string

	win      []byte // trailing RAM window covering [winStart, n)
	winStart int64
	n        int64 // bytes downloaded and written so far (monotonic watermark)
	size     int64 // total size from Content-Length; -1 until known
	done     bool  // body fully drained successfully
	err      error // terminal error (header or body)

	used     uint64 // seq of the most recent open, for LRU ordering
	refs     int    // live reader handles
	reserved int64  // bytes this entry has added to PackCache.curBytes
	evicted  bool   // path unlinked; close fds once refs hits 0

	closeOnce sync.Once // guards closing rfd exactly once
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

// Cleanup removes the cache's temp directory and all files in it, closing any
// open read/write handles first. Readers that still hold a *packCachedFile keep
// their entry pointer but will see closed fds; new opens after Cleanup fail.
func (c *PackCache) Cleanup() error {
	c.mu.Lock()
	dir := c.dir
	entries := c.entries
	c.entries = map[string]*packEntry{}
	c.curBytes = 0
	c.mu.Unlock()
	for _, e := range entries {
		e.closeRead()
		e.mu.Lock()
		if e.wfd != nil {
			e.wfd.Close()
		}
		e.mu.Unlock()
	}
	return os.RemoveAll(dir)
}

// open returns a billy.File for key. On first use it fetches the object header
// (so not-found and auth errors surface synchronously) and launches a pump that
// streams the body to a temp file; the returned reader serves bytes as they
// arrive without waiting for the whole download. Each call returns an
// independent handle with its own cursor over the shared entry.
func (c *PackCache) open(ctx context.Context, client s3Client, bucket, key, name string) (billy.File, error) {
	c.mu.Lock()
	e := c.entries[key]
	if e == nil {
		e = &packEntry{cache: c, key: key, size: -1}
		e.cond = sync.NewCond(&e.mu)
		c.entries[key] = e
	}
	// Reserve a reference before releasing the lock so a concurrent open of
	// another key can't evict-and-close this entry out from under us. Eviction
	// may still unlink it (unlink-while-open), but won't close its fds while
	// refs > 0.
	e.refs++
	c.seq++
	e.used = c.seq
	c.mu.Unlock()

	e.once.Do(func() { c.start(ctx, client, bucket, key, e) })

	e.mu.Lock()
	startErr := e.err
	e.mu.Unlock()
	if startErr != nil {
		c.release(e)
		return nil, startErr
	}

	return &packCachedFile{e: e, name: name}, nil
}

// start fetches the object and launches the background pump. It runs once per
// entry (guarded by entry.once) and outside c.mu, so concurrent opens of
// different keys don't serialise on the network call. A header/GetObject error
// is recorded on the entry and the entry is dropped so a later open retries.
func (c *PackCache) start(ctx context.Context, client s3Client, bucket, key string, e *packEntry) {
	began := time.Now()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	observeS3("GetObject", began, err)
	if err != nil {
		if isNotFound(err) {
			err = &os.PathError{Op: "open", Path: key, Err: fs.ErrNotExist}
		} else {
			err = fmt.Errorf("pack cache GetObject %q: %w", key, err)
		}
		c.dropFailed(e, err)
		return
	}

	tmp, terr := os.CreateTemp(c.dir, "obj-")
	if terr != nil {
		out.Body.Close()
		c.dropFailed(e, fmt.Errorf("pack cache temp file: %w", terr))
		return
	}
	rfd, rerr := os.Open(tmp.Name())
	if rerr != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		out.Body.Close()
		c.dropFailed(e, fmt.Errorf("pack cache reopen %q: %w", tmp.Name(), rerr))
		return
	}

	size := int64(-1)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}

	e.mu.Lock()
	e.wfd, e.rfd, e.path, e.size = tmp, rfd, tmp.Name(), size
	e.mu.Unlock()

	if size > 0 {
		c.mu.Lock()
		e.reserved = size
		c.curBytes += size
		c.evictLocked(key)
		c.mu.Unlock()
	}

	go c.pump(e, out.Body)
}

// dropFailed records a terminal error on the entry, wakes any waiters, and
// removes it from the map so a later open re-fetches.
func (c *PackCache) dropFailed(e *packEntry, err error) {
	e.mu.Lock()
	e.err = err
	e.cond.Broadcast()
	e.mu.Unlock()
	c.mu.Lock()
	if c.entries[e.key] == e {
		delete(c.entries, e.key)
	}
	c.mu.Unlock()
}

// pump streams body into the entry's temp file, advancing the watermark and the
// trailing RAM window as bytes arrive, and broadcasting so blocked readers wake.
func (c *PackCache) pump(e *packEntry, body io.ReadCloser) {
	defer body.Close()
	chunk := make([]byte, 256<<10)
	for {
		m, rerr := body.Read(chunk)
		if m > 0 {
			if _, werr := e.wfd.Write(chunk[:m]); werr != nil {
				c.failStream(e, fmt.Errorf("pack cache write %q: %w", e.key, werr))
				return
			}
			e.mu.Lock()
			e.win = append(e.win, chunk[:m]...)
			e.n += int64(m)
			if int64(len(e.win)) > ringCap {
				drop := int64(len(e.win)) - ringCap
				e.win = e.win[drop:]
				e.winStart += drop
			}
			e.cond.Broadcast()
			e.mu.Unlock()
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			c.failStream(e, fmt.Errorf("pack cache download %q: %w", e.key, rerr))
			return
		}
	}

	if cerr := e.wfd.Close(); cerr != nil {
		c.failStream(e, fmt.Errorf("pack cache flush %q: %w", e.key, cerr))
		return
	}

	e.mu.Lock()
	e.wfd = nil
	e.done = true
	e.size = e.n
	e.win = nil // every byte is on disk now; drop the RAM window
	e.winStart = e.n
	e.cond.Broadcast()
	e.mu.Unlock()

	// If we under-reserved (size was unknown), reconcile the budget now.
	c.mu.Lock()
	if e.reserved != e.n {
		c.curBytes += e.n - e.reserved
		e.reserved = e.n
		c.evictLocked(e.key)
	}
	c.mu.Unlock()

	c.maybeCloseEvicted(e)
}

// failStream records a mid-stream download error, discards the partial temp
// file, and drops the entry. Already-open readers see the error on their next
// blocked or out-of-range read.
func (c *PackCache) failStream(e *packEntry, err error) {
	e.mu.Lock()
	if e.wfd != nil {
		e.wfd.Close()
		e.wfd = nil
	}
	path := e.path
	reserved := e.reserved
	e.reserved = 0
	e.err = err
	e.cond.Broadcast()
	e.mu.Unlock()

	e.closeRead()
	if path != "" {
		os.Remove(path)
	}

	c.mu.Lock()
	c.curBytes -= reserved
	if c.entries[e.key] == e {
		delete(c.entries, e.key)
	}
	c.mu.Unlock()
}

// release drops one reader reference. When the last reader of an evicted entry
// closes, its shared read fd is closed (the temp file is already unlinked).
func (c *PackCache) release(e *packEntry) {
	c.mu.Lock()
	e.refs--
	closeNow := e.refs == 0 && e.evicted
	c.mu.Unlock()
	if closeNow {
		e.closeRead()
	}
}

// maybeCloseEvicted closes an evicted entry's read fd if no readers remain.
// Called after the pump finishes, since an entry can be evicted mid-stream.
func (c *PackCache) maybeCloseEvicted(e *packEntry) {
	c.mu.Lock()
	closeNow := e.refs == 0 && e.evicted
	c.mu.Unlock()
	if closeNow {
		e.closeRead()
	}
}

// closeRead closes the shared read fd exactly once.
func (e *packEntry) closeRead() {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		rfd := e.rfd
		e.mu.Unlock()
		if rfd != nil {
			rfd.Close()
		}
	})
}

// evictLocked removes least-recently-opened entries until the cache is within
// budget. keep is never evicted (it is the entry the caller just populated).
// Callers hold c.mu. A non-positive maxBytes disables eviction. Eviction unlinks
// the temp file even while readers or the in-flight writer hold it open; they
// survive via their fds on the unlinked inode (Linux). The shared read fd is
// closed only once the last reader leaves (see release/maybeCloseEvicted).
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
		os.Remove(victim.path) // open fds survive on Linux (unlinked inode)
		victim.mu.Lock()
		victim.evicted = true
		refs := victim.refs
		victim.mu.Unlock()
		c.curBytes -= victim.reserved
		delete(c.entries, victim.key)
		if refs == 0 {
			victim.closeRead()
		}
	}
}

// readAt fills p from the entry, blocking until the full requested range has
// been downloaded (or the download finishes or fails). This matches ReadAt's
// fill-or-error contract, which go-git's packfile.FSObject relies on. Bytes in
// the RAM window are served from memory; bytes that have scrolled below the
// window are served from the shared read fd.
func (e *packEntry) readAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errNegativeOffset
	}
	if len(p) == 0 {
		return 0, nil
	}
	e.mu.Lock()
	for {
		if e.err != nil {
			err := e.err
			e.mu.Unlock()
			return 0, err
		}
		if e.size >= 0 && off >= e.size {
			e.mu.Unlock()
			return 0, io.EOF
		}
		if off+int64(len(p)) <= e.n {
			if off >= e.winStart {
				n := copy(p, e.win[off-e.winStart:])
				e.mu.Unlock()
				return n, nil
			}
			rfd := e.rfd
			e.mu.Unlock()
			return rfd.ReadAt(p, off)
		}
		if e.done {
			// Range extends past EOF: serve what exists from disk; os.File.ReadAt
			// returns the partial read plus io.EOF.
			rfd := e.rfd
			e.mu.Unlock()
			return rfd.ReadAt(p, off)
		}
		e.cond.Wait()
	}
}

// readSome fills p with whatever is available at off (at least one byte once any
// data past off exists), without waiting for the full buffer. It backs the
// sequential Read path, whose callers (io.ReadAll, io.Copy) tolerate short
// reads.
func (e *packEntry) readSome(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errNegativeOffset
	}
	if len(p) == 0 {
		return 0, nil
	}
	e.mu.Lock()
	for {
		if e.err != nil {
			err := e.err
			e.mu.Unlock()
			return 0, err
		}
		if e.size >= 0 && off >= e.size {
			e.mu.Unlock()
			return 0, io.EOF
		}
		if off < e.n {
			if off >= e.winStart {
				n := copy(p, e.win[off-e.winStart:])
				e.mu.Unlock()
				return n, nil
			}
			rfd := e.rfd
			avail := e.n - off
			e.mu.Unlock()
			if int64(len(p)) > avail {
				p = p[:avail]
			}
			return rfd.ReadAt(p, off)
		}
		if e.done { // off >= n and size unknown path: nothing more is coming
			e.mu.Unlock()
			return 0, io.EOF
		}
		e.cond.Wait()
	}
}

// sizeOrWait returns the total size, waiting for the download to finish if the
// size was not advertised in the object header (Content-Length absent).
func (e *packEntry) sizeOrWait() (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for {
		if e.size >= 0 {
			return e.size, nil
		}
		if e.err != nil {
			return 0, e.err
		}
		if e.done {
			return e.n, nil
		}
		e.cond.Wait()
	}
}

// packCachedFile is a read-only billy.File backed by a (possibly still
// downloading) pack cache entry. Each handle carries its own cursor; ReadAt and
// the shared read fd are offset-explicit, so concurrent handles never interfere.
type packCachedFile struct {
	e      *packEntry
	name   string
	pos    int64
	closed bool
}

func (f *packCachedFile) Name() string { return f.name }

func (f *packCachedFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	n, err := f.e.readSome(p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *packCachedFile) ReadAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	return f.e.readAt(p, off)
}

func (f *packCachedFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		size, err := f.e.sizeOrWait()
		if err != nil {
			return 0, err
		}
		f.pos = size + offset
	default:
		return 0, fmt.Errorf("s3fs: invalid whence %d", whence)
	}
	return f.pos, nil
}

func (f *packCachedFile) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, os.ErrClosed
	}
	f.e.mu.Lock()
	size := f.e.size
	if size < 0 {
		size = f.e.n
	}
	f.e.mu.Unlock()
	return newFileInfo(f.name, size, time.Now()), nil
}

func (f *packCachedFile) Close() error {
	if f.closed {
		return ErrFileClosed
	}
	f.closed = true
	f.e.cache.release(f.e)
	return nil
}

func (f *packCachedFile) Write(p []byte) (int, error)              { return 0, ErrCantWriteToReadOnly }
func (f *packCachedFile) WriteAt(p []byte, off int64) (int, error) { return 0, ErrCantWriteToReadOnly }
func (f *packCachedFile) Truncate(size int64) error                { return ErrTruncateNotSupported }
func (f *packCachedFile) Lock() error                              { return ErrLockNotSupported }
func (f *packCachedFile) Unlock() error                            { return ErrLockNotSupported }

// Compile-time assertion: the streaming handle satisfies billy.File.
var _ billy.File = (*packCachedFile)(nil)
