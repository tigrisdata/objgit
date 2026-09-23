package tigris

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// PackCache materializes whole pack .bin containers as local files under one
// directory, shared by every Storer in the process, and evicts them
// least-recently-used once their combined size passes a budget. It is what
// turns a *second* clone of the same repository into zero pack downloads: the
// prefetch tier in packindex.go otherwise drops its copy when the Storer that
// built it goes out of scope.
//
// It also carries the watermark stream for the download it is running, so that
// a Storer waiting on another Storer's download can still read the bytes that
// have landed. See partial, fill, and Storer.partialPack.
//
// Entries are keyed by pack id, which is the hex SHA-256 of the .bin's own
// bytes (see packwriter.go). Two Storers over different prefixes — different
// repositories — that ask for the same id are therefore asking for
// byte-identical content, so sharing one file between them is deduplication
// and not leakage. The cache verifies the digest against the id while the
// download streams to disk, so a hit can only ever be the exact bytes the
// caller named.
//
// Ownership splits in two: the cache owns the file on disk, and each caller
// owns its own descriptor opened from it. Eviction is an unlink, and an open
// descriptor outlives the unlink on every platform this runs on, so a clone
// already reading a pack never breaks when the cache decides to reclaim it.
// The disk space returns when the last reader closes. That is also why no
// reference counting appears here: the kernel already does it.
//
// A nil *PackCache is not usable; Storers without one fall back to a private
// unlinked temp file per instance. See Storer.fetchWholePack.
//
// The same type also serves erofs snapshot images, through NewSnapshotCache
// and GetChecked: a snapshot id is not a content digest, so those callers
// supply the digest a download must have instead of reusing the id.
type PackCache struct {
	dir      string
	maxBytes int64
	suffix   string             // file name suffix of a cached entry; binSuffix for packs
	observe  func(event string) // optional; see NewSnapshotCache
	now      func() time.Time   // wall clock for EvictIdle; a field so tests can move it

	mu      sync.Mutex
	entries map[string]*cacheEntry
	cur     int64  // bytes charged against maxBytes by settled entries
	seq     uint64 // monotonic clock; entry.used orders the LRU
}

// cacheEntry is one pack's slot. It is published to the map *before* its
// download runs, so a second caller for the same id waits on ready instead of
// starting a duplicate download.
type cacheEntry struct {
	id     string
	path   string
	size   int64     // 0 until the download settles; also marks an entry unevictable
	used   uint64    // LRU stamp, higher is more recent
	usedAt time.Time // wall-clock time of the last claim; EvictIdle reads it
	ready  chan struct{}
	err    error // download outcome; read only after ready closes

	// stream is the in-progress view of this entry's download: set once the
	// staging file exists, cleared when the download settles either way. It
	// lives here, and not on the caller, so that a Storer parked in Get's
	// singleflight wait can still read the bytes that have landed — see
	// PackCache.partial and Storer.partialPack.
	stream atomic.Pointer[packStream]
}

// NewPackCache creates a cache in a fresh directory under parent (the OS temp
// directory when parent is empty), holding at most maxBytes of packs. A
// maxBytes of zero or less means no budget and therefore no eviction, which
// only makes sense for tests.
func NewPackCache(parent string, maxBytes int64) (*PackCache, error) {
	return newFileCache(parent, "objgit-packs-*", binSuffix, maxBytes, nil)
}

// NewSnapshotCache creates a cache for snapshot images, in a fresh
// objgit-snapshots-* directory under parent. It is the same cache as
// NewPackCache, with its own directory and budget. Snapshot ids are not
// content digests, so callers use GetChecked, not Get. observe, when not
// nil, receives "hit", "miss", "evict_budget", and "evict_idle".
func NewSnapshotCache(parent string, maxBytes int64, observe func(event string)) (*PackCache, error) {
	return newFileCache(parent, "objgit-snapshots-*", ".erofs", maxBytes, observe)
}

// newFileCache is the shared constructor behind NewPackCache and
// NewSnapshotCache. pattern names the temp directory (os.MkdirTemp's
// pattern), and suffix names the file extension a settled entry gets on
// disk.
func newFileCache(parent, pattern, suffix string, maxBytes int64, observe func(string)) (*PackCache, error) {
	dir, err := os.MkdirTemp(parent, pattern)
	if err != nil {
		return nil, fmt.Errorf("tigris: create cache directory: %w", err)
	}
	return &PackCache{
		dir:      dir,
		maxBytes: maxBytes,
		entries:  make(map[string]*cacheEntry),
		suffix:   suffix,
		observe:  observe,
		now:      time.Now,
	}, nil
}

// emit reports event to the observer n times, when one is installed. Callers
// must call it outside c.mu: the observer is arbitrary caller code, and
// running it under the lock would let a slow or reentrant callback stall
// every other claim, forget, or fill in the process.
func (c *PackCache) emit(event string, n int) {
	if c.observe == nil {
		return
	}
	for range n {
		c.observe(event)
	}
}

// Cleanup removes the cache directory and everything in it. Call it once at
// shutdown. Descriptors already handed out keep working.
func (c *PackCache) Cleanup() error {
	if c == nil {
		return nil
	}
	return os.RemoveAll(c.dir)
}

// Get returns a read-only descriptor positioned at the start of pack id,
// downloading it through fetch if the cache does not already hold it. fetch
// must write the pack's whole body to the writer it is given; the cache
// hashes those bytes as they pass and refuses content whose SHA-256 disagrees
// with id.
//
// The caller owns the returned descriptor and must close it.
func (c *PackCache) Get(id string, fetch func(io.Writer) error) (*os.File, error) {
	return c.GetChecked(id, expectID(id, fetch))
}

// expectID adapts a pack fetch, whose digest is its own id, to the shape
// GetChecked wants.
func expectID(id string, fetch func(io.Writer) error) func(io.Writer) (string, error) {
	return func(w io.Writer) (string, error) { return id, fetch(w) }
}

// GetChecked is Get for ids that are not content digests, such as a snapshot
// id (see NewSnapshotCache): fetch writes the body and reports the SHA-256 it
// must have. A body whose digest disagrees is refused and not cached.
//
// The caller owns the returned descriptor and must close it.
func (c *PackCache) GetChecked(id string, fetch func(w io.Writer) (wantSHA256 string, err error)) (*os.File, error) {
	// Bounded, because an entry can in principle be evicted by competing
	// downloads between the fill and the open. Exhausting the budget returns an
	// error, and packindex.go's bulk tier degrades to ranged GETs on one.
	for try := 0; try < 3; try++ {
		e, mine := c.claim(id)
		if mine {
			c.emit("miss", 1)
			slog.Debug("cache entry not cached, downloading", "id", id, "try", try)
			c.fill(e, fetch)
		} else {
			c.emit("hit", 1)
			// Either a settled entry or a download another caller is already
			// running; both mean this caller downloads nothing.
			slog.Debug("cache entry served from cache", "id", id, "path", e.path)
		}
		<-e.ready
		if e.err != nil {
			return nil, e.err
		}

		f, err := os.Open(e.path)
		switch {
		case err == nil:
			return f, nil
		case errors.Is(err, os.ErrNotExist):
			// Evicted between the lookup and the open. Forget this entry and
			// go round again, which re-downloads it.
			slog.Debug("cached entry vanished before it could be opened, retrying", "id", id, "try", try)
			c.forget(e)
		default:
			return nil, fmt.Errorf("tigris: open cached entry %s: %w", id, err)
		}
	}
	return nil, fmt.Errorf("tigris: cached entry %s was evicted faster than it could be read", id)
}

// partial reports the in-progress view of id's download, or nil when no
// download of it is running here. Any caller gets it, not only the one that
// started the download, which is what lets a Storer waiting on another
// Storer's download still read the bytes that have landed.
//
// The bytes it hands back have not been checksummed yet — that only happens
// when the download ends. This matches the ranged-GET tier it stands in for
// exactly: that tier never verifies a pack's digest either. The checksum
// guards what the cache keeps, not what one read sees.
func (c *PackCache) partial(id string) *packStream {
	c.mu.Lock()
	e, ok := c.entries[id]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	return e.stream.Load()
}

// claim returns id's entry, reporting whether this caller is the one that must
// download it. Touching the LRU stamp here (rather than on completion) keeps a
// pack that many concurrent readers want from looking stale.
func (c *PackCache) claim(id string) (*cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	if e, ok := c.entries[id]; ok {
		e.used = c.seq
		e.usedAt = c.now()
		return e, false
	}
	e := &cacheEntry{
		id:     id,
		path:   filepath.Join(c.dir, id+c.suffix),
		used:   c.seq,
		usedAt: c.now(),
		ready:  make(chan struct{}),
	}
	c.entries[id] = e
	return e, true
}

// forget drops e from the map if it is still the entry registered for its id,
// so the next Get starts a fresh download. An entry replaced by a later Get is
// left alone.
func (c *PackCache) forget(e *cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cur, ok := c.entries[e.id]; ok && cur == e {
		delete(c.entries, e.id)
		c.cur -= e.size
	}
}

// fill runs the download for an entry this caller claimed, then releases every
// waiter. A failed download un-registers the entry, so the failure is not
// cached: the per-Storer packAccess.start guard in packindex.go is what stops
// a retry storm, and a genuinely transient error stays retryable by a later
// request.
//
// While the copy runs, the bytes already on disk are readable through
// e.stream, so readers of this pack never have to wait for the whole container
// — see packindex.go's watermark tier. The stream is torn down before ready
// closes, so nothing can be served out of a body that failed its checksum.
func (c *PackCache) fill(e *cacheEntry, fetch func(io.Writer) (string, error)) {
	defer close(e.ready)

	start := time.Now()

	// Same directory as the final path, so the rename below cannot cross a
	// filesystem boundary.
	tmp, err := os.CreateTemp(c.dir, "download-*")
	if err != nil {
		e.err = fmt.Errorf("tigris: stage pack %s: %w", e.id, err)
		slog.Debug("pack download failed", "pack", e.id, "err", e.err)
		c.forget(e)
		return
	}

	// A second, read-only descriptor on the same inode. It has to be its own
	// descriptor because the write one is closed below, and it keeps working
	// across the rename because a rename moves the name and not the file.
	// Failing to open it costs the watermark tier, not the download.
	var progress *atomic.Int64
	if rd, oerr := os.Open(tmp.Name()); oerr == nil {
		ps := &packStream{f: rd}
		progress = &ps.n
		e.stream.Store(ps)
		// The descriptor outlives fill: a reader may still hold the stream when
		// the download settles. Closing it is the cleanup's job, once nothing
		// references the stream at all.
		runtime.AddCleanup(ps, func(f *os.File) { f.Close() }, rd)
	} else {
		slog.Debug("cannot open the staging file for watermark reads", "pack", e.id, "err", oerr)
	}

	size, err := verifiedCopy(tmp, e.id, fetch, progress)
	e.stream.Store(nil)
	if cerr := tmp.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("tigris: stage pack %s: %w", e.id, cerr)
	}
	if err == nil {
		err = os.Rename(tmp.Name(), e.path)
	}
	if err != nil {
		os.Remove(tmp.Name())
		e.err = err
		slog.Debug("pack download failed", "pack", e.id, "dur", time.Since(start), "err", err)
		c.forget(e)
		return
	}

	c.mu.Lock()
	e.size = size
	c.cur += size
	slog.Debug("cache entry filled",
		"id", e.id,
		"bytes", size,
		"dur", time.Since(start),
		"cache_bytes", c.cur,
		"cache_max_bytes", c.maxBytes,
		"cache_entries", len(c.entries))
	n := c.evictLocked(e.id)
	c.mu.Unlock()
	c.emit("evict_budget", n)
}

// evictLocked unlinks least-recently-used entries until the budget is met,
// never touching keep (the entry being admitted) or an entry whose download
// has not settled, and reports how many it removed. An in-flight download's
// bytes are therefore uncounted, so several concurrent downloads can overshoot
// the budget until they land.
//
// A single entry larger than the whole budget is admitted anyway — the read
// has to work — and simply evicts everything else.
func (c *PackCache) evictLocked(keep string) int {
	if c.maxBytes <= 0 {
		return 0
	}
	n := 0
	for c.cur > c.maxBytes {
		var victim *cacheEntry
		for _, e := range c.entries {
			if e.id == keep || e.size == 0 {
				continue
			}
			if victim == nil || e.used < victim.used {
				victim = e
			}
		}
		if victim == nil {
			// nothing evictable left; the survivors are all in use
			slog.Debug("cache over budget with nothing evictable",
				"cache_bytes", c.cur, "cache_max_bytes", c.maxBytes, "cache_entries", len(c.entries))
			return n
		}
		delete(c.entries, victim.id)
		c.cur -= victim.size
		// Open descriptors survive the unlink and keep serving reads; the disk
		// space comes back when the last one closes.
		os.Remove(victim.path)
		n++
		slog.Debug("cache entry over budget, uncached",
			"id", victim.id,
			"bytes", victim.size,
			"admitted", keep,
			"cache_bytes", c.cur,
			"cache_max_bytes", c.maxBytes,
			"cache_entries", len(c.entries))
	}
	return n
}

// EvictIdle unlinks every settled entry that no caller claimed in the last
// maxIdle, and reports how many it removed. It never touches a download that
// has not settled. Open descriptors keep working, as with budget eviction.
func (c *PackCache) EvictIdle(maxIdle time.Duration) int {
	c.mu.Lock()
	cutoff := c.now().Add(-maxIdle)
	n := 0
	for id, e := range c.entries {
		if e.size == 0 || !e.usedAt.Before(cutoff) {
			continue
		}
		delete(c.entries, id)
		c.cur -= e.size
		os.Remove(e.path)
		n++
		slog.Debug("cache entry idle, uncached", "id", id, "bytes", e.size, "idle_since", e.usedAt)
	}
	c.mu.Unlock()
	c.emit("evict_idle", n)
	return n
}

// verifiedCopy runs fetch into w, hashing as it goes, and rejects a body whose
// SHA-256 disagrees with the digest fetch itself reports. It reports how many
// bytes landed.
//
// id is not the value being verified against — it names the cache entry, for
// the error message only. For a pack, whose id is its own digest, expectID
// makes the two the same string.
//
// progress, when non-nil, is the watermark a concurrent reader polls: it is
// bumped after every write, so a byte counted there is already in w.
func verifiedCopy(w io.Writer, id string, fetch func(io.Writer) (string, error), progress *atomic.Int64) (int64, error) {
	h := sha256.New()
	cnt := &countingWriter{w: io.MultiWriter(w, h), progress: progress}
	want, err := fetch(cnt)
	if err != nil {
		return 0, err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		return 0, fmt.Errorf("tigris: downloaded entry %s failed checksum (got %s, want %s)", id, got, want)
	}
	return cnt.n, nil
}

type countingWriter struct {
	w        io.Writer
	n        int64
	progress *atomic.Int64
}

// Write publishes the running total only after the underlying writer returns,
// so the watermark never admits a byte that is not yet on disk.
func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	if c.progress != nil {
		c.progress.Store(c.n)
	}
	return n, err
}
