package tigris

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// PackCache materializes whole pack .bin containers as local files under one
// directory, shared by every Storer in the process, and evicts them
// least-recently-used once their combined size passes a budget. It is what
// turns a *second* clone of the same repository into zero pack downloads: the
// bulk-fetch tier in packindex.go otherwise drops its copy when the request
// that built it ends.
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
type PackCache struct {
	dir      string
	maxBytes int64

	mu      sync.Mutex
	entries map[string]*cacheEntry
	cur     int64  // bytes charged against maxBytes by settled entries
	seq     uint64 // monotonic clock; entry.used orders the LRU
}

// cacheEntry is one pack's slot. It is published to the map *before* its
// download runs, so a second caller for the same id waits on ready instead of
// starting a duplicate download.
type cacheEntry struct {
	id    string
	path  string
	size  int64  // 0 until the download settles; also marks an entry unevictable
	used  uint64 // LRU stamp, higher is more recent
	ready chan struct{}
	err   error // download outcome; read only after ready closes
}

// NewPackCache creates a cache in a fresh directory under parent (the OS temp
// directory when parent is empty), holding at most maxBytes of packs. A
// maxBytes of zero or less means no budget and therefore no eviction, which
// only makes sense for tests.
func NewPackCache(parent string, maxBytes int64) (*PackCache, error) {
	dir, err := os.MkdirTemp(parent, "objgit-packs-*")
	if err != nil {
		return nil, fmt.Errorf("tigris: create pack cache directory: %w", err)
	}
	return &PackCache{
		dir:      dir,
		maxBytes: maxBytes,
		entries:  make(map[string]*cacheEntry),
	}, nil
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
	// Bounded, because a pack can in principle be evicted by competing
	// downloads between the fill and the open. Exhausting the budget returns an
	// error, and packindex.go's bulk tier degrades to ranged GETs on one.
	for try := 0; try < 3; try++ {
		e, mine := c.claim(id)
		if mine {
			c.fill(e, fetch)
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
			c.forget(e)
		default:
			return nil, fmt.Errorf("tigris: open cached pack %s: %w", id, err)
		}
	}
	return nil, fmt.Errorf("tigris: cached pack %s was evicted faster than it could be read", id)
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
		return e, false
	}
	e := &cacheEntry{
		id:    id,
		path:  filepath.Join(c.dir, id+binSuffix),
		used:  c.seq,
		ready: make(chan struct{}),
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
// cached: the outer per-Storer once guard in packindex.go is what stops a
// retry storm, and a genuinely transient error stays retryable by a later
// request.
func (c *PackCache) fill(e *cacheEntry, fetch func(io.Writer) error) {
	defer close(e.ready)

	// Same directory as the final path, so the rename below cannot cross a
	// filesystem boundary.
	tmp, err := os.CreateTemp(c.dir, "download-*")
	if err != nil {
		e.err = fmt.Errorf("tigris: stage pack %s: %w", e.id, err)
		c.forget(e)
		return
	}

	size, err := verifiedCopy(tmp, e.id, fetch)
	if cerr := tmp.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("tigris: stage pack %s: %w", e.id, cerr)
	}
	if err == nil {
		err = os.Rename(tmp.Name(), e.path)
	}
	if err != nil {
		os.Remove(tmp.Name())
		e.err = err
		c.forget(e)
		return
	}

	c.mu.Lock()
	e.size = size
	c.cur += size
	c.evictLocked(e.id)
	c.mu.Unlock()
}

// evictLocked unlinks least-recently-used packs until the budget is met,
// never touching keep (the entry being admitted) or an entry whose download
// has not settled. An in-flight download's bytes are therefore uncounted, so
// several concurrent downloads can overshoot the budget until they land.
//
// A single pack larger than the whole budget is admitted anyway — the read has
// to work — and simply evicts everything else.
func (c *PackCache) evictLocked(keep string) {
	if c.maxBytes <= 0 {
		return
	}
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
			return // nothing evictable left; the survivors are all in use
		}
		delete(c.entries, victim.id)
		c.cur -= victim.size
		// Open descriptors survive the unlink and keep serving reads; the disk
		// space comes back when the last one closes.
		os.Remove(victim.path)
	}
}

// verifiedCopy runs fetch into w, hashing as it goes, and rejects a body whose
// SHA-256 disagrees with the pack id. It reports how many bytes landed.
func verifiedCopy(w io.Writer, id string, fetch func(io.Writer) error) (int64, error) {
	h := sha256.New()
	cnt := &countingWriter{w: io.MultiWriter(w, h)}
	if err := fetch(cnt); err != nil {
		return 0, err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != id {
		return 0, fmt.Errorf("tigris: downloaded pack %s failed checksum (got %s)", id, got)
	}
	return cnt.n, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
