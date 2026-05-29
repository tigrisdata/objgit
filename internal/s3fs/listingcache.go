// listingcache.go caches directory listings so that Stat/Open of a path whose
// parent folder has been listed can be answered without an S3 round-trip: an
// absent child returns "not found" for free, a sub-directory is recognised for
// free, and a present file is served from the head cache — seeded with each
// file's size/mtime straight from the same ListObjectsV2 — so it too is free
// unless the caller needs the x-amz-meta-* user metadata a listing can't carry.
//
// Everything is held in process-local sync.Maps with a conventional per-entry
// TTL. There is no cross-process sharing: it is a plain in-memory cache. Two
// mechanisms keep it correct:
//
//   - per-entry expiry (expires = now + TTL): a stale entry is ignored once its
//     TTL passes, bounding how long a write this process can't see stays hidden.
//   - a per-prefix local generation, bumped on every local write under the
//     prefix (and its ancestors): an entry whose stored generation no longer
//     matches is ignored, so the next read re-lists and sees a local write
//     immediately (read-after-write correctness).
//
// Subtree caching (RecursivePrefixes). For namespaces that are bounded and
// listed folder-by-folder by callers — refs/ above all — a delimited listing per
// folder is wasteful: one delimiter-less ListObjectsV2 over refs/ returns the
// whole subtree, from which every descendant folder's listing (and every
// negative lookup beneath it) is synthesised in memory. So a prefix at or under a
// recursive root is served from a single cached subtree scan instead of a listing
// per folder. The trade-offs: a subtree write must invalidate every ancestor
// (invalidate walks up), and a subtree larger than MaxSubtreeKeys is abandoned —
// the cache records that and falls back to delimited per-folder listing.
package s3fs

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"golang.org/x/sync/singleflight"
)

// childKind distinguishes the two things a listing can report under a prefix.
type childKind uint8

const (
	kindFile childKind = iota
	kindDir
)

// childEntry is one immediate child of a listed prefix. Name is the child's base
// name (no separator). Size/Mtime are populated for files only; dirs (S3 common
// prefixes) carry zero values.
type childEntry struct {
	Name  string
	Kind  childKind
	Size  int64
	Mtime int64 // unix nanoseconds
}

// headData is the size/mtime/metadata an s3.HeadObjectOutput is rebuilt from.
// Size and Mtime can be seeded straight from a ListObjectsV2 entry; Meta (the
// x-amz-meta-* user metadata for the Unix-metadata feature) only comes from an
// actual HeadObject, so a listing-seeded entry leaves it nil — see hasMeta.
type headData struct {
	Size  int64             // bytes
	Mtime int64             // unix nanoseconds
	Meta  map[string]string // x-amz-meta-* user metadata; nil when listing-seeded
}

// headCacheEntry is one object's cached head metadata. A read is a hit only when
// the entry is unexpired and its gen still matches the parent prefix's current
// generation — so a local write under the prefix (which bumps that generation)
// invalidates every cached head beneath it without a map scan. hasMeta is false
// for entries seeded from a listing (size/mtime only); a caller that needs the
// user metadata treats those as a miss and fills via a real HeadObject.
type headCacheEntry struct {
	data    headData
	gen     uint64
	expires time.Time
	hasMeta bool
}

// listingEntry is one folder's cached children, tagged with the prefix's
// generation at fill time and an expiry. Same hit rule as headCacheEntry.
type listingEntry struct {
	entries []childEntry
	gen     uint64
	expires time.Time
}

// subtreeObject is one object in a recursive subtree scan: its full canonical
// key plus the size/mtime ListObjectsV2 returns, enough to synthesise any
// descendant folder's listing and to seed the head cache.
type subtreeObject struct {
	Key   string
	Size  int64
	Mtime int64 // unix nanoseconds
}

// subtreeData is a recursive listing of a root prefix. When Truncated it hit
// MaxSubtreeKeys and is unsafe for negative lookups, so callers fall back to
// delimited per-folder listing for that root.
type subtreeData struct {
	Objects   []subtreeObject
	Truncated bool
}

// subtreeEntry is a cached subtreeData, tagged with the root's generation and an
// expiry.
type subtreeEntry struct {
	data    subtreeData
	gen     uint64
	expires time.Time
}

// CacheConfig configures a ListingCache. TTL must be > 0 (callers gate the whole
// feature on it). The remaining fields have sane zero-value behaviour.
type CacheConfig struct {
	TTL             time.Duration // per-entry lifetime; bounds negative staleness
	RefreshInterval time.Duration // warmer tick; <=0 disables the warmer
	IdleTTL         time.Duration // drop un-accessed prefixes from the warmer

	// DisableHeadPrefetch turns off seeding the head cache from listings (the
	// size/mtime of every file in a folder, taken straight from ListObjectsV2).
	// The zero value keeps seeding enabled.
	DisableHeadPrefetch bool

	// RecursivePrefixes are namespaces served from one delimiter-less subtree
	// scan rather than a delimited listing per folder (see the package comment).
	// Each is normalised to end in "/". A nil slice defaults to {"refs/"}; an
	// explicit empty non-nil slice disables subtree caching entirely.
	RecursivePrefixes []string
	// MaxSubtreeKeys caps a subtree scan: past it the subtree is abandoned and
	// that root falls back to delimited per-folder listing (<=0 → 50000).
	MaxSubtreeKeys int
}

// ListingCache is a process-local cache of directory listings, recursive
// subtrees, and per-object head metadata, with per-entry TTL and per-prefix
// generation invalidation. It is safe for concurrent use and is shared by
// pointer across an S3FS and all of its Chroot children.
type ListingCache struct {
	ttl       time.Duration
	cfg       CacheConfig
	client    s3Client
	bucket    string
	separator string
	roots     []string // normalised RecursivePrefixes, longest first

	clock func() time.Time // overridable in tests

	// The three caches. Each value is the *Entry type above; reads honour the
	// entry's generation and expiry. sf dedupes concurrent listing/subtree fills,
	// headSF the head fills, so a fan-out of identical lookups lists S3 once.
	listings sync.Map // prefix → listingEntry
	subtrees sync.Map // root   → subtreeEntry
	heads    sync.Map // object key → headCacheEntry
	sf       singleflight.Group
	headSF   singleflight.Group

	hits, misses atomic.Int64 // listing + subtree cache outcomes, for metrics

	mu   sync.Mutex
	gens map[string]uint64    // per-prefix local generation
	seen map[string]time.Time // prefixes accessed → driven by the warmer
}

// NewListingCache builds a process-local cache. client/bucket/separator are the
// root filesystem's, so cached prefixes are full-canonical keys that every
// Chroot view agrees on.
func NewListingCache(cfg CacheConfig, client s3Client, bucket, separator string) *ListingCache {
	if cfg.MaxSubtreeKeys <= 0 {
		cfg.MaxSubtreeKeys = 50000
	}
	if cfg.RecursivePrefixes == nil {
		cfg.RecursivePrefixes = []string{"refs/"}
	}
	return &ListingCache{
		ttl:       cfg.TTL,
		cfg:       cfg,
		client:    client,
		bucket:    bucket,
		separator: separator,
		roots:     normalizeRoots(cfg.RecursivePrefixes),
		clock:     time.Now,
		gens:      map[string]uint64{},
		seen:      map[string]time.Time{},
	}
}

// list returns the immediate children of prefix. prefix is a full-canonical key
// prefix: "" for the bucket root, otherwise ending in the separator. A prefix at
// or under a recursive root is synthesised from one cached subtree scan;
// otherwise it is a delimited per-folder listing.
func (c *ListingCache) list(ctx context.Context, prefix string) ([]childEntry, error) {
	c.touch(prefix)

	if root, ok := c.recursiveRoot(prefix); ok {
		st, err := c.subtree(ctx, root)
		// A complete subtree answers the folder in memory. On error or a
		// truncated (oversized) subtree, fall through to delimited listing.
		if err == nil && !st.Truncated {
			return synthesizeListing(st.Objects, prefix), nil
		}
	}

	return c.listFolder(ctx, prefix)
}

// listFolder serves a single delimited folder listing from the cache, filling it
// from S3 on a miss (deduped via singleflight).
func (c *ListingCache) listFolder(ctx context.Context, prefix string) ([]childEntry, error) {
	gen := c.gen(prefix)
	if v, ok := c.listings.Load(prefix); ok {
		e := v.(listingEntry)
		if e.gen == gen && c.clock().Before(e.expires) {
			c.hits.Add(1)
			return e.entries, nil
		}
	}
	c.misses.Add(1)
	return c.fillFolder(ctx, prefix, gen)
}

// fillFolder lists prefix from S3, stores the result, and seeds the head cache.
// Concurrent fills for the same prefix are coalesced; the warmer also calls it
// to force a refresh.
func (c *ListingCache) fillFolder(ctx context.Context, prefix string, gen uint64) ([]childEntry, error) {
	v, err, _ := c.sf.Do("L\x00"+prefix, func() (any, error) {
		entries, err := listChildren(ctx, c.client, c.bucket, c.separator, prefix)
		if err != nil {
			return nil, err
		}
		c.listings.Store(prefix, listingEntry{entries: entries, gen: gen, expires: c.clock().Add(c.ttl)})
		// ListObjectsV2 already carries every file's size and mtime, so warm the
		// head cache from it without a single extra HeadObject.
		c.seedHeads(prefix, entries)
		return entries, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]childEntry), nil
}

// subtree returns the recursive listing of root from the cache, filling it via
// one delimiter-less ListObjectsV2 scan on a miss (deduped via singleflight).
func (c *ListingCache) subtree(ctx context.Context, root string) (subtreeData, error) {
	gen := c.gen(root)
	if v, ok := c.subtrees.Load(root); ok {
		e := v.(subtreeEntry)
		if e.gen == gen && c.clock().Before(e.expires) {
			c.hits.Add(1)
			return e.data, nil
		}
	}
	c.misses.Add(1)
	return c.fillSubtree(ctx, root, gen)
}

// fillSubtree scans root recursively, stores the result, and (when complete)
// seeds the head cache for every file under it.
func (c *ListingCache) fillSubtree(ctx context.Context, root string, gen uint64) (subtreeData, error) {
	v, err, _ := c.sf.Do("S\x00"+root, func() (any, error) {
		objs, truncated, err := listSubtree(ctx, c.client, c.bucket, root, c.cfg.MaxSubtreeKeys)
		if err != nil {
			return subtreeData{}, err
		}
		data := subtreeData{Objects: objs, Truncated: truncated}
		c.subtrees.Store(root, subtreeEntry{data: data, gen: gen, expires: c.clock().Add(c.ttl)})
		// A complete subtree seeds the head cache for every file under the root
		// in one shot; a truncated one is untrustworthy, so leave heads to the
		// per-folder fallback path.
		if !truncated {
			c.seedSubtreeHeads(objs)
		}
		return data, nil
	})
	if err != nil {
		return subtreeData{}, err
	}
	return v.(subtreeData), nil
}

// recursiveRoot reports the longest configured recursive root at or above prefix.
func (c *ListingCache) recursiveRoot(prefix string) (string, bool) {
	for _, r := range c.roots { // longest first
		if prefix == r || strings.HasPrefix(prefix, r) {
			return r, true
		}
	}
	return "", false
}

// synthesizeListing builds the immediate children of prefix from a subtree's
// flat object set: a key whose remainder after prefix contains a separator is a
// child directory (deduped), otherwise a child file. Order follows the objects'
// (lexicographic) S3 order, dirs reported before files like listChildren.
func synthesizeListing(objs []subtreeObject, prefix string) []childEntry {
	seenDir := map[string]bool{}
	var dirs, files []childEntry
	for _, o := range objs {
		if !strings.HasPrefix(o.Key, prefix) {
			continue
		}
		rest := o.Key[len(prefix):]
		if rest == "" {
			continue // the prefix's own placeholder object
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			name := rest[:i]
			if name == "" || seenDir[name] {
				continue
			}
			seenDir[name] = true
			dirs = append(dirs, childEntry{Name: name, Kind: kindDir})
			continue
		}
		files = append(files, childEntry{Name: rest, Kind: kindFile, Size: o.Size, Mtime: o.Mtime})
	}
	return append(dirs, files...)
}

// normalizeRoots cleans recursive-prefix config: drops blanks, ensures a
// trailing separator, dedupes, and sorts longest-first so recursiveRoot returns
// the most specific match.
func normalizeRoots(prefixes []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range prefixes {
		if p == "" {
			continue
		}
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		if seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}

// headInfo returns the object's head metadata, served from the head cache
// (seeded from listings, or filled on demand here). needMeta reports whether the
// caller requires the x-amz-meta-* user metadata: when true a listing-seeded
// entry (size/mtime only) is insufficient and a real HeadObject is issued.
func (c *ListingCache) headInfo(ctx context.Context, objKey string, needMeta bool) (*s3.HeadObjectOutput, error) {
	d, err := c.headLoad(ctx, objKey, needMeta)
	if err != nil {
		return nil, err
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(d.Size),
		LastModified:  aws.Time(time.Unix(0, d.Mtime)),
		Metadata:      d.Meta,
	}, nil
}

// headLoad returns objKey's head metadata from the head cache. An entry hits only
// while unexpired, still tagged with its parent prefix's current generation (so
// invalidate's bump drops every cached head under the prefix), and — when
// needMeta — carrying user metadata. A miss fills via a single deduped HeadObject.
func (c *ListingCache) headLoad(ctx context.Context, objKey string, needMeta bool) (headData, error) {
	prefix, _ := splitKey(objKey)
	gen := c.gen(prefix)
	if v, ok := c.heads.Load(objKey); ok {
		e := v.(headCacheEntry)
		if e.gen == gen && c.clock().Before(e.expires) && (e.hasMeta || !needMeta) {
			return e.data, nil
		}
	}

	// Miss: dedupe concurrent fills for the same key so only one HeadObject goes
	// to S3.
	v, err, _ := c.headSF.Do(objKey, func() (any, error) {
		start := time.Now()
		ho, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &c.bucket, Key: &objKey})
		observeS3("HeadObject", start, err)
		if err != nil {
			return headData{}, err
		}
		d := headData{
			Size:  aws.ToInt64(ho.ContentLength),
			Mtime: aws.ToTime(ho.LastModified).UnixNano(),
			Meta:  ho.Metadata,
		}
		c.heads.Store(objKey, headCacheEntry{data: d, gen: gen, expires: c.clock().Add(c.ttl), hasMeta: true})
		return d, nil
	})
	if err != nil {
		return headData{}, err
	}
	return v.(headData), nil
}

// seedHeads warms the head cache for every file in a freshly-listed folder
// directly from the listing's size/mtime — no HeadObject round-trips. The
// entries are marked metadata-less (hasMeta=false), so a later read that needs
// user metadata still triggers a real HeadObject; a read that doesn't (the
// common case, and objgitd's only case) is served entirely from this seed.
func (c *ListingCache) seedHeads(prefix string, entries []childEntry) {
	if c.cfg.DisableHeadPrefetch {
		return
	}
	gen := c.gen(prefix)
	exp := c.clock().Add(c.ttl)
	for _, e := range entries {
		if e.Kind != kindFile {
			continue
		}
		c.heads.Store(prefix+e.Name, headCacheEntry{
			data:    headData{Size: e.Size, Mtime: e.Mtime},
			gen:     gen,
			expires: exp,
		})
	}
}

// seedSubtreeHeads seeds the head cache for every file in a complete subtree
// scan, the recursive analogue of seedHeads. Each entry is tagged with its own
// parent prefix's generation so per-folder invalidation still applies.
func (c *ListingCache) seedSubtreeHeads(objs []subtreeObject) {
	if c.cfg.DisableHeadPrefetch {
		return
	}
	exp := c.clock().Add(c.ttl)
	for _, o := range objs {
		prefix, base := splitKey(o.Key)
		if base == "" {
			continue // directory placeholder
		}
		c.heads.Store(o.Key, headCacheEntry{
			data:    headData{Size: o.Size, Mtime: o.Mtime},
			gen:     c.gen(prefix),
			expires: exp,
		})
	}
}

// isNotFound reports whether err is an S3 "object does not exist" error.
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey":
			return true
		}
	}
	return false
}

func (c *ListingCache) gen(prefix string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gens[prefix]
}

// invalidate bumps the local generation of prefix and every ancestor up to the
// bucket root, so the next read re-lists. Entries under the old generation are
// simply ignored (and swept at expiry). The ancestor walk is what lets a subtree
// entry (cached under a coarse root) notice a write to any descendant folder — at
// the cost of a broader blast radius: a write also re-lists the coarser folders
// above it on their next read.
func (c *ListingCache) invalidate(prefix string) {
	if c == nil {
		return
	}
	now := c.clock()
	c.mu.Lock()
	for _, p := range ancestorPrefixes(prefix) {
		c.gens[p]++
		c.seen[p] = now
	}
	c.mu.Unlock()
}

// ancestorPrefixes returns prefix and every parent prefix up to and including
// the bucket root (""). Each non-root element ends in "/".
func ancestorPrefixes(prefix string) []string {
	out := []string{prefix}
	for prefix != "" {
		p := strings.TrimSuffix(prefix, "/")
		if i := strings.LastIndex(p, "/"); i >= 0 {
			prefix = p[:i+1]
		} else {
			prefix = ""
		}
		out = append(out, prefix)
	}
	return out
}

func (c *ListingCache) touch(prefix string) {
	c.mu.Lock()
	c.seen[prefix] = c.clock()
	c.mu.Unlock()
}

// RunWarmer refreshes every recently-accessed prefix on each tick — re-listing
// it so a hot entry never lapses into a client-visible miss — then sweeps expired
// entries from all three caches (there is no LRU, so the warmer is what bounds
// their growth). It evicts prefixes unused for longer than IdleTTL. It returns
// when ctx is cancelled; it is a no-op when the refresh interval is non-positive.
func (c *ListingCache) RunWarmer(ctx context.Context) {
	if c == nil || c.cfg.RefreshInterval <= 0 {
		<-ctx.Done()
		return
	}
	t := time.NewTicker(c.cfg.RefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.warmOnce(ctx)
		}
	}
}

func (c *ListingCache) warmOnce(ctx context.Context) {
	now := c.clock()
	c.mu.Lock()
	prefixes := make([]string, 0, len(c.seen))
	for p, last := range c.seen {
		if c.cfg.IdleTTL > 0 && now.Sub(last) > c.cfg.IdleTTL {
			delete(c.seen, p)
			continue
		}
		prefixes = append(prefixes, p)
	}
	c.mu.Unlock()

	// Refresh via the same routing as the read path: prefixes under a recursive
	// root refresh that root's subtree (deduped across siblings), the rest their
	// delimited folder listing.
	roots := map[string]bool{}
	for _, p := range prefixes {
		if root, ok := c.recursiveRoot(p); ok {
			roots[root] = true
			continue
		}
		if _, err := c.fillFolder(ctx, p, c.gen(p)); err != nil {
			slog.Debug("listing-cache warm failed", "prefix", p, "err", err)
		}
	}
	for root := range roots {
		if _, err := c.fillSubtree(ctx, root, c.gen(root)); err != nil {
			slog.Debug("subtree-cache warm failed", "root", root, "err", err)
		}
	}

	c.sweepExpired(now)
}

// sweepExpired drops expired entries from all three caches. Stale-generation
// entries that have not yet expired are left; they are never read (generation
// mismatch) and age out here once their TTL passes.
func (c *ListingCache) sweepExpired(now time.Time) {
	c.listings.Range(func(k, v any) bool {
		if !now.Before(v.(listingEntry).expires) {
			c.listings.Delete(k)
		}
		return true
	})
	c.subtrees.Range(func(k, v any) bool {
		if !now.Before(v.(subtreeEntry).expires) {
			c.subtrees.Delete(k)
		}
		return true
	})
	c.heads.Range(func(k, v any) bool {
		if !now.Before(v.(headCacheEntry).expires) {
			c.heads.Delete(k)
		}
		return true
	})
}

// CacheStats is a snapshot of the cache's counters for export to metrics.
type CacheStats struct {
	Hits, Misses                          int64
	ListingItems, SubtreeItems, HeadItems int64
}

// Stats snapshots hit/miss counters and resident item counts.
func (c *ListingCache) Stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}
	return CacheStats{
		Hits:         c.hits.Load(),
		Misses:       c.misses.Load(),
		ListingItems: syncMapLen(&c.listings),
		SubtreeItems: syncMapLen(&c.subtrees),
		HeadItems:    syncMapLen(&c.heads),
	}
}

func syncMapLen(m *sync.Map) int64 {
	var n int64
	m.Range(func(_, _ any) bool { n++; return true })
	return n
}

// splitKey splits a canonical key into its parent prefix and base name. The
// prefix matches ReadDir's convention: "" for a root-level key, otherwise it
// ends in "/".
func splitKey(key string) (prefix, base string) {
	if i := strings.LastIndex(key, "/"); i >= 0 {
		return key[:i+1], key[i+1:]
	}
	return "", key
}
