// listingcache.go caches directory listings so that Stat/Open of a path whose
// parent folder has been listed can be answered without an S3 round-trip: an
// absent child returns "not found" for free, a sub-directory is recognised for
// free, and only a present file still issues a HeadObject/GetObject for its
// authoritative content/metadata.
//
// The store is github.com/golang/groupcache so the listing can be shared across
// a fleet via its consistent-hash peer pool. groupcache is fill-once and
// immutable (no Set, Delete, or TTL), so two facts are encoded into the cache
// key instead:
//
//   - a time window floor(now/TTL): when it advances the key changes, forcing a
//     re-list. This bounds staleness from writers this process cannot see.
//   - a per-prefix local generation, bumped on every local write under the
//     prefix: this moves the key so the next local read re-lists and sees the
//     write immediately (read-after-write correctness groupcache alone cannot
//     give). The stale entry under the old key is simply never queried again.
package s3fs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/golang/groupcache"
)

// maxPrefetchInFlight bounds the background HeadObject precaches in flight at
// once, so listing a large folder can't spawn an unbounded goroutine/S3 storm.
// Overflow files are simply fetched on demand instead.
const maxPrefetchInFlight = 64

// childKind distinguishes the two things a listing can report under a prefix.
type childKind uint8

const (
	kindFile childKind = iota
	kindDir
)

// childEntry is one immediate child of a listed prefix. name is the child's
// base name (no separator). size/mtime are populated for files only; dirs
// (S3 common prefixes) carry zero values.
type childEntry struct {
	Name  string    `json:"n"`
	Kind  childKind `json:"k"`
	Size  int64     `json:"s,omitempty"`
	Mtime int64     `json:"m,omitempty"` // unix nanoseconds
}

// headData is the cached result of a HeadObject: everything the file metadata
// helpers need. Meta carries the x-amz-meta-* user metadata for the
// Unix-metadata feature (listings can't, which is why heads are cached too).
type headData struct {
	Size  int64             `json:"s"`
	Mtime int64             `json:"m"` // unix nanoseconds
	Meta  map[string]string `json:"u,omitempty"`
}

// DefaultGroupName is the groupcache group every objgitd uses; it must match
// across peers for cross-process sharing to route correctly.
const DefaultGroupName = "objgit-listings"

// CacheConfig configures a ListingCache. TTL must be > 0 (callers gate the whole
// feature on it). The remaining fields have sane zero-value behaviour.
type CacheConfig struct {
	TTL             time.Duration // key window; bounds cross-process staleness
	RefreshInterval time.Duration // warmer tick; <=0 disables the warmer
	IdleTTL         time.Duration // drop un-accessed prefixes from the warmer
	SizeBytes       int64         // groupcache LRU budget (<=0 → 64 MiB)
	Name            string        // groupcache group name (default DefaultGroupName)

	// DisableHeadPrefetch turns off the background HeadObject precache that
	// warms the head cache for every file in a listing. The zero value keeps
	// prefetching enabled.
	DisableHeadPrefetch bool

	Self  string   // this node's groupcache base URL ("" → single-process)
	Peers []string // peer base URLs, including Self, when sharing
}

// ListingCache wraps a groupcache group with the window/generation key scheme
// and a background warmer. It is safe for concurrent use and is shared by
// pointer across an S3FS and all of its Chroot children.
type ListingCache struct {
	group     *groupcache.Group
	headGroup *groupcache.Group    // per-object HeadObject metadata
	pool      *groupcache.HTTPPool // nil in single-process mode
	ttl       time.Duration
	cfg       CacheConfig

	clock       func() time.Time // overridable in tests
	prefetchSem chan struct{}    // bounds background head precaches

	mu   sync.Mutex
	gens map[string]uint64    // per-prefix local generation
	seen map[string]time.Time // prefixes accessed → driven by the warmer
}

// NewListingCache builds a cache backed by a groupcache group whose getter lists
// the requested prefix from S3. client/bucket/separator are captured for the
// getter; they are the root filesystem's, so cached prefixes are full-canonical
// keys that every Chroot view agrees on.
func NewListingCache(cfg CacheConfig, client s3Client, bucket, separator string) *ListingCache {
	if cfg.Name == "" {
		cfg.Name = DefaultGroupName
	}
	if cfg.SizeBytes <= 0 {
		cfg.SizeBytes = 64 << 20
	}
	c := &ListingCache{
		ttl:         cfg.TTL,
		cfg:         cfg,
		clock:       time.Now,
		prefetchSem: make(chan struct{}, maxPrefetchInFlight),
		gens:        map[string]uint64{},
		seen:        map[string]time.Time{},
	}

	listGetter := groupcache.GetterFunc(func(ctx context.Context, key string, dest groupcache.Sink) error {
		prefix := keyPrefix(key)
		entries, err := listChildren(ctx, client, bucket, separator, prefix)
		if err != nil {
			return err
		}
		data, err := json.Marshal(entries)
		if err != nil {
			return err
		}
		// Precache each file's HeadObject in the background, off this request's
		// critical path; groupcache singleflight coalesces a background precache
		// with any foreground head lookup for the same object.
		c.prefetchHeads(prefix, entries)
		return dest.SetBytes(data)
	})
	c.group = groupcache.NewGroup(cfg.Name, cfg.SizeBytes, listGetter)

	headGetter := groupcache.GetterFunc(func(ctx context.Context, key string, dest groupcache.Sink) error {
		objKey := keyPrefix(key)
		start := time.Now()
		ho, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &objKey})
		observeS3("HeadObject", start, err)
		if err != nil {
			return err
		}
		data, err := json.Marshal(headData{
			Size:  aws.ToInt64(ho.ContentLength),
			Mtime: aws.ToTime(ho.LastModified).UnixNano(),
			Meta:  ho.Metadata,
		})
		if err != nil {
			return err
		}
		return dest.SetBytes(data)
	})
	c.headGroup = groupcache.NewGroup(cfg.Name+"-heads", cfg.SizeBytes, headGetter)

	if cfg.Self != "" {
		c.pool = groupcache.NewHTTPPoolOpts(cfg.Self, &groupcache.HTTPPoolOptions{})
		peers := cfg.Peers
		if len(peers) == 0 {
			peers = []string{cfg.Self}
		}
		c.pool.Set(peers...)
	}

	return c
}

// list returns the immediate children of prefix, served from groupcache (which
// fills via S3 on a miss). prefix is a full-canonical key prefix: "" for the
// bucket root, otherwise ending in the separator.
func (c *ListingCache) list(ctx context.Context, prefix string) ([]childEntry, error) {
	c.touch(prefix)

	var data []byte
	if err := c.group.Get(ctx, c.groupKey(prefix), groupcache.AllocatingByteSliceSink(&data)); err != nil {
		return nil, err
	}
	var entries []childEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// groupKey encodes the prefix, the current time window, and the prefix's local
// generation. The prefix is first so the getter can recover it.
func (c *ListingCache) groupKey(prefix string) string {
	window := c.clock().UnixNano() / int64(c.ttl)
	return prefix + "\x00" + strconv.FormatInt(window, 10) + "\x00" + strconv.FormatUint(c.gen(prefix), 10)
}

// headKey is groupKey's analogue for the per-object head cache. It carries the
// object key plus the same window and its parent prefix's generation, so a
// write under that prefix (which bumps the generation) re-heads the object.
func (c *ListingCache) headKey(objKey string) string {
	prefix, _ := splitKey(objKey)
	window := c.clock().UnixNano() / int64(c.ttl)
	return objKey + "\x00" + strconv.FormatInt(window, 10) + "\x00" + strconv.FormatUint(c.gen(prefix), 10)
}

// keyPrefix recovers the object key / prefix a getter was asked for, stripping
// the "\x00window\x00gen" suffix groupKey/headKey append.
func keyPrefix(key string) string {
	if i := strings.IndexByte(key, 0); i >= 0 {
		return key[:i]
	}
	return key
}

// headInfo returns the object's HeadObject metadata, served from the head cache
// (prewarmed in the background from listings, or filled on demand here).
func (c *ListingCache) headInfo(ctx context.Context, objKey string) (*s3.HeadObjectOutput, error) {
	var data []byte
	if err := c.headGroup.Get(ctx, c.headKey(objKey), groupcache.AllocatingByteSliceSink(&data)); err != nil {
		return nil, err
	}
	var d headData
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, err
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(d.Size),
		LastModified:  aws.Time(time.Unix(0, d.Mtime)),
		Metadata:      d.Meta,
	}, nil
}

// prefetchHeads warms the head cache for every file in a freshly-listed folder,
// each via a detached background goroutine so the work never blocks the request
// that triggered the listing. Beyond maxPrefetchInFlight concurrent precaches it
// drops extras (logged), which are then fetched on demand.
func (c *ListingCache) prefetchHeads(prefix string, entries []childEntry) {
	if c.cfg.DisableHeadPrefetch {
		return
	}
	for _, e := range entries {
		if e.Kind != kindFile {
			continue
		}
		objKey := prefix + e.Name
		select {
		case c.prefetchSem <- struct{}{}:
		default:
			slog.Debug("head precache skipped: prefetch pipeline full", "key", objKey)
			continue
		}
		go func(k string) {
			defer func() { <-c.prefetchSem }()
			var data []byte
			if err := c.headGroup.Get(context.Background(), c.headKey(k), groupcache.AllocatingByteSliceSink(&data)); err != nil {
				slog.Debug("head precache failed", "key", k, "err", err)
			}
		}(objKey)
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

// invalidate bumps a prefix's local generation so the next read in this process
// re-lists. groupcache cannot delete; moving the key is how we invalidate.
func (c *ListingCache) invalidate(prefix string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.gens[prefix]++
	c.seen[prefix] = c.clock()
	c.mu.Unlock()
}

func (c *ListingCache) touch(prefix string) {
	c.mu.Lock()
	c.seen[prefix] = c.clock()
	c.mu.Unlock()
}

// RunWarmer pre-fills the current-window key for every recently-accessed prefix
// on each tick, smoothing window-rollover misses, and evicts prefixes unused for
// longer than IdleTTL. It returns when ctx is cancelled; it is a no-op when the
// refresh interval is non-positive.
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

	for _, p := range prefixes {
		var data []byte
		if err := c.group.Get(ctx, c.groupKey(p), groupcache.AllocatingByteSliceSink(&data)); err != nil {
			slog.Debug("listing-cache warm failed", "prefix", p, "err", err)
		}
	}
}

// PoolHandler returns the groupcache peer HTTP handler, or nil in single-process
// mode. Serve it on the address passed as -groupcache-bind.
func (c *ListingCache) PoolHandler() http.Handler {
	if c == nil || c.pool == nil {
		return nil
	}
	return c.pool
}

// CacheStats is a flat snapshot of groupcache counters for export to metrics.
type CacheStats struct {
	Gets, CacheHits, Loads, LocalLoads, PeerLoads, LocalLoadErrs int64
	MainBytes, MainItems, MainEvictions                          int64
	HotBytes, HotItems                                           int64
}

// Stats snapshots the underlying groupcache counters.
func (c *ListingCache) Stats() CacheStats {
	if c == nil || c.group == nil {
		return CacheStats{}
	}
	main := c.group.CacheStats(groupcache.MainCache)
	hot := c.group.CacheStats(groupcache.HotCache)
	return CacheStats{
		Gets:          c.group.Stats.Gets.Get(),
		CacheHits:     c.group.Stats.CacheHits.Get(),
		Loads:         c.group.Stats.Loads.Get(),
		LocalLoads:    c.group.Stats.LocalLoads.Get(),
		PeerLoads:     c.group.Stats.PeerLoads.Get(),
		LocalLoadErrs: c.group.Stats.LocalLoadErrs.Get(),
		MainBytes:     main.Bytes,
		MainItems:     main.Items,
		MainEvictions: main.Evictions,
		HotBytes:      hot.Bytes,
		HotItems:      hot.Items,
	}
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
