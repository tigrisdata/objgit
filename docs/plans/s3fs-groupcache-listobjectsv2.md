# Plan: groupcache-backed directory-listing cache for `internal/s3fs`

## Context

`objgitd` stores git repos as S3/Tigris objects. Every `Stat`/`Open` on a path that
**doesn't exist** costs up to **two** S3 round-trips: a `HeadObject` (→ `NoSuchKey`)
then a directory-probe `ListObjectsV2` (`internal/s3fs/basic.go:140`+`:163`; the probe
in `OpenFile` at `:98`). git does an enormous number of these — loose objects on a
packed repo, `packed-refs` vs. loose refs, `info/`, `config`, alternates, `.keep`/`.idx`
siblings — each paid in series against an object store.

The fix: cache **directory listings**, keyed by parent prefix, and answer `Stat`/`Open`
from them. A listing records every child name plus its kind/size/mtime, so a lookup of a
key whose parent prefix is cached answers with **zero** round-trips when the child is
absent (negative hit) or is a sub-directory, and with one `HeadObject`/`GetObject` (for
authoritative content/metadata, which listings don't carry) when it's a file. The first
`Stat`/`Open` touching an un-cached folder lists that **parent folder** in full and
populates the cache, warming every sibling at once.

**Backend (user decision): `github.com/golang/groupcache`,** so the listing cache is
shared across processes via groupcache's consistent-hash peer pool. groupcache is a
fill-once, **immutable** cache: no `Set`, no `Delete`, no TTL. We adapt to that with two
techniques baked into the cache **key**:

- **Window-encoded TTL.** The key carries a time window `floor(now/TTL)`. When the
  window advances the key changes → groupcache miss → the getter re-lists from S3. Old
  keys age out by LRU. Staleness is bounded by one TTL window.
- **Per-prefix local generation for precise in-process invalidation.** The key also
  carries a process-local counter per prefix, bumped on every local write under that
  prefix. A local write moves the key, so the next read in this process re-lists and
  sees its own write immediately — recovering read-after-write correctness that
  groupcache alone cannot give (no delete). The stale entry under the old key is simply
  never queried again by us.

**Accepted limitation (explicit user choice):** cross-process and post-restart staleness
is bounded by the TTL window — another process (or this one after a restart that resets
the local generation) may read a just-written object as "not found" for up to one
window. This is safe for git's _content_ (objects are immutable and content-addressed, so
positive listings never go wrong), and the window bounds the negative-staleness risk.
Operators tune it with `-s3-cache-ttl`; `-s3-cache-ttl 0` disables the cache and restores
today's exact behavior.

**Defaults:** on by default, tunable; single-process unless peers are configured.

## Correctness model

- **Cache key space is the canonical full S3 key.** `Chroot` returns a _new_ `*S3FS`
  with a different `root`, so the `*ListingCache` is **shared by pointer** across a root
  fs and all chroot children, and prefixes are full-canonical (`fs3.key()`/`cleanPath` —
  root-joined, leading slash stripped). `chroot.go:21` must copy the cache pointer.
- **groupcache key** = `prefix "\x00" window "\x00" localgen`, where
  `window = now.Unix() / int64(ttl.Seconds())` and `localgen = c.gen(prefix)`. The
  getter parses `prefix` back off the key (segment before the first `\x00`) and lists it.
- **Population is via the getter only** (groupcache has no `Set`). Both `ReadDir` and the
  `Stat`/`Open` resolution route through `group.Get`, so each folder is listed once per
  (window, localgen) and the result feeds both. groupcache's singleflight dedupes
  concurrent identical `Get`s (one list even when a clone fans out across siblings), and
  the owning peer dedupes across the fleet.
- **Listing payload** carries enough to serve both consumers without a second call:
  per child `{name, kind(file|dir), size, mtimeUnixNano}` (`CommonPrefixes`→dir with
  zero size/mtime, `Contents`→file; file wins on the pathological file+prefix collision,
  matching today's Head-first precedence). Encoded compactly (JSON is fine; listings are
  small).
- **In-process writes bump the local generation** of the parent prefix at _write
  completion_, moving the key so the next read re-lists:
  - `s3WriteFile.Close` / `s3MultipartUploadFile.Close` — after the upload succeeds.
  - `Rename` (covers `TempFile`→final pack promotion), `Remove`, `MkdirAll`.
- **Positive `Stat`/`Open` of a file** is served from a second cache, the **head
  cache** (see [Head precache](#head-precache)), rather than a foreground `HeadObject`.
  Listings omit the user-metadata the unix-metadata feature needs, so head metadata is
  cached per object and **prefetched in the background** when a listing is filled. `Open`
  still issues `GetObject` for the body but skips its `HeadObject`. A `NoSuchKey` from a
  delete racing the listing maps to `NotExist`.

## Changes

### go.mod

- Add `github.com/golang/groupcache` (effectively dependency-free).

### New file: `internal/s3fs/listingcache.go`

The cache wrapper, groupcache group, key logic, and background warmer.

```go
type CacheConfig struct {
    TTL, RefreshInterval, IdleTTL time.Duration
    SizeBytes                     int64    // groupcache LRU budget
    Name                          string   // group name (default "objgit-listings")
    DisableHeadPrefetch           bool     // zero value = background head prefetch on
    Self  string                  // this node's groupcache URL ("" = single-process)
    Peers []string                // peer URLs (incl. self) when sharing
}

type childKind uint8 // kindFile / kindDir
type childEntry struct { Name string; Kind childKind; Size, Mtime int64 }
type headData   struct { Size, Mtime int64; Meta map[string]string }

type ListingCache struct {
    group       *groupcache.Group           // listings, keyed by prefix
    headGroup   *groupcache.Group           // HeadObject metadata, keyed by object key
    pool        *groupcache.HTTPPool         // nil in single-process mode
    ttl         time.Duration
    cfg         CacheConfig
    clock       func() time.Time             // overridable in tests
    prefetchSem chan struct{}                // bounds background head precaches
    mu          sync.Mutex
    gens        map[string]uint64            // per-prefix local generation
    seen        map[string]time.Time         // prefixes accessed → driven by the warmer
}
```

- `NewListingCache(cfg, client, bucket, separator) *ListingCache` — creates the
  groupcache `Group` (name `"objgit-listings"`, `cfg.SizeBytes`) with a `GetterFunc`
  closure over `client`/`bucket`/`separator` that parses the prefix from the key, runs a
  full paginated `listChildren`, and encodes the payload into the sink. When
  `cfg.Self != ""`, builds a `groupcache.NewHTTPPoolOpts(cfg.Self, …)` and `pool.Set(cfg.Peers…)`.
- `list(ctx, prefix) ([]childEntry, error)` — record `seen[prefix]=now`; build the
  groupcache key; `group.Get(ctx, key, AllocatingByteSliceSink(&buf))`; decode. This is
  the one entry point both `ReadDir` and `Stat`/`Open` use.
- `gen(prefix)` / `invalidate(prefix)` — `invalidate` bumps `gens[prefix]` and records
  `seen` so the warmer re-warms the new key (no groupcache delete needed).
- `runWarmer(ctx)` / `RunWarmer(ctx)` — `time.NewTicker(RefreshInterval)`; each tick drop
  `seen` entries idle past `IdleTTL`, then `group.Get` the current key for each remaining
  prefix (pre-fills the new window before clients wait and smooths window-rollover
  herds). No-op when `RefreshInterval<=0`. Returns on `ctx.Done()` (errgroup idiom).
- `PoolHandler() http.Handler` / `Stats()` accessors for `main` to serve peers and export
  metrics.
- `splitKey(key) (prefix, base)` — split on last `/`; no slash → `("", key)`.

### Head precache

A **second groupcache group** (`<name>-heads`, keyed by object key) caches each file's
`HeadObject` result — `headData{size, mtimeUnixNano, meta}` — so positive `Stat`/`Open`
never pay a foreground `HeadObject`. The head key reuses the window + the **parent
prefix's** local generation, so a write under that prefix re-heads the object exactly as
it re-lists the folder.

- **Background prefetch (`prefetchHeads`).** When the listing getter fills a folder, it
  launches a **detached goroutine per file** (`go headGroup.Get(context.Background(), …)`)
  to warm the head cache off the request's critical path; groupcache singleflight
  coalesces a background precache with any foreground head lookup for the same object. A
  semaphore (`maxPrefetchInFlight = 64`) bounds the in-flight precaches so a large folder
  can't spawn an unbounded goroutine/S3 storm — overflow files are fetched on demand
  (logged at debug). `CacheConfig.DisableHeadPrefetch` turns this off (zero value = on).
- **`headInfo(ctx, key) (*s3.HeadObjectOutput, error)`** serves a foreground lookup from
  the head cache (a warm hit costs no round-trip; a miss fills via one `HeadObject`).
- `Stat`/`Open` of a present file call `headInfo`; `newS3ReadFile` gains an optional
  precomputed `*s3.HeadObjectOutput` so it skips its own `HeadObject` and only `GetObject`s
  the body.

### `internal/s3fs/filesystem.go`

- Add `cache *ListingCache` to `S3FS`; `WithListingCache(c) Option`. `NewS3FS`'s `client`
  parameter widens to an `s3Client` interface (the concrete `*storage.Client` satisfies
  it) so tests can substitute a counting stub.

### `internal/s3fs/chroot.go`

- Copy `cache: fs3.cache` into the new `S3FS` literal (`chroot.go:21`).

### `internal/s3fs/dir.go`

- Extract `listChildren(ctx, client, bucket, separator, prefix) ([]childEntry, error)` —
  the paginated `ListObjectsV2` loop, classifying `CommonPrefixes`→dir, `Contents`→file
  with size/mtime, dirs-then-files preserving S3 order. A free function so the getter
  (which holds only the raw client) can reuse it. Used by `ReadDir` (cache off) and both
  getters.
- `ReadDir`: when `cache != nil`, get the entries via `cache.list(ctx, prefix)` and
  build `[]fs.DirEntry` from them (rebuilding `newDirInfo`/`newFileInfo` from the payload);
  otherwise list directly as today.
- `MkdirAll` — `cache.invalidate(parent prefix of filename)` after the `PutObject`.

### `internal/s3fs/basic.go`

- Helper `resolve(ctx, key) (childEntry, found, known bool)`: `(_,_,false)` when
  `cache==nil`; else `prefix,base := splitKey(key)`, `entries, err := cache.list(...)`; on
  error `known=false` (fall back to the live path — cache problems never fail the op);
  else scan `entries` for `base` and return it.
- `Stat`: after the temp-buffer check, call `resolve`. If `known`: absent →
  `&os.PathError{Op:"stat",…,Err: fs.ErrNotExist}`; dir → `newDirInfo`; file →
  `cache.headInfo`→`newFileInfoFromHead` (head `NoSuchKey` → `NotExist`). Not `known` → the
  existing `HeadObject`+probe fallback.
- `OpenFile` `O_RDONLY` (after temp check): same `resolve`; absent → `NotExist`, dir →
  `newS3DirFile`, file → `cache.headInfo` then `newS3ReadFile(…, ho)` (skips its
  `HeadObject`); not `known` → existing fallback with a nil head.
- `Rename` — invalidate parent prefix of both `src` and `dst` on success.
- `Remove` — invalidate parent prefix of `key` on success.

### `internal/s3fs/file.go`

- Add a `cache *ListingCache` field + constructor arg to `s3WriteFile` /
  `s3MultipartUploadFile`; their `Close` calls `cache.invalidate(parent prefix of f.key)`
  after a successful upload (nil-guarded). Update the two `OpenFile` call sites
  (`basic.go:114`,`:117`).

### `internal/metrics/metrics.go`

- A Prometheus collector that reads `ListingCache.Stats()` (groupcache `group.Stats`:
  Gets, CacheHits, Loads, LocalLoads, PeerLoads, LoaderErrors; plus `CacheStats` bytes/
  items/evictions) and exports them as `objgit_s3_listing_cache_*` gauges/counters. No
  `repo` label. (groupcache fills are already counted as `ListObjectsV2` via `observeS3`.)
  `main` registers it only when the cache is enabled.

### `cmd/objgitd/main.go`

- New flags (kebab-case + flagenv) in the `var (...)` block at `main.go:32`:
  - `-s3-cache-ttl` (`Duration`, default `60s`) — window size; `<=0` disables the cache.
  - `-s3-cache-refresh` (`Duration`, default `30s`) — warmer interval; `<=0` disables the
    warmer (lazy fill still works).
  - `-s3-cache-idle` (`Duration`, default `10m`) — drop un-accessed prefixes from the warmer.
  - `-s3-cache-size` (`Int64`, default `64<<20`) — groupcache LRU budget.
  - `-groupcache-self` (`String`, default `""`) — this node's groupcache base URL; empty =
    single-process.
  - `-groupcache-peers` (`String`, default `""`) — comma-separated peer URLs.
  - `-groupcache-bind` (`String`, default `""`) — listen addr serving `PoolHandler()`.
- When `ttl > 0`: build `cache := s3fs.NewListingCache(cfg, client, *bucket, "/")`, pass
  `s3fs.WithListingCache(cache)` into `NewS3FS` (`main.go:78`), register the metrics
  collector, and in the errgroup:
  - if `-groupcache-bind != ""`, `net.Listen` + serve `cache.PoolHandler()` (plus the
    Serve/`Shutdown`-on-`gCtx.Done()` two-goroutine idiom used by the other listeners).
  - `g.Go(func() error { cache.RunWarmer(gCtx); return nil })`.
- Add the cache settings to the startup `slog.Info` line.

## Why this is acceptably safe

Within a single git operation the repeated negative lookups happen within milliseconds,
so even a 60s window eliminates essentially all redundant `HeadObject`+probe pairs, and
the first miss in a folder warms every sibling via one parent listing (deduped by
groupcache singleflight, fleet-wide via the owning peer). Local writes bump the
per-prefix generation, so a push reads its own objects immediately. git object _content_
is immutable, so positive/`ReadDir` results are never wrong about what exists — only the
_recency_ of newly-added entries is window-bounded. The residual risk is a cross-process
(or post-restart) negative read of an object another writer just created, bounded by one
TTL window — the limitation the user explicitly accepted; `-s3-cache-ttl 0` opts out
entirely. Note: writes fragment the shared keyspace for written prefixes (each writer's
`localgen` differs), so cross-process sharing is strongest on the read-only prefixes that
dominate clone/fetch and weakest on actively-pushed prefixes — which is the desirable
bias.

## Verification

1. `go build ./...`; `go mod tidy`; `go test ./...`.
2. **Cache disabled = no behavior change:** the cache is only wired when `-s3-cache-ttl>0`;
   existing protocol tests (`go test ./cmd/objgitd/...`, needs `git` on PATH) must pass
   with the cache off and on (single-process, no peers).
3. **New unit tests in `internal/s3fs`** (table-driven `tt`, counting-stub `storage.Client`):
   - **Populate-on-miss:** first `Stat` of an absent key in a never-listed folder issues
     exactly one `ListObjectsV2` (the parent) and **zero** `HeadObject`; a second absent
     sibling → **zero** S3; a present sibling → only `HeadObject`.
   - `ReadDir` then `Stat`/`Open` of an absent sibling → zero S3; a dir child → zero S3.
   - **Local invalidation:** `Create`+`Close` / `Rename` / `Remove` / `MkdirAll` bump the
     generation so a following `Stat`/`ReadDir` re-lists and sees the change (read-after-write).
   - **Window TTL:** advancing time past `TTL` changes the key → re-list (inject a clock or
     a settable `now` in `ListingCache` for the test).
   - **Warmer:** `RunWarmer` re-`Get`s accessed prefixes and evicts idle ones past `IdleTTL`.
   - **Chroot sharing:** `ReadDir` on the root then `Stat` of an absent child through a
     chroot resolves from the same cached prefix (same canonical key).
   - **Head precache:** one listing fill warms every file's head in the background; once
     warm, `Stat` of a file does **zero** further `HeadObject`s (counting-stub tests
     disable prefetch for determinism; a dedicated test enables it and waits for warmup).
4. **Two-process sharing (manual):** run two `objgitd` with `-groupcache-self`/`-peers`
   pointing at each other; clone through one and confirm via `objgit_s3_listing_cache_*`
   (PeerLoads > 0 on the non-owner) that listings are served cross-process.
5. **End-to-end:** `./objgitd -bucket $BUCKET -allow-push`; clone a packed repo twice and
   confirm `objgit_s3_requests_total{operation="HeadObject"}` grows far slower than with
   `-s3-cache-ttl 0`.
