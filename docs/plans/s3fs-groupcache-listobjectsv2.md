# Plan: directory-listing cache for `internal/s3fs`

> **Status note.** This started as a groupcache-backed, fleet-shareable cache
> (hence the filename). It shipped as a **process-local `sync.Map` cache** —
> groupcache was ripped out for simplicity. There is no cross-process sharing,
> no peer pool, and no window-encoded key; TTL is a plain per-entry expiry. The
> sections below are revised to match what shipped; ignore any lingering peer/
> window phrasing.

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

**Backend: process-local `sync.Map`s** (one for folder listings, one for recursive
subtrees, one for per-object heads). Each cached entry carries two pieces of bookkeeping:

- **Per-entry TTL.** An entry stores `expires = now + TTL`; past it the entry is ignored
  and re-listed. This bounds how long a write this process can't see stays hidden. (There
  is no background warmer requirement for correctness; the warmer just keeps hot entries
  fresh and sweeps expired ones, since a `sync.Map` has no LRU.)
- **Per-prefix local generation for precise invalidation.** A process-local counter per
  prefix is bumped on every local write under that prefix (and its ancestors — see
  subtree caching). An entry whose stored generation no longer matches is ignored, so the
  next read re-lists and sees the write immediately (read-after-write correctness).
  Concurrent identical fills are coalesced with `golang.org/x/sync/singleflight`.

**Accepted limitation:** negative staleness is bounded by the TTL — a just-deleted-or-
created object may read stale for up to one TTL. Safe for git's _content_ (objects are
immutable and content-addressed, so positive listings never go wrong); the TTL bounds the
negative-staleness risk. Operators tune it with `-s3-cache-ttl`; `-s3-cache-ttl 0`
disables the cache and restores today's exact behavior.

**Defaults:** on by default, tunable; always single-process.

## Correctness model

- **Cache key space is the canonical full S3 key.** `Chroot` returns a _new_ `*S3FS`
  with a different `root`, so the `*ListingCache` is **shared by pointer** across a root
  fs and all chroot children, and prefixes are full-canonical (`fs3.key()`/`cleanPath` —
  root-joined, leading slash stripped). `chroot.go:21` must copy the cache pointer.
- **Cache key** is just the canonical prefix; the entry holds `{gen, expires}` alongside
  its payload. A read hits only when `entry.gen == gen(prefix)` and `now < entry.expires`.
- **Both `ReadDir` and `Stat`/`Open` route through `list()`**, so each folder is listed
  once and the result feeds both; `singleflight` coalesces concurrent identical fills (one
  list even when a clone fans out across siblings).
- **Listing payload** carries enough to serve both consumers without a second call:
  per child `{name, kind(file|dir), size, mtimeUnixNano}` (`CommonPrefixes`→dir with
  zero size/mtime, `Contents`→file; file wins on the pathological file+prefix collision,
  matching today's Head-first precedence). Stored as a Go value in the `sync.Map` — no
  serialization.
- **In-process writes bump the local generation** of the parent prefix at _write
  completion_, moving the key so the next read re-lists:
  - `s3WriteFile.Close` / `s3MultipartUploadFile.Close` — after the upload succeeds.
  - `Rename` (covers `TempFile`→final pack promotion), `Remove`, `MkdirAll`.
- **Positive `Stat`/`Open` of a file** is served from a second cache, the **head
  cache** (see [Head cache](#head-cache)), rather than a foreground `HeadObject`. The head
  cache is **seeded straight from each listing** — `ListObjectsV2` already returns every
  file's size and mtime — so a positive lookup costs no extra round-trip. Listings omit
  the user-metadata the unix-metadata feature needs, so a caller that requires it (i.e.
  unix-metadata enabled) treats a listing-seeded entry as a miss and fills via a real
  `HeadObject`. `Open` still issues `GetObject` for the body but skips its `HeadObject`. A
  `NoSuchKey` from a delete racing the listing maps to `NotExist`.

## Changes

### go.mod

- No new direct dependency. `golang.org/x/sync/singleflight` (already vendored for
  `errgroup`) dedupes concurrent fills.

### New file: `internal/s3fs/listingcache.go`

The three `sync.Map` caches, the TTL/generation key logic, and the background warmer.

```go
type CacheConfig struct {
    TTL, RefreshInterval, IdleTTL time.Duration
    DisableHeadPrefetch           bool     // zero value = seed heads from listings on
    RecursivePrefixes             []string // nil → {"refs/"}; empty → subtree caching off
    MaxSubtreeKeys                int      // <=0 → 50000
}

type childKind uint8 // kindFile / kindDir
type childEntry struct { Name string; Kind childKind; Size, Mtime int64 }
type headData   struct { Size, Mtime int64; Meta map[string]string }
type headCacheEntry struct { data headData; gen uint64; expires time.Time; hasMeta bool }
type listingEntry  struct { entries []childEntry; gen uint64; expires time.Time }
type subtreeEntry  struct { data subtreeData;     gen uint64; expires time.Time }

type ListingCache struct {
    ttl       time.Duration
    cfg       CacheConfig
    client    s3Client
    bucket    string
    separator string
    roots     []string             // normalised RecursivePrefixes, longest first
    clock     func() time.Time     // overridable in tests
    listings  sync.Map             // prefix     → listingEntry
    subtrees  sync.Map             // root       → subtreeEntry
    heads     sync.Map             // object key → headCacheEntry
    sf, headSF singleflight.Group  // coalesce concurrent fills
    hits, misses atomic.Int64      // for metrics
    mu        sync.Mutex
    gens      map[string]uint64    // per-prefix local generation
    seen      map[string]time.Time // prefixes accessed → driven by the warmer
}
```

- `NewListingCache(cfg, client, bucket, separator)` — applies defaults and normalises the
  recursive roots; no groups, no pool.
- `list(ctx, prefix) ([]childEntry, error)` — the one entry point both `ReadDir` and
  `Stat`/`Open` use. Routes recursive prefixes to `subtree`; otherwise `listFolder`
  (sync.Map lookup gated on gen+expiry, `fillFolder` via singleflight on a miss).
- `gen(prefix)` / `invalidate(prefix)` — `invalidate` bumps the generation of `prefix`
  **and every ancestor** and records `seen` (so the warmer refreshes them).
- `RunWarmer(ctx)` — `time.NewTicker(RefreshInterval)`; each tick drops `seen` entries idle
  past `IdleTTL`, re-fills the rest (routing recursive→subtree, deduped), then sweeps
  expired entries from all three maps (no LRU, so the warmer bounds growth). No-op when
  `RefreshInterval<=0`. Returns on `ctx.Done()`.
- `Stats()` accessor exports hit/miss counters and resident item counts to metrics.
- `splitKey(key) (prefix, base)` — split on last `/`; no slash → `("", key)`.

### Head cache

The per-object head cache is a **`sync.Map`** of `headCacheEntry`, keyed by
canonical object key — like the listing and subtree caches. An
entry is a hit only while (a) unexpired (`expires`, one TTL past fill), (b) still tagged
with its parent prefix's current local generation — so `invalidate`'s generation bump
drops every cached head under the prefix without a map scan, mirroring listing
invalidation — and (c) carrying user metadata when the caller needs it (`hasMeta`).

- **Seeded from listings (`seedHeads`), not separately fetched.** When the listing getter
  fills a folder, it stores a `headCacheEntry` for every file **directly from the
  `ListObjectsV2` data** (size + mtime), with `hasMeta=false`. This warms the head cache
  with **zero** extra `HeadObject` calls — the listing already paid for the data.
  `CacheConfig.DisableHeadPrefetch` turns seeding off (zero value = on).
- **`headInfo(ctx, key, needMeta) (*s3.HeadObjectOutput, error)`** serves a foreground
  lookup. A warm hit costs no round-trip. `needMeta` reports whether the caller needs the
  x-amz-meta-* user metadata (true iff unix-metadata is enabled): when true, a
  listing-seeded (`hasMeta=false`) entry is treated as a miss and filled via one real
  `HeadObject` (which stores `hasMeta=true`). `headSF` (a `singleflight.Group`) dedupes
  concurrent fills for the same key.
- The warmer also **sweeps expired head entries** each tick — the `sync.Map` has no LRU,
  so the warmer is what bounds its growth to roughly the live working set.
- `Stat`/`Open` of a present file call `headInfo` with `needMeta = fs3.unixMeta != nil`;
  `newS3ReadFile` gains an optional precomputed `*s3.HeadObjectOutput` so it skips its own
  `HeadObject` and only `GetObject`s the body.

### Subtree caching (`RecursivePrefixes`)

Caching one folder per `ListObjectsV2` is wasteful for bounded namespaces that callers
walk folder-by-folder — `refs/` above all (`refs/`, `refs/heads/`, `refs/tags/`,
`refs/remotes/…`, each its own delimited list). A **single delimiter-less
`ListObjectsV2` over `refs/`** returns the whole subtree; every descendant folder's
listing and every negative lookup beneath it is then synthesised in memory.

- **A second `sync.Map`** (`subtrees`, keyed by root) holds `subtreeData{ Objects
  []subtreeObject; Truncated bool }` — the flat key+size+mtime set under a root — in a
  `subtreeEntry{gen, expires}`, the same TTL/generation scheme as folder listings.
- **`list(prefix)` routes**: if `recursiveRoot(prefix)` matches a configured root, serve
  from `subtree(root)` via `synthesizeListing(objects, prefix)` (remainder-after-prefix
  with a `/` ⇒ child dir, deduped; else child file). Otherwise the existing delimited
  path. A complete subtree is authoritative for negative lookups (it has *all* keys under
  the root); only `!Truncated` subtrees are trusted.
- **Bounded.** `listSubtree` stops once it exceeds `MaxSubtreeKeys` (default 50000) and
  reports `Truncated`; `list` then **falls back to the delimited per-folder listing**, so
  an unbounded namespace can't blow up memory. The truncated marker is itself cached, so
  the fallback costs one near-free subtree-cache hit plus the folder list.
- **Invalidation walks ancestors.** `invalidate(prefix)` now bumps the generation of
  `prefix` **and every parent up to `""`** (`ancestorPrefixes`), so a write to
  `refs/tags/v1` moves the `refs/` subtree key (and the root listing's). Trade-off: a
  broader blast radius — a write also re-lists the coarser folders above it on their next
  read. Acceptable because writes are pushes and a re-list after a push is expected.
- **Head seeding** extends to subtrees: a complete scan seeds every file's head
  (`seedSubtreeHeads`), each tagged with its own parent prefix's generation. A truncated
  scan seeds nothing (leaves heads to the fallback path).
- **Warmer routing** mirrors `list`: seen prefixes under a recursive root warm that root's
  subtree (deduped across siblings) rather than per-folder.
- **Config**: `CacheConfig.RecursivePrefixes` (nil ⇒ `{"refs/"}`; explicit empty ⇒ off)
  and `MaxSubtreeKeys`. `main.go` exposes `-s3-cache-recursive-prefixes` (default `refs/`,
  empty disables) and `-s3-cache-max-subtree-keys` (default 50000).

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
  (which holds only the raw client) can reuse it. Used by `ReadDir` (cache off) and the
  listing getter.
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

- A Prometheus collector that reads `ListingCache.Stats()` (`{Hits, Misses, ListingItems,
  SubtreeItems, HeadItems}`) and exports `objgit_s3_listing_cache_hits_total`,
  `_misses_total`, and `_items{kind=listing|subtree|head}`. No `repo` label. (Cache fills
  are already counted as `ListObjectsV2`/`HeadObject` via `observeS3`.) `main` registers it
  only when the cache is enabled.

### `cmd/objgitd/main.go`

- Flags (kebab-case + flagenv):
  - `-s3-cache-ttl` (`Duration`, default `60s`) — per-entry TTL; `<=0` disables the cache.
  - `-s3-cache-refresh` (`Duration`, default `30s`) — warmer interval; `<=0` disables the
    warmer (lazy fill still works).
  - `-s3-cache-idle` (`Duration`, default `10m`) — drop un-accessed prefixes from the warmer.
  - `-s3-cache-recursive-prefixes` (`String`, default `refs/`) — comma-separated subtree
    roots; empty disables subtree caching.
  - `-s3-cache-max-subtree-keys` (`Int`, default `50000`) — subtree scan cap.
- When `ttl > 0`: build `cache := s3fs.NewListingCache(cfg, client, *bucket, "/")`, pass
  `s3fs.WithListingCache(cache)` into `NewS3FS`, register the metrics collector, and add
  `g.Go(func() error { cache.RunWarmer(gCtx); return nil })` to the errgroup.
- Add the cache settings to the startup `slog.Info` line.

## Why this is acceptably safe

Within a single git operation the repeated negative lookups happen within milliseconds,
so even a 60s TTL eliminates essentially all redundant `HeadObject`+probe pairs, and the
first miss in a folder warms every sibling via one parent listing (deduped by
singleflight). Local writes bump the per-prefix generation (and its ancestors'), so a
push reads its own objects immediately. git object _content_ is immutable, so
positive/`ReadDir` results are never wrong about what exists — only the _recency_ of
newly-added entries is TTL-bounded. The residual risk is a negative read of an object
another process just created (this cache is process-local), bounded by one TTL; `-s3-cache-ttl 0`
opts out entirely.

## Verification

1. `go build ./...`; `go mod tidy`; `go test ./...`.
2. **Cache disabled = no behavior change:** the cache is only wired when `-s3-cache-ttl>0`;
   existing protocol tests (`go test ./cmd/objgitd/...`, needs `git` on PATH) must pass
   with the cache off and on.
3. **New unit tests in `internal/s3fs`** (table-driven `tt`, counting-stub `storage.Client`):
   - **Populate-on-miss:** first `Stat` of an absent key in a never-listed folder issues
     exactly one `ListObjectsV2` (the parent) and **zero** `HeadObject`; a second absent
     sibling → **zero** S3; a present sibling → only `HeadObject`.
   - `ReadDir` then `Stat`/`Open` of an absent sibling → zero S3; a dir child → zero S3.
   - **Local invalidation:** `Create`+`Close` / `Rename` / `Remove` / `MkdirAll` bump the
     generation so a following `Stat`/`ReadDir` re-lists and sees the change (read-after-write).
   - **TTL expiry:** advancing time past `TTL` expires the entry → re-list (inject a clock
     or a settable `now` in `ListingCache` for the test).
   - **Warmer:** `RunWarmer` re-fills accessed prefixes and evicts idle ones past `IdleTTL`.
   - **Chroot sharing:** `ReadDir` on the root then `Stat` of an absent child through a
     chroot resolves from the same cached prefix (same canonical key).
   - **Head seeding:** one listing fill seeds every file's head from the `ListObjectsV2`
     data with **zero** `HeadObject`s; `Stat` of a seeded file then does **zero** further
     `HeadObject`s (counting-stub tests disable seeding for determinism; a dedicated test
     enables it and asserts no heads are issued).
   - **Subtree caching:** one read of any `refs/` folder scans the subtree once; other
     `refs/` folders and negative lookups beneath them then do **zero** S3; a write to a
     sibling `refs/` folder re-scans (ancestor invalidation) and is visible; a subtree
     past `MaxSubtreeKeys` falls back to a delimited listing yet still returns correctly.
4. **End-to-end:** `./objgitd -bucket $BUCKET -allow-push`; clone a packed repo twice and
   confirm `objgit_s3_requests_total{operation="HeadObject"}` grows far slower than with
   `-s3-cache-ttl 0`, and watch `objgit_s3_listing_cache_hits_total` climb.
