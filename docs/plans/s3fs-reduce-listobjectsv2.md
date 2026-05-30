# Reduce ListObjectsV2 volume in objgitd

## Context

objgitd issues a high volume of `ListObjectsV2` calls against Tigris. The
codebase already has a sophisticated listing cache (`internal/s3fs/listingcache.go`)
with a **subtree-scan** optimization meant to collapse per-folder listings into a
single recursive scan — but investigation shows that optimization is **effectively
dead in any real deployment**, and a background warmer multiplies the remaining
waste. This plan fixes the root cause so the subtree optimization actually
engages.

## Root-cause findings (from reading the code)

**1. Subtree caching never matches chrooted repos — the big one.**
Every repo is served through a `Chroot`: both `loadOrInit` (`cmd/objgitd/git_protocol.go:192`)
and go-git's `FilesystemLoader.Load` (`plumbing/transport/loader.go:46`) chroot the
filesystem to the repo path. So canonical S3 keys are `<repo>.git/refs/...` and
`<repo>.git/objects/...`.

But the recursive-root match is anchored at the bucket root:

```go
// listingcache.go:290
func (c *ListingCache) recursiveRoot(prefix string) (string, bool) {
    for _, r := range c.roots { // default {"refs/"}
        if prefix == r || strings.HasPrefix(prefix, r) { ... }
    }
}
```

`strings.HasPrefix("myrepo.git/refs/heads/", "refs/")` is **false**. So the
default `-s3-cache-recursive-prefixes refs/` matches nothing once a repo lives at
`myrepo.git/…`, and **every** listing falls to delimited per-folder mode
(`listFolder`).

**2. Loose-object negative lookups storm `objects/<xx>/`.**
go-git runs non-exclusive, so `hasObject` returns nil without listing
(`dotgit.go:646`) and `Object`/`ObjectStat` go straight to
`fs.Open`/`fs.Stat("objects/<xx>/<38hex>")` for any object not already in the
in-memory LRU (`dotgit.go:753,771`). Because objgitd keeps packs whole (no loose
objects), each distinct two-hex prefix probed → `resolve` → `cache.list("…/objects/<xx>/")`
→ one delimited `ListObjectsV2` returning empty. Up to **256 list calls per clone**,
none of which the (dead) subtree cache can absorb.

**3. The warmer multiplies it.**
`list()` calls `touch(prefix)` for every listed prefix, so `seen` accumulates all
~256 `objects/<xx>/` prefixes plus the refs folders. With `-s3-cache-refresh 30s`
and `-s3-cache-idle 10m`, `RunWarmer` re-lists every seen prefix every 30s for up
to 10 minutes — on the order of **thousands of background `ListObjectsV2` calls
following a single clone**, even with no further traffic. (`warmOnce` _does_
dedupe recursive prefixes to their root — but since no prefix matches a recursive
root today, nothing is deduped.)

## Fix: register each chroot repo root as a recursive subtree root

The architecture already gives us the right unit: **a chroot == a repo**. Make
each `Chroot` register its own root with the listing cache as a recursive subtree
prefix. Then one delimiter-less `ListObjectsV2` over `myrepo.git/` serves the
entire repo (refs, `objects/pack/` listing, HEAD, config, packed-refs, _and_ every
loose-object negative lookup) from a single scan — for objgitd's whole-pack repos
that subtree is a dozen-ish objects.

This single change:

- revives the subtree optimization (currently dead) for the real chrooted layout;
- extends it to `objects/`, eliminating the ~256 negative-lookup list calls/clone;
- collapses the warmer from ~260 lists/tick/repo to **1** (warmOnce already dedupes
  to the root).

`MaxSubtreeKeys` (default 50000) still guards pathological repos: an oversized
subtree is marked `Truncated` and the code falls back to delimited per-folder
listing automatically (`list()` at `listingcache.go:206`).

### Changes

**`internal/s3fs/listingcache.go`** — make `roots` mutable and add a registrar:

- Change `roots []string` to a concurrency-safe snapshot. Simplest:
  `roots atomic.Pointer[[]string]` (kept longest-first), or a `sync.RWMutex`
  guarding the slice. `recursiveRoot` is hot and read-only, so an atomic snapshot
  read is preferred.
- Add `func (c *ListingCache) registerRoot(root string)`: normalize to a trailing
  `/`, no-op if already present or if `c == nil`, otherwise insert and re-sort
  longest-first (reuse the logic in `normalizeRoots`, `listingcache.go:331`).
- `recursiveRoot` reads the snapshot instead of the immutable field. No logic
  change — once `myrepo.git/` is a root it returns it for any prefix beneath it.

**`internal/s3fs/chroot.go`** — register the new root on chroot:

- After building `nfs` (`chroot.go:21`), if `fs3.cache != nil && p != ""` call
  `fs3.cache.registerRoot(p + "/")`. `p` is the joined canonical root
  (e.g. `myrepo.git`); the cache wants prefixes ending in `/`.

No change needed in `cmd/objgitd` — both load paths chroot already, so registration
happens automatically the first time a repo's storer is built.

### Notes / decisions

- **Keep the `-s3-cache-recursive-prefixes refs/` default.** It is now redundant
  for chrooted repos but harmless, and still works for a bucket-root single-repo
  layout or as an explicit override.
- **Whole-repo root vs. per-subdir (`objects/`+`refs/`).** Whole-repo is simpler
  and strictly fewer scans (1 vs 2). Invalidation blast radius is fine: `invalidate`
  already walks to the bucket root (`listingcache.go:472`), so any push/ref update
  invalidates the repo subtree and the next read re-scans once.
- **Optional hardening (only if a giant-repo regression shows up):** have `warmOnce`
  skip a root whose last subtree result was `Truncated`, so the warmer doesn't
  re-scan up to `MaxSubtreeKeys` every tick for a pathological repo. Not part of the
  core fix; objgitd's whole-pack repos never hit this.

## Verification

1. `go build ./...` and `go test ./internal/s3fs/...`.
2. **New table-driven test** in `internal/s3fs/listingcache_test.go` using the
   existing `stubClient` (has an atomic `lists` counter and honors delimiter/
   pagination, `listingcache_test.go:25-96`). Pattern after `TestListingCacheChrootShares`
   (`listingcache_test.go:461`):
   - Seed the stub with `myrepo.git/refs/heads/main`, `myrepo.git/objects/pack/pack-X.pack`,
     `myrepo.git/objects/pack/pack-X.idx`, `myrepo.git/HEAD`.
   - `Chroot("myrepo.git")`, then drive the loose-object access pattern: `Stat`
     several non-existent `objects/<xx>/<hash>` paths across different `xx`,
     `ReadDir("objects/pack")`, `ReadDir("refs/heads")`.
   - Assert `stub.lists` is **1** (one subtree scan), versus the pre-fix behavior of
     one list per distinct `objects/<xx>/` prefix + one per refs folder.
   - Add a write (`Create`+`Rename` a new pack, or invalidate) and assert exactly
     one additional scan on the next read (re-scan after invalidation).
   - Use `xe-go:go-table-driven-tests` conventions (see MEMORY: tests here follow
     that skill).
3. **Manual end-to-end** against a real bucket: run `./objgitd -bucket $BUCKET
-http-bind :8080 -allow-push`, push a repo, then `git clone` it twice. Watch
   `objgit_s3_requests_total{api="ListObjectsV2"}` on `/metrics` (or the
   listing-cache hit/miss log emitted from `main.go:111`): the per-clone
   `ListObjectsV2` count should drop from order-of-hundreds to a small constant,
   and the steady-state warmer rate should drop to ~1 per repo per refresh tick.
