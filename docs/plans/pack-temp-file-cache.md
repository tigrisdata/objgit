# Local temp-file pack cache

## Problem

Serving a clone runs go-git's `upload-pack`, whose delta-compression phase
(`deltaSelector.ObjectsToPack` → `Encoder.Encode`) re-reads the repository's pack
objects **thousands of times** with random access. Commit `76bb55d` made pack
reads lazy: every object access issues a fresh S3 `GetObject`. Measured on a
318-object repo (200 KB pack), serving one clone made **8,500+ GetObject calls
and climbing** — the clone never finishes within any reasonable timeout. Real
repos (kefka: 19.6 MB pack, far more objects) are effectively unservable.

The old eager reader buffered the whole pack in RAM once, so the thousands of
re-reads were in-memory and instant — at the cost of holding whole packs in RAM,
which `76bb55d` set out to avoid.

A secondary failure rode on top: among those thousands of GetObjects, some reused
stale keep-alive connections to Tigris and, with `context.TODO()` (no timeout),
hung forever. That is fixed separately by `internal/s3fs/resilient.go` (hardened
HTTP client). This plan addresses the round-trip explosion.

## Approach

Materialise each pack-directory file (`.pack`, `.idx`, `.rev`) to a **local temp
file** on first open, and serve all reads from that local file. This gives
go-git cheap repeated random access (local disk) without buffering whole packs in
RAM. The user explicitly chose this over RAM buffering, accepting the local-disk
dependency the project otherwise avoids.

Pack-dir files are immutable and content-addressed (`pack-<sha>.pack`), so a
downloaded temp file is valid for the file's whole lifetime and safe to cache by
S3 key.

## Design

`internal/s3fs/packcache.go`:

- `PackCache` — keyed by S3 object key. Each entry downloads its object once
  (`sync.Once`) into a temp file under a per-process temp dir, then records the
  local path + size. A total-bytes LRU budget evicts least-recently-opened
  entries (`os.Remove`; open readers keep working — Linux unlinked-while-open).
- `open(ctx, client, bucket, key, name)` returns a `packCachedFile` wrapping an
  **independent** `*os.File` (`os.Open` of the cached path, so each reader has its
  own seek cursor). `Close` releases the fd but keeps the cached temp file.
- `Cleanup()` removes the temp dir; `main` defers it.
- Download streams `GetObject` (full object) → temp file via `io.Copy`. NoSuchKey
  maps to `fs.ErrNotExist` (matches `newS3ReadFile`).

`packCachedFile` embeds `*os.File` (Read/ReadAt/Seek/Close/Stat) and adds billy's
`Lock`/`Unlock` (no-ops); `Write`/`WriteAt`/`Truncate` return read-only errors;
`Name` returns the logical path.

Wiring:

- `S3FS.packCache *PackCache`, propagated in `Chroot` (shared by pointer like
  `cache`). `WithPackCache(*PackCache) Option`.
- `OpenFile` `O_RDONLY`: after the temp/dir short-circuits, if `packCache != nil`
  and the key is a pack-dir file, return `packCache.open(...)` instead of
  `newS3ReadFile`.
- `main.go`: `-pack-cache-bytes` (default 2 GiB; 0 disables) and
  `-pack-cache-dir` (default `os.TempDir()`); construct `PackCache`, wire via
  `WithPackCache`, `defer Cleanup()`.

## Tests

- `packcache_test.go` (table-driven, stub `s3Client` serving in-memory bytes):
  download-once (N opens → 1 GetObject), independent seek cursors across
  concurrent readers, correct bytes via Read and ReadAt, ErrClosed after Close,
  NoSuchKey → ErrNotExist, LRU eviction frees disk while keeping an already-open
  reader valid.
- End-to-end: clone the 318-object repo and kefka through the server; assert the
  GetObject count is small (≈ pack-dir file count, not thousands) and `git fsck`
  passes.
