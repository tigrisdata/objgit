# Stream pack downloads: serve reads while downloading (don't block clones)

## Context

Commit `e114543` added `internal/s3fs/packcache.go` to fix clone hangs: immutable
pack-directory files (`.pack`/`.idx`/`.rev`) are now downloaded **whole** to a
local temp file on first open and served from disk, collapsing thousands of S3
`GetObject` round-trips into one. That fixed the hang, but introduced a new
latency wall: `PackCache.open` blocks inside `e.once.Do` → `download` →
`io.Copy(tmp, out.Body)` until the **entire** object lands on disk. No byte of
the pack can be read until the last byte arrives, and every concurrent opener of
the same key serializes behind that single `sync.Once`. For a multi-hundred-MB
pack this is dead wait before upload-pack can even start walking the pack header.

The goal: **overlap the download with reads.** Stream the S3 body into the temp
file, advancing a watermark as bytes land, and hand callers a reader
_immediately_. Reads of already-downloaded byte ranges return at once; reads
ahead of the watermark block only until that specific range arrives (the S3 body
is a single sequential stream, so the watermark is monotonic and any requested
offset below `size` is eventually satisfiable). This turns "wait for the whole
pack, then serve" into "serve as it streams."

Per the design decisions taken up front:

- **RAM bound = bounded trailing window ("free read prefix").** We write-through
  to the temp file as we download, so every byte below the watermark is already
  durable on disk. The in-RAM buffer therefore only needs to hold a fixed-size
  _trailing window_ `[n-ringCap, n)`; older bytes are dropped from RAM and served
  from disk on demand. Peak RAM ≈ `inflight_entries × ringCap`, independent of
  pack size. (Reads that fall below the window — e.g. a backward seek before the
  download finishes — re-read from the disk fd, which is correct and cheap via
  the page cache.)
- **Eviction preserves unlink-while-open.** Eviction `os.Remove`s the temp path
  even while readers (and the in-flight writer) hold it open; they survive via
  their already-open fds on the unlinked inode. To make this work with streaming
  we give the entry **one shared read fd** opened at creation (before any unlink)
  rather than `os.Open`-per-reader, plus a refcount so the fds are closed only
  once the last reader is gone.

## Current shape (what changes)

All in `internal/s3fs/packcache.go`. The entry/open/download trio is rewritten;
`isPackCacheable`, `NewPackCache`, `Cleanup`, and the `WithPackCache`/`basic.go`
wiring stay as-is. `basic.go:77-84` still calls `fs3.packCache.open(...)` and
gets back a `billy.File`.

Today:

- `packEntry` = `{key, once, path, size, err, used}` — `once` runs the full
  blocking `download`.
- `download` = `GetObject` + `io.Copy(tmp, body)` + `tmp.Close()`, fully
  synchronous.
- `open` blocks in `once.Do`, then `os.Open(e.path)` per reader → `packCachedFile`
  embedding an independent `*os.File`.
- `evictLocked` unlinks victim temp files; open readers survive via their own fds.

## Design

### 1. Rework `packEntry` into a streaming entry

```go
type packEntry struct {
    key  string
    once sync.Once // guards the header GetObject + pump launch only

    mu   sync.Mutex
    cond *sync.Cond // broadcast as n advances, and when done/err flips

    wfd  *os.File // write side: pump appends sequentially
    rfd  *os.File // shared read side: readers ReadAt at offsets < n (survives unlink)
    path string

    win      []byte // trailing RAM window; win covers [winStart, n)
    winStart int64
    n        int64 // bytes downloaded+written so far (monotonic watermark)
    size     int64 // total, from Content-Length (-1 if unknown)
    done     bool  // body fully drained, success
    err      error // terminal error (header or body)

    used    uint64 // LRU, set on each open
    refs    int    // live reader handles
    evicted bool   // path unlinked; close fds when refs hits 0
}
```

`ringCap` is a package const (start ~4 MiB; small enough to bound RAM, large
enough to absorb the read-ahead the scanner does). Optionally surface as a flag
later — not required for v1.

### 2. `open`: launch the pump once, return immediately

```go
func (c *PackCache) open(ctx, client, bucket, key, name) (billy.File, error) {
    // find/create entry under c.mu (unchanged)
    e.once.Do(func() { c.start(ctx, client, bucket, key, e) })
    if e.err != nil { return nil, e.err } // header GetObject failed (e.g. not-found)
    // refs++, seq/used update under c.mu
    return &packCachedFile{e: e, name: name}, nil // NO blocking download
}
```

`c.start` does the **header** fetch synchronously (so not-found / auth errors
surface to the caller exactly as today, and `size` is known before the first
read), then hands the body to a background goroutine:

```go
func (c *PackCache) start(ctx, client, bucket, key, e) {
    out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket:&bucket, Key:&key})
    observeS3("GetObject", start, err)
    if err != nil { /* notFound→fs.ErrNotExist; drop entry from map; e.err=... */ return }
    tmp, _ := os.CreateTemp(c.dir, "obj-")
    rfd, _ := os.Open(tmp.Name())          // shared read fd, opened before any unlink
    e.wfd, e.rfd, e.path = tmp, rfd, tmp.Name()
    if out.ContentLength != nil { e.size = *out.ContentLength } else { e.size = -1 }
    c.mu.Lock(); c.curBytes += max(e.size,0); c.evictLocked(key); c.mu.Unlock() // reserve budget
    go c.pump(e, out.Body)
}
```

Note the `GetObject` runs **outside** `c.mu` (inside `once.Do`), so concurrent
opens of _different_ keys never serialize on the global lock; only the brief
header fetch for the _same_ key is serialized (subsequent openers see `once`
already done and attach instantly).

### 3. `pump`: write-through + advance watermark + trim RAM window

```go
func (c *PackCache) pump(e *packEntry, body io.ReadCloser) {
    defer body.Close()
    chunk := make([]byte, 256<<10)
    for {
        m, rerr := body.Read(chunk)
        if m > 0 {
            if _, werr := e.wfd.Write(chunk[:m]); werr != nil { e.fail(werr); return }
            e.mu.Lock()
            e.win = append(e.win, chunk[:m]...)
            e.n  += int64(m)
            if int64(len(e.win)) > ringCap {            // drop prefix now safe on disk
                drop := int64(len(e.win)) - ringCap
                e.win = e.win[drop:]; e.winStart += drop
            }
            e.cond.Broadcast()
            e.mu.Unlock()
        }
        if rerr == io.EOF { break }
        if rerr != nil { e.fail(rerr); return }
    }
    e.wfd.Close()
    e.mu.Lock()
    e.done = true; e.size = e.n; e.win = nil; e.winStart = e.n // free RAM; disk has it all
    e.cond.Broadcast(); e.mu.Unlock()
    // if evicted && refs==0 during streaming, close rfd + remove (orphan cleanup)
}
```

`e.fail(err)` sets `e.err`, broadcasts, closes `wfd`, removes the temp file, and
drops the entry from the map so a later open re-downloads (mirrors today's
failed-download cleanup). Write-through means **every byte `< n` is on disk**, so
`win` is purely a hot cache; trimming its prefix never loses data.

### 4. Reader: `packCachedFile` becomes a streaming view over the entry

Keep the type name `packCachedFile` (minimizes churn in `basic.go` and the
existing tests, which call `.Read`/`.ReadAt`/`.Seek`/`.Name`/`.Close`). It no
longer embeds `*os.File`; it holds `*packEntry` + its own cursor:

```go
type packCachedFile struct {
    e      *packEntry
    name   string
    pos    int64
    closed bool
}
```

Core read primitive — `readAt` blocks only until the requested range is
available:

```go
func (f *packCachedFile) ReadAt(p []byte, off int64) (int, error) {
    if f.closed { return 0, os.ErrClosed }       // FSObject reopen contract (see CLAUDE.md)
    return f.e.readAt(p, off)
}

func (e *packEntry) readAt(p []byte, off int64) (int, error) {
    if off < 0 { return 0, errNegativeOffset }
    e.mu.Lock()
    for {
        if e.err != nil { err := e.err; e.mu.Unlock(); return 0, err }
        if e.size >= 0 && off >= e.size { e.mu.Unlock(); return 0, io.EOF }
        if off+int64(len(p)) <= e.n {            // full range available
            // serve from RAM window if covered, else from disk fd
            if off >= e.winStart {
                n := copy(p, e.win[off-e.winStart:])
                e.mu.Unlock(); return n, nil
            }
            rfd := e.rfd; e.mu.Unlock()
            return rfd.ReadAt(p, off)             // below window → disk (page cache)
        }
        if e.done {                               // range extends past EOF
            rfd := e.rfd; e.mu.Unlock()
            return rfd.ReadAt(p, off)             // returns (partial, io.EOF) correctly
        }
        e.cond.Wait()                             // ahead of watermark → wait for more
    }
}
```

`Read`/`Seek` reuse `readAt` against `f.pos` (Seek tracks pos; `SeekEnd` waits
for `size` to be known — it is set at header time, so no real wait). `Write*`,
`Truncate`, `Lock`/`Unlock`, `Name`, `Stat` mirror the current
`packCachedFile`; `Stat().Size()` returns `e.size`.

`Close`:

```go
func (f *packCachedFile) Close() error {
    if f.closed { return ErrFileClosed-or-nil-per-current }
    f.closed = true
    f.e.cache.release(f.e) // refs--; if evicted && refs==0 → close rfd, (wfd already closed)
    return nil
}
```

### 5. Eviction with refcount + unlink-while-open

`evictLocked` picks the least-recently-used victim (skipping `keep` and entries
with no `path` yet), then:

- `os.Remove(victim.path)` — unlink; live `wfd`/`rfd`/reader fds keep working on
  the inode (Linux).
- `victim.evicted = true`, `curBytes -= victim.size`, delete from `entries` map
  (new opens re-download).
- If `victim.refs == 0 && victim.done`, close `rfd` now; otherwise the last
  `release` (or `pump` finishing) closes it. The shared `rfd` is what makes
  unlink-while-open work for readers that haven't issued a disk read yet —
  there's no per-reader `os.Open(path)` that could race the unlink.

`Cleanup` additionally closes any open `wfd`/`rfd` before `os.RemoveAll(dir)`.

## Files to modify

- `internal/s3fs/packcache.go` — the whole rewrite above (entry, `open`,
  `start`, `pump`, `readAt`, reader methods, `evictLocked`, refcount/`release`,
  `Cleanup`). Add `ringCap` const and `errNegativeOffset`.
- `internal/s3fs/packcache_test.go` — keep existing tests passing (they use a
  synchronous full-bytes body, which still works: the pump drains it before the
  first read in practice, and reads block-then-serve regardless). Add a
  controllable body to exercise the streaming path (see Verification).
- No changes needed in `basic.go`/`chroot.go`/`filesystem.go`/`main.go`:
  `open` still returns a `billy.File`; `WithPackCache`, flags, and Chroot
  sharing are untouched.

Reuse: the read-while-write blocking pattern is the same idea already proven in
`internal/s3fs/tempfs.go` (`tempBuffer.readAt` returns `io.EOF` past the end for
the _write_ path); here the read path **blocks on a cond** instead of returning
EOF, because go-git's `packfile.FSObject`/scanner on the clone path expects
`ReadAt` to fill its buffer, not retry. The `observeS3("GetObject", …)` metric
hook stays at the header fetch. The FSObject `os.ErrClosed` contract
(`internal/s3fs/file.go`, documented in CLAUDE.md) is preserved by the `closed`
guard returning `os.ErrClosed` from `Read`/`ReadAt`/`Seek`.

## Verification

1. `go test ./internal/s3fs/...` — existing `TestPackCache*` must stay green
   (downloads-once, independent cursors, closed-read-fails, missing-object,
   eviction, `isPackCacheable`).
2. New table-driven tests (follow `xe-go:go-table-driven-tests`) in
   `packcache_test.go` using a **gated body** — an `io.ReadCloser` that releases
   bytes only when the test signals (channel/`sync.Cond`), wrapping the
   `packStub` pattern:
   - **serve-while-downloading:** open returns before the body is fully
     released; `ReadAt` at a low offset returns once that prefix is released,
     _before_ the tail is; assert it does not wait for EOF.
   - **read ahead of watermark blocks then unblocks:** `ReadAt` for an offset
     past the released watermark blocks; releasing more bytes unblocks it with
     correct data (run the read in a goroutine, assert it's pending, release,
     assert completion).
   - **below-window reads hit disk:** with `ringCap` set small in the test,
     release > ringCap bytes, then `ReadAt` at offset 0 (now trimmed from RAM)
     returns correct bytes from the disk fd.
   - **error mid-stream:** body returns a non-EOF error after N bytes; in-flight
     and subsequent `ReadAt`s past N return that error; a later open re-downloads.
   - **header not-found:** unchanged — `open` returns `fs.ErrNotExist`
     synchronously (no goroutine launched).
   - **eviction unlink-while-open:** existing test semantics, plus a variant
     evicting an entry that is _still streaming_ and confirming the holding
     reader finishes correctly.
   - **os.ErrClosed after Close** during streaming.
   - **concurrent readers** of one streaming key all see identical full bytes;
     `GetObject` count == 1.
3. End-to-end clone against a real bucket (manual): build
   `go build -o objgitd ./cmd/objgitd`, run with `-pack-cache-bytes` set, and
   `git clone http://localhost:8080/<repo>.git` of a repo with a large pack;
   confirm the clone makes progress (objects counting) while the pack is still
   downloading rather than stalling until it completes, and that a second
   concurrent clone of the same repo benefits from the shared in-flight entry.
   Watch `objgit_s3_requests_total{operation="GetObject"}` stays ~one per
   pack-dir file.
4. `go test ./cmd/objgitd/...` — protocol tests still pass (clone/fetch over
   HTTP, git://, SSH) with `git` on PATH.
