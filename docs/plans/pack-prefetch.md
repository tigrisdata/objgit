# Plan: non-blocking pack prefetch

## Context

A packed object read went through four tiers in `packObject`
(`internal/storage/tigris/packindex.go`):

1. The staging file of an in-flight push.
2. A local `.bin` that this `Storer` had already downloaded whole.
3. The read that passed `packBulkFetchThreshold` (32 distinct objects), which
   downloaded the whole container.
4. One ranged `GetObject`.

Tier 3 was the problem. `downloadPack` ran `a.once.Do(...)` inline, so the 33rd
read of a container waited for all of it. A container holds up to
`maxPackBytes` (128 MiB). Every concurrent read of the same container waited on
the same `sync.Once`. A clone therefore stopped dead in the middle of itself.

The goal: a packed read never waits for a whole-container download.

Decisions from the user:

- **The download starts on the first packed read**, and not after 32.
  `packBulkFetchThreshold` and its `seen` map are deleted.
- **Reads serve out of the file while it fills.** A `.bin` streams to disk in
  offset order, and `snapshotEntries` gives objects out in offset order, so a
  watermark makes most of the download window free.
- **Downloads get a concurrency cap.** They used to be capped by accident,
  because the read that started one waited for it.

A data race was fixed on the way. `bulkCopy` read `packAccess.f` with no
synchronization while `once.Do` wrote it.

## Design

### `packStream`

A descriptor over the file being filled, plus a committed byte count:

```go
type packStream struct {
	f *os.File
	n atomic.Int64
}

func (ps *packStream) readerFor(off, length int64) (*io.SectionReader, bool)
```

`readerFor` returns false when `off+length > ps.n.Load()`. Reads go through
`ReadAt`, which is a pread, so they are safe next to the writer that appends to
the same file.

`countingWriter` in `packcache.go` publishes `n`. It stores the total after the
underlying writer returns, so the watermark never admits a byte that is not yet
on disk.

### Two publishers

There are two, because a container can be staged in two places.

Without a `PackCache`, `openWholePack` owns a private temp file. It stores the
stream on the `packAccess`.

With one, `PackCache.fill` owns the staging file. It opens a second, read-only
descriptor on the same inode and stores the stream on the `cacheEntry`. That
descriptor survives the `os.Rename`, and `runtime.AddCleanup` closes it once
nothing holds the stream.

The stream lives on the cache entry, and not on the caller, for one reason: a
`Storer` parked in the cache's singleflight wait downloads nothing itself, but
must still read the bytes the owner has landed. `Storer.partialPack` reads
both places.

Both publishers clear the stream before the download settles. Nothing is ever
served out of a body that failed its checksum.

### `startPackFetch`

`downloadPack` becomes `startPackFetch`. It returns at once, and launches the
download in a goroutine under `packAccess.start`.

`packAccess.done` closes when the download settles. That is the happens-before
edge that makes `f` and `err` safe to read, and `bulkCopy` selects on it with a
`default` so it never waits.

Failure stays sticky for that `Storer`, exactly as `once` made it before.

The goroutine captures the `Storer`, so its `packIndex` stays reachable and the
`runtime.AddCleanup` in `fetchWholePack` cannot fire early. `s.ctx` is the
process context from `main.go`, and nothing rebinds it per request, so a
download outlives the request that started it. With a cache installed it is
still warming that cache. At shutdown it is abandoned, and not waited on.

### The cap

`Storer.fetchSem` is a channel of `maxLivePackFetches` (4), built in `New` and
shared by `Scoped`. In production `main.go` builds one root `Storer`, so the
budget is process-wide.

The closure that `streamPack` returns takes a slot, around the `GetObject` and
the `io.Copy`. This is the one place that touches the network, and with a cache
installed it runs only for the owner of a download. A caller that waits on
someone else's download therefore never holds a slot while doing nothing.

### New tier order

1. The staging file of an in-flight push.
2. A finished download.
3. `startPackFetch`, then the watermark.
4. One ranged `GetObject`.

The prefetch starts before tier 3 is read. On the first read it costs nothing,
and it gives that read a chance at a stream another request has warmed.

## Files

| Action | Path                                    | Purpose                                            |
| ------ | --------------------------------------- | -------------------------------------------------- |
| Edit   | `internal/storage/tigris/packindex.go`  | `packStream`, `packAccess`, `startPackFetch`, tiers |
| Edit   | `internal/storage/tigris/packcache.go`  | `cacheEntry.stream`, `partial`, progress plumbing   |
| Edit   | `internal/storage/tigris/tigris.go`     | `Storer.fetchSem`, observer contract                |
| Edit   | `internal/storage/tigris/iter.go`       | doc comment for the deleted threshold               |

## Tests

`fakeS3` gained a gate (`holdBinBodies`). It makes a full `.bin` body give out
a set number of bytes and then stop until the test releases it. A test can
therefore park a download at a known watermark, or wedge one open forever. It
also tracks a high-water mark of parked bodies, for the cap.

Two helpers wait: `waitPackFetch` for a settled download, and `waitWatermark`
for a byte count.

| Test                                        | What it pins                                        |
| ------------------------------------------- | --------------------------------------------------- |
| `TestPackPrefetchStartsOnFirstRead`         | One read starts one download, over a ranged GET     |
| `TestPackReadNeverBlocksOnPrefetch`         | Reads return while a download is wedged open        |
| `TestPackWatermarkServesPartialDownload`    | Below the watermark is free, above it is a GET      |
| `TestPackWatermarkThroughCacheSingleflight` | A waiter reads the owner's partial file             |
| `TestPackPrefetchConcurrencyCap`            | No more than `maxLivePackFetches` downloads at once |
| `TestPackPrefetchChecksumFailure`           | A failed download leaves no readable partial file   |
| `TestIterConvergesOnTheLocalCopy`           | One download covers a whole walk                    |

NOTE: An assertion on `nfullBinGets` must come after `waitPackFetch`. Reads do
not wait for the download, so a small fixture can finish before the goroutine
has issued its GET.

Observers in tests now need a mutex. A `Storer`'s observer fires from the
prefetch goroutine as well, so `countingObserver` in `client_test.go` replaces
the bare maps.

## Verification

```sh
go build ./...
go test ./internal/storage/tigris/...
go test -race -count=4 ./internal/storage/tigris/...
go test ./...
```

Against a real bucket:

```sh
go build -o objgitd ./cmd/objgitd
./objgitd -bucket $BUCKET -http-bind :8080 -allow-push
git push http://localhost:8080/acme/big.git main
git clone http://localhost:8080/acme/big.git
git clone http://localhost:8080/acme/big.git two
```

Run the daemon with debug logs. Confirm that ranged GETs continue while
`fetching whole pack` is open, and that they stop after `fetched whole pack`.
The second clone must download no packs.

## Out of scope

- A flag for `maxLivePackFetches`. It is a constant. A
  `-pack-prefetch-concurrency` flag can follow if tuning needs it.
- Prometheus counters for prefetch outcomes and watermark hits. `WithObserver`
  already reports every `GetObject`.
- Repacking and GC. The gap in
  [the backend reference](../reference/tigris-backend.md) is unchanged.
