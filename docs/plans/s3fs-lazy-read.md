# Lazy, range-based reads for s3fs

## Context

Today `internal/s3fs/file.go`'s `newS3ReadFile` does a `GetObject` **and**
`ioutil.ReadAll`s the entire object body into a `*bytes.Reader` at `Open` time
(file.go:83-99). Every read handle therefore holds the whole object in RAM —
including whole pack files, which since `feat: keep pushed packs whole` can be
the entire repository. This wastes memory and forces a full download even when
the consumer reads only a few bytes (e.g. a 1-byte `FSObject` probe, or an
`.idx` fanout lookup).

The goal: opening a file in read mode should **not** download anything. A
sequential `Read` should lazily start a `GetObject` whose body is read on
demand; `ReadAt` should fetch only the bytes it needs via S3 **Range** requests,
backed by a bounded in-memory read-ahead window so the many small sequential
`ReadAt` calls go-git issues (zlib ~512B reads, idx binary-search lookups) don't
amplify into hundreds of tiny GETs.

### Access patterns this must serve (from go-git v6)

- **Loose objects**: sequential `Read` start→finish → streaming body is ideal.
- **Pack `.pack`**: `FSObject.Reader` probes with a 1-byte `ReadAt` and reopens
  the file on `os.ErrClosed`, then `io.NewSectionReader(...).ReadAt` at the
  object offset; the `Scanner` uses `Seek`+sequential `Read`. → both paths.
- **Index `.idx`**: pure `ReadAt` at arbitrary offsets (header, fanout, hash and
  offset tables). → random access, bursty-sequential.

## Approach

Rewrite `s3ReadFile` to be lazy with two independent, access-pattern-matched
mechanisms, and **drop both** the eager `GetObject`/`ReadAll` _and_ the dedicated
`HeadObject` from the read path.

### Metadata without a dedicated HeadObject

`enrichedFileInfo` only reads `ContentLength`, `LastModified`, and `Metadata`
(fileinfo.go:50-96) — all of which `s3.GetObjectOutput` also carries. So instead
of a standalone `HeadObject`, derive the metadata from whatever fetch happens
first:

- If `basic.go` supplied a cache-resident `head` (the `headInfo` path), use it
  as today — zero fetches.
- Otherwise leave `head == nil` / `size == -1` (unknown) and fetch **nothing**
  at open. The first `Read`/`ReadAt` does a `GetObject` anyway; build a synthetic
  `*s3.HeadObjectOutput` from its response (`ContentLength`, `LastModified`,
  `Metadata`, `ETag`, `ContentType`) and populate `head`/`size`.
  - For a **non-ranged** GET (sequential `Read` from offset 0), `ContentLength`
    _is_ the full size.
  - For a **ranged** GET (`ReadAt`, or `Read` after a `Seek`), `ContentLength`
    is the range length; parse the full size from `GetObjectOutput.ContentRange`
    (`bytes <start>-<end>/<total>` → `total`). Add a tiny `parseContentRange`
    helper.
- `Stat()` falls back to a lazy `HeadObject` **only** when `head` is still nil
  (a consumer stats without ever reading and the cache didn't seed it). This is
  the sole residual `HeadObject`, and it's rare.

Net effect on the uncached open→read path: **one** `GetObject` total, down from
today's `HeadObject` + full `GetObject`.

### New `s3ReadFile` shape (`internal/s3fs/file.go`)

Replace the `reader *bytes.Reader` field. New fields (one `sync.Mutex` guards
all mutable state for simplicity; reads on a single handle are effectively
serial):

```
client            s3Client
bucket, key, name string
head              *s3.HeadObjectOutput  // nil until first fetch (or cache-supplied)
size              int64          // full object size; -1 = unknown until first fetch
mu                sync.Mutex
closed            bool

// sequential streaming path (Read/Seek)
pos               int64          // logical cursor
body              io.ReadCloser  // open GetObject body, nil until first Read
bodyPos           int64          // object offset the body currently sits at

// random-access read-ahead window (ReadAt)
win               []byte         // cached chunk
winStart          int64          // object offset of win[0]
```

`newS3ReadFile`: if `head` is supplied, set `size = *head.ContentLength`;
otherwise set `head = nil`, `size = -1`. Either way return immediately — **no
GetObject, no HeadObject**.

A shared `adoptMeta(out, ranged)` helper populates `head`/`size` from a
`*s3.GetObjectOutput` the first time one comes back (no-op if `head` already
set): synthesize `s3.HeadObjectOutput{ContentLength, LastModified, Metadata, …}`;
set `size` from `ContentLength` when not ranged, else from
`parseContentRange(out.ContentRange)`.

### `Read(p)` — lazy streaming body

- Return `os.ErrClosed` if closed.
- If `size >= 0` and `pos >= size`, return `0, io.EOF`.
- If `body == nil`: issue `GetObject` with `Range: bytes=<pos>-` (omit `Range`
  when `pos == 0`), `observeS3("GetObject", …)`, `adoptMeta` from the response,
  set `body`, `bodyPos = pos`.
- Read from `body` into `p`, advance `pos` and `bodyPos` by `n`. On the body's
  `io.EOF`, return it through.

### `Seek(offset, whence)` — drop body on move

- Return `os.ErrClosed` if closed.
- Compute new absolute position (`SeekStart`/`SeekCurrent`/`SeekEnd` using
  `size`). Reject negative.
- If `body != nil` and the new position `!= pos`, **close `body` and nil it**
  (next `Read` reopens with the new `Range`). Set `pos`.

### `ReadAt(p, off)` — Range request + read-ahead window

- Return `os.ErrClosed` if closed (preserves the `FSObject` reopen contract).
- If `size >= 0` and `off >= size`, return `0, io.EOF`.
- Serve from `win` when `[off, off+len(p))` is covered by
  `[winStart, winStart+len(win))`: `copy` and return.
- On miss: fetch `chunk := max(len(p), readChunkSize)` bytes (clamped to `size`
  when known) starting at `off` via `GetObject` with
  `Range: bytes=<off>-<off+chunk-1>`, `observeS3("GetObject", …)`, `adoptMeta`
  from the response (learns `size` from `ContentRange` on the first call),
  `io.ReadFull` the body into a new `win`, set `winStart = off`, then `copy`
  into `p`.
- EOF handling **without pre-known size**: if S3 returns `416 InvalidRange`
  (`off` at/after EOF) treat as `0, io.EOF` — and the `416` response still
  carries the total via `ContentRange`, so `adoptMeta` can set `size`. If the
  satisfied length `< len(p)` because the range hit EOF, return the bytes copied
  plus `io.EOF` (honour the `io.ReaderAt` contract: short read ⇒ non-nil error).

`readChunkSize` is a package const defaulting to `1 << 20` (1 MiB). RAM per open
handle is bounded to one chunk (+ at most one streaming body buffer), not the
whole object. (No flag/Option for now — confirmed not needed.)

### `Stat()` — lazy head fallback

- If `head != nil` (cache-seeded or already adopted from a fetch), return
  `enrichedFileInfo{HeadObjectOutput: *head, …}` as today.
- If `head == nil` (nothing read yet, no cache seed), do a one-time lazy
  `HeadObject`, store it, and return it. This is the only surviving
  `HeadObject` call.

### `Close()`

- Idempotent: `ErrFileClosed` if already closed.
- Close `body` if non-nil, drop `win`, set `closed = true`. `Read`/`ReadAt`/
  `Seek` then return `os.ErrClosed` — exactly the contract `TestS3ReadFileClosed`
  and `FSObject` depend on.

### Small helpers

- Range header formatter: `bytes=start-` and `bytes=start-end`. AWS SDK
  `s3.GetObjectInput.Range` is a `*string`.
- `parseContentRange(s *string) int64`: parse the `/<total>` suffix of a
  `bytes start-end/total` header; returns -1 if absent/unparseable.
- `adoptMeta(out *s3.GetObjectOutput, ranged bool)`: build and store the
  synthetic `head` + `size` if not already set.

## Files to modify

- **`internal/s3fs/file.go`** — the rewrite above (`s3ReadFile` struct, `newS3ReadFile`,
  `Read`, `ReadAt`, `Seek`, `Close`; `Stat`/`Name`/`Write*`/`Lock` unchanged).
  `s3WriteFile`, `s3MultipartUploadFile`, `s3DirFile` untouched.
- **`internal/s3fs/file_test.go`** — `TestS3ReadFileClosed` builds
  `&s3ReadFile{name: "k", reader: bytes.NewReader(...)}`; update the literal to
  the new struct (e.g. set `closed` via `Close()` on a handle with a stub
  client / non-nil `size`) while keeping the three `os.ErrClosed` subtests.

## Things that stay the same

- The `tempReadFile` / `tempBuffer` path (tempfs.go) is a separate read path for
  in-flight temp packs — unchanged.
- The `OpenFile` resolution logic in `basic.go` (cache `resolve`, `headInfo`,
  directory probe) — unchanged; it still hands `head` (or nil) to
  `newS3ReadFile`.
- `ListingCache`, metrics observer wiring, `s3Client` interface — unchanged
  (`GetObjectInput.Range` already exists on the SDK type).

## Verification

1. `go build ./...` and `go test ./internal/s3fs/...` — unit tests, including
   the updated `TestS3ReadFileClosed`.
2. Add a table-driven test (follow the **`xe-go:go-table-driven-tests`** skill
   for structure/naming) with the existing `stubClient` (listingcache_test.go)
   extended to honour the `Range` field (return the sliced bytes and a matching
   `ContentRange`/`ContentLength`, and `416` for out-of-range), asserting:
   - open issues **no** `HeadObject` and **no** `GetObject`;
   - `Read` issues the first `GetObject` lazily, then streams; `Stat` after it
     reports the size derived from the response (no `HeadObject`);
   - `ReadAt` learns `size` from `ContentRange` and serves a second nearby
     offset from the window without a second `GetObject`;
   - `ReadAt` past EOF returns `io.EOF` (via short read / `416`);
   - `Stat` on a never-read handle triggers exactly one lazy `HeadObject`;
   - post-`Close` calls return `os.ErrClosed`.
3. `go test ./cmd/objgitd/...` — full protocol tests (clone/push over HTTP,
   git://, SSH) exercise real pack/idx reads through go-git's `FSObject` and
   `Scanner`, validating the streaming + range paths end to end (requires `git`
   on PATH).
4. Manual smoke (optional): run `./objgitd -bucket $BUCKET -allow-push`, push a
   repo, clone it back, and confirm pack/idx reads succeed while watching
   `objgit_s3_*` metrics for a sane GetObject count (read-ahead should keep it
   far below one-per-ReadAt).
