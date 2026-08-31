# Compression for pack containers in `internal/storage/tigris`

## Context

`objgitd` does not store git's own pack format. `PackfileWriter`
(`internal/storage/tigris/packwriter.go:30`) decodes an incoming packfile
through a scratch go-git storage, then re-encodes every object **fully
resolved and completely uncompressed** into its own `packs/<id>.bin` +
`packs/<id>.cue` container pair. That choice bought a very cheap read path: a
packed read is one ranged `GetObject` straight into a byte range, with no
delta chain to resolve.

It also means the bucket holds every blob at full size. Git would have stored
the same content zlib-compressed. Every byte of that difference is paid twice:
once in stored bytes, and again in egress on every clone that bulk-downloads a
`.bin`. `docs/reference/tigris-backend.md:371` already lists this as unbuilt
roadmap item 7.

The `.cue` sheet is where the fix has to live. `cueRecord.length` currently
does double duty — it is both the byte span in the `.bin` and the git object
size — and the comment at `packindex.go:66` says so outright: "`== the git
object's size, since .bin payloads are raw`". Splitting that one field into
*stored span* and *raw size* is the whole enabling change.

Intended outcome, measured against this repository: **2.40x on stored pack
bytes** (3,297,625 → 1,376,080), recovering 58.3% of what containers hold
today. For reference, compressing with no floor at all would reach 2.52x — the
2 KiB floor gives up 0.12x of ratio to skip 62% of the objects. The
sub-threshold
ranged-`GetObject` read tier keeps working exactly as it does now, and
existing v1 containers in live buckets stay readable forever.

## Decisions already made

- **Codec: zstd** (`github.com/klauspost/compress/zstd`). Already pinned at
  v1.18.0 in `go.sum` transitively, so the direct `require` costs no new
  module resolution.
- **Granularity: one zstd frame per object.** The ranged-GET tier survives
  unchanged.
- **Objects smaller than 2 KiB are never compressed** — see the measured
  floor below.
- **Never store compressed unless it wins.** Decided exactly (compress and
  compare) for anything that fits in memory, and by a 64 KiB probe above that.
- Records stay in **hash order**. Sorting by `.bin` offset would let the
  offset column be dropped entirely (running sum of stored lengths), but
  sorted-hash prefix compression is worth about the same, and hash order
  avoids baking "the `.bin` is a tight concatenation" into a durable format.

## The floor, measured

Every object in this repository (901 objects, 3.3 MB), one zstd level-3 frame
each:

| band | n | raw | zstd | ratio | objects that shrink |
| --- | --- | --- | --- | --- | --- |
| <512B | 342 | 59,757 | 56,064 | **1.07x** | 143 / 342 |
| 512B–2K | 213 | 201,466 | 137,184 | 1.47x | 213 / 213 |
| 2K–8K | 228 | 1,165,860 | 459,811 | 2.54x | 228 / 228 |
| 8K–32K | 115 | 1,472,502 | 561,729 | 2.62x | 115 / 115 |
| 32K–64K | 1 | 55,453 | 16,709 | 3.32x | 1 / 1 |
| ≥64K | 2 | 342,587 | 76,608 | 4.47x | 2 / 2 |

Cumulative bytes recovered by floor choice: **64 KiB → 8.1%**, 32 KiB → 9.2%,
8 KiB → 36.9%, **2 KiB → 58.3%**, 512 B → 60.2%, no floor → 60.4%.

By type: trees **1.11x** (hash-dominated, nearly incompressible), commits
1.49x, blobs 2.70x.

Constants, tunable:

- `compressionFloor` — **2 KiB**, sitting on the knee. Below it there is
  almost nothing left (2 KiB → 512 B buys 1.9 more points; 512 B → 0 buys
  0.2), and `<512B` is the only actively bad band: 1.07x, with fewer than half
  its objects even coming out smaller. 2 KiB still skips 555 of 901 objects
  with no probe work, keeping the small-object write path allocation-free.
- `inMemoryCap` — **1 MiB**. At or below it, compress and compare exactly;
  above it, use the 64 KiB probe. Bounds the wasted encode at ~2 ms and
  confines the rewind path to large objects only.
- `minGainBytes` — require the compressed form to be **smaller by at least
  64 bytes**. A zstd frame header plus checksum is ~13–17 bytes; this keeps a
  trivial win from buying a codec branch. No percentage margin: at these
  sizes decode is ~2–4 µs against a 10–50 ms ranged GET, so decode cost is not
  a reason to refuse a real win.

Trees could additionally be skipped by type — the cue record already carries
it — but they are only 3.5% of bytes here, so that is a CPU optimization, not
a storage one. The 2 KiB floor already excludes most of them. Not in scope.

## 1. Cue format v2 — `packindex.go`

Header stays 16 bytes, so a reader parses it once and then branches on the
version byte:

```
off  len  field                     v1             v2
0    4    magic                     "OGC" + 0x01   "OGC" + 0x02
4    1    hash width                same           same
5    1    record-block codec        (reserved, 0)  0 = raw, 1 = zstd
6    2    reserved, must be zero    same           same
8    8    record count (N)          same           same
16   ...  record block              fixed-width    columnar, maybe zstd
```

Record block, **columnar** rather than interleaved. `N` comes from the
header, so every column boundary is computable:

```
[N * hashLen]  hashes   (hash order, as v1)
[N]            types
[N]            flags     bit 0..3 = payload codec (0 raw, 1 zstd); rest reserved
[N * 8]        offsets   big-endian uint64, byte offset in .bin
[N * 8]        stored    big-endian uint64, span to range-GET
[N * 8]        raw       big-endian uint64, git object size
```

`recWidth: hashLen+17 → hashLen+26`.

Columnar matters: interleaved records put 20–32 bytes of hash entropy every
26+ bytes and starve zstd's match finder. Split into columns, the
incompressible noise is quarantined in the hash column and the other five are
nearly free — types take ~4 distinct values, flags 2, and the three uint64
columns have all-zero high bytes. For SHA-1 at a full 32768-record container,
v1 is 1.21 MB and v2 lands around 740 KB, of which ~640 KB is the hash column
and cannot be helped much.

Apply the same win-or-don't rule to the block itself: encode columnar, try
`EncodeAll`, keep whichever is smaller, set header byte 5 to match.

### Struct changes

- `cueRecord`: rename `length` → `raw`, add `stored int64` and `codec uint8`.
- `packEntry`: same rename, same additions.

Rename rather than keep `length`, because its doc comment is now actively
wrong and "used the wrong length for a byte range" is precisely the bug class
a stale name causes. Churn across `pack_test.go` is mechanical.

### `parseCue` / `encodeCue`

- `encodeCue` always emits v2.
- `parseCue` branches on the magic's 4th byte: **1** → existing fixed-width
  path, every record getting `codec=raw` and `stored == raw`; **2** → new
  path. Both return the same `[]cueRecord`, so `ensurePacksBuilt`
  (`packindex.go:297`) and `packIndex.register` (`:185`) need no changes at
  all.
- v2 length validation replaces v1's `want != len(raw)`: after
  decompression, require `len(block) == N*(hashLen+26)`.

## 2. Write path — `packwriter.go`

`packSegment` (`:154`) grows a `codec` decision per object. `add` (`:178`)
tracks **two** counters where it tracked one: `rawN` (bytes read out of the
object) and `storedN` (bytes written into the `.bin`).

Three bands, by `obj.Size()`:

```
< 2 KiB          raw. No probe, no buffer, no allocation — copied straight
                 through exactly as today. 62% of objects take this path.

2 KiB .. 1 MiB   exact. Read it, EncodeAll, keep whichever is smaller
                 (needs >= minGainBytes). Never mispredicts, so
                 stored <= raw holds by construction with no rewind.

> 1 MiB          probe. EncodeAll the first 64 KiB; if it wins, stream the
                 whole object through an encoder, else copy raw.
```

The middle band is where nearly every object lives, and making it exact rather
than predictive is what keeps the rewind below out of the common path.

Three things that matter more than they look:

**The integrity check stays on the raw side.** `add`'s existing
`n != obj.Size()` check (`:189`) becomes `rawN != obj.Size()`. That check is
what makes `obj.Size()` trustworthy everywhere else; comparing compressed
bytes against a declared object size would silently destroy it.

**The 128 MiB container cap keeps working unchanged.** `packwriter.go:113`
compares `seg.offset + obj.Size()` against the cap. `seg.offset` is now
stored bytes while `obj.Size()` is raw — apples to oranges, except the
win-or-don't policy makes `obj.Size()` a valid *upper bound*, so the
expression stays correct and merely seals slightly early. Update the comment;
leave the logic.

**Handle probe misprediction in the >1 MiB band only, so `stored <= raw` is
exact everywhere.** 64 KiB of text followed by 100 MiB of random bytes passes
the probe and then inflates, breaking the invariant the cap leans on. Fix: if
`storedN >= rawN` at end-of-object, truncate the `.bin` back to `g.offset`,
reopen `obj.Reader()`, and copy raw. The two lower bands cannot reach this
code — one never compresses and the other decides exactly.

That rewind requires moving the sha256 out of `add` and into `seal` (`:206`)
as one pass over the finished staging file — a rewind poisons a running hash.
`seal` already closes the file, so it reopens and hashes there. Cost is one
extra sequential read of a page-cached file, off the network path. Gain is an
exact guarantee that compression never costs space.

`sum` still covers the **stored** bytes, so the pack id remains the checksum
of exactly what gets uploaded, and `verifiedCopy` (`packcache.go:255`) plus
`fetchWholePack` (`packindex.go:380`) need no changes.

## 3. Read path — `packindex.go`, `objects.go`

`length` → `raw` keeps meaning the git size, so `objHead.size`
(`objects.go:38`), `decodeBody` (`objects.go:222`), `EncodedObjectSize`, and
every caller outside `packindex.go` are untouched.

All four tiers in `packObject` (`packindex.go:468`) change one word —
`e.length` → `e.stored` — and route through one new helper:

| Tier | Line | Change |
| --- | --- | --- |
| staged local `.bin` | `:485` | `SectionReader(f, e.offset, e.stored)` |
| already-downloaded bulk copy | `:494` | same |
| bulk-download trigger | `:500` | same |
| ranged GET | `:511` | `bytes=%d-%d` computed over `e.stored` |

New helper `decodePacked(h, hs, codec, r)`: when `codec == zstd`, read the
stored bytes and `DecodeAll` before handing off to the existing `decodeBody`.
`decodeBody` already does `io.ReadAll` into a `MemoryObject`, so there is no
reason for streaming decoders on the read side.

**One shared `*zstd.Decoder` and `*zstd.Encoder` per package, package-level.**
`DecodeAll` and `EncodeAll` are both safe for concurrent use; constructing a
decoder per read is the expensive mistake here.

`binSize` (`:229`) becomes `max(offset+stored)`, which is the `.bin`'s true
length, so the mismatch check at `:389` keeps working. `snapshotEntries`
(`:240`) sorts by `.bin` offset and is unaffected.

Two knock-on effects that get a doc line, not a code change:
`packBulkFetchThreshold = 32` was tuned against uncompressed ranged GETs and
is now conservative; `PackCache` fits more packs per disk budget but pays
decode on every read.

## 4. Rollout — v2 is a one-way door

An old binary hits the `bytes.Equal` magic check (`packindex.go:97`) and
hard-fails on v2. That is the correct direction — better than misreading —
but it means **rolling back a deploy loses access to every pack written in
the window.**

So gate writing, not reading. Reader support for both versions is
unconditional. v2 *writing* goes behind:

- `WithPackCompression(bool)` Option in `tigris.go`, alongside
  `WithPackCache` (`tigris.go:124`); field on `Storer`, copied by `Scoped`
  (`:191`) like `maxPack`/`maxPackBytes`.
- `-pack-compression` flag in `cmd/objgitd/main.go` next to
  `-pack-cache-bytes` (`:47`), default **true**, `PACK_COMPRESSION` via
  flagenv per the kebab-case convention.

Deploy once with it off for a release if you want the safe two-phase
rollout. No migration pass and no rewrite of existing packs; repacking
(roadmap item 5) converts old containers over time.

## 5. Metrics — `internal/metrics/metrics.go`

Add a `promauto.NewCounterVec` for codec decisions labelled `raw`/`zstd`, and
a histogram of `stored/raw` ratio. The sniff policy's hit rate is exactly
what you need to see in production before tuning `minCompressionGain`. Follow
the existing vector + thin-helper shape (`ObserveS3`, `:99`).

## Files to modify

| Path | Change |
| --- | --- |
| `internal/storage/tigris/packindex.go` | cue v2 codec, `parseCue` version branch, `cueRecord`/`packEntry` fields, four read tiers, `decodePacked`, `binSize` |
| `internal/storage/tigris/packwriter.go` | sniff + two counters in `add`, rewind on misprediction, sha256 moves to `seal`, cap comment |
| `internal/storage/tigris/tigris.go` | `WithPackCompression`, `Storer` field, package doc layout comment (`:15`) |
| `internal/storage/tigris/objects.go` | `e.length` → `e.raw` at `:38` |
| `cmd/objgitd/main.go` | `-pack-compression` flag, wire the Option |
| `internal/metrics/metrics.go` | codec counter + ratio histogram |
| `internal/storage/tigris/pack_test.go` | see below |
| `docs/reference/tigris-backend.md` | cue layout table (`:151`), payload description (`:41`, `:50`, `:72`), retire build-order item 7 (`:371`) |
| `docs/architecture/tigris-storer.md` | "payload is raw" claims at `:117`, `:173` |

## Tests — `internal/storage/tigris/pack_test.go`

Reuse the existing helpers rather than writing new scaffolding:
`sortedRecs` (`:25`), `tamper` (`:110`), `buildPackFixture` (`:166`),
`buildSizedPackFixture` (`:535`), `writePack` (`:254`), `packContents`
(`:358`), `assertContainers` (`:389`), `blobHashes` (`:892`).

- **`TestCueRoundTrip` (`:32`)** — add a `codec` dimension to the table:
  mixed codecs in one cue, zero records, both hash widths.
- **`TestCueParseRejectsCorruption` (`:78`)** — the existing
  `{"non-zero reserved", tamper(valid, 5, 1)}` case **must move off byte 5**,
  which v2 repurposes as the record-block codec. Retarget it at byte 6 or 7.
  Add: unknown version byte, corrupt zstd block, decompressed length
  disagreeing with `N`.
- **A literal v1 byte fixture that must still parse.** The back-compat
  guarantee cannot be tested through an encoder that no longer emits v1, so
  this needs hand-written bytes.
- **Write-path cases**, built with `buildSizedPackFixture` (`:535`) since it
  already takes explicit sizes. One case per band boundary:
  - highly compressible blob **just under 2 KiB → `codec=raw`**, and the same
    content just over → `codec=zstd`. The floor boundary, and the case most
    likely to regress if someone later "improves" the threshold.
  - incompressible random blob over 2 KiB → `codec=raw`, `stored == raw`,
    proving the exact compare-and-keep-smaller rule
  - a blob whose compressed form saves fewer than `minGainBytes` → `codec=raw`
  - zero-size object; and a sub-512-byte tree, which must never be compressed
  - **over 1 MiB**, compressible → `codec=zstd` via the probe
  - **over 1 MiB**, compressible head with incompressible tail → exercises the
    truncate-and-recopy rewind and asserts `stored <= raw`
- **Existing tier tests** (`TestPackBulkDownloadThreshold` `:903`,
  `TestPackBulkDownloadVerifies` `:968`, `TestIterUsesBulkDownload` `:1023`,
  `TestPackPendingVisibleBeforeUpload` `:730`) — parameterize by codec so
  every tier is proven against both.
- **`TestPackfileWriterSplitsAtByteLimit` (`:561`)** must still hold with
  compression on: assert no container's `.bin` exceeds the cap.
- Assert the pack id still equals sha256 of the uploaded `.bin` bytes.

## Verification

```sh
go build ./...
go test ./internal/storage/tigris/...          # unit + format tests
go test ./cmd/objgitd/...                      # protocol tests; needs git on PATH
go test ./...                                  # full suite
go vet ./...
```

End-to-end against a real bucket:

```sh
OBJGIT_TIGRIS_LIVE_BUCKET=$BUCKET go test -run TestLiveBucket ./internal/storage/tigris/...

go build -o objgitd ./cmd/objgitd
./objgitd -bucket $BUCKET -http-bind :8080 -allow-push
# in another shell: push a real repo with compressible history, then re-clone
git push http://localhost:8080/probe.git main
git clone http://localhost:8080/probe.git /tmp/probe-rt
git -C /tmp/probe-rt fsck            # objects must verify byte for byte
```

Then confirm the win and the back-compat path:

1. `t3 ls s3://$BUCKET/probe.git/packs/` — compare `.bin` and `.cue` sizes
   against the same push made with `-pack-compression=false`.
2. Push with `-pack-compression=false`, restart with it **on**, push again,
   then clone. One bucket now holds a v1 and a v2 container and the clone must
   resolve objects out of both.
3. Watch `/metrics` for the codec counter and ratio histogram. On a source
   repository expect roughly a 60/40 split by count — most trees and commits
   fall under the 2 KiB floor and land `raw`, while source blobs land `zstd`
   at ~2.5x. A repo of PNGs or video should be `raw` even well above the
   floor, decided by the compare rather than the floor.
4. Sanity-check the aggregate against the numbers in **The floor, measured**:
   summing `stored` vs `raw` across a pushed repo's cues should land near
   2.4x for source-heavy content. A result near 1.0x means the floor or the
   gain check is misconfigured.
