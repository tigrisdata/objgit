# Replace the scratch go-git storage with a container sink

**Status:** stage 1 of 4 implemented (`internal/storage/tigris/segmentstore.go`,
9 tests, full suite green). Stages 2 to 4 not started.

Landed separately while this was being written, both from the earlier specs:
`b130832` caps the zstd encoder concurrency, `877cd16` bounds concurrent pushes.
Those change the memory baseline this plan should be measured against, so
re-baseline with `cmd/membench` before evaluating stage 3.

Supersedes `var/no-materializing-deltas.md`, whose subject
(`planObjects`) this deletes outright.

## Context

A push currently materializes objects three times over and round-trips the
whole pack through a second on-disk git repository:

```
wire -> Scanner(tee) -> dotgit.PackWriter -> temp .pack
                                          -> buildIndex -> Parser.Parse
                                               (resolve every delta, write into scratch)
packWriter.Close -> planObjects        (resolve every delta AGAIN, for hash/type/size)
                 -> resolveDeltaBases  (8 more filesystem.Storage handles over scratch)
                 -> payloadFor -> deltaForm -> Scanner.WriteObject
                                          (materialize a THIRD time, to pick delta form)
                               -> .bin/.cue
```

### Live heap

Site 1 (`Parser.Parse` -> `processDelta` -> `ensureContent`) was 67 to 79% of
live heap early in a push. Site 2 (`planObjects` -> `getMemoryObject` ->
`ApplyDelta`) was 63 to 82% of live heap at the concurrent peak. Every push
pays roughly 476 MiB of working set regardless of how warm the process is.

### Allocation churn, which is the larger story

Live heap understates site 2 badly, because the objects it materializes are
freed the moment their metadata has been read and so barely register in
`inuse_space`. Total allocation tells a different story. Measured over two
runs (13.07 GB across 3 pushes, 89.33 GB across 20), a single push of a 48 MiB
pack allocates:

**~4.4 GB per push, about 93x the size of the pack.**

By allocation site:

| Site | Share | Per push |
| --- | --- | --- |
| `bytes.growSlice` | 47.9% | 2.09 GB |
| `plumbing.(*MemoryObject).Write` | 31.3% | 1.36 GB |
| everything else | 20.8% | ~0.9 GB |

By objgit entry point, cumulative:

```
main.writePack                    11323 MB   84.6%
  packWriter.Close                10637 MB   79.5%
    packWriter.planObjects         8880 MB   66.4%
```

**`planObjects` alone is 66% of all allocation per push**, roughly 2.9 GB. It
is the metadata walk that wants only hash, type, and size.

The `MemoryObject.Write` traces also name a second contributor not visible in
the live-heap profiles: `payloadFor` -> `deltaForm` (`packwriter.go:333`) ->
`deltaFormOn` (`packwriter.go:339`) -> `Scanner.WriteObject`, which
re-materializes objects a third time while deciding whether to store each one
as a delta.

**This inverts the earlier ranking.** Judged by live heap, site 1 looked like
the bigger term and this plan looked like it addressed the smaller half. Judged
by allocation, site 2 and the delta-form probe dominate, and this plan targets
roughly two thirds of the churn. Churn is also what the GC is working against,
which is why `GOGC=50` bought 24% of peak and 67% of settled heap for under 3%
of wall clock.

The scratch storage exists to provide three things: delta resolution, random
access by hash, and after-the-fact delta-form lookup. Only the first is
irreducible.

## The load-bearing constraint

**The temp `.pack` file cannot be removed.** `parser.go:106` reads:

```go
if p.scanner.seeker == nil {
    p.lowMemoryMode = false
}
```

and with low memory mode off, `scanner.go:526` retains `oh.content` for every
object rather than re-inflating from disk on demand. Driving the parser
directly off the network reader would therefore *increase* memory, not reduce
it.

So this plan keeps a seekable temp `.pack` and removes the `filesystem.Storage`
wrapped around it. What goes away is the `.idx` build, the loose/packed object
store, `planObjects`, `resolveDeltaBases`, and the eight worker storages.

## Design

`packSegment` is already the container this needs. `newPackSegment`
(`packwriter.go:402`) opens an `os.CreateTemp` staging file, `add` appends
payloads, `seg.recs` holds one `cueRecord` per object (hash, type, codec,
offset, stored, raw, base), and `seal` checksums and uploads. That is an
append-only blob plus a hash-to-offset side index, in the format the bucket
already stores. Nothing needs inventing, and notably a tar file would be
strictly worse: 512-byte header plus block padding per entry is over 11 MB of
overhead on this repository's 21637 objects, and would still need transcoding
to `.bin` before upload.

The new flow:

```
wire -> Scanner(tee) -> temp .pack                       (unchanged, seekable)
     -> pass 1: header-only scan of the temp .pack       (delta topology)
     -> pass 2: Parser.Parse(storage: segmentStore)      (resolve once, stream out)
                  -> RawObjectWriter(typ, sz) -> packSegment -> .bin/.cue
```

### `segmentStore`

A new type implementing `storer.EncodedObjectStorer`, backed by the
`packSegment` staging files and an in-memory index.

- `RawObjectWriter(typ, sz)` returns a writer that appends through the existing
  `writePayload` band logic, hashes as it streams (exactly as `stageWriter`
  already does at `writer.go:33`), and on `Close` appends a `cueRecord` and
  records `hash -> (segment, offset, stored, raw, codec)` in the index. Seals
  and opens a new segment when the byte cap is reached.
- `EncodedObject(typ, hash)` serves the parser's REF-delta base lookups
  (`parser.go:342`) from the index plus `ReadAt` on the staging file, running
  `decodeBody` for the codec.
- `HasEncodedObject`, `EncodedObjectSize` read the index.
- `IterEncodedObjects` walks the index.
- `NewEncodedObject`, `SetEncodedObject` delegate to the existing `Storer`
  implementations.
- `AddAlternate` returns the same unsupported error the `Storer` already does.
- `LowMemoryMode() bool` returns true, so the parser keeps its low-memory path.

### Delta preservation

This is the part that actually needs care, and it is why the header-only pass
exists. The parser hands out fully resolved content only, and `RawObjectWriter`
rejects delta types outright (`writer.go:35`), so a naive sink would store every
object whole and inflate the bucket. `TestPackfileWriterKeepsDeltas` and
`TestPackfileWriterDemotesDeltaAcrossSplit` encode the current contract and must
keep passing.

Because the temp `.pack` is seekable, a header-only pass over it is cheap: seek
to each offset, parse the `ObjectHeader`, read `Type`, `OffsetReference`, and
`Reference`, and never inflate content. That yields `offset -> base offset` for
OFS deltas and `offset -> base hash` for REF deltas.

To turn offsets into hashes, register a `packfile.Observer` via
`WithScannerObservers`. `parser.go:146` and `:150` call
`OnInflatedObjectHeader(oh.Type, oh.Size, oh.Offset)` and
`OnInflatedObjectContent(oh.Hash, oh.Offset, oh.Crc32, nil)` for every object,
where `oh.Type` and `oh.Size` are the *resolved* values and `oh.Offset` is the
pack offset. Joining on offset gives `hash -> base hash`, which is precisely
what `plannedObject.base` holds today, obtained without a single delta
application.

Note the observer's `content` argument is always `nil` on this path, so
observers cannot carry object bytes. The bytes must come through
`RawObjectWriter`; this is why both mechanisms are needed rather than either
alone.

### Ordering and the containment rule

`Close` today plans the whole object set before writing, which lets `emit`
place bases before deltas and lets `inSeg` guarantee a delta's base is in the
same container. Streaming gives that up.

Decision: **keep the two-pass shape, but make pass 1 cheap.** The header-only
pass already walks every object; have it build the same `order` slice
`planObjects` produces today, from headers alone. Pass 2 then drives the parser
and writes in that order. This preserves `emit`, `inSeg`, and the split-demotion
behaviour exactly, at the cost of the parser resolving in its own order rather
than ours.

If that ordering mismatch proves awkward, the fallback is to accept pack order
and demote any delta whose base did not land in the same segment, which is
already the documented behaviour when a split strands a delta.

### Base read-back across a sealed segment

If a late REF delta references an object in a segment that already sealed and
uploaded, `EncodedObject` must still find it. Decision: **keep every staging
file until the push completes**, then delete. Disk, not memory, and it matches
the current lifetime of the scratch directory.

## What this does not fix

Site 1 remains. `ensureContent` (`parser.go:236`) inflates each delta into a
pooled buffer and patches it into another, whatever the sink is. It is
inherent: a git object's hash is over its resolved content, so every delta must
be reconstructed at least once. The floor is one resolved object plus its base
at a time.

Expected recovery is site 2 in full, the delta-form probe in full, the `.idx`
build, and the eight worker storages. Against the churn measurement that is
about two thirds of the 4.4 GB a push allocates today. Against live heap it is
the larger of the two terms at the concurrent peak but a less dramatic figure.

**Gate the result on `alloc_space`, not `inuse_space`.** Site 2's objects are
short-lived by construction, so an `inuse_space` comparison will understate
this change by a wide margin. `cmd/membench` writes `allocs-final.pb.gz` for
exactly this; divide its total by the number of pushes in the run.

## Staging

Each stage is independently testable and leaves the tree working.

1. **DONE. `segmentStore` in isolation** (`internal/storage/tigris/segmentstore.go`).
   The type, its index, and its `EncodedObjectStorer` methods, driven directly
   by tests. The live write path still uses scratch, so nothing user-visible
   changed. Notes from building it:
   - `RawObjectWriter` stages each object to its own temp file while hashing,
     then hands it to the existing `packSegment.add` as a `stagedObject`. The
     staging hop is what keeps the object out of memory in one piece; reusing
     `add` is what keeps the container format decisions in one place.
   - Staging files stay open for the life of the store, so a base written into
     a segment that filled early is still readable when its delta arrives.
   - An index miss falls through to the backing `Storer`, which is the thin-pack
     path; a miss in both surfaces as `plumbing.ErrObjectNotFound`.
   - Duplicate objects within one pack are stored once.
2. **The header-only pass.** Topology extraction and `order` construction from
   the temp `.pack`, tested against the same fixtures `planObjects` is tested
   with, asserting identical output.
3. **Wire it up.** `packWriter.Close` drives `Parser.Parse` with `segmentStore`
   and the observer instead of the scratch storage. This is the stage where the
   existing pack tests are the gate.
4. **Delete the dead code.** `planObjects`, `resolveDeltaBases`, `payloadFor`,
   `deltaScanWorkers`, and the scratch `filesystem.Storage`.

## Risks

- **Format correctness is not caught by a memory benchmark.** A wrong `raw` or
  `typ` in a cue record writes a repository that pushes fine and reads corrupt
  later. Stage 2 must assert byte-identical `cueRecord` output against the
  current implementation before stage 3 switches anything over.
- **Thin packs.** `parser.go:342` falls back to `storage.EncodedObject` for a
  REF delta base not in the pack. Against `segmentStore` that is an index miss;
  it must then fall through to the real `Storer` and hence the bucket. The
  current scratch storage has no alternates, so this path may not behave
  identically today, and the difference needs establishing rather than assuming.
- **Failure atomicity.** A failed push today discards a scratch directory and
  the bucket is untouched. Streaming into segments means partial containers can
  exist. `packSegment.discard()` exists; the invariant that nothing seals until
  the parse completes must be explicit.
- **SHA-256.** `writePack` threads `packfile.WithSHA256()` from the config
  extension. Both new passes need the matching object ID size or they will
  mis-parse every header.
- **go-git v6 is alpha.** This couples objgit to `Parser`, `Scanner`,
  `ObjectHeader`, and `Observer`. All are exported, none are stable.
