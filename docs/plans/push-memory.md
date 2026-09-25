# Plan: bounded memory for large pushes

## Goal

A push of any repository, of any size, must complete in bounded memory. The
memory that one push uses must not grow with the size of the history, and must
not grow with the size of one object.

## The problem

A push of golang/go (465 MiB pack, 708,000 objects) used more than 21 GiB of
heap. On a laptop it filled swap and did not complete. A push of Xe/x (50 MB
pack, 21,000 objects) used 516 MiB.

Almost all of that memory was in go-git, and not in this repository.

`PackfileWriter` gave the pushed pack to a scratch go-git
`filesystem.Storage`. That storage indexes the pack with a `packfile.Parser`
that has no storage attached. Without a storage, the parser does not use its
low-memory mode. It keeps the inflated body of every base object and every
resolved delta until the whole pack is parsed. Memory therefore grew with the
uncompressed size of the full history.

Three smaller costs came after the parse:

- The plan pass called `IterEncodedObjects`, which loads each full object to
  read its hash, type, and size.
- Eight workers opened eight more copies of the scratch index to find delta
  bases.
- The container walk resolved each object again through the go-git packfile
  reader.

A fourth cost was in `cmd/objgitd/receivepack.go`. `writePack` ran a go-git
`Scanner` over the socket to find the end of the pack. That reader cannot
seek, so the scanner buffered the full inflated body of each object. One large
blob therefore needed its full size in memory.

## Phase 1: replace the scratch storage (done)

`internal/storage/tigris/indexpack.go` is a small version of `git index-pack`.
It does the work in two passes.

1. `scanPack` reads the pack once, as it arrives. It stages the bytes in a
   temp file and verifies the trailer checksum. For each object it keeps about
   60 bytes: the offset, the size, the type, and the base of a delta. It
   inflates each entry to find where the entry ends, but it keeps no body.
2. The resolver walks each delta tree depth first, from its base. A body lives
   only while a delta below it needs it as a base. A linear chain therefore
   holds two bodies at a time. Each object is inflated, resolved, and hashed
   once.

A body stays in memory when it is 8 MiB or less, and when all bodies in memory
stay under 64 MiB. Other bodies go to a temp directory for the push. The delta
applier reads its base through `io.ReaderAt`, so it never loads a base on disk
into memory.

`writePack` now hands the stream to the storer through an optional
`ReadPack(io.Reader) error` method. The storer finds the end of the pack in the
same pass that stages and indexes it. `packScanner` is an `io.ByteReader`, so
zlib does not read past the end of the pack. This matters on git:// and SSH,
where the client keeps the connection open after the pack.

### Results of phase 1

The numbers come from a local harness. It feeds a real pack to
`PackfileWriter` over an S3 fake that discards bodies.

| Push                         | Before                   | After                     |
| ---------------------------- | ------------------------ | ------------------------- |
| golang/go, peak heap         | more than 21 GiB         | 521 MiB                   |
| golang/go, time              | did not complete         | 23 s                      |
| Xe/x, peak heap              | 516 MiB                  | about 165 MiB             |
| Xe/x, time                   | 14.7 s                   | 1.3 s                     |
| 266 MB blob and one delta    | both bodies in memory    | 35 MiB peak heap          |

A second test reads every object back through a new `Storer` and verifies
its hash. It passes for all 708,000 objects of golang/go. It also passes for a
chain of 50 links, a chain of 197 links, and the 266 MB blob. Each case passes
with the default memory bound, and again with every body sent to disk.

The two writers store the same deltas. For Xe/x, both keep 12,016 deltas and
both store 39 MiB.

### Spill to disk by default?

A run that sends every body to disk took 140 s, against 23 s. System time went
from 5 s to 109 s. Most objects are a few kilobytes, so each one costs file
system calls that are larger than the work. Each copy command in a delta also
becomes one `pread`. The peak heap fell by only 33 MiB, because the 64 MiB
budget already bounds the bodies in memory.

## A bug that phase 1 found

A stored delta chain of exactly 50 links did not read back. `deltaBase`
refused depth 50, but depth 50 is the link that reaches the whole object. Git
uses a default `pack.depth` of 50, so normal pushes make such chains. The
writer on `main` has the same fault. The memory problem hid it, because
golang/go never got far enough.

A client can also send chains of up to 4095 links, and a read walks only 50.
Two changes correct this:

- A read now accepts a chain of exactly `maxDeltaDepth` links.
- If a stored chain is already `maxDeltaDepth` links long, the writer stores
  the next object whole. In a chain of 197 links, this stores 10 of 803 objects
  whole.

## Phase 2: make the pack index compact (next)

After phase 1, the memory that remains grows with the number of objects, at
about 390 bytes for each live object. For golang/go that is about 275 MiB.
For a repository of 10 million objects, it is about 4 GB.

| Part                           | Bytes per object | golang/go |
| ------------------------------ | ---------------: | --------: |
| `packIndex.entries` map        |         about 210 |   149 MiB |
| `cueRecord` slices of the push |         about 70 |    48 MiB |
| push scan index                |         about 50 |    35 MiB |
| zstd history (fixed)           |                – |    64 MiB |

The map is the largest part. Reads use it too: a clone or a fetch builds the
same map for every object in the repository. A smaller map therefore helps
both the push and the read path.

### Design

Replace `entries map[plumbing.Hash]packEntry` with a list of sorted tables.

- A table holds the hashes of its objects as one `[]byte`, sorted, with
  `hashSize` bytes for each hash.
- A parallel slice holds a small fixed-size record for each object: the pack
  number, the offset, the stored length, the raw size, the type, and the
  codec.
- The base of a delta is an index into the same table. A base in another
  table goes in a small side map. The writer keeps a base and its delta in the
  same container, so the side map is almost always empty.
- A table has a slice of pack ids. Each record holds a `uint32` index into
  that slice, and not a `string`.

This takes about 50 bytes for each object, against about 210 bytes now.

`packLookup` does a binary search in each table. The cold build makes one
table from every `.cue`. Each container that a push registers adds one table.
When there are more than 16 tables, the index merges them into one table, so
a lookup never searches more than 16 tables.

`deregister` removes a table that is still alone. For a table that a merge
absorbed, it adds the pack id to a set of dead packs, and a lookup skips a
record from a dead pack. `deregister` then needs only the pack id, so
`packJob` no longer holds its `[]cueRecord` until the upload completes.

`snapshotEntries` keeps its order, by pack and then by offset. It reads that
order from the tables.

`packEntry` stays as the type that `packLookup` returns. The lookup builds it
from a record, so callers do not change.

### Order of work

1. Add the table type with lookup, merge, and dead packs. Give it
   table-driven tests.
2. Move `indexRecords`, `register`, `deregister`, `packLookup`, and
   `snapshotEntries` to the tables.
3. Change `packJob` and `deregister` to use the pack id only.
4. Make the cold build parse each `.cue` into a table, and then release the
   parsed records.
5. Measure the push and a cold index build of golang/go again.

## Phase 3: work to examine later

- **Index size for very large repositories.** After phase 2, about 110 bytes
  for each object remain during a push. For 10 million objects that is about
  1.1 GB. A table can live in a memory-mapped temp file, and then the kernel
  can page it out.
- **The zstd encoder.** It keeps 64 MiB of history for each process.
  `EncodeAll` inputs are at most `inMemoryCap` (1 MiB), except `.cue` blocks.
  A 1 MiB window cuts that to about 8 MiB. This changes the compressed bytes,
  so it needs its own measurement.
- **Large bases in their own container.** An object larger than
  `maxPackBytes` gets a container of its own. A delta against it lands in the
  next container, so the writer stores that delta whole. This behavior is
  older than this plan.
