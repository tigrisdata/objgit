# Plan: bin/cue pack containers for `internal/storage/tigris`

**Status:** Implemented. This document records the design and the reasons
for it. The format itself is specced in
[`docs/reference/tigris-backend.md`](../reference/tigris-backend.md).

## The problem

`internal/storage/tigris` did not implement `storer.PackfileWriter`. So
`cmd/objgitd/receivepack.go`'s `writePack` always took its fallback path,
`packfile.UpdateObjectStorage`, which writes one object at a time. A push
of 20,000 objects sent 20,000 `PutObject` requests.

An earlier change made those writes asynchronous. That change removed the
wait, but not the request count.

## Options that we did not take

**Store the git packfile as it arrives.** The write is free, because the
push already has this format. The read is not. Git packs use delta
compression, so a read must resolve a chain of deltas against remote
data. This needs ranged reads plus delta patching, or a download and an
unpack step for each pack.

**Use an erofs or squashfs image.** This gives random access. It also
needs image-building tools (`mkfs.erofs`, `squashfs-tools`) and a Go
reader that can work over ranged remote reads. That is a large dependency
for this problem.

## The design

A flat container, like the `.bin` and `.cue` pair of a CD image:

- `packs/<id>.bin` holds the raw bytes of every object, one after
  another. No compression. No deltas.
- `packs/<id>.cue` holds a fixed-width index, sorted by hash. One record
  gives the hash, the type, the offset, and the length.

The payload is raw, so a read is one ranged `GetObject`. Nothing needs
reconstruction. Delta resolution happens one time, at write time.

`<id>` is the hex SHA-256 of the `.bin`. The name is a checksum, so a
reader can verify a whole download almost for free.

### Delta resolution without a pack parser

`PackfileWriter` writes the incoming pack to a scratch
`storage/filesystem.Storage` on a local temporary directory. That storer
decodes the pack. Its `IterEncodedObjects(AnyObject)` returns only full
objects, never `OFSDeltaObject` or `REFDeltaObject`, at any delta depth.
The walk then copies each object into the `.bin`.

This is the load-bearing fact of the whole design. It is verified against
go-git v6.0.0-alpha.4: `storage/filesystem/object.go` reads through
`getFromPackfile(h, canBeDelta=false)`, and
`plumbing/format/packfile/packfile.go`'s `getMemoryObject` resolves each
base and sets the type of the base on the result. Deltas reach a caller
only through the separate `DeltaObject` method, which nothing here calls.

The result: this package contains no pack-parsing code.

### The read path

A read looks in three places, in this order:

1. The local staging file of a push that has not finished its upload.
2. The pack index.
3. The loose key `objects/<hex>`.

Tier 3 stays because `SetEncodedObject` can run outside a push, and
because objects written before this change are still loose.

Each `Storer` builds its pack index one time, from `packs/*.cue`. A
packed read is one ranged GET. After a `Storer` reads more than 32
different objects from one pack, it downloads that whole `.bin` one time
and serves all later reads from the local copy. A clone therefore costs
about one large GET.

The threshold counts different objects, not reads. Repeated reads of a
few objects never start a download.

### Uploads share one queue

`packJob` and `looseJob` both satisfy one `uploadJob` interface and ride
the same `internal/bundler` queue. One `flush()` therefore waits for both
kinds and reports either kind of failure. `refs.go` needed no change: its
existing `SetReference` flush already covers packs.

`run` uploads the `.bin` before the `.cue`. A cold index build trusts
every `.cue` that it lists, so a `.cue` must never appear without its
`.bin`.

## Faults found during the work

**Content-addressed staging paths are unsafe.** The loose-object path
staged bytes at a path derived from the content hash. Two unrelated
pushes can hold identical content — an empty tree, an empty blob, a
common `.gitignore` — so one push's cleanup could delete a file that
another push's upload still held open. Both paths now use
`os.CreateTemp` names. The pending map carries the path.

**In-memory registration is mandatory, not an optimization.** On git://
and SSH, one connection is one `Storer`. Ref advertisement can build the
cold pack index before the push writes its pack. A post-receive hook then
reads the new objects through that same `Storer`. Without registration at
`Close`, those reads consult a stale index and fail.

## Known gaps

- No repacking or garbage collection. Packs collect forever: one for each
  push, and one more for each `maxPackObjects` (32768) objects in a push
  above that size. A cold index build costs one small GET for each pack.
  Compaction must also delete a `.bin` that has no `.cue`, which a failed
  upload can leave behind.
- ~~No pack cache across requests.~~ Done. One process now holds one
  least-recently-used cache of whole `.bin` files, under a disk budget,
  shared by every `Storer`. See `packcache.go` and the reference doc's
  [pack cache section](../reference/tigris-backend.md#the-pack-cache).
- A true git-push-to-tigris end-to-end test needs `fakeS3` in a shared
  package. Today `TestPackfileWriterRoundTrip` covers the same path with
  real pack bytes from the `git` CLI.

## Verification

```bash
go build ./... && go vet ./... && go test ./...
go test -race ./internal/storage/tigris/...
go test -run 'TestCue|TestPack' ./internal/storage/tigris/
OBJGIT_TIGRIS_LIVE_BUCKET=<bucket> go test -run TestLiveBucket ./internal/storage/tigris/
```

The live-bucket tests skip without that variable. They pin two behaviors
that the fake client can only approximate: that a single-range
`GetObject` returns the exact slice, and that a whole-object `GetObject`
returns bytes whose SHA-256 matches the pack name.

For a manual check, push a repository of a few thousand objects. The
bucket must then hold a matched `.bin` and `.cue` for each container the
push wrote, and `objects/` must stay almost empty. Clone the result back
and run `git fsck`. To see the write cap split a small repository, lower
`maxPackObjects` before you build.
