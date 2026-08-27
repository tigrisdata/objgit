# `internal/storage/tigris` — a go-git `storage.Storer` on one Tigris bucket

This package implements `storage.Storer` for Tigris. It speaks
`storage.Storer` straight to the bucket. It does not use the
[s3fs](s3fs.md) filesystem layer.

This is the repository storage that `cmd/objgitd` runs on. `main.go` builds
one `*Storer` over the single `-bucket`. `internal/repofs.BucketResolver` then
calls `Storer.Scoped(ref.Path())` for each request, so every repository gets
its own key prefix inside that one bucket. There is no `billy.Filesystem`, and
there is no bucket or keypair for each repository.

The `.cue` binary layout and the failure modes are specced in
[../reference/tigris-backend.md](../reference/tigris-backend.md).

## Scoping

`Scoped(prefix)` returns a cheap, independent `*Storer`. It shares the client,
the bucket, the object format, and the observer, but it roots every key under
`prefix + "/"`.

Scoping nests: `s.Scoped("a").Scoped("b")` gives the prefix `"a/b/"`. Scoping
with `""` does nothing. The layout below is unprefixed only when no `Scoped`
call has run.

## Layout

| Key                | Contents                                            |
| ------------------ | --------------------------------------------------- |
| `objects/<hex>`    | One loose git object.                               |
| `packs/<id>.bin`   | One flat container of full objects.                 |
| `packs/<id>.cue`   | The record index for one container.                 |
| `refs/<name>`      | Reference text. A symbolic ref reads `ref: target`. |

Shallow marks, the worktree index, and the repository configuration sit at
root-level keys. They carry the same prefix as everything else.

Each loose object stores its type and size in user metadata, as `git-type` and
`git-size`. `EncodedObject` reads both off the `GetObject` response, so a
fetch costs one round trip and not a HEAD plus a GET. `headInfo` is the
HEAD-only path, behind `HasEncodedObject` and `EncodedObjectSize`, for callers
that need no body.

Writes go through a local staging file. That file hashes the content and names
the key. When the claimed hash disagrees with the recomputed hash, the package
refuses the write.

`storerFor` in `cmd/objgitd` treats a repository as existing once it has a
`HEAD` reference. `git.Init` always sets `HEAD`, so a missing `HEAD` is the
create-on-first-push signal. This is the same role that the "config file
present" check of dotgit played when repository storage was a
`billy.Filesystem`.

## Writes are asynchronous (`upload.go`)

`RawObjectWriter.Close` and `PackfileWriter.Close` stage bytes locally and
hand off to an `internal/bundler` queue. They do not block on `PutObject`.

Every `Storer` owns its own uploader and pack index. This is true for the
`Storer` from `New` and for each `Scoped` descendant. The backlog or the
failure of one repository can therefore never block or poison another.

`SetReference` and `CheckAndSetReference` call `flush()` first. `flush()`
waits for every queued upload and surfaces the first error. A reference
therefore never goes live while it points at an object whose upload failed or
is still in flight.

`newUploader` sets two bundler limits above their defaults.

`HandlerLimit` is 8, and the default is 1. The bundler cuts a bundle every 10
items, and at every pack container. With the default, one bundle runs at a
time: `handleBundle` uploads a bundle's jobs together, but the next bundle
waits for all of them. A large push then spends its time on those barriers and
not on bandwidth. 8 bundles overlap instead. Each job owns its own staging file
and its own key, so no job needs another to finish first.

`BufferedByteLimit` is 4 GiB, and the default is 1 GiB. This budget is what
`AddWait` blocks on, so it bounds the bytes that one push stages ahead of its
uploads.

NOTE: This budget is local disk, and not memory. Staged bytes live in temp
files, and `run()` streams each file to `PutObject`. The 1 GiB default reads as
a memory figure and is too tight here: 8 concurrent 128 MiB containers fill all
of it, which leaves the pack walk no room to run ahead.

`enqueue` gives the bundler a size for each job. The bundler holds that size
against `BufferedByteLimit`. `sizeHint` clamps the size to that limit.

CAUTION: The clamp is not a nicety. The bundler takes the size from a semaphore
whose capacity *is* `BufferedByteLimit`, and `x/sync/semaphore` parks a size
larger than the whole capacity until the context is done. An unclamped
oversized job therefore hangs its push. The byte cap keeps every ordinary
container far under the limit, but a lone object above the cap can still make a
container that no limit holds.

CAUTION: A staged object that is not yet uploaded must stay readable through
the same `Storer`. The delta-base resolution of go-git reads back objects that
the same push wrote moments earlier, long before any flush. Without the
pending overlay
in `objects.go`, a real push fails with `apply delta patch: object not found`.
This is why `registerPending` runs *before* `enqueue`, and why
`EncodedObject` falls through to S3 only on `os.ErrNotExist`.

CAUTION: Local staging paths must never come from content hashes. Two
unrelated pushes can produce identical bytes, such as a shared empty tree, an
empty blob, or a common `.gitignore`. A content-addressed temp path then lets
the cleanup of one push delete a file that the in-flight upload of another
push still holds open. `looseJob` and the pack writer both use `os.CreateTemp`
names for this reason.

## Packs (`packwriter.go`, `packindex.go`)

The package implements `storer.PackfileWriter`. A push therefore costs two
PUTs for each container it fills, and not one PUT for each object. The two
caps below set how much one container holds.

It does not store the git pack format. Git packs use delta compression, so one
read would have to resolve a chain of deltas.

`PackfileWriter` instead writes the incoming pack to a scratch
`storage/filesystem.Storage` on a local temp directory. That storage decodes
the pack and resolves every delta for us. `IterEncodedObjects` then hands back
full objects and never deltas, so this package holds no pack-parsing code.

The walk copies each object into a flat `packs/<id>.bin`, and one record into
`packs/<id>.cue`. A record holds the hash, the type, the offset, and the
length. `<id>` is the hex SHA-256 of the `.bin`, so the name doubles as a
checksum. Both files upload through the same uploader as loose objects, so the
existing `SetReference` flush covers them.

Two caps bound one container, and the walk seals on the first one it reaches:

- `maxPackObjects` (1<<15), the object count.
- `maxPackBytes` (128 MiB), the payload size.

The walk opens the next container lazily, on the next object. A push that
divides exactly by a cap therefore leaves no empty trailing container. A huge
first push becomes a series of bounded containers, instead of one enormous PUT.

An object count alone is a poor proxy for container size. 32768 tiny objects
make a few megabytes. 32768 large blobs make tens of gigabytes. Container size
is what sets the size of one PUT, of one bulk `.bin` download, and of one pack
cache eviction, so the byte cap bounds all three.

The two caps differ in where they run. The walk checks the byte cap *before* it
adds an object, and the object cap after. A container at 127 MiB must not
swallow a 500 MiB blob and land at 627 MiB.

CAUTION: One container can exceed the byte cap. An object larger than the whole
cap gets a container to itself, because it has to live somewhere. This is the
only container that can pass the cap, and it always holds exactly one object.

**Both caps are write-side only.** The read path is pack-agnostic: `packIndex`
merges every `packs/*.cue` that it lists, and every `packEntry` names its own
pack. Any number of containers, of any size, therefore resolves the same way.
`withMaxPackObjects` and `withMaxPackBytes` lower the caps in tests.

One consequence is worth keeping. `snapshotEntries` sorts by pack and then by
offset, so a full iteration drains one container at a time. It does not hold
the bulk download of every container open at once.

## Reads

Reads check three tiers, in this order:

1. The local staging file of an unfinished push.
2. The pack index.
3. The loose `objects/<hex>` key.

The pack index is built once for each `Storer`, from the `packs/*.cue` files.
Those files are small, and no `.bin` body is read to build it.

A packed read is one ranged `GetObject`. The payload is raw, so there is
nothing to reconstruct.

After one `Storer` reads more than 32 **distinct** objects from one pack
(`packBulkFetchThreshold`), it downloads that whole `.bin` once. It then
serves every later read locally. This is what turns a clone into roughly one
large GET instead of thousands of small ones.

## The pack cache (`packcache.go`)

`WithPackCache` decides where that bulk-downloaded copy lands.

Without the option, each `Storer` gets a private temp file that is unlinked
but still open. The file dies with the request, so the *next* clone downloads
the pack again.

With the option, one process-wide `*PackCache` keeps whole `.bin` files in a
local directory, under a byte budget, and evicts the least recently used
entry. Every `Storer` shares that one cache.

`main.go` builds the cache from two flags. `-pack-cache-dir` names the parent
directory, and an empty value uses the OS temp directory.
`-pack-cache-bytes` sets the disk budget. Its default is 2 GiB, and `0`
disables caching.

The cache is the one piece of `Storer` state that `Scoped` shares on purpose,
instead of replacing. Entries are keyed by pack id, which is the SHA-256 of
the bytes of the `.bin` itself. A hit across two repositories is therefore
byte-identical content, which makes it deduplication and not leakage. The
cache also re-verifies that digest while the download streams to disk.

Two design notes are worth not deriving a second time:

- **There is no refcounting.** The cache owns the file on disk, each caller
  owns its own descriptor, and eviction is `os.Remove`. An open descriptor
  outlives the unlink, so a clone that is mid-read never breaks, and the space
  returns when the last reader closes.
- **A failed download is not cached.** The entry is dropped, so a later
  request can retry. The `packAccess.once` of each `Storer`, in
  `packindex.go`, is what prevents a retry storm inside one request.

## Testing seams

Tests can use a fake S3 client, through the `s3API` interface. That interface
is shaped like the `s3Client` of s3fs.

The `WithObserver` option adds the metrics seam, in the same way that s3fs
does. The `main` package can therefore wire both packages to
`metrics.ObserveS3`.

## Remaining gap

The build order in the reference document tracks this. There is no repacking
and no GC, so packs accumulate. Each push leaves at least one pack, and a push
above either cap leaves more.
