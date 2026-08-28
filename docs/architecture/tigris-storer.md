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
| `packed-refs`      | Every reference in one object. See "References".     |
| `refs/<name>`      | One legacy loose reference. Read-only.               |

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

## References (`packedrefs.go`, `refcache.go`)

One object holds every reference. Before this, one object held one reference.
That cost a push of N refs N `GetObject` calls and N `PutObject` calls. It also
cost every clone, fetch and push one `GetObject` per reference, because the
listing gave only key names.

Two keys hold reference state:

| Key            | Role                                                      |
| -------------- | --------------------------------------------------------- |
| `packed-refs`  | Every reference. Its ETag is the compare-and-swap token.   |
| `refs/<name>`  | One legacy loose reference. Read-only. The fold deletes it. |

`packed-refs` does not start with `refs/`, so the loose listing cannot return
it.

### Format

A 16-byte plaintext header, then a body that is raw or one zstd frame. The
header copies the shape `.cue` uses: magic `OGR`, a version byte, a body-codec
byte, and the reference count as a big-endian `uint32`.

The body is text. It holds one reference per line, sorted by name:

```
HEAD	ref: refs/heads/main
refs/heads/main	0a1b2c...
refs/tags/v1.0.0	3d4e5f...
```

The separator is a tab. `git-check-ref-format` forbids whitespace inside a
reference name, so a tab can never be ambiguous.

Sorting buys three things. Tag names that share a prefix become adjacent, so
zstd collapses them. One set of references always encodes to identical bytes,
so a test can assert bytes. A person who decompresses the body can grep it.

A value is a hex hash, or `ref: <target>` for a symbolic reference. Both come
from `encodeRefValue`, which the loose layer also uses, so the two formats
cannot drift apart. Git keeps `HEAD` loose because its own `packed-refs` cannot
hold a symbolic reference. This format can.

A corrupt object is an error, and never an empty reference set. An empty set
makes a repository look brand new, and git then accepts a force-push over the
top of it. This matches what `errBadCue` does for a pack index, and it is
stricter than the loose layer, which logs and skips one bad key. Every packed
reference shares one object, so one bad line makes all of them untrustworthy.

### Reading

`refCache` mirrors `packIndex`. It builds once per instance, and it is not
sticky on error. `repofs` calls `Scoped` once per request, so one cache serves
one request.

A build costs two calls: one `GetObject` for `packed-refs`, and one
`ListObjectsV2` for the loose layer. A 404 on the first is an empty map, and
not an error. After the fold the list returns nothing, so a build is one round
trip.

The list runs on every build. No flag records that a repository is folded. One
cheap round trip is the price of a safety net, and the net catches anything that
writes a loose reference outside this code.

### The merge rule

**A loose reference wins over a packed reference with the same name.**

That rule holds because of one invariant: a write through the packed path
deletes the loose keys for every name it touched, before it reports success. So
a loose key can exist only if something wrote it after the last packed write. It
is therefore newer.

The opposite rule is a bug. If a packed reference wins, and if a fleet runs two
releases at once, that fleet swallows every push made by the older binaries.

### Writing

`commitRefs` is the one path behind every mutator. It flushes the upload queue
once for the whole batch, which holds the rule that a reference never names an
object whose upload did not finish. Then it applies the batch to a copy of the
merged view and writes it with one conditional `PutObject`.

**That single `PutObject` is the commit point.** A fresh repository sends
`If-None-Match: *`. Every later write sends `If-Match: <etag>`. A refused
precondition drops the cache, re-reads, re-validates, re-applies, and retries,
up to `maxRefCASRetries` times.

This is also what makes `CheckAndSetReference` atomic. The compare and the write
are now one request. Earlier releases compared and then wrote, and the window
between the two raced.

Two errors come out of the retry loop, and they say different things:

- A caller expectation that fails after a rebuild is
  `storage.ErrReferenceHasChanged`. That is go-git's own error, and receive-pack
  turns it into a per-reference rejection.
- Exhausting the retries with no failed expectation is `ErrRefContention`.
  Nothing the caller asked for was violated, so a retry is reasonable.

`objgit_ref_cas_retries_total` counts the retries. Contention is the only way
this path degrades quietly: every retry rewrites the whole object, so it shows
up as push latency long before it shows up as an error.

### The fold

The fold runs once per repository. The first reference-mutating operation folds
every loose reference into its own commit `PutObject`, then deletes the loose
keys with `DeleteObjects`, 1000 keys per call.

The order is `PutObject` and then `DeleteObjects`. If the process stops between
the two, the loose keys still win under the merge rule, which is the correct
value, and the next write retries the delete. The opposite order loses a
reference outright. If the delete runs first, and if the write then fails, a
reference that existed only as a loose key is gone.

A failed delete surfaces to the caller. For a removal the reason is hard, since
a surviving loose key resurrects the reference.

`PackRefs` runs the fold on demand, so an operator can pay the cost before a
large push rather than on it. It used to be a no-op. `CountLooseRefs` now counts
only the legacy layer, so it returns 0 once folded, which is what go-git reads
the number to mean.

### Batching

`cmd/objgitd/receivepack.go` hands a whole push over through one optional
method:

```go
UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error
```

It is one flattened method and not a batch object, because Go needs an exact
signature match. A storer without the method falls back to the per-reference
path.

A push of 100,000 tags costs one `PutObject`. An advertisement costs two calls.

### Two constraints

CAUTION: This design needs a Single-region or Multi-region bucket. A Global or
Dual-region bucket reads eventually, so a compare-and-swap can evaluate against
a stale read. See
<https://www.tigrisdata.com/docs/concepts/consistency/>.

Peeled tags are a known gap. Git writes a `^<hash>` line so an advertisement
does not open every annotated tag object. This format does not, so advertising
100,000 annotated tags still reads 100,000 tag objects. Those reads go through
`packIndex` and the pack cache, so they are cheap, but they are not free. The
format can add a third tab-separated field for the peeled hash. A v1 reader
refuses such a line rather than misreading it, so adding the field is a version
bump and not a new key.

### Turning packed-ref writes on

`WithPackedRefs` is off by default. Reading packed references is not gated, so
every binary that carries this code can already read the format.

Flip the default only when every node runs a binary that can read packed
references. Two steps, in one commit:

1. In `New` (`internal/storage/tigris/tigris.go`), set the `packedRefs` field's
   initial value to `true`.
2. In `internal/storage/tigris/refcommit_test.go`, delete
   `TestPackedWritesAreGated` and add its opposite: a default storer writes
   `packed-refs` and not a loose key.

CAUTION: Do not flip the default and change the format in one release. A
rollback then has no binary that can read what the window wrote.

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
the pack for us, so this package holds no pack-parsing code.

The writer makes two passes. The first reads the hash, type, size, and delta
base of every object, then orders them so a base always precedes the delta
that needs it — `IterEncodedObjects` returns hash order, which gives no such
guarantee. The second pass copies each payload into a flat `packs/<id>.bin`
and adds one record to `packs/<id>.cue`. A record holds the hash, the type,
the payload codec, the offset, the stored length, the raw size, and the base
hash. `<id>` is the hex SHA-256 of the `.bin`, so the name doubles as a
checksum. Both files upload through the same uploader as loose objects, so the
existing `SetReference` flush covers them.

A payload is the object, or the delta the pushing client already computed for
it. Keeping that delta is what stops every later clone from deriving one
again: `Storer` implements `storer.DeltaObjectStorer`, so go-git's packer
reuses the stored delta instead of running its rolling-hash search. See
[the backend reference](../reference/tigris-backend.md#how-a-read-rebuilds-a-delta).

A delta is only kept when its base lands in the same container. When a
container seals between the two, the writer stores the whole object instead.
That costs a few deltas at each boundary and buys back the rule that every
container resolves on its own, so an interrupted push can never leave a delta
whose base never uploaded.

An object of 2 KiB or more is stored as one zstd frame when that frame is
smaller by at least 64 bytes. Smaller objects are always raw. One frame for
each object is what keeps a packed read one ranged GET. See
[the backend reference](../reference/tigris-backend.md#payload-compression)
for the size bands and the measurements behind them.

One cap bounds a container: `maxPackBytes` (128 MiB), the stored payload size.
Nothing bounds the object count.

The walk opens the next container lazily, on the next object. A push that
divides exactly by the cap therefore leaves no empty trailing container. A huge
first push becomes a series of bounded containers, instead of one enormous PUT.

An object count is a poor proxy for container size. A thousand tiny objects
make a few kilobytes. A thousand large blobs make tens of gigabytes. Container
size is what sets the size of one PUT, of one prefetched `.bin` download, and of
one pack cache eviction, so the byte cap bounds all three.

The walk checks the cap *before* it adds an object. A container at 127 MiB must
not swallow a 500 MiB blob and land at 627 MiB.

CAUTION: One container can exceed the cap. An object larger than the whole cap
gets a container to itself, because it has to live somewhere. This is the only
container that can pass the cap, and it always holds exactly one object.

**The cap is write-side only.** The read path is pack-agnostic: `packIndex`
merges every `packs/*.cue` that it lists, and every `packEntry` names its own
pack. Any number of containers, of any size, therefore resolves the same way.
`withMaxPackBytes` lowers the cap in tests.

One knock-on effect is worth knowing. A `.cue` record block grows with the
number of records, and no cap holds that number down, so the decode bound
`cueMaxDecoded` (256 MiB, see `compress.go`) is what keeps a hostile `.cue`
from exhausting memory. It admits a container whose objects average 45 bytes
apiece, which no real repository approaches.

One consequence is worth keeping. `snapshotEntries` sorts by pack and then by
offset, so a full iteration drains one container at a time. It does not hold
the download of every container open at once. That offset order is also what
makes the watermark tier work; see [Reads](#reads).

## Reads

Reads check three tiers, in this order:

1. The local staging file of an unfinished push.
2. The pack index.
3. The loose `objects/<hex>` key.

The pack index is built once for each `Storer`, from the `packs/*.cue` files.
Those files are small, and no `.bin` body is read to build it.

A packed read is one ranged `GetObject` over the record's `stored` span. There
is no delta chain to reconstruct. When the record names the zstd codec, the
read decompresses one frame; otherwise the bytes are already the object.

## The pack prefetch (`packindex.go`)

The first packed read of a container starts a download of that whole `.bin`
(`startPackFetch`). The download runs in the background. The read that started
it does not wait for it.

This is what turns a clone into roughly one large GET instead of thousands of
small ones. A container holds up to `maxPackBytes`, so a read that waited for
one stalled the clone in the middle of itself.

`packObject` therefore has four tiers, and no tier waits:

1. The local staging file of an unfinished push.
2. A finished download of the container.
3. The part of a running download that is already on disk.
4. One ranged `GetObject`.

Tier 3 is a watermark. A `.bin` streams to disk in offset order, so a read is
local as soon as the download passes the end of that object. `snapshotEntries`
hands objects out in offset order too, so a full iteration reaches this tier
early and stays on it.

A read above the watermark takes tier 4, and pays for those bytes twice. This
is the cost of never blocking, and the watermark is what keeps it small.

The download verifies the SHA-256 of the bytes against the pack id before tier
2 accepts them. Tier 3 does not, because the digest is only known at the end.
This is the same trust tier 4 gives: a ranged GET verifies no digest either.

NOTE: A failed download is sticky for that `Storer`. Later reads stay on ranged
GETs. They do not retry, and they do not fail.

`maxLivePackFetches` (4) bounds how many downloads run at once. The cap used to
exist by accident, because the read that started a download blocked on it.
`Storer.fetchSem` holds it, and `Scoped` shares that channel, so one root
`Storer` gives the whole process one budget.

CAUTION: A `Storer`'s observer callback (`WithObserver`) now fires from these
background goroutines as well as from request goroutines. It must be safe for
concurrent use. `metrics.ObserveS3` already is.

## The pack cache (`packcache.go`)

`WithPackCache` decides where the downloaded copy lands.

Without the option, each `Storer` gets a private temp file that is unlinked
but still open. The file dies with the `Storer`, so the *next* clone downloads
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

The cache also owns the watermark stream of a download it is running, on
`cacheEntry.stream`. `PackCache.partial` hands it to any caller, and not only
to the one that started the download. This is what lets a second `Storer`,
parked in the cache's singleflight wait, still read tier 3.

Three design notes are worth not deriving a second time:

- **There is no refcounting.** The cache owns the file on disk, each caller
  owns its own descriptor, and eviction is `os.Remove`. An open descriptor
  outlives the unlink, so a clone that is mid-read never breaks, and the space
  returns when the last reader closes.
- **A failed download is not cached.** The entry is dropped, so a later
  request can retry. The `packAccess.start` of each `Storer`, in
  `packindex.go`, is what prevents a retry storm inside one `Storer`.
- **The stream is cleared before the download settles.** A reader can then
  take one ranged GET in that window, which is correct. The alternative
  serves bytes out of a body that failed its checksum.

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
