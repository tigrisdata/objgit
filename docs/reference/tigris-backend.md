# A Tigris backend for go-git object storage

This document gives a design for `storer.EncodedObjectStorer` backed by
Tigris, the S3-compatible object store from Fly.io. It records the
background facts, the layout decisions, and the build order.

## How go-git knows the hash of an object

`plumbing/storer/object.go` defines the contract. `SetEncodedObject` saves
an object and returns a hash. Three facts drive the backend design:

- The storer never computes a hash on its own. Every implementation asks
  the object itself (`storage/memory/storage.go`, `storage/filesystem/object.go`).
- `plumbing.MemoryObject` computes the hash the first time `Hash()` runs,
  then caches it. It computes only when the content length equals the
  declared size. Before that, it returns `ZeroHash`
  (`plumbing/memory.go`).
- The hash covers the header `<type> <size>\0` plus the raw content.
  It uses SHA-1 by default and SHA-256 when the repository selects the
  sha256 object format (`plumbing/hasher.go`).

Two more details matter for correctness:

- `Hash()` caches its result. Changes to type or content after the first
  call do not change the cached hash.
- The loose-object writer on disk computes its own hash during the
  stream and names the file with it (`storage/filesystem/dotgit/writers.go`).
  Nothing checks that this matches `obj.Hash()`.

CAUTION: The standard storers trust `obj.Hash()` and store without any
verification. A wrong claim puts bytes under a false address, and every
later read returns wrong data. This backend recomputes the hash while the
bytes copy, and it rejects a mismatch before any upload.

## Layout in the bucket

One bucket holds everything. One S3 object holds one git object.

```
objects/<hex>      loose object, keyed by its content hash
packs/<id>.bin     up to 32768 objects or 128 MiB, raw bytes concatenated
packs/<id>.cue     that pack's index: hash, type, offset, length
```

Layout decisions:

- Flat keys, no `<2-char>` fanout. The fanout on disk exists to keep
  directories small. Object storage keys behave like one sorted hash map,
  so fanout buys nothing here.
- Raw, uncompressed payload. Simple read path: wrap the bytes in a
  `plumbing.MemoryObject`. A zlib variant is possible later for lower
  egress and storage cost.
- User metadata carries the object type and size, under the keys
  `git-type` and `git-size`. With this, `HasEncodedObject`,
  `EncodedObjectSize`, and type-filtered iteration run on HEAD requests
  alone, with no body download.

Writes need no locking or dedup logic. Identical bytes hash to identical
keys, so concurrent writers of the same object write identical data to
the same key, and any winner suffices. Conditional writes such as
`If-None-Match: *` are safe to use but not required for correctness.

## The pack container

A push arrives as one git packfile. This package does not store that
format. Git packs use delta compression, so one read must resolve a
chain of deltas. This package stores a flat container instead. One read
is then one ranged `GetObject`, with no reconstruction step.

A container is two objects:

- `packs/<id>.bin` holds the raw bytes of every object, one after
  another. The bytes carry no compression and no deltas.
- `packs/<id>.cue` holds the index. One record gives the hash, the type,
  the offset, and the length of one object.

A push writes one container, or more than one. Two limits bound one
container, and both live in `packindex.go`:

- `maxPackObjects`, 32768 objects.
- `maxPackBytes`, 128 MiB of payload.

The writer seals a container when it reaches the first of the two. A
push above either limit writes more containers. An object count alone is
a poor proxy for size: 32768 tiny objects make a few megabytes, and
32768 large blobs make tens of gigabytes.

Both limits apply to writes only. Reads accept a repository with any
number of containers, and a container of any size.

The `<id>` is the hex SHA-256 of the content of the `.bin`. The name is
therefore a checksum. A reader that downloads a whole `.bin` can verify
the bytes against the name.

### The write path for packs

`PackfileWriter` writes the incoming pack to a scratch
`storage/filesystem.Storage` on a local temporary directory. That storer
decodes the pack and resolves every delta. `IterEncodedObjects` then
returns full objects, never deltas. This package writes no pack-parsing
code of its own.

The writer copies each object into the `.bin` and adds one record to the
`.cue`. It then deletes the scratch directory. The two files upload
asynchronously. `SetReference` waits for those uploads, so a ref never
points to a pack that the bucket does not hold.

The writer seals the current container when that container reaches
either limit. It then opens the next container for the next object. It
opens a container only when an object needs one, so a push that divides
exactly by a limit writes no empty container. Each sealed container is
complete on its own: it has its own id, its own `.cue`, and its own
upload.

The two limits differ in where the writer checks them. It checks the
byte limit before it adds an object, and the object count after. A
container at 127 MiB must not swallow a 500 MiB blob and land at 627
MiB.

An object larger than 128 MiB gets a container to itself, because it has
to live somewhere. That container is larger than the limit. It is the
only container that can be, and it always holds exactly one object.

CAUTION: `enqueue` in `upload.go` clamps the size it gives the bundler
to the `BufferedByteLimit` of that bundler. The bundler takes the size
from a semaphore whose capacity *is* that limit, and
`x/sync/semaphore` parks a size above the whole capacity until the
context is done. Without the clamp, a container above the limit hangs
its push. Only the lone-object container of the paragraph above can
reach that size.

An error part way through a large push leaves the sealed containers in
the bucket. Each one holds real objects and needs no repair. The error
fails the push, so no ref points into the incomplete set.

### The `.cue` binary format

All integers are big-endian. The header is 16 bytes:

| Offset | Size | Field                                                |
| ------ | ---- | ---------------------------------------------------- |
| 0      | 4    | Magic `OGC\x01`. The last byte is the format version |
| 4      | 1    | Hash width: 20 for SHA-1, 32 for SHA-256             |
| 5      | 3    | Reserved. Must be zero                               |
| 8      | 8    | Record count                                         |

One record follows for each object. A record is `hash width + 17` bytes:

| Offset         | Size       | Field                                        |
| -------------- | ---------- | -------------------------------------------- |
| 0              | hash width | Object hash, raw bytes                       |
| hash width     | 1          | Object type: 1 commit, 2 tree, 3 blob, 4 tag |
| hash width + 1 | 8          | Offset into the `.bin`                       |
| hash width + 9 | 8          | Length of the object                         |

Records are sorted by hash. The length field is also the size of the git
object, because the payload is raw.

The parser rejects a bad magic, a hash width that disagrees with the
repository, a non-zero reserved field, or a length that disagrees with
the record count. A damaged index gives an error. It never looks like a
missing object.

### The read path for packs

A read looks in three places, in this order:

1. The local staging file of a push that has not finished its upload.
2. The pack index, when the object is in a pack.
3. The loose key `objects/<hex>`.

Each `Storer` builds its pack index one time. The build lists
`packs/*.cue` and reads each one. These files are small. The build reads
no `.bin` body.

A read of a packed object sends one ranged `GetObject` into the `.bin`.
After a `Storer` reads more than 32 different objects from one pack, it
downloads the whole `.bin` one time. All later reads of that pack use
the local copy. A clone therefore costs about one large GET for each
pack, and not one GET for each object.

Iteration returns the packed objects one pack at a time, in offset
order. A push above the write limit spans several packs, and this order
keeps one whole-pack copy open at a time instead of one for each pack.

The code verifies the SHA-256 of the copy against the pack name before
it trusts the bytes.

### The pack cache

One process holds one pack cache. The cache is a local directory that
keeps whole `.bin` files under a disk budget. Every `Storer` in the
process shares it, so a second clone of the same repository downloads
nothing.

The cache keys each file by pack id. That id is the SHA-256 of the
content, so two repositories that ask for one id ask for the same bytes.
A shared file is therefore deduplication and not leakage. The cache also
verifies the digest while the download streams to disk. A hit can only
be the bytes that the caller named.

The cache owns the file on disk. Each caller owns a separate file
descriptor. To evict a file, the cache unlinks it. An open descriptor
outlives the unlink, so a clone that reads a pack does not fail when the
cache reclaims that pack. The disk space returns when the last reader
closes. This is also why the code holds no reference counts: the kernel
holds them.

Eviction is least-recently-used. A read of a cached pack makes that pack
recent again. The cache admits a pack that is larger than the whole
budget, because the read must succeed.

Two more properties are deliberate:

- Many concurrent readers cause one download. The cache publishes an
  entry before the download starts, so a second caller waits instead of
  starting a second download.
- The cache does not keep a failed download. The entry goes away, so a
  later request can try again. The `once` guard in `packindex.go` stops
  a retry storm inside one request.

Without a cache, a `Storer` puts its copy in a temporary file that the
code unlinks at once, and the copy dies with the request.

The daemon builds the cache from two flags. `-pack-cache-dir` gives the
parent directory, and defaults to the OS temporary directory.
`-pack-cache-bytes` gives the budget, and defaults to 2 GB. A budget of
`0` disables the cache.

## Interface map

| Interface method     | Backend operation                                                            |
| -------------------- | ---------------------------------------------------------------------------- |
| `NewEncodedObject`   | Builds a `plumbing.MemoryObject` in memory                                   |
| `RawObjectWriter`    | Stages a temp file. Uploads on `Close`, keyed by the computed hash           |
| `SetEncodedObject`   | Copies to a staged temp file. Verifies the hash. Uploads on the verified key |
| `EncodedObject`      | `GetObject`. Decodes into a `MemoryObject`                                   |
| `HasEncodedObject`   | `HeadObject`                                                                 |
| `EncodedObjectSize`  | `HeadObject`. Prefers the `git-size` metadata value                          |
| `IterEncodedObjects` | `ListObjectsV2` under `objects/`. Fetches each entry lazily                  |
| `AddAlternate`       | Not implemented at first                                                     |

Delta object types stay rejected, as on disk:
`OFSDeltaObject` and `REFDeltaObject` return `plumbing.ErrInvalidType`.

## Write path

A writer stages bytes into one temp file. A `plumbing.NewHasher` tee runs
next to the file write, so the hash grows as the bytes stream past.
`Close` seeks the file back and runs one `PutObject` whose key comes from
the finished hash. Core shapes:

```go
type stageWriter struct {
	s      *Storer
	f      *os.File
	typ    plumbing.ObjectType
	pend   int64 // declared bytes left before ErrOverflow
	wrote  int64
	hasher plumbing.Hasher
	done   bool
}

func (w *stageWriter) write(p []byte) (int, error) {
	n, err := io.MultiWriter(w.f, w.hasher).Write(p)
	w.wrote += int64(n)
	w.pend -= int64(n)
	return n, err
}

func (w *stageWriter) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	defer os.Remove(w.f.Name())

	h := w.hasher.Sum()
	if err := w.upload(context.Background(), h); err != nil {
		return fmt.Errorf("failed to upload %s: %w", keyOf(h), err)
	}
	return w.f.Close()
}

func (w *stageWriter) upload(ctx context.Context, h plumbing.Hash) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to rewind the staging file: %w", err)
	}
	_, err := w.s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.s.bucket),
		Key:    aws.String(keyOf(h)),
		Body:   w.f,
		Metadata: map[string]string{
			metaType: w.typ.String(),
			metaSize: strconv.FormatInt(w.wrote, 10),
		},
	})
	return err
}
```

`SetEncodedObject` copies the source reader through the same writer, then
compares hashes before it allows the upload:

```go
	got := w.hasher.Sum()
	want := obj.Hash()
	if got.String() != want.String() {
		w.discard()
		return plumbing.ZeroHash, ErrHashMismatch
	}
	if err := w.Close(); err != nil {
		return plumbing.ZeroHash, err
	}
	return got, nil
```

Notes:

- `io.WriteCloser` carries no context. `Close` runs uploads with
  `context.Background()`. Give the `Storer` a context slot if deadlines
  matter in practice.
- Writes hold no state on the `Storer` itself, so all methods are safe
  for concurrent use.

## Read path

`EncodedObject` fetches the full body and decodes it into a
`MemoryObject`. This is fine up to blob sizes in the tens of megabytes.
Larger payloads need an object backed by the response stream, not a full
buffer.

Iteration lists keys with the paginated `ListObjectsV2` form, then serves
entries lazily, in the manner of `storer.EncodedObjectLookupIter`. The
cheap version pays a full GET per non-matching object during type
filters. Reading `git-type` from a ranged GET of the first chunk removes
most of that cost later.

## Testing seam

Tests fake one narrow local interface, `s3API`, that wraps exactly four
S3 operations: get, put, head, and list. Table-driven cases cover it with
no network. End-to-end tests point the SDK at an `httptest.Server` that
speaks minimal S3.

Before production use, record which error a real Tigris bucket returns
for a missing key. Absence checks match typed errors such as
`types.NotFound` today. The match pattern depends on those facts.

## Build order

Items 1 to 4 and item 6 are complete.

1. **Done.** Implement the `Storer` methods against the `s3API` seam. Run
   unit tests against a fake client.
2. **Done.** Record which error a real bucket returns for a missing key.
   Fix the absence checks around that fact.
3. **Done.** Implement `PackfileWriter`. Without it, every push sends one
   PUT for each object. With it, a push sends two PUTs for each
   container. See [The pack container](#the-pack-container).
4. **Done.** Load pack indexes into memory. Serve packed objects with
   ranged `GetObject` requests into the pack. A `Storer` downloads a
   whole pack after 32 different objects from it.
5. Add repacking and garbage collection. Today packs collect forever, one
   for each container that a push fills. A cold index build costs one
   small GET for each pack. A compaction pass must also delete a `.bin` that has
   no `.cue`, which a failed upload can leave behind.
6. **Done.** Cache packs across requests. One process holds one
   least-recently-used cache of whole `.bin` files, under a disk budget.
   See [The pack cache](#the-pack-cache).
7. Switch the payload format to the zlib objfile form from
   `plumbing/format/objfile` when egress or storage cost matters.
8. Add the optional `PromisorPackfileWriter` support when partial clones
   matter for your users.
