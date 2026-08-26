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
packs/             reserved for packfiles and their indexes
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

1. Implement the `Storer` methods against the `s3API` seam. Run unit
   tests against a fake client.
2. Record which error a real bucket returns for a missing key. Fix the
   absence checks around that fact.
3. Implement `PackfileWriter`. Without it, every push sends one PUT per
   object. With it, a push becomes one pack upload plus one index upload.
4. Load pack indexes into memory at startup. Serve packed objects with
   ranged `GetObject` requests into the pack.
5. Switch the payload format to the zlib objfile form from
   `plumbing/format/objfile` when egress or storage cost matters.
6. Add the optional `PromisorPackfileWriter` support when partial clones
   matter for your users.
