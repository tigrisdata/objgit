# Packed refs with compare-and-swap

Status: approved, not implemented.
Date: 2026-08-28.
Package: `internal/storage/tigris`.

## Problem

One loose object holds one ref. The key is `refs/<name>`. See `refs.go`.

A push of N refs costs N `GetObject` calls and N `PutObject` calls:

- `updateReferences` in `cmd/objgitd/receivepack.go` runs one `GetObject`
  for each existence check.
- `SetReference` runs one `flush()` and one `PutObject` for each ref.

The read path costs more. `listLooseRefs` lists the `refs/` prefix. Then it
calls `Reference` for every key it found. That is one `GetObject` per ref. A
repository with 100,000 tags pays 100 list pages and 100,000 `GetObject` calls.
It pays that on every clone, every fetch and every push.

`CheckAndSetReference` has a third fault. It compares, and then it writes. The
two steps are not atomic. The comment at `refs.go:22` records this hole.

## Goals

1. A push of any number of refs costs one `PutObject` call.
2. A ref advertisement costs a fixed number of calls, whatever the ref count.
3. `CheckAndSetReference` becomes atomic.
4. Buckets that hold loose refs today keep working, with no offline tool.
5. A rollback to the previous release keeps every ref readable.

## Non-goals

- **Peeled tags.** Git writes a `^<hash>` line in its own `packed-refs` file so
  that an advertisement does not open every annotated tag object. This design
  does not. An advertisement of 100,000 annotated tags still reads 100,000 tag
  objects. Those reads go through `packIndex` and the local pack cache, so they
  are cheap, but they are not free. This is a separate problem. The format
  reserves a third field for the peeled hash, so a later change is a version
  bump and not a new key.
- **More than 100,000 refs.** The design target is 10,000 to 100,000 refs. At
  1,000,000 refs the rewrite cost per push becomes the next bottleneck. The
  fix for that scale is a write-ahead log of mutation segments over a
  compacted base object. Section "Rejected alternatives" records why we did
  not build it now.
- **Sharding the packed object by ref name.** One object is enough at the
  target scale.

## Design

### Keys

Two keys hold ref state. Both sit under the per-Storer prefix from `Scoped`.

| Key          | Role                                                     |
| ------------ | -------------------------------------------------------- |
| `packed-refs`| Every ref for this repository. Its ETag is the CAS token. |
| `refs/<name>`| A legacy loose ref. Read-only. Deleted by the fold.      |

`packed-refs` sits at the root, beside `shallow`, `index` and `config`. The
name comes from the git file with the same role. The key does not start with
`refs/`, so `listKeys(s.prefix + refPrefix)` cannot return it.

### Format

The object has a 16-byte plaintext header, and then a body. The header copies
the shape that `.cue` uses in `packindex.go`:

| Offset | Bytes | Content                                        |
| ------ | ----- | ---------------------------------------------- |
| 0      | 3     | Magic `OGR`.                                   |
| 3      | 1     | Format version. This design writes version 1.  |
| 4      | 1     | Body codec: `codecRaw` or `codecZstd`.          |
| 5      | 4     | Ref count, big-endian `uint32`.                 |
| 9      | 7     | Reserved. Zero.                                 |

The body is raw, or one zstd frame. It reuses the encoder in `compress.go`.

The body is text. It holds one ref per line, sorted by name:

```
HEAD	ref: refs/heads/main
refs/heads/main	0a1b2c...
refs/tags/v1.0.0	3d4e5f...
```

The separator is a tab. `git-check-ref-format` forbids whitespace inside a ref
name, so a tab can never be ambiguous.

The name comes first, and the lines are sorted. This buys three things. Tag
names with a shared prefix become adjacent, so zstd collapses them. A given
set of refs always encodes to identical bytes, so a test can assert bytes and
not sets. A person who decompresses the body by hand can grep it.

A value is either a hex hash, or `ref: <target>` for a symbolic ref. That is
what `encodeRefValue` and `decodeRefValue` already produce and parse today.
Both functions are reused without change.

Symbolic refs live in this object. Git keeps `HEAD` loose because its own
format cannot hold a symbolic ref. This format can, and `refKey(HEAD)` already
puts `HEAD` in the same namespace.

At 100,000 refs the body is about 4.5 MB of text. It compresses to about 1.5 to
2 MB. The 40 hex characters of a SHA-1 carry 4 bits per byte, so zstd halves
the hash bytes even though a hash is otherwise incompressible.

### The cache

A new `refCache` hangs off the `Storer`. It mirrors `packIndex`:

```go
type refCache struct {
	mu    sync.Mutex
	built bool
	etag  string // CAS token. Empty means packed-refs does not exist.
	refs  map[plumbing.ReferenceName]*plumbing.Reference
	loose map[plumbing.ReferenceName]*plumbing.Reference // legacy, empty after the fold
}
```

`Scoped` gives each returned Storer a fresh `refCache`, next to the fresh `up`
and `packs` it already makes. `repofs` calls `Scoped` once per request
(`internal/repofs/repofs.go:94`), so one cache serves one request.

The cache builds once per instance. It is not sticky on error. A transient S3
failure is retried on the next call, and not remembered. `ensurePacksBuilt`
takes the same position and says why.

### Read path

A build costs two calls:

1. `GetObject packed-refs`. Keep the ETag. Decode the body into `refs`. A 404
   gives an empty map and an empty ETag, and is not an error.
2. `ListObjectsV2` on the `refs/` prefix. For each key found, `GetObject` it
   and put it in `loose`.

After the fold, step 2 returns no keys. A build is then one round trip. Today
the same work is one list page per 1000 refs plus one `GetObject` per ref.

Step 2 runs on every build. It is not skipped, and no "migration done" flag
records that it can be. One cheap round trip is the price of a safety net. The
net catches anything that writes a loose ref outside this code, including an
older binary during a rolling deploy.

### Merge rule

**A loose ref wins over a packed ref with the same name.**

That rule holds because of this invariant:

> A write through the packed path deletes the loose keys for every name it
> touched, before it reports success.

So a loose key can exist only if something wrote it after the last packed
write. A loose key is therefore newer, and must win.

The opposite rule is a bug. If a packed ref wins, and if a fleet runs two
releases at once, that fleet swallows every push made by the older binaries.

### Write path

One method sits behind every mutator:

```go
// refExpectation is one CheckAndSetReference precondition: the caller believes
// that name currently holds old. A nil old means "the ref must not exist".
type refExpectation struct {
	name plumbing.ReferenceName
	old  *plumbing.Reference
}

func (s *Storer) commitRefs(sets []*plumbing.Reference, removes []plumbing.ReferenceName, expect []refExpectation) error
```

1. Run `s.up.flush()` once for the whole batch, and not once per ref. This
   holds the invariant that `refs.go:45` protects: a ref must never name an
   object whose upload did not finish. One flush for a batch is also most of
   the speed win.
2. Build the cache if it is not built.
3. Make sure that every entry in `expect` matches the cache.
4. Copy the map. Apply `sets` and `removes`. Fold in `loose` if it is not
   empty. Encode the result.
5. `PutObject packed-refs`. Send `IfMatch: etag`. If the ETag is empty, send
   `IfNoneMatch: "*"` instead. **This one call is the commit point.**
6. If the call returns 412, drop the cache and go back to step 2. Retry at
   most 8 times.
7. On success, swap the new map and the new ETag into the cache. Do not
   re-read the object.
8. Delete the loose keys that step 4 folded, with `DeleteObjects`. That call
   takes up to 1000 keys, so 100,000 keys cost 100 calls.

Step 8 needs `DeleteObjects` added to the `s3API` interface in `tigris.go`.

Tigris supports both conditional headers, and evaluates them against the
latest state. See <https://www.tigrisdata.com/docs/objects/conditionals/>.

### Errors

Step 6 produces two different errors. Do not conflate them.

- An entry in `expect` that fails after a rebuild is
  `storage.ErrReferenceHasChanged`. That is go-git's own error, and
  receive-pack turns it into a correct per-ref rejection.
- Eight retries with no failed expectation is contention, and not a changed
  ref. Return a new `ErrRefContention`.

Step 3 and step 5 together make `CheckAndSetReference` atomic. The compare and
the write are now one conditional request. This closes the hole at
`refs.go:22`.

### The legacy fold

The fold runs once per repository. It is not a lasting two-layer state.

The first ref-mutating operation on a repository folds every loose ref into its
own commit PUT at step 5. It then deletes the loose keys at step 8.

The order is PUT and then DELETE. That order is deliberate:

- If the process stops between the two steps, the loose keys still win under
  the merge rule. That result is correct. The next write retries the delete.
- The opposite order loses a ref outright. If the DELETE runs first, and if the
  PUT then fails, a ref that existed only as a loose key is gone.

A fold of 100,000 loose refs costs one `PutObject` call and 100
`DeleteObjects` calls, once, on one push.

### Two methods stop lying

`PackRefs()` is a no-op today. It becomes "fold the legacy loose refs now".
That gives an operator a way to pay the fold cost on demand, before a large
push.

`CountLooseRefs()` returns the count of legacy loose keys. After the fold it
returns 0. That is the honest answer, and it is what go-git wants the number
to mean.

### The batch seam

`cmd/objgitd/receivepack.go` must hand the storer a whole push of ref updates
in one call. A pair of interfaces cannot do this. Go needs an exact signature
match, so a method that returns `*tigris.RefBatch` does not satisfy an
interface that declares `NewRefBatch() refBatch`.

So the seam is one method, declared where it is consumed:

```go
// refUpdater is the optional bulk ref-update surface. A storer that has one
// turns a push's N ref writes into a single round trip. A storer without one
// falls back to the per-ref SetReference and RemoveReference path.
type refUpdater interface {
	UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error
}
```

Every type in that method comes from `plumbing`. Both packages already import
it. So there is no new shared package, no nested interface, and a test fake is
one struct.

`updateReferences` at `receivepack.go:287` keeps its per-command validation.
That validation is now nearly free, because every `referenceExists` call reads
the cached map instead of the bucket. The function then splits the commands
into `sets` and `removes` and makes one call. `memory.Storage` does not
implement `refUpdater`, so the tests that use it take the fallback path.

### One behavior change

A push applies its ref updates one at a time today. A failure in the middle
leaves some updates applied and some not.

One `PutObject` call makes the batch all-or-nothing. That is better, and it
matches `git push --atomic`. It is still a change: on failure every command in
the push reports the same error, where the first N used to succeed. The
`report-status` encoding permits this. Record it in
`docs/architecture/transports.md`.

### Rollout

The rollout copies the `WithPackCompression` pattern, for the reason its own
comment gives at `tigris.go:150`. Ship the reader first, so the rollback
window is empty.

| Release | `WithPackedRefs` | Reads                            | Writes       |
| ------- | ---------------- | -------------------------------- | ------------ |
| 1       | off by default   | `packed-refs`, then loose on top | loose keys   |
| 2       | on by default    | `packed-refs`, then loose on top | `packed-refs`|

Release 1 changes nothing in production, because no `packed-refs` object exists
yet. It exists so that release 2 can be rolled back. A rollback to release 1
still reads every ref.

Without release 1, a rollback makes every ref written in the window invisible.

## Testing

The one piece of test infrastructure this needs: teach the counting fake
`s3API` in `client_test.go` to track an ETag per key, and to honor `IfMatch`
and `IfNoneMatch`. A failed condition returns a `smithy.APIError` whose code is
`PreconditionFailed`. That fake covers the whole CAS path with no bucket.

Tests are table-driven with `tt`, as `AGENTS.md` requires.

| Test                    | Asserts                                                     |
| ----------------------- | ----------------------------------------------------------- |
| Format round-trip       | Empty set, one ref, symbolic refs, 100,000 refs.             |
| Byte stability          | One set of refs encodes to identical bytes every time.       |
| Merge, loose only       | A ref that exists only as a loose key is visible.            |
| Merge, both layers      | The loose ref wins.                                          |
| Fold                    | The fold deletes every key it folded.                        |
| Fold, stopped part-way  | PUT succeeds and `DeleteObjects` fails. Refs still resolve. The next write retries the delete. |
| CAS retry               | One 412, then a rebuild, then success.                       |
| CAS exhaustion          | Eight 412 responses give `ErrRefContention`.                 |
| Expectation failure     | A concurrent writer moves the ref. The result is `storage.ErrReferenceHasChanged`, and not `ErrRefContention`. |
| Call count, write       | A push of 500 refs is exactly one `PutObject` call.          |
| Call count, read        | An advertisement is exactly one `GetObject` and one `ListObjectsV2` call. |
| Live bucket             | Real 412 behavior for `IfMatch` and `IfNoneMatch`. Goes in `livebucket_test.go`. |
| End to end              | `git push` of many tags through `runGit` and `seedRepo`, with the same call counts. |

The two call-count tests are the point of the whole change. They must fail
before the work and pass after it.

## Metrics

The S3 observer already counts every call by operation name, so the win needs
no new metric.

Add one counter for CAS retries to `internal/metrics`. Contention is the only
way this design degrades quietly.

## Files

| Path                                       | Change                                                                    |
| ------------------------------------------ | ------------------------------------------------------------------------- |
| `internal/storage/tigris/packedrefs.go`    | New. Header and text body, encode and decode.                             |
| `internal/storage/tigris/refcache.go`      | New. The cache, and the CAS commit loop.                                  |
| `internal/storage/tigris/refs.go`          | Rewire all seven ref methods onto the cache. Add `UpdateReferences`.      |
| `internal/storage/tigris/tigris.go`        | `packedRefsKey`, `WithPackedRefs`, the cache field, `Scoped`, `DeleteObjects` on `s3API`. |
| `cmd/objgitd/receivepack.go`               | The `refUpdater` assertion in `updateReferences`.                          |
| `internal/metrics`                         | The CAS retry counter.                                                     |
| `docs/architecture/tigris-storer.md`       | The new ref layout.                                                        |
| `docs/architecture/transports.md`          | The atomicity change.                                                      |

## Risks

| Risk                                                     | Response                                                                 |
| -------------------------------------------------------- | ------------------------------------------------------------------------ |
| Tigris rejects conditional writes on some bucket type.   | The live-bucket test runs before any other work. It is the gate.          |
| A Global or Dual-region bucket reads eventually. A stale read makes CAS unsafe. | Document that this design needs a Single-region or Multi-region bucket. See <https://www.tigrisdata.com/docs/concepts/consistency/>. |
| Two releases run at once and both write refs.            | The merge rule makes the loose write win, so the older binary's push is honored. |
| Write amplification: every push rewrites up to 2 MB.     | Accepted at the target scale. The next section names the fix if it hurts. |
| The 100,000-ref fold makes one push slow.                | `PackRefs()` lets an operator run the fold first.                        |

## Rejected alternatives

**A write-ahead log of mutation segments over a compacted base object.** A
push writes one small segment. Its sequence number comes from an
`IfNoneMatch: "*"` call. A compaction folds segments into a new base when the
segment count passes a threshold. Readers ignore every key at or below the
base sequence, so a delete is never load-bearing.

This costs a kilobyte per push where the chosen design costs up to 2 MB. It is
the right design above roughly 1,000,000 refs, and it is the right design for
a repository that takes a push every few seconds. At the 100,000-ref target it
buys a smaller PUT in exchange for sequence allocation, a layered reader, and a
compaction path. The chosen design has none of those. Note that the chosen
design is this one with the compaction threshold fixed at 1, so this is a
later change and not a rewrite.

**A tags-only append log, with branches left loose.** Tags are effectively
write-once, so a log for `refs/tags/` needs no tombstones and no CAS. This is
the smallest possible change. It was rejected for two reasons. It leaves the
read amplification unfixed for a repository with many branches. It also means
two ref code paths in the storer forever.
