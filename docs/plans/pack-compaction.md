# Plan: pack compaction and tombstone reaping

## Context

Every push leaves at least one pack container. A push above `maxPackBytes`
leaves more. Nothing deletes a container, so packs collect forever. This is
item 5 of the build order in
[../reference/tigris-backend.md](../reference/tigris-backend.md).

Two costs grow with the pack count:

- A cold index build reads one `.cue` for each pack. `ensurePacksBuilt` runs
  this for every repository on the first packed read of a request.
- Two clients that push the same objects store those objects twice. The pack
  id is the SHA-256 of the `.bin`, so the two containers get different names
  and both survive. Nothing deduplicates them.

A failed upload adds a third cost. `packJob.run` uploads the `.bin` and then
the `.cue`. If the second upload fails, the bucket keeps a `.bin` that no
reader can see, because `ensurePacksBuilt` indexes `.cue` files only.

Decisions from the user:

- **Compaction only.** A pass copies every object forward. It never drops an
  unreachable object. Deletion of a source pack is therefore lossless, and the
  grace period is the only safety mechanism that the design needs.
- **A background sweeper in the daemon** runs the passes. There is no
  subcommand and no post-push trigger.
- **An explicit tombstone object** records which packs to delete and when.
- **The pack count triggers a pass.** A pass merges the smallest packs. A
  repository under the count threshold gets no work.
- **A pack is never edited.** A pass writes new containers, uploads them, and
  deletes the old ones later.

The last decision is not a preference. The pack id is the SHA-256 of its own
`.bin`, so an edit changes the name of the pack. Every pack in the bucket is
immutable by construction.

## Design

### The rewrite merges whole packs, not objects

A merge bin-packs whole source packs into one output container. It
deduplicates hashes inside that output. It does not iterate objects across
pack boundaries.

This gives one invariant:

```text
hashes(output) = hashes(A) ∪ hashes(B) ∪ ...
```

for the source packs A, B, ... that the pass packs into that output.
Deduplication collapses copies of one hash. It never removes a hash.

The invariant buys three properties at no cost.

**Delta containment holds without a check.** The write-side rule in
`packwriter.go` already guarantees that the base of every delta in pack A is
also in pack A. Because `A` is a subset of the output, the base moves with the
delta. The pass needs no `inSeg` map and no call to `payloadFor`.

**Every payload is a verbatim byte copy.** The pass copies the `stored` span
of each record from the source `.bin` into the output `.bin`. The new
`cueRecord` is the old record with a new `offset`. The fields `codec`,
`stored`, `raw`, `base`, and `typ` all carry over. Compaction never runs a
compressor and never resolves a delta.

**Size accounting is a safe upper bound.** The output is never larger than the
sum of the source `.bin` sizes, because deduplication only removes bytes. The
pass therefore bin-packs against `maxPackBytes` with sizes from the survey,
before it reads one byte of payload.

One refinement applies to a hash that two source packs in the same output both
hold. If one copy is a delta and the other is whole, keep the whole copy. Both
copies are correct. A kept delta can point at a delta from the other source
pack, which makes the chain one link longer for each pass. The whole copy
stops that growth. The pass has both records in memory already, so this costs
nothing.

This also corrects the duplicate-push problem. Two concurrent pushes of the
same objects leave two packs of a similar size. Those two packs sort next to
each other under "merge the smallest", so one output usually holds both, and
deduplication collapses them.

### Selection, and the guard against a treadmill

`-compact-max-packs` sets the threshold. Its default is 16.

The threshold gates the merge alone. The reap step, the survey, the
subsumption backstop, and the orphan cleanup run on every pass. A repository
under the threshold still needs them, because a failed upload leaves an orphan
`.bin` at any pack count.

Above the threshold, the pass sorts the packs by `.bin` size, smallest first.
It takes the smallest packs and bin-packs them into outputs of at most
`maxPackBytes`. It takes packs until the projected count reaches the
threshold. The projected count is `total - taken + outputs`.

CAUTION: A pass must not run when it cannot reduce the pack count. Packs that
are already near `maxPackBytes` merge into about as many outputs as inputs. A
pass then rewrites the repository on every cycle and bills egress for no
result. Two rules prevent this:

1. Skip a source pack larger than `maxPackBytes / 2`.
2. If `outputs >= taken`, abandon the pass.

Compaction does not recompress. A pack written with `-pack-compression=false`
keeps its raw payloads through a merge. Recompression needs a decode of every
payload, which is the cost that the verbatim copy exists to avoid.

### The tombstone object

The key is `pack-tombstones` at the root of the repository prefix. It sits
outside `packs/`, for the same reason that `packed-refs` sits outside `refs/`.
No pack listing can return it.

The format copies `packed-refs` exactly, byte for byte. A 16-byte plaintext
header comes first. Bytes 0 to 2 hold the magic `OGT`. Byte 3 holds the
version. Byte 4 holds the body codec. Bytes 5 to 8 hold the entry count as a
big-endian `uint32`. The body is text, and it holds one entry for each line,
sorted by pack id:

```text
3f2a...	1756684800
9c81...	1756688400
```

The separator is a tab. The second field is the deadline as a Unix time in
seconds. A hex id can hold no tab, so the field is never ambiguous.

Writes use the same compare-and-swap loop as `commitRefs` in `refcache.go`. A
first write sends `If-None-Match: *`. Every later write sends
`If-Match: <etag>`. This plan factors that loop out of `refcache.go` into one
`casPut` helper, so the two callers cannot drift apart.

### The reap step

The reap step runs at the start of a pass, before the survey. A crash in the
middle of a pass therefore reaps on the next cycle.

The step reads `pack-tombstones` and divides the entries by deadline. For each
expired entry it deletes the `.bin` and the `.cue` with `DeleteObjects`, 1000
keys for each call. Then it writes the tombstone object again, without those
entries.

The order is delete first, tombstone second. If the process stops between the
two, the tombstone names a pack that is already gone. A `DeleteObjects` call
against a missing key succeeds, so the next reap corrects the record. The
opposite order loses the record of a pack that still exists.

### The backstop for tombstone drift

A tombstone object is state that can disagree with the bucket. Two cases leak
packs, and both close with data that the survey already holds.

**A crash between the upload and the tombstone write.** The merged pack
exists, the sources exist, and no tombstone names them. Nothing deletes them.

**A `.bin` with no `.cue`.** A failed `.cue` upload leaves it. No reader can
see it and no tombstone names it.

After the survey, the pass computes subsumption. A pack is subsumed when every
hash in its `.cue` is also in the `.cue` of some other pack. If a subsumed
pack has no tombstone, the pass writes one.

CAUTION: Measure the deadline of that tombstone from the `LastModified` of the
newest pack that subsumes it. The `LastModified` of the subsumed pack is
wrong. An old source pack then gets no grace at all, and a live reader loses
the bytes under it.

The pass deletes a `.bin` that has no `.cue` directly, with no tombstone,
after the `LastModified` of that `.bin` passes the grace period. No reader can
hold a reference to it, because no `.cue` ever named it. The age test is what
divides an orphan from an upload that is still in flight.

Subsumption runs over records that the survey already loaded, so it costs CPU
and no extra round trips. The tombstone object is therefore an optimization
and not a correctness dependency. If it is lost, the next pass rebuilds it.

### The sweeper

A new package, `internal/compactor`, holds one goroutine. `main.go` starts it
on the errgroup, in the same shape as the other listeners.

A ticker fires every `-compact-interval`. The interval carries a jitter of 25
percent, so a fleet of daemons does not wake at one time.

The sweeper enumerates repositories with two levels of `ListObjectsV2` and
`Delimiter: "/"`. One call at the root returns the organization prefixes. One
call for each organization returns the repository prefixes. Two levels match
`RepoRef.Path()`, which is `path.Join(OrgID, Name)`.

For each repository the sweeper takes a lease, calls
`Base.Scoped(path).Compact(ctx, opts)`, and releases the lease. It runs one
repository at a time. Compaction is bulk I/O, and it must not compete with
traffic that serves a client.

The lease is `compact-lease` at the root of the repository prefix. The sweeper
takes it with `If-None-Match: *`. The body holds an owner id and an expiry
time. The sweeper deletes the key to release it, and it ignores a lease that
is expired.

NOTE: The lease saves cost. It is not a correctness requirement. Selection is
deterministic, because the pass sorts by size and breaks a tie by pack id. Two
daemons that compact one repository at the same time therefore build
byte-identical outputs. Identical bytes get one content-addressed id, so the
two PUT calls are idempotent. A fault in the lease cannot damage a repository.

### Concurrency with a live push

The pass needs no lock against a push, for four reasons:

- A pack that lands after the survey is not in the input of that pass.
- A pack is immutable, and the sweeper is the only writer that deletes one.
  Nothing can modify a pack that the survey read.
- Both the sources and the merged pack exist through the overlap window. A
  cold index build merges both sets of `.cue` records, and the last write wins
  for each hash. The two copies of a duplicated object are byte-identical, so
  either entry is correct. This is the same property that makes two concurrent
  duplicate pushes harmless today.
- Compaction never writes a reference, so it cannot interact with the
  `packed-refs` compare-and-swap.

### Flags

| Flag                  | Default | Purpose                                     |
| --------------------- | ------- | ------------------------------------------- |
| `-compact-interval`   | `0`     | Time between sweeps. `0` disables the sweeper. |
| `-compact-max-packs`  | `16`    | Pack count that triggers a pass.            |
| `-compact-grace`      | `1h`    | Time between the merge and the delete.      |

Each flag is kebab-case with a `flagenv` fallback, such as `COMPACT_INTERVAL`.

The interval defaults to `0`, which turns the subsystem off. This matches
`-allow-push` and `-allow-hooks`, which are opt-in because they carry
consequences. It does not match `-pack-compression` and `-packed-refs`, which
default to on. Those two flags delete nothing. Turn the interval on by default
in a later release, after the pass runs in production.

The grace period must be longer than the longest request. A `Storer` builds
its pack index once for each request. The index of a reader is therefore never
more stale than the request that holds it. A large clone can run for many
minutes.

### Metrics

`internal/metrics` holds every vector, as it does today:

- `objgit_compact_runs_total{result}`
- `objgit_compact_duration_seconds`
- `objgit_compact_packs_merged_total`
- `objgit_compact_packs_deleted_total`
- `objgit_compact_objects_deduped_total`
- `objgit_compact_bytes_reclaimed_total`
- `objgit_compact_orphan_bins_deleted_total`

### Testing

Unit tests are table-driven with `tt`, against the `s3API` fake. The pass uses
get, put, list, and delete, and the fake covers all of them. The cases:

- A repository under the threshold gets no work.
- The treadmill guard abandons a pass of packs near the cap.
- Deduplication collapses one hash that two source packs hold.
- A delta still reads after a merge. This is the test for the containment
  invariant.
- A duplicated hash keeps the whole copy over the delta copy.
- A tombstone deadline comes from the subsuming pack, not the subsumed pack.
- The reap deletes an expired entry and keeps a live one.
- The reap succeeds against a key that is already gone.
- The subsumption backstop writes a tombstone that no pass wrote.
- An orphan `.bin` older than the grace period is deleted. A younger one
  survives.
- A refused compare-and-swap on the tombstone retries.

One end-to-end test covers the whole pass. Seed a repository with `seedRepo`,
push several times to build many packs, run a pass, then clone. The clone must
match a clone taken before the pass. Gate the test with
`exec.LookPath("git")`, as `http_test.go` does.

## Remaining gap

This plan does not reclaim space from an object that no reference reaches. A
force-push or a deleted branch still leaves its objects in the bucket forever.
Reachability needs a second safety mechanism, because a push that uploaded its
objects but has not yet committed its reference looks unreachable. The rewrite
stage of this plan is where that filter goes later.
