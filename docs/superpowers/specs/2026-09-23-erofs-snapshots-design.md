# Design: erofs snapshots of pushed trees

## Goal

After a push, `objgitd` builds one EROFS image of the tree at the tip of each
updated ref. It stores the image in the bucket, under the prefix of the
repository. A later web UI opens the image and reads directories and files
from it, with no git object walk.

The package is `github.com/Xe/erofs`. The design started on v0.6.1. The code
now needs v0.8.0, for the streaming builder.

## Decisions

These decisions come from the design discussion on 2026-09-23.

| Question                 | Decision                                                                  |
| ------------------------ | ------------------------------------------------------------------------- |
| Which commits get images | The new tip of each updated branch and tag, on push. Other commits get an image on demand, through the same `Ensure` function, when a later feature asks for one. |
| When the build runs      | Synchronously, inside the push, after report-status. A message queue replaces this later. |
| Size limit               | None. Every tree gets an image. We measure first, then decide on a limit. |
| Compression              | Zstandard, at the default level of the builder.                            |
| How a reader gets bytes  | `Open` downloads the whole image into a local cache directory, next to the pack cache. The cache evicts least-recently-used images when it passes its byte budget, and on a timer when an image is idle too long. |
| Builder memory           | Bounded. `Ensure` uses `AddFileFunc` from erofs v0.8.0, which implements [2026-09-23-erofs-streaming-builder-design.md](2026-09-23-erofs-streaming-builder-design.md). |

## Non-goals

- A web UI. This design only produces images and gives a function to open one.
- Garbage collection of images in the bucket. Packs have no GC either.
- A backfill command for repositories that exist before this change. `Ensure`
  makes this simple to add later.
- Images of commits that are not ref tips, at push time.
- Putting a new image into the local cache at build time. The first `Open`
  after a push downloads it.

## Object layout

One new key family goes into the layout table in
[../../architecture/tigris-storer.md](../../architecture/tigris-storer.md):

| Key                               | Contents                               |
| --------------------------------- | -------------------------------------- |
| `snapshots/erofs/v1/<tree>.erofs` | One EROFS image of the git tree `<tree>`. |

The key uses the **tree** hash, not the commit hash. Many commits can point at
one tree: a branch and a tag on one commit, a revert, or a merge with no
change. These commits share one image.

For one version of the builder, the image bytes depend only on the tree and on
the `v1` mapping below. The builder pins every mtime with
`erofs.WithEpoch(time.Unix(0, 0))`. As a result, two builds of one tree give
identical bytes, and a second build is never necessary.

`v1` names the mapping from git to EROFS, and the build options. A change to
either (for example, a different compression algorithm, or a new gitlink rule)
uses `v2`. A `v2` build never overwrites a `v1` image, and a reader asks for
the version that it knows.

The key does not start with `objects/`, `packs/`, or `refs/`. As a result, the
pack index listing, the loose object reads, and the loose ref listing never
see it.

Each image carries user metadata:

| Metadata key   | Value                                   |
| -------------- | --------------------------------------- |
| `erofs-format` | `1`                                     |
| `erofs-sha256` | The hex SHA-256 of the image bytes. The local cache uses it to reject a bad download. |
| `git-tree`     | The hex tree hash.                      |
| `erofs-files`  | The count of regular files.             |

The web UI finds an image in two steps. It resolves the commit to its tree
through the git storer. Then it opens the key for that tree.

## The mapping from git to EROFS

| Git entry mode    | EROFS entry                                    |
| ----------------- | ---------------------------------------------- |
| `040000` tree     | Directory, mode `0755`.                        |
| `100644` blob     | Regular file, mode `0644`.                     |
| `100755` blob     | Regular file, mode `0755`.                     |
| `120000` symlink  | Symlink. The target is the blob content.       |
| `160000` gitlink  | Empty directory, mode `0755`.                  |

The gitlink rule is the same result that `git checkout` gives for a submodule
that is not initialized. The image does not record the submodule commit. A
reader that needs it reads the tree entry from git.

The owner and the group of every entry are 0. The root directory has mode
`0755`.

Two EROFS limits are stricter than git:

| Limit                | EROFS                   | Git             | Where the limit is |
| -------------------- | ----------------------- | --------------- | ------------------ |
| Entry name length    | 255 bytes               | No fixed limit  | `ondisk.NameLen`. The builder does not check it. `Validate` does. |
| Symlink target size  | 1020 bytes (`NameLen*4`) | No fixed limit | `readSymlink` in `inode.go`. The builder writes a longer target, but the reader refuses it. |

`Ensure` checks both limits before it adds the entry. If an entry is over a
limit, the build fails with an error that names the path. It does not skip the
entry, because an image with a missing file is worse than no image.

## The `internal/snapshot` package

One new package holds the build and the open. It depends on go-git and on
`github.com/Xe/erofs`. It does not import `internal/storage/tigris`.

### `Store`

```go
// Store holds snapshot images for one repository. Keys are relative to the
// repository prefix.
type Store interface {
	// StatSnapshot returns fs.ErrNotExist when key is absent.
	StatSnapshot(ctx context.Context, key string) (size int64, err error)
	PutSnapshot(ctx context.Context, key string, body io.ReadSeeker, size int64, meta map[string]string) error
	// OpenSnapshot returns a local copy of the image at key. The caller
	// must close it. It returns fs.ErrNotExist when key is absent.
	OpenSnapshot(ctx context.Context, key string) (File, error)
}

// File is a local image. *os.File satisfies it.
type File interface {
	io.ReaderAt
	io.Closer
}
```

`*tigris.Storer` implements `Store` on its scoped prefix, in a new file
`internal/storage/tigris/snapshot.go`:

- `StatSnapshot` sends one `HeadObject`.
- `PutSnapshot` sends one `PutObject` directly. It does not use the upload
  queue, because no ref waits for the image.
- `OpenSnapshot` gets the image through the snapshot cache. See "The snapshot
  cache".

The daemon gets the store with a type assertion, `st.(snapshot.Store)`. This is
the same optional-method pattern that `UpdateReferences` uses. If the storer
does not implement `Store`, the daemon skips snapshots and writes one debug
log line. The protocol tests use `memory.NewStorage()`, so they wrap it in a
test storer that adds a map-backed `Store`.

### `Ensure`

```go
type Result struct {
	Key     string
	Status  string // "built" or "exists"
	Files   int
	Bytes   int64
	Elapsed time.Duration
}

func Ensure(ctx context.Context, objs storer.EncodedObjectStorer, store Store, tree plumbing.Hash, tmpDir string) (Result, error)
```

`Ensure` is the one unit of work. The push path calls it now. A web UI, a
backfill, or a queue consumer calls it later, with no change to it.

The steps:

1. Call `StatSnapshot` on the key. If the key exists, return `exists`.
2. Create a temp file in `tmpDir` with `os.CreateTemp`. Do not name the file
   after the tree hash. Two pushes of one tree can run at the same time. The
   CAUTION about content-addressed temp paths in
   [../../architecture/tigris-storer.md](../../architecture/tigris-storer.md)
   applies here too.
3. Make an `erofs.NewBuilder` on the file, with these options:
   - `WithBlockSize(12)`, for 4096-byte blocks
   - `WithEpoch(time.Unix(0, 0))`
   - `WithCompression(erofs.CompressionZstd)`
4. Walk the tree with `object.GetTree` and the tree entries, depth first.
   Add each entry through `AddDir`, `AddFileFunc`, or `AddSymlink`, as the
   mapping gives. For a file, read only the size with `EncodedObjectSize`.
   The open function loads the blob with `object.GetBlob` and returns
   `blob.Reader()`, so the builder reads the content during `Build`.
5. Call `Build`.
6. Rewind the file and compute its SHA-256.
7. Rewind the file again. Call `PutSnapshot` with the file, its size, and the
   metadata.
8. Remove the temp file.

The builder keeps the compressed form of a file only when that form is
smaller. It stores files of one block or less, and files with a known
compressed extension (for example `.png` or `.gz`), without compression. The
builder decides this internally, and `Ensure` does not change it.

Two pushes of one tree at the same time both build, and both put identical
bytes. The second `PutObject` replaces the first with the same content. This
is correct, so `Ensure` uses no lock and no conditional write.

The walk uses tree entries and not `tree.Files()`. The `FileIter` of go-git
skips gitlinks (`plumbing/object/file.go`), so an image built from it has no
submodule directories.

### `Open`

```go
// Snapshot is an open image. Close releases the local file.
type Snapshot struct {
	*erofs.FS
	f File
}

func (s *Snapshot) Close() error

func Open(ctx context.Context, store Store, tree plumbing.Hash) (*Snapshot, error)
```

`Open` calls `OpenSnapshot` for the key of `tree`, then calls `erofs.Open` on
the local file. Every read after that is a local read. A directory listing
does many small reads, so a local file is necessary for a usable web UI.

If the key is absent, `Open` returns `fs.ErrNotExist`. It does not build the
image. A caller that wants the image to exist calls `Ensure` first.

## The snapshot cache

The snapshot cache is a second `*tigris.PackCache`, with its own directory
and its own budget. It uses the same code as the pack cache, so it has the
same properties:

- One download for each image, however many callers ask at the same time.
- A byte budget. When a new image makes the total pass the budget, the cache
  deletes the least-recently-used images until the total is in the budget
  again.
- Eviction is an unlink. A `Snapshot` that is open keeps its descriptor, so a
  reader never breaks. The disk space returns when the last reader closes.
- A failed download is not cached.
- Each `objgitd` process starts with an empty cache directory and removes it
  at shutdown.

The directory is `objgit-snapshots-*`, in the same parent as the pack cache
(`-pack-cache-dir`, or the OS temp directory).

### Changes to `PackCache`

`PackCache` needs four small changes. The pack cache does not use the new
functions, so its behavior does not change.

1. **A name for the directory.** `NewPackCache` makes `objgit-packs-*` today.
   Add an unexported constructor that takes the directory pattern.
   `NewPackCache` calls it with `objgit-packs-*`. A new `NewSnapshotCache`
   calls it with `objgit-snapshots-*`.

2. **A digest from the fetch.** `Get(id, fetch)` requires `id` to be the
   SHA-256 of the bytes. A snapshot id is the tree hash, so this is not true.
   Add this function:

   ```go
   // GetChecked is Get, but fetch returns the SHA-256 that the bytes must have.
   func (c *PackCache) GetChecked(id string, fetch func(io.Writer) (wantSHA256 string, err error)) (*os.File, error)
   ```

   `verifiedCopy` compares against the returned digest instead of `id`. `Get`
   becomes `GetChecked` with a fetch that returns `id`. The snapshot fetch
   sends `GetObject`, copies the body, and returns the `erofs-sha256`
   metadata value. If the metadata is absent, the fetch returns an error, and
   the cache keeps nothing.

3. **An idle sweep.** Add a wall-clock stamp, `usedAt`, next to the `used`
   sequence number. `claim` sets both. Then add this function:

   ```go
   // EvictIdle unlinks every settled entry that no caller has asked for
   // since maxIdle ago. It never touches a download that has not settled.
   func (c *PackCache) EvictIdle(maxIdle time.Duration) int
   ```

4. **An observer.** `internal/storage/tigris` does not import
   `internal/metrics`. It takes callbacks, as `WithObserver` does. Add an
   optional `observe func(event string)` field. `NewSnapshotCache` takes it as
   a parameter, and `NewPackCache` leaves it nil. The cache calls it with one
   of these events:

   | Event          | When                                                 |
   | -------------- | ---------------------------------------------------- |
   | `hit`          | `GetChecked` returns a file with no download.         |
   | `miss`         | `GetChecked` starts a download.                       |
   | `evict_budget` | `evictLocked` unlinks an entry to meet the budget.    |
   | `evict_idle`   | `EvictIdle` unlinks an entry.                         |

   The callback runs outside the cache lock. `main.go` wires it to
   `metrics.ObserveSnapshotCache`.

The snapshot cache id is `erofs-v1-<tree>`. It does not contain the
repository prefix, so two repositories that hold one tree share one local
file. This is deduplication and not leakage, for the same reason as for
packs: a caller reaches the id only through a tree that its own repository
holds, and the bytes are an image of that tree.

### Wiring

`tigris.WithSnapshotCache(*PackCache)` is a new `Option`, like
`WithPackCache`. `Scoped` shares the snapshot cache, as it shares the pack
cache.

If the storer has no snapshot cache, `OpenSnapshot` downloads the image to a
private temp file and unlinks it at once. The returned descriptor is the only
reference. This is the same fallback that `fetchWholePack` uses.

`main.go` makes the snapshot cache when `-snapshot-cache-bytes` is more than
0. It starts one goroutine in the errgroup. The goroutine calls
`EvictIdle(*snapshotCacheMaxIdle)` every `*snapshotCacheMaxIdle / 4`, and
stops when the context is done. At shutdown, `main.go` calls `Cleanup` on the
snapshot cache, next to the call for the pack cache.

## Daemon wiring

### Flags

| Flag                       | Default | Environment               | Meaning                          |
| -------------------------- | ------- | ------------------------- | -------------------------------- |
| `-erofs-snapshots`         | `true`  | `EROFS_SNAPSHOTS`         | Build an image for each updated ref tip after a push. |
| `-snapshot-timeout`        | `2m`    | `SNAPSHOT_TIMEOUT`        | Wall-clock limit for the snapshots of one push. |
| `-snapshot-cache-bytes`    | `2 GiB` | `SNAPSHOT_CACHE_BYTES`    | Disk budget for the local snapshot cache. `0` disables the cache. |
| `-snapshot-cache-max-idle` | `1h`    | `SNAPSHOT_CACHE_MAX_IDLE` | The cache deletes an image that nobody opened for this long. `0` disables the idle sweep. |

`*daemon` gets two fields, `snapshots bool` and `snapshotTimeout
time.Duration`. The temp directory for builds is `-pack-cache-dir`, or the OS
temp directory when that flag is empty.

### The push path

`(*daemon).receivePack` in `cmd/objgitd/hooks.go` changes in four places:

1. It takes the ref snapshot before the push, and it sets `onUpdated`, when
   `d.allowHooks || d.snapshots`. Today it does this only for hooks.
2. `snapshotRefs` returns branches **and** tags. `runHooks` skips every update
   that is not a branch, so the hook behavior does not change.
3. `onUpdated` calls `d.runSnapshots` first, then `d.runHooks`.
4. `d.runSnapshots` does these steps for each update that is not a deletion:
   1. Peel the new hash to a commit. An annotated tag goes through
      `object.GetTag` and `Tag.Commit`. A tag that points at a tree or a blob
      is skipped.
   2. Get the tree hash of the commit.
   3. Skip a tree that this push already handled.
   4. Call `snapshot.Ensure`.

All snapshots of one push share one context, from
`context.WithTimeout(context.Background(), d.snapshotTimeout)`. This is the
same pattern that `runHook` uses.

Snapshots run before hooks, so a hook can print a link to an image that
already exists.

The push slot from `-max-concurrent-pushes` stays held while `onUpdated`
runs. The push cap therefore also bounds the count of concurrent builds.

### Progress output

If the client negotiated sideband, each ref writes one line to `progress`:

```text
remote: objgit: snapshot refs/heads/main (tree 3d4e5f6): built, 1234 files, 5.6 MiB, 1.2s
remote: objgit: snapshot refs/tags/v1.0.0 (tree 3d4e5f6): exists
remote: objgit: snapshot refs/heads/wip (tree 0a1b2c3): failed
```

The size in the `built` line is the size of the compressed image. The failure
line does not show the error text, because the error can contain bucket
details. The log carries the full error.

### Failures

A snapshot failure never fails the push. The refs are committed before
`onUpdated` runs, and report-status has gone to the client. The daemon logs
the failure with `slog.Error` and the `"err"` key, and it counts the failure in
a metric.

`onUpdated` does not run when the client does not negotiate report-status.
Hooks have the same limit today. Every current git client sends report-status.

## Metrics

New series go in `internal/metrics`, with two helpers,
`ObserveSnapshot(result string, dur time.Duration, bytes int64)` and
`ObserveSnapshotCache(event string)`. The second helper maps each cache event
to one of the two cache counters:

| Series                                   | Type      | Labels   | Meaning                  |
| ---------------------------------------- | --------- | -------- | ------------------------ |
| `objgit_snapshot_builds_total`           | counter   | `result` | `built`, `exists`, or `error`. |
| `objgit_snapshot_build_duration_seconds` | histogram | none     | Time for one `Ensure` that built an image. |
| `objgit_snapshot_image_bytes`            | histogram | none     | Size of one built image, after compression. |
| `objgit_snapshot_cache_opens_total`      | counter   | `result` | `hit` or `miss` for each `OpenSnapshot`. |
| `objgit_snapshot_cache_evictions_total`  | counter   | `reason` | `budget` or `idle`.      |

The size histogram is the data for the later decision about a size limit. Use
exponential buckets from 64 KiB to 16 GiB. The cache counters are the data for
the choice of `-snapshot-cache-bytes` and `-snapshot-cache-max-idle`.

There is no `repo` label, per the rule in
[../../architecture/metrics.md](../../architecture/metrics.md).

## Known risks

- **Memory.** Resolved by erofs v0.8.0. Before it, `erofs.Builder` kept the
  bytes and the compressed blocks of every file until `Build` returned, up to
  approximately twice the size of the tree.
- **Push latency.** The client waits for the build. For a large tree the build
  reads every blob once and compresses it. The pack prefetch makes the reads
  local after the first read of each container.
- **The single PUT limit.** `PutSnapshot` sends one `PutObject`. An image
  larger than 5 GiB fails. The repository has no multipart upload path today.
  If the size histogram shows images near this limit, add one.
- **A crash during a build** leaves no image and no partial object. `PutObject`
  is atomic. The next `Ensure` for that tree builds it.
- **An image larger than the cache budget.** The cache admits it, then evicts
  every other settled image. The pack cache has the same behavior for a large
  container. The size histogram shows if this happens.

## Testing

All tests are table-driven with `tt`.

`internal/snapshot` unit tests use `memory.NewStorage()` for git objects and a
map-backed `Store`:

| Test                     | What it proves                                           |
| ------------------------ | -------------------------------------------------------- |
| `TestEnsureMapsModes`    | Every row of the mapping table. It opens the image with `Open` and compares the mode, the content, and the symlink target of each entry. |
| `TestEnsureValidates`    | `erofs.Validate` returns no errors for each fixture tree. |
| `TestEnsureCompresses`   | A tree with one large text file gives an image smaller than the file. The file reads back unchanged. |
| `TestEnsureDeterministic`| Two builds of one tree give identical bytes and the same `erofs-sha256`. |
| `TestEnsureExists`       | A second `Ensure` returns `exists` and calls `PutSnapshot` zero times. |
| `TestEnsureLimits`       | A 256-byte name, and a 1021-byte symlink target, each give an error that names the path, and no `PutSnapshot` call. |
| `TestEnsureEmptyTree`    | The empty tree gives a valid image with a root directory only. |
| `TestOpenMissing`        | `Open` of a tree with no image returns `fs.ErrNotExist`. |

`internal/storage/tigris` tests use the existing `fakeS3`:

| Test                                  | What it proves                                    |
| ------------------------------------- | ------------------------------------------------- |
| `TestPutSnapshotKeyAndMetadata`       | The key is under the scoped prefix, and the metadata is present. |
| `TestStatSnapshotNotFound`            | A 404 becomes `fs.ErrNotExist`.                    |
| `TestOpenSnapshotCachesDownload`      | Two `OpenSnapshot` calls send one `GetObject`. Two scoped storers with the same tree also send one. |
| `TestOpenSnapshotRejectsBadDigest`    | A body that does not match `erofs-sha256` gives an error, and the cache keeps nothing. |
| `TestOpenSnapshotMissingDigest`       | An object with no `erofs-sha256` gives an error.   |
| `TestOpenSnapshotNoCache`             | With no snapshot cache, `OpenSnapshot` still returns a readable file. |
| `TestPackCacheEvictIdle`              | `EvictIdle` removes only settled entries older than the limit. An open descriptor stays readable after its entry is evicted. |
| `TestPackCacheGetUnchanged`           | `Get` still refuses bytes whose SHA-256 is not `id`. |
| `TestPackCacheObserver`               | The observer gets `miss`, then `hit`, then `evict_budget` and `evict_idle`, each in the correct case. |
| `TestSnapshotKeysInvisible`           | A snapshot key does not appear in the pack index or in `IterReferences`. |

`cmd/objgitd` protocol tests shell out to `git`, gated with
`exec.LookPath("git")`:

- `TestSmartHTTPPushSnapshots`: push a branch and an annotated tag on one
  commit, with `-erofs-snapshots` on. Exactly one image exists. A file reads
  back through `snapshot.Open`. The client output contains the `remote:
  objgit: snapshot` lines.
- `TestPushSnapshotsOff`: no image exists after a push with the flag off.
- `TestHooksIgnoreTags`: a pushed tag does not run the hook, now that
  `snapshotRefs` returns tags.

## Documentation changes

- `docs/architecture/tigris-storer.md`: add the new key to the layout table.
  In the pack cache section, add `GetChecked`, `EvictIdle`, and the second
  instance.
- A new page `docs/architecture/snapshots.md`, linked from
  `docs/architecture/README.md` and from the table in `AGENTS.md`.
- `docs/architecture/metrics.md`: add the new series and helpers.
- `AGENTS.md`: add `internal/snapshot` to the table of code locations.

## The path to a message queue

The queue changes only the caller. The message is `{repoPath, tree}`. The
consumer resolves the storer for `repoPath` and calls `Ensure`. `Ensure` is
idempotent, so a queue that delivers a message twice is correct. After the
move, `runSnapshots` sends messages and does not build, and the push no longer
waits for the build.
