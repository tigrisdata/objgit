# erofs snapshots (`internal/snapshot`)

After a push, `objgitd` can build one EROFS image of the tree at the tip of
each updated branch and tag. It stores the image in the bucket, under the
prefix of the repository. A later reader, such as a web UI, opens the image
and reads files from it with no git object walk.

The `-erofs-snapshots` flag controls this. It is on by default, and
`-erofs-snapshots=false` turns it off. The images come from
`github.com/Xe/erofs`, and they use Zstandard compression.

The design is in
[../superpowers/specs/2026-09-23-erofs-snapshots-design.md](../superpowers/specs/2026-09-23-erofs-snapshots-design.md).

## Object layout

| Key                               | Contents                                  |
| --------------------------------- | ----------------------------------------- |
| `snapshots/erofs/v1/<tree>.erofs` | One EROFS image of the git tree `<tree>`. |

The key uses the tree hash, not the commit hash. Many commits can point at one
tree, such as a branch and a tag on one commit. These commits share one image.

The builder pins every mtime to the Unix epoch. As a result, two builds of one
tree give identical bytes, and a second build is never necessary.

`v1` names the mapping from git to EROFS, and the build options. A change to
either one uses `v2`. A `v2` build never overwrites a `v1` image.

The key does not start with `objects/`, `packs/`, or `refs/`. The pack index,
the loose object reads, and the loose ref listing therefore never see it.

Each image carries user metadata:

| Metadata key   | Value                                                     |
| -------------- | --------------------------------------------------------- |
| `erofs-format` | `1`                                                       |
| `erofs-sha256` | The hex SHA-256 of the image. The cache rejects a download that does not match it. |
| `git-tree`     | The hex tree hash.                                        |
| `erofs-files`  | The count of regular files.                               |

## The mapping from git to EROFS

| Git entry mode   | EROFS entry                              |
| ---------------- | ---------------------------------------- |
| `040000` tree    | Directory, mode `0755`.                  |
| `100644` blob    | Regular file, mode `0644`.               |
| `100664` blob    | Regular file, mode `0644`.               |
| `100755` blob    | Regular file, mode `0755`.               |
| `120000` symlink | Symlink. The target is the blob content. |
| `160000` gitlink | Empty directory, mode `0755`.            |

The gitlink rule gives the same result as `git checkout` of a submodule that
is not initialized.

Two EROFS limits are stricter than git:

| Limit               | EROFS      | Checked by                                          |
| ------------------- | ---------- | --------------------------------------------------- |
| Entry name length   | 255 bytes  | `Ensure`. The builder does not check it.            |
| Symlink target size | 1020 bytes | `Ensure`. The builder writes more, but the reader refuses it. |

If an entry is over a limit, the build fails with an error that names the
path. `Ensure` does not skip the entry, because an image with a missing file
is worse than no image.

## `Ensure`

`snapshot.Ensure` is the one unit of work. The push path calls it now. A web
UI, a backfill, or a queue consumer can call it later with no change.

1. It calls `StatSnapshot`. If the image exists, the result is `exists`.
2. It walks the tree entries, depth first, and adds each one to an
   `erofs.Builder` on a temp file.
3. It builds the image, computes its SHA-256, and calls `PutSnapshot`.

The walk uses tree entries and not `tree.Files()`. The `FileIter` of go-git
skips gitlinks.

Two pushes of one tree can run `Ensure` at the same time. Both build, and both
put identical bytes. This is correct, so `Ensure` takes no lock. The temp file
name is random for the same reason.

`Ensure` streams file content into the builder. The walk reads only the
size of each blob, through `EncodedObjectSize`. It gives the builder the size
and an open function with `AddFileFunc`. During `Build`, the builder opens
each blob one time, compresses it one pcluster at a time, and spools the
compressed blocks to a temp file (`WithSpoolDir`, the same directory as the
image). As a result, the memory for a build does not depend on the size of
the tree. `TestEnsureMemory` builds 256 MiB of content with a live heap of
less than 64 MiB.

This needs `github.com/Xe/erofs` v0.8.0 or later. The image bytes are the same
as the bytes that v0.6.1 wrote for the same tree.

## `Open` and the snapshot cache

`snapshot.Open` downloads the whole image to a local file, then opens it with
`erofs.Open`. Every read after the download is local. The caller closes the
`Snapshot`. If the image is absent, `Open` returns `fs.ErrNotExist`. It does
not build the image.

The local file comes from the snapshot cache. The cache is a second
`tigris.PackCache`, from `NewSnapshotCache`, in an `objgit-snapshots-*`
directory next to the pack cache. It has the properties of the pack cache:

- One download for each image, however many callers ask at the same time.
- A byte budget, `-snapshot-cache-bytes`. When a new image passes the budget,
  the cache deletes the least-recently-used images.
- An idle sweep. Every quarter of `-snapshot-cache-max-idle`, the cache
  deletes the images that nobody opened for that long.
- Eviction is an unlink. An open `Snapshot` keeps working.
- A failed download is not cached.

A snapshot id is a tree hash, not the digest of the bytes. The cache therefore
uses `GetChecked`, whose fetch returns the digest from the `erofs-sha256`
metadata. A body that does not match is refused.

The cache id is `erofs-v1-<tree>`, with no repository prefix. Two repositories
that hold one tree share one local file. This is deduplication and not
leakage. A caller gets to the id only through a tree that its own repository
holds.

If the storer has no snapshot cache, `OpenSnapshot` downloads the image to a
private temp file and unlinks it at once.

## The push path

`(*daemon).receivePack` calls `runSnapshots` in `onUpdated`, after the refs
are committed and report-status is sent. Snapshots run before hooks.

`runSnapshots` handles branches first, then tags. It peels annotated tags to
their commit, and it skips a tag of a tree or a blob. A tree that one push
already handled is not built again.

Each ref writes one progress line to the client:

```text
remote: objgit: snapshot refs/heads/main (tree 3d4e5f6): built, 1234 files, 5.6 MiB, 1.2s
remote: objgit: snapshot refs/tags/v1.0.0 (tree 3d4e5f6): exists
remote: objgit: snapshot refs/heads/wip (tree 0a1b2c3): failed
```

A snapshot failure never fails the push. The daemon logs the error and counts
it in `objgit_snapshot_builds_total{result="error"}`. The failure line does
not show the error text, because the text can contain bucket details.

The push slot from `-max-concurrent-pushes` stays held while snapshots run.
The push cap therefore also limits the count of concurrent builds, and so the
temp disk that builds use at one time.

The daemon gets the store with a type assertion, `st.(snapshot.Store)`. A
storer without the methods skips snapshots.

## Flags

| Flag                       | Default | Meaning                                                  |
| -------------------------- | ------- | -------------------------------------------------------- |
| `-erofs-snapshots`         | `true`  | Build an image for each updated ref tip after a push.    |
| `-snapshot-timeout`        | `2m`    | Wall-clock limit for the snapshots of one push.          |
| `-snapshot-cache-bytes`    | `2 GiB` | Disk budget for the snapshot cache. `0` disables the cache. |
| `-snapshot-cache-max-idle` | `1h`    | The cache deletes an image that nobody opened for this long. `0` disables the sweep. |

## Known risks

- **Temp disk.** A build writes the image and a spool of compressed blocks
  to the temp directory. Each one is at most the size of the image.
- **Push latency.** The client waits for the build.
- **The single PUT limit.** An image larger than 5 GiB fails, because
  `PutSnapshot` sends one `PutObject`.
- **A crash during a build** leaves no image and no partial object. The next
  `Ensure` for that tree builds it.
- **An image larger than the cache budget** is admitted, and it evicts every
  other image.

## The path to a message queue

The queue changes only the caller. The message is `{repoPath, tree}`. The
consumer resolves the storer for `repoPath` and calls `Ensure`. `Ensure` is
idempotent, so a queue that delivers a message twice is correct.
