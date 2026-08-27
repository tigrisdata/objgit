# `internal/s3fs` — a billy.Filesystem on Tigris

This package is vendored from the s3fs of Austin Poor. It is adapted to
**billy v6** and to the Tigris `storage-go` client. It treats an S3 bucket as
a filesystem, so go-git's `filesystem.NewStorage` can store loose objects and
packs against it.

In `cmd/objgitd` this package now backs only `sysFS`, which is daemon-level
state and holds the SSH host key. Repository git storage is
[`internal/storage/tigris`](tigris-storer.md), and not this package.

## The temp filesystem (`tempfs.go`)

This is the piece that is hard to guess from the outside.

The streaming `PackWriter` of go-git creates a temp pack file, then reopens
that same path for reading **at once**. It reads the file back while it still
writes to it, because it builds the index at the same time.

S3 cannot do that on a single live object. Until the final `Rename` uploads
the bytes, they therefore live in an in-memory `tempBuffer`, registered on the
`S3FS` by canonical key.

`readAt` returns `(0, io.EOF)` past the current end of the buffer. The
`syncedReader` of go-git can then tell "no data yet" apart from a hard error,
and it retries.

## The key rule

All S3 keys go through `S3FS.key`, which calls `cleanPath` and strips the
leading slash.

CAUTION: Every new S3 operation must funnel through `S3FS.key`. Any operation
that does not will desync chroot and path semantics.

## The `ReadAt` contract

Packs are read back through the `packfile.FSObject` of go-git. It probes the
handle of a packed object with a 1-byte `ReadAt`, and it reopens the pack when
it gets `os.ErrClosed`.

The read file in `internal/s3fs/file.go` therefore returns `os.ErrClosed` from
`Read`, `ReadAt`, and `Seek` once it is closed. It does not dereference its
niled `*bytes.Reader`. A post-receive hook that reads the commit of the push
takes exactly this path.
