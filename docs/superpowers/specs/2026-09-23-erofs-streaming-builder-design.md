# Design: a streaming file source for `erofs.Builder`

This spec is for work in the `github.com/Xe/erofs` repository, not in objgit.
Copy it into `docs/superpowers/specs/` of that repository before you start.
Read the `CLAUDE.md` of that repository first.

The code references are to `github.com/Xe/erofs` v0.6.1.

## Problem

`Builder.AddFile(p, info, data []byte)` takes the whole content of a file.
The builder keeps each slice on `buildInode.data` until `Build` returns. With
compression on, `tryCompressInodes` also keeps the compressed blocks of every
file, on `compressedFileData.blocks`, until `Build` returns. The memory for
one build is therefore up to twice the total size of every file in the image.

objgit builds one Zstandard-compressed image of a git tree after each push
(see [2026-09-23-erofs-snapshots-design.md](2026-09-23-erofs-snapshots-design.md)).
A 2 GiB tree costs up to 4 GiB of heap for each build. objgit already reads
each blob through an `io.Reader`, so it does not need the bytes in memory at
all.

`AddFromFS` has the same problem. It calls `fs.ReadFile` for each file, so
`mkfs.erofs --dir` holds the whole directory tree in memory.

## Goal

Add a way to give the builder a file as a size and an open function. The
memory for a build must not depend on file content, with compression on or
off. It must depend only on the count of inodes, the count of directory
entries, and the count of lclusters.

## Non-goals

- A change to the on-disk format. The output must stay bytewise compatible
  with the kernel driver and with `mkfs.erofs`.
- A change to the reader.
- Parallel compression. The builder compresses one group at a time, as it does
  today.

## API

```go
// AddFileFunc adds a regular file whose content comes from open. The builder
// calls open once, during Build, and reads exactly size bytes from it. It
// closes the reader before it calls open for the next file.
func (b *Builder) AddFileFunc(p string, info fs.FileInfo, size int64, open func() (io.ReadCloser, error)) error

// WithSpoolDir sets the directory for the temp file that holds compressed
// blocks during Build. The default is os.TempDir(). Build removes the file
// before it returns.
func WithSpoolDir(dir string) BuildOption
```

The rules for `AddFileFunc`:

- The builder calls `open` during `Build`, not during `AddFileFunc`.
- The builder calls `open` exactly once for each file. If `size` is 0, it
  does not call `open`.
- If the reader returns fewer or more than `size` bytes, `Build` returns an
  error that names the path, the declared size, and the count of bytes read.
- If `open` or a read returns an error, `Build` returns it, wrapped with the
  path.
- Only one reader is open at a time.

`AddFile` does not change. Internally it can become `AddFileFunc` with an
open function over `bytes.NewReader(data)`. The implementation decides this.

## The invariant

**An image built with `AddFileFunc` is byte-identical to the same image built
with `AddFile`.** This is true with no compression, with LZ4, and with
Zstandard. Every test below compares the bytes of the two builds.

## How the build works today

`Build` (`builder.go:251`) runs these steps in this order:

| Step | Function                               | Needs the file bytes?                                      |
| ---- | -------------------------------------- | ---------------------------------------------------------- |
| 3    | `computeLayouts` (`builder.go:550`)    | No. It uses only the length.                               |
| 3b   | `tryCompressInodes` (`builder.go:533`) | Yes. It compresses each file to decide if it stays flat.   |
| 4    | `assignNIDs`                           | No. It uses the metadata sizes from steps 3 and 3b.        |
| 5    | `layoutDataBlocks` (`builder.go:688`)  | No. It uses the length, or the count of compressed blocks. |
| 6    | `writeMetadata` (`builder.go:747`)     | Yes, for the inline tail of a flat file.                   |
| 7    | `writeDataBlocks` (`builder.go:849`)   | Yes.                                                       |

Two facts make streaming possible:

1. For a flat file, steps 3, 4, and 5 use only the length. After step 5, the
   builder knows where the full blocks go (`ino.startBlk * blockSize`) and
   where the inline tail goes (`ino.metaOff + 64`). One open of the file at
   step 7 can write both parts.
2. For a compressed file, `tryCompressFile` (`builder_compress.go:66`)
   compresses one pcluster group at a time. The group boundaries come from the
   size alone. The metadata size comes from the count of lclusters, which also
   comes from the size alone (`computeCompressedMetaSize`). Only two results
   need the bytes: the count of physical blocks for the file, and the decision
   to keep the file flat (`!anyBig`).

## The design

There are two kinds of lazy file. The builder decides the kind from the size
and the path alone, with the same test that `tryCompressFile` uses at its
start:

| Kind      | Test                                                                                                                     | When the builder opens it |
| --------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------------- |
| Flat-only | Compression is off, or the algorithm is not LZ4 or Zstandard, or `size <= blockSize`, or `isIncompressible(path, size)`. | At step 7.                |
| Candidate | All other files.                                                                                                         | At step 3b.               |

### Flat-only files

1. **Store the source on the inode.** Add `open func() (io.ReadCloser, error)`
   to `buildInode`. `ino.size` already holds the size.
2. **Use `ino.size`, not `len(ino.data)`, for layout.** Replace each
   `len(ino.data)` that means "the file size" in `computeLayouts`,
   `layoutDataBlocks`, `writeInode` (`builder.go:773`), and
   `writeDataBlocks`. Symlinks keep the target in `ino.data`, and `ino.size`
   already equals its length, so this change does not affect them.
3. **Write the inode header without the tail.** In `writeInode`, if
   `ino.open != nil`, write the 64-byte inode and skip the inline tail.
4. **Write the data at step 7.** For each flat-only lazy inode with
   `ino.size > 0`:
   1. Call `open`.
   2. Copy the full blocks to `ino.startBlk * blockSize`, through one
      `blockSize` buffer. For `InodeFlatPlain`, pad the last block with zeros,
      as `writeDataBlocks` does today.
   3. For `InodeFlatInline`, copy the tail bytes to `ino.metaOff + 64`.
   4. Read one more byte. If the read does not return `io.EOF`, return the
      size error.
   5. Close the reader.

### Candidate files

1. **Open a spool at the start of step 3b.** The spool is one temp file, from
   `os.CreateTemp` in the spool directory. `Build` removes it before it
   returns, on success and on error.
2. **Compress from the reader.** Change `tryCompressFile` so that it reads
   each group from an `io.Reader` into one reused buffer, with
   `io.ReadFull`. The group loop and the index entries do not change. Each
   physical block that the loop makes today goes to the end of the spool, and
   not into `blocks`. This is true for both kinds of group:
   - A compressed group writes its right-aligned compressed blocks.
   - An incompressible group writes each lcluster as one raw block, padded
     with zeros.
3. **Replace `blocks [][]byte`.** In `compressedFileData`, replace `blocks`
   with the spool offset of the first block and the count of blocks.
   `layoutCompressedBlocks` (`builder_compress.go:335`) uses the count.
   `writeCompressedBlocks` (`builder_compress.go:348`) copies the blocks from
   the spool to `ino.startBlk * blockSize`, through one `blockSize` buffer.
4. **Keep the spool for a flat fallback.** If no group of the file benefits
   (`!anyBig`), the file stays flat, as today. In this case every group was
   incompressible, so the spool already holds the raw file data, padded to
   blocks. Keep the spool offset on the inode. At step 7, copy the file from
   the spool, and not from `open`. The tail for `InodeFlatInline` is the first
   `size % blockSize` bytes of the last spool block. As a result, the builder
   still calls `open` once.
5. **Check the size.** After the last group, read one more byte. If the read
   does not return `io.EOF`, return the size error.

The spool is at most the size of the data area of the image. Memory for
compression is one group buffer, `(K + 1) * blockSize` bytes, and the output
of one `compressGroup` call. Pass a reused destination slice to
`EncodeAll` and to `lz4.CompressBlock`, so that each group does not allocate a
new one.

### `AddFromFS`

Move `AddFromFS` to the lazy path. Use `info.Size()` and `fsys.Open(p)`. After
this change, a file that changes size between the walk and `Build` gives the
size error. Put this fact in the doc comment of `AddFromFS`.

### Documentation

- `README.md`: add a short example of `AddFileFunc`.
- `CHANGELOG.md`: add entries for `AddFileFunc`, `WithSpoolDir`, and the new
  behavior of `AddFromFS`.

## Testing

Use table-driven tests, as the repository does today.

`TestAddFileFuncMatchesAddFile` builds each case twice, once with `AddFile`
and once with `AddFileFunc`, and compares the bytes. It also runs `Validate`
on the result and reads each file back through `Open`. Use a block size of
4096, and use these files:

| Case            | Content                                                | Size          | What it covers                                      |
| --------------- | ------------------------------------------------------ | ------------- | --------------------------------------------------- |
| empty           | none                                                   | 0             | `open` is not called.                               |
| tiny            | text                                                   | 1             | Flat inline, tail only.                             |
| max inline      | text                                                   | 4096 − 64     | The largest tail that fits in the inode block.      |
| over inline     | text                                                   | 4096 − 63     | The tail does not fit. Flat plain.                  |
| one block       | text                                                   | 4096          | Flat plain, no tail. Not a candidate.               |
| block plus one  | text                                                   | 4097          | The smallest candidate.                             |
| multi block     | text                                                   | 3 × 4096 + 17 | Many groups and a tail.                             |
| random          | random bytes, `.bin` name                              | 1 MiB + 5     | A candidate that falls back to flat from the spool. |
| mixed groups    | 64 KiB text, then 64 KiB random                        | 128 KiB + 9   | Compressed and PLAIN groups in one file.            |
| known extension | text, `.png` name                                      | 64 KiB        | Flat-only because of `isIncompressible`.            |
| large           | text                                                   | 10 MiB + 5    | The copy loops over many buffers.                   |
| mixed tree      | All of the above, in nested directories, with symlinks | —             | The inode order and the offsets between files.      |

Run the table three times: with no compression, with
`CompressionAutoLZ4`, and with `CompressionZstd`.

Other tests:

| Test                           | What it proves                                                                                                              |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `TestAddFileFuncShortRead`     | A reader that returns `size − 1` bytes gives an error that names the path. Run it for a flat-only file and for a candidate. |
| `TestAddFileFuncLongRead`      | A reader that returns `size + 1` bytes gives the same error. Run it for both kinds.                                         |
| `TestAddFileFuncOpenError`     | An error from `open` comes out of `Build`, wrapped with the path.                                                           |
| `TestAddFileFuncOpensOnce`     | Each `open` runs exactly once, for both kinds and for the flat fallback. Zero-size files do not call `open`.                |
| `TestAddFileFuncOneReaderOpen` | No more than one reader is open at a time.                                                                                  |
| `TestSpoolRemoved`             | The spool directory is empty after `Build`, on success and after a read error.                                              |
| `TestAddFileFuncMemory`        | See below.                                                                                                                  |
| `TestAddFromFSLazy`            | `AddFromFS` over an `fstest.MapFS` gives the same bytes as v0.6.1 gave for the same tree. Keep a golden file from v0.6.1.   |

`TestAddFileFuncMemory` is the acceptance test for this spec. It builds 1 GiB
of content, in 1024 files of 1 MiB each, from a reader that generates
compressible text. Each `open` call runs `runtime.GC`, then reads
`runtime.MemStats.HeapAlloc`, and keeps the largest value. The largest value
must stay below 64 MiB. Run it with no compression and with Zstandard. If it
is slow, gate it with `testing.Short`.

`TotalAlloc` is not a correct measure here. It counts every allocation, also
the ones that the collector already freed. The test measures live heap, which
is what the goal limits.

## Release

Release this change as v0.7.0. The API only grows, but `AddFromFS` has a new
failure mode and now writes a temp file, so a minor version is correct.

## The consumer change in objgit

After v0.7.0, objgit changes `snapshot.Ensure`:

- It calls `AddFileFunc` for each blob, with `blob.Size` and a function that
  returns `blob.Reader()`.
- It passes `WithSpoolDir` with the same temp directory that it uses for the
  image.
- It no longer reads blob content during the tree walk.

The objgit snapshot tests stay the same. `TestEnsureDeterministic` also
proves that the image bytes do not change when objgit moves to the new API.
