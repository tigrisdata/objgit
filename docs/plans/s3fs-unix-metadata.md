# Plan: Optional Unix-permission metadata in s3fs

## Context

`internal/s3fs/` is a go-billy filesystem mapped onto Tigris (S3) storage,
consumed today by `cmd/objgitd` to back git repositories. It never reads or
writes POSIX attributes: every file reports mode `0666`, directories `ModeDir`,
uid/gid are absent, and `PutObject` carries no user metadata
(`internal/s3fs/fileinfo.go`, `internal/s3fs/file.go`).

`docs/reference/how-tigris-fs-unix-metadata.md` defines a convention for storing
Unix attributes as `x-amz-meta-*` headers (uid, gid, mode, rdev, mtime,
`--symlink-target`). This change implements that convention in s3fs as an
**opt-in** feature with three session knobs: **uid**, **gid**, and **umask**.

### Constraints / assumptions

- **Off by default.** When disabled, behavior is byte-for-byte what it is today.
  `objgitd` itself never sets the option — the git protocol does not surface
  POSIX attributes, so there's no win in enabling the feature for repo storage.
  Other consumers of `internal/s3fs` (current or future) can opt in.
- **s3fs deals in numeric uid/gid.** The package does not resolve names→IDs or
  IDs→names itself; it exposes optional helper functions and lets callers decide
  whether/how to resolve.
- Scope = **create-time write + read** (uid/gid/mode/mtime). No chmod/chown
  writeback (`billy.Change`) and no symlink/device support in this pass — those
  are noted as follow-ups.

## Approach

### 1. New package `internal/s3fs/unixmeta`

New file `internal/s3fs/unixmeta/unixmeta.go` implementing the doc verbatim:

- `Attrs` struct, `PosixMode(os.FileMode) uint32`, `GoFileMode(uint32) os.FileMode`,
  `Encode(Attrs) map[string]string`, `Decode(meta map[string]string, defaults Attrs) Attrs`.
- Provide optional, caller-invoked helpers (the package itself never calls them;
  callers decide whether to resolve names):
  - `LookupUID(name string) (uint32, error)` — `user.Lookup`, fall back to parsing
    `name` as a decimal uint32.
  - `LookupGID(name string) (uint32, error)` — `user.LookupGroup`, same fallback.
    No reverse (uid/gid → name) resolution is provided.
- Table-driven tests `unixmeta_test.go`: PosixMode/GoFileMode round-trip across
  file/dir/symlink/setuid/sticky; Encode→Decode round-trip; malformed-header
  tolerance; missing-key-keeps-default.

### 2. Opt-in config on `S3FS` (`internal/s3fs/filesystem.go`)

Add a nil-able config (nil = disabled, preserving current behavior):

```go
type unixMetaConfig struct { uid, gid uint32; umask os.FileMode }

type S3FS struct {
    client *storage.Client
    bucket string
    root, separator string
    unixMeta *unixMetaConfig // nil => feature off
    // ...existing tempfs fields...
}

type Option func(*S3FS)
func WithUnixMetadata(uid, gid uint32, umask os.FileMode) Option

func NewS3FS(client *storage.Client, bucket string, opts ...Option) (billy.Filesystem, error)
```

Variadic options keep the existing `cmd/objgitd` caller compiling unchanged.
`Chroot` must copy `unixMeta` onto the new `*S3FS` so chrooted views inherit the
session config.

### 3. Write path (`internal/s3fs/file.go`)

Thread `*unixMetaConfig` into `newS3WriteFile` and `newS3MultipartUploadFile`
(plumbed from `OpenFile` in `internal/s3fs/basic.go`). In each `Close()`:

- If config is nil → unchanged (no `Metadata`).
- Else set `PutObjectInput.Metadata = unixmeta.Encode(unixmeta.Attrs{UID, GID,
  Mode: 0o666 &^ umask, Mtime: time.Now()})`. (Multipart: `Metadata` on
  `CreateMultipartUploadInput` at construction — `CompleteMultipartUpload`
  cannot attach user metadata.)

### 4. Read path (`internal/s3fs/basic.go`, `internal/s3fs/fileinfo.go`)

- Extend `simpleFileInfo` with an optional `sys *FileStat` field; `Sys()` returns
  the pointer when set, `nil` otherwise. `FileStat{UID, GID uint32}` lets
  consumers read the raw numeric ownership and resolve to names themselves.
- Add `newFileInfoFromHead(name, head, cfg)` that, when `cfg != nil`, runs
  `unixmeta.Decode(head.Metadata, defaults)` with defaults `{UID: cfg.uid,
  GID: cfg.gid, Mode: 0o666 &^ cfg.umask, Mtime: head.LastModified}` and builds
  a fully populated `simpleFileInfo`. When `cfg == nil`, keep the current
  `0666` path by delegating to `newFileInfo`.
- Wire this into `Stat` (`internal/s3fs/basic.go`). `Lstat` already delegates
  to `Stat`.
- **ReadDir** (`internal/s3fs/dir.go`): `ListObjectsV2` does not return user
  metadata, so list entries keep default modes; full attributes come from
  `Stat`. (`ls` stats entries for the long format.) Documented limitation;
  avoids an N-Head fan-out.

### 5. Consumer wiring

No consumer in this repo currently opts in:

- `cmd/objgitd` calls `s3fs.NewS3FS(client, *bucket)` with no options. Git
  packfile/loose-object storage gains nothing from POSIX metadata, so adding
  `-fs-*` flags to objgitd would be ceremony without payoff. The variadic
  signature means the call site does not change.

External callers (or a future objgit binary that exposes a POSIX-shaped surface)
opt in with:

```go
s3fs.NewS3FS(client, bucket, s3fs.WithUnixMetadata(uid, gid, umask))
```

and resolve any name strings to numeric IDs with
`unixmeta.LookupUID` / `unixmeta.LookupGID` before passing them in.

## Files touched

- `internal/s3fs/unixmeta/unixmeta.go` (new), `internal/s3fs/unixmeta/unixmeta_test.go` (new)
- `internal/s3fs/filesystem.go` — config + `Option` + `WithUnixMetadata`
- `internal/s3fs/chroot.go` — propagate `unixMeta` to the chrooted `*S3FS`
- `internal/s3fs/basic.go` — plumb config into `OpenFile`; decode in `Stat`
- `internal/s3fs/file.go` — encode metadata in write/multipart `Close`
- `internal/s3fs/fileinfo.go` — `FileStat`, optional `sys` field, decode constructor

## Verification

- `go test ./internal/s3fs/...` — unixmeta round-trip + tolerance tests pass.
- `go test ./...` — full suite, including the git-protocol tests, still passes
  with the feature off (regression guard for the default path).
- `go build ./...` — `objgitd` still compiles.
- Manual (needs Tigris creds + `BUCKET`): a separate driver could open an
  `s3fs` with `WithUnixMetadata`, write a file, then `HeadObject` (via
  `tigris-objects` MCP / aws cli) and confirm `x-amz-meta-uid/gid/mode/mtime`
  are present and correct; read it back and confirm `Stat().Mode()` reflects
  `0666 &^ umask`. With the feature off, the same flow should show **no**
  `x-amz-meta-*` headers.

## Deferred (not in this change)

- `billy.Change` (chmod/chown/chtimes) writeback via metadata-rewriting CopyObject.
- Symlink target / device-node (`rdev`) storage.
- Per-entry metadata in `ReadDir`.
