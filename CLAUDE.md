# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`objgitd` is a single-binary Git server that stores repositories as objects in a Tigris/S3 bucket instead of on a local filesystem. It speaks two transports against the same backend:

- **Smart HTTP** (`-http-bind`, default `:8080`) — primary transport, where auth middleware would wrap.
- **git://** (`-git-bind`, default `:9418`) — unauthenticated TCP, opt-in.

Module path: `tangled.org/xeiaso.net/objgit`. Go 1.26.

## Commands

```text
go build ./...                  # build everything
go build -o objgitd ./cmd/objgitd
go test ./...                   # full test suite
go test ./cmd/objgitd/...       # protocol tests (require `git` on PATH; skipped otherwise)
go test -run TestSmartHTTP ./cmd/objgitd/...

# Run locally. Flags can also come from env via flagenv (UPPER_SNAKE of the flag name).
# A .env file in CWD is auto-loaded by godotenv.
./objgitd -bucket $BUCKET -http-bind :8080 -allow-push
```

`flagenv` maps `-allow-push` → `ALLOW_PUSH`, `-bucket` → `BUCKET`, etc. Tigris client credentials come from the standard AWS SDK chain (`AWS_PROFILE` etc.).

## Architecture

### The `daemon` is the shared backend

`cmd/objgitd/main.go` constructs one `*daemon` holding `(fs billy.Filesystem, loader transport.Loader, allowPush bool)` and serves it through both transports concurrently under an `errgroup`. Repository resolution, the `allowPush` gate, and **create-on-first-push** all live on `*daemon` (`loadOrInit` in `git_protocol.go`) so both transports behave identically.

- `cmd/objgitd/git_protocol.go` — git:// TCP server: `Serve` → `handle` decodes a `packp.GitProtoRequest`, then dispatches to `transport.UploadPack` / `UploadArchive` / `ReceivePack`.
- `cmd/objgitd/http.go` — `*daemon` implements `http.Handler` directly. Dispatch is by **URL suffix** (`/info/refs`, `/git-upload-pack`, `/git-receive-pack`) because repo paths are variable-depth and `http.ServeMux` wildcards can't capture a prefix before a fixed suffix. Smart-HTTP uses the same go-git server commands with `StatelessRPC: true` (and `AdvertiseRefs: true` for `GET /info/refs`).

### Two subtle protocol points

1. **`streamingStorer` in `git_protocol.go`** wraps the storer for git:// receive-pack to **hide its `PackfileWriter` capability**. `UpdateObjectStorage`'s `PackfileWriter` path uses `io.CopyBuffer` and only returns on `io.EOF`, which deadlocks over a persistent TCP socket (the client is waiting for report-status). Hiding the capability falls through to `Parser.Parse`, which knows the pack's end from the format itself. HTTP doesn't need this — request bodies have a real EOF — so HTTP keeps the faster PackfileWriter path. Trade-off: git:// pushes write loose objects (one S3 PUT per object).

2. **No-op closers everywhere.** `transport.UploadPack`/`ReceivePack` call `Close` on the reader (and sometimes the writer) between negotiation rounds. The git:// socket can't survive that, and the HTTP `ResponseWriter` doesn't implement `Close`. Wrap with `io.NopCloser` (reader) and `ioutil.WriteNopCloser` from `go-git/v6/utils/ioutil` (writer).

### `internal/s3fs` — billy.Filesystem on Tigris

Vendored from Austin Poor's s3fs and adapted to **billy v6** and the Tigris `storage-go` client. Treats an S3 bucket as a filesystem so go-git's `filesystem.NewStorage` can store loose objects and packs against it.

The non-obvious piece is `tempfs.go`. go-git's streaming `PackWriter` creates a temp pack file, **immediately reopens the same path for reading**, and reads it back concurrently while writing — to build the index. S3 cannot do that on a single live object, so until the final `Rename` actually uploads, the bytes live in an in-memory `tempBuffer` registered on the `S3FS` by canonical key. `readAt` returns `(0, io.EOF)` past the current end so go-git's `syncedReader` can distinguish "no data yet" from a hard error and retry.

All S3 keys go through `S3FS.key` → `cleanPath` + leading-slash strip. Any new S3 op must funnel through there or chroot/path semantics will desync.

### `internal/slog.go`

Trivial JSON-handler init. The convention across the codebase is `slog` with `"err"` (not `"error"`) as the error key.

## Conventions

- Flags use **kebab-case** and are paired with `flagenv` for env fallback.
- Errgroup owns server lifecycle; `signal.NotifyContext` provides cancellation; HTTP shutdown uses a 10s `context.WithTimeout`.
- Tests are **table-driven with `tt`** and gated by `exec.LookPath("git")` when they shell out to a real git client (see `http_test.go`, `git_protocol_test.go`). Shared helpers (`runGit`, `tryGit`, `seedRepo`) live in `git_protocol_test.go` and are reused across the package.
- Plans for non-trivial work go in `docs/plans/` (see `git-http-protocol.md` for the style).

## Tigris / object storage notes

When working with Tigris buckets, access keys, or IAM, the `tigris-storage` skills are available. Tigris is S3-compatible; the client used here is `github.com/tigrisdata/storage-go` (a thin Tigris-aware wrapper that the s3fs layer talks to directly).
