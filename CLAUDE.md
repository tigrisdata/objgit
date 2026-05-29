# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`objgitd` is a single-binary Git server that stores repositories as objects in a Tigris/S3 bucket instead of on a local filesystem. It speaks three transports against the same backend:

- **Smart HTTP** (`-http-bind`, default `:8080`) — primary transport. Carries an HTTP Basic credential into the auth seam.
- **git://** (`-git-bind`, default `:9418`) — unauthenticated TCP, opt-in.
- **SSH** (`-ssh-bind`, default off) — public-key transport, opt-in. Host key persisted in the bucket.

All three funnel authorization through one pluggable `internal/auth.Authorizer` (see [The auth seam](#the-auth-seam-internalauth)).

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
./objgitd -bucket $BUCKET -ssh-bind :2222 -allow-push   # git clone ssh://git@host:2222/repo.git
```

SSH tests additionally need `ssh` and `ssh-keygen` on PATH (skipped otherwise);
run them with `go test -run TestSSH ./cmd/objgitd/...`.

`flagenv` maps `-allow-push` → `ALLOW_PUSH`, `-bucket` → `BUCKET`, etc. Tigris client credentials come from the standard AWS SDK chain (`AWS_PROFILE` etc.).

## Architecture

### The `daemon` is the shared backend

`cmd/objgitd/main.go` constructs one `*daemon` holding `(fs billy.Filesystem, loader transport.Loader, authz auth.Authorizer)` (plus the hooks fields) and serves it through all three transports concurrently under an `errgroup`. Repository resolution, authorization, and **create-on-first-push** all live on `*daemon` (`loadOrInit` in `git_protocol.go`) so every transport behaves identically.

- `cmd/objgitd/git_protocol.go` — git:// TCP server: `Serve` → `handle` decodes a `packp.GitProtoRequest`, then dispatches to `transport.UploadPack` / `UploadArchive` / `ReceivePack`. Also holds the shared `operationFor(service)` helper (receive-pack → `auth.Write`, else `auth.Read`).
- `cmd/objgitd/http.go` — `*daemon` implements `http.Handler` directly. Dispatch is by **URL suffix** (`/info/refs`, `/git-upload-pack`, `/git-receive-pack`) because repo paths are variable-depth and `http.ServeMux` wildcards can't capture a prefix before a fixed suffix. Smart-HTTP uses the same go-git server commands with `StatelessRPC: true` (and `AdvertiseRefs: true` for `GET /info/refs`).
- `cmd/objgitd/ssh.go` — SSH server (gliderlabs/ssh): `newSSHServer` builds the server and host key; `handleSSH` is the per-session dispatcher, a sibling of `handle` (see [Git over SSH](#git-over-ssh-sshgo)).

### Two subtle protocol points

1. **`streamingStorer` in `git_protocol.go`** wraps the storer for git:// receive-pack to **hide its `PackfileWriter` capability**. `UpdateObjectStorage`'s `PackfileWriter` path uses `io.CopyBuffer` and only returns on `io.EOF`, which deadlocks over a persistent TCP socket (the client is waiting for report-status). Hiding the capability falls through to `Parser.Parse`, which knows the pack's end from the format itself. HTTP doesn't need this — request bodies have a real EOF — so HTTP keeps the faster PackfileWriter path. Trade-off: git:// pushes write loose objects (one S3 PUT per object).

2. **No-op closers everywhere.** `transport.UploadPack`/`ReceivePack` call `Close` on the reader (and sometimes the writer) between negotiation rounds. The git:// socket can't survive that, and the HTTP `ResponseWriter` doesn't implement `Close`. Wrap with `io.NopCloser` (reader) and `ioutil.WriteNopCloser` from `go-git/v6/utils/ioutil` (writer).

**SSH shares both gotchas with git://**, not HTTP: an `ssh.Session` is a persistent bidirectional stream, so `handleSSH` uses `streamingStorer{}` for receive-pack and wraps the session in the same no-op closers. (HTTP keeps the faster `PackfileWriter` path because the request body has a real EOF.)

### The auth seam (`internal/auth`)

Every transport authorizes through one interface so a real authn/authz layer can
drop in without touching transport code. `internal/auth` is deliberately
transport-neutral — it imports only `context` and `golang.org/x/crypto/ssh` (for
the public-key wire type), **not** gliderlabs/ssh or go-git.

- `Authorizer.Authorize(ctx, auth.Request) auth.Decision`. The `Request` carries
  `Repo`, `Operation` (`Read`/`Write`), a `Credential`, and a `Transport` tag.
- `Credential` is a sum type (sealed via an unexported method): `Anonymous{}`
  (git://, or HTTP/SSH with nothing presented), `PublicKey{Key}` (SSH), and
  `BasicAuth{Username, Password}` (HTTP — **unvalidated**; the Authorizer owns
  the user store).
- `Decision` is `Allow` / `Deny` / `Unauthenticated`. `Unauthenticated` is the
  seam that lets HTTP issue a `401 WWW-Authenticate` challenge; SSH and git://
  treat anything other than `Allow` as denial.

Each transport's job is only to **collect** the credential, **map** the git
service to an `Operation` (via `operationFor`), and **render** the `Decision` in
its own dialect (pktline error / `401`/`403` / stderr + non-zero exit). The lone
implementation today is `auth.AllowAnonymous{AllowWrite}`: read for everyone,
write only when set — wired in `main.go` as `AllowAnonymous{AllowWrite: *allowPush}`,
so `-allow-push` is now just this default's config rather than a field on `daemon`.

### Git over SSH (`ssh.go`)

A third sibling to `git_protocol.go` / `http.go`. Like git:// (and unlike wish's
`git/git.go`, which execs the real `git-upload-pack` binary), objgitd answers the
protocol **natively** with the same `transport.*` functions — no `git` binary, no
on-disk checkout. `handleSSH` mirrors `handle`: `s.Command()` is already
shlex-split by gliderlabs/ssh, so `gitServiceFor(cmd[0])` selects the service, the
repo path is `strings.TrimPrefix(cmd[1], "/")` (so `ssh://host/foo.git` and
scp-style `host:foo.git` resolve the same), and the session is the protocol
stream (reader + writer).

- **Connect vs. authorize.** `PublicKeyHandler` returns `true` for every key — it
  must be set or the server won't offer pubkey auth at all — and authorization
  happens per-command via `d.authz` (`Cred: auth.PublicKey{Key: s.PublicKey()}`).
- **Host key** lives in the bucket at `.objgit/ssh_host_ed25519_key`
  (`loadOrCreateHostKey`): generated ed25519 on first start, reused after, so no
  host-key-changed warnings across restarts. No local-disk dependency.
- Receive-pack goes through **`d.receivePack`** (not `transport.ReceivePack`), so
  push hooks fire over SSH too.
- Protocol v2 (`GIT_PROTOCOL` via `s.Environ()`) is intentionally not forwarded
  yet; v0/v1 is sufficient.

### Push hooks (`hooks.go`, sandboxed via kefka)

When `-allow-hooks` is set, a successful `receive-pack` runs the repository's
`.objgit/hooks/receive-pack` script. The hook output is **streamed to the
pushing client live**, rendered as `remote: ...` lines, so hooks run
**synchronously**: the client waits for them to finish (bounded by
`-hook-timeout`) before the push completes. They still **cannot reject a push** —
they run after refs are updated and report-status is sent (post-receive
semantics). Deleted branches are skipped.

The streaming forces a small fork of go-git's `transport.ReceivePack` into
`cmd/objgitd/receivepack.go` (`receivePackStreaming`): go-git constructs the
sideband `Muxer` internally and sends the closing flush-pkt before returning, so
there is no public seam to inject `remote:` progress. The fork adds one
`onUpdated(progress io.Writer)` callback, invoked after report-status but before
that final flush; `progress` writes to the sideband `ProgressMessage` channel
(band 2) when the client negotiated sideband, or is `nil` otherwise (hooks then
fall back to `slog`-only output). All three transports call `d.receivePack`
(`hooks.go`), which drives the fork: because `transport.ReceivePack` does not
report which refs it changed, it snapshots branch refs before and after and
diffs them (`snapshotRefs`/`diffRefs`) inside `onUpdated`, then runs each hook
synchronously, streaming through `progress`. **HTTP** additionally wraps its
`ResponseWriter` in a flush-on-write writer (`flushWriter` in `http.go`) so
`net/http` buffering doesn't hold the `remote:` lines back; git:// and SSH write
through a live socket and need no such wrapper.

The script is read from the pushed commit's tree, so a branch carries its own
hook. It runs in a **kefka** virtual shell (`tangled.org/xeiaso.net/kefka`),
which is *not* an OS sandbox: it is an `mvdan.cc/sh` interpreter wired to a
`billy.Filesystem` plus a fixed registry of commands (coreutils only here). The
sandbox filesystem is an `internal/mountfs` composite of `/src` (a lazy
read-only `internal/treefs` view of the commit tree — blobs fetched on open, no
checkout to disk) and `/tmp` (a writable `memfs` for scratch; `HOME`/`TMPDIR`
point here). Writing anywhere but `/tmp` fails — and a redirect into `/src`
aborts the script. Hook stdout/stderr and exit status are logged via `slog`
only, never relayed to the pusher. `internal/kefkash` vendors kefka's
unexported `billysh` handler wiring (its `OpenHandler` is adapted to permit
writes so `/tmp` redirections work; the filesystem enforces read-only `/src`).

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
