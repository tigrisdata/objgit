# Stream push-hook output to the client live

## Context

Today, push hooks (`-allow-hooks`) run **asynchronously** in a goroutine _after_
`transport.ReceivePack` has already sent the client its complete response. Hook
stdout/stderr is captured into `bytes.Buffer`s and only ever reaches `slog` —
the pusher never sees it. We want hook output to appear in the `git push`
client's terminal as `remote: ...` lines, streamed live as the hook writes them.

In the git smart protocol, server-side progress reaches the client over the
**sideband** channel (`sideband.ProgressMessage`, band 2), which the client
renders prefixed with `remote: `. The blocker: go-git's
`transport.ReceivePack` constructs the sideband `Muxer` _internally_, writes
report-status through it, and sends the closing flush-pkt **before returning**.
Once the client sees that flush-pkt it stops reading the sideband, so there is
no public seam to append `remote:` lines after the call, and no way to reach the
internal muxer during the call.

**Decision (confirmed with user): output-only, non-rejecting.** Hooks keep
post-receive semantics — they run after refs are updated and report-status is
sent; output streams but a hook failure cannot undo the push. This also flips
hooks from async to **synchronous**: the client now waits for hooks to finish
(bounded by `-hook-timeout`, default 60s) before the push completes. That wait
is inherent to streaming output live and is the accepted trade-off.

## Approach

Fork the small `ReceivePack` server command into objgitd with a hook seam, and
make hook execution stream to a band-2 writer instead of buffering. This mirrors
the repo's existing pattern of vendoring third-party internals (`internal/s3fs`,
`internal/kefkash`).

### 1. Fork `ReceivePack` with a post-update hook callback

New file `cmd/objgitd/receivepack.go`. Copy `transport.ReceivePack`
(receive_pack.go:30–168) plus its unexported helpers `sendReportStatus`,
`closeWriter`, `setStatus`, `referenceExists`, `updateReferences`
(receive_pack.go:170–256) verbatim. Everything else it touches is already
exported: `AdvertiseRefs`, `ProtocolVersion`, `ReceivePackService`,
`ErrUnsupportedVersion`, `ErrUpdateReference`, `packfile.UpdateObjectStorage`,
`sideband.NewMuxer`, the `capability`/`packp`/`pktline` packages.

Add **one parameter**: a callback invoked after `updateReferences` +
`sendReportStatus`, **before** the final `pktline.WriteFlush(w)`:

```go
func receivePackStreaming(ctx, st, r, w, opts,
    onUpdated func(progress io.Writer)) error
```

- When sideband was negotiated (`useSideband`), build a band-2 progress writer
  that wraps the same `*sideband.Muxer` (`writer`), e.g. a tiny adapter whose
  `Write` calls `mux.WriteChannel(sideband.ProgressMessage, p)`, and pass it to
  `onUpdated`. The flush already at line 159–163 then closes the stream after
  hooks have run.
- When sideband was **not** negotiated (client sent `no-progress` / no sideband
  cap — rare; `git push` negotiates `sideband-64k` by default), pass `nil`;
  hooks fall back to slog-only (no band to write to without corrupting
  report-status).
- Keep the `streamingStorer`/PackfileWriter and no-op-closer behavior intact:
  the fork calls `packfile.UpdateObjectStorage(st, rd)` on whatever storer the
  caller passes, exactly as today.

### 2. Rework `receivePack` to drive the fork synchronously (`hooks.go`)

`d.receivePack` (hooks.go:83) stops calling `transport.ReceivePack` and the
async goroutine. Instead it calls `receivePackStreaming` and runs hooks inside
the `onUpdated` callback:

- Take the **before** snapshot, call `receivePackStreaming(...)`.
- Inside `onUpdated(progress)`: take the **after** snapshot, `diffRefs`, and for
  each non-deleted update call `runHook` synchronously, passing `progress` (or
  `nil`). Snapshots still use `readStorer`.
- Because hooks now complete before `receivePackStreaming` returns, the
  `d.hookWG` goroutine machinery is no longer needed.

### 3. Stream hook stdout/stderr (`hooks.go`)

`runHook` (hooks.go:131) gains a `progress io.Writer` parameter:

- Replace the `outBuf`/`errBuf` `bytes.Buffer`s wired into `interp.StdIO` and
  `kefkash.CallHandler` with a writer that targets `progress` when non-nil.
  git does not distinguish hook stdout from stderr on the wire, so route both to
  the single band-2 `progress` writer.
- When `progress == nil` (no sideband), keep the current buffer→slog path so the
  log-only fallback still works.
- Continue logging the exit status via `slog` either way (the `interp.ExitStatus`
  / "hook: finished" record). Full output need not be duplicated into the log
  once it streams; a short summary line is enough.

### 4. HTTP needs explicit flushing (`http.go`)

git:// and SSH write through a live socket, so band-2 packets reach the client
immediately. HTTP buffers in `net/http`, so for output to appear _live_ rather
than at the end, each band-2 write must be flushed. In `handleRPC`
(http.go:118–132), for the receive-pack path wrap `out` so writes call
`http.Flusher.Flush()` (when `w` implements it) after each pktline. A small
flush-on-write `io.Writer` wrapper around the `ResponseWriter`, still wrapped by
`ioutil.WriteNopCloser`, is enough. SSH (`ssh.go:202`) and git://
(`git_protocol.go:161`) call sites need no change beyond pointing at the new
`receivePack` signature (unchanged externally).

### 5. Remove the now-dead async machinery

- Drop `hookWG sync.WaitGroup` (git_protocol.go:65–66) and its
  `d.hookWG.Add/Done` use in `hooks.go`.
- Remove the shutdown drain in `main.go:149` (`go func(){ d.hookWG.Wait(); ... }`)
  and the surrounding `drained` plumbing. Hooks are synchronous now, so a push
  in flight already holds the connection until hooks finish; normal connection
  draining covers shutdown.
- Keep `allowHooks` and `hookTimeout` exactly as they are.

### 6. Docs

Update `CLAUDE.md` "Push hooks" section: hooks now run **synchronously** and
stream stdout/stderr to the client over sideband band 2 (`remote:` lines); they
still **cannot reject a push** (output-only, post-update); the client waits for
hooks (bounded by `-hook-timeout`); falls back to slog-only when the client
doesn't negotiate sideband. Note the `cmd/objgitd/receivepack.go` fork and why
(go-git keeps the muxer internal and flushes before returning).

## Critical files

- `cmd/objgitd/receivepack.go` — **new**, forked `receivePackStreaming` + helpers.
- `cmd/objgitd/hooks.go` — synchronous `receivePack`, streaming `runHook`, drop `hookWG` use.
- `cmd/objgitd/http.go` — flush-on-write wrapper for the receive-pack response.
- `cmd/objgitd/git_protocol.go` — remove `hookWG` field; call sites unchanged otherwise.
- `cmd/objgitd/main.go` — remove `hookWG` shutdown drain.
- `CLAUDE.md` — update the Push hooks section.

## Reuse / references

- `sideband.NewMuxer`, `sideband.ProgressMessage`, `Muxer.WriteChannel` —
  go-git v6 `plumbing/protocol/packp/sideband` (already an indirect dep).
- `snapshotRefs`/`diffRefs`/`runHooks` (hooks.go:38–126) — reused as-is bar the
  signature/flow changes above.
- `kefkash.CallHandler`/`FsysOpenHandler` etc. and the `mountfs`/`treefs`
  sandbox wiring in `runHook` — unchanged except where the output buffers are
  swapped for the progress writer.
- `ioutil.WriteNopCloser` (go-git) — keep wrapping the HTTP/ssh/git writers.

## Verification

- `go build ./...` and `go test ./...` (protocol + SSH tests gated on `git`/`ssh`
  on PATH).
- **Add a streaming assertion** to the hook tests: seed a repo whose
  `.objgit/hooks/receive-pack` echoes a sentinel line, push it with
  `-allow-hooks`, and assert the sentinel appears as a `remote:` line in the
  `git push` **stderr** — across HTTP, git://, and SSH. Reuse the existing
  table-driven harness (`runGit`/`seedRepo` in `git_protocol_test.go`).
- Manual: `./objgitd -bucket $BUCKET -http-bind :8080 -allow-push -allow-hooks`,
  push a repo with a hook that prints + `sleep`s, and confirm `remote:` lines
  appear incrementally (not all at once at the end) over HTTP, then repeat with
  `-ssh-bind` and `-git-bind`.
