# Push hooks (`hooks.go`)

When `-allow-hooks` is set, a successful `receive-pack` runs the repository
script at `.objgit/hooks/receive-pack`.

Hook output streams to the pushing client live, as `remote: ...` lines. Hooks
therefore run **synchronously**: the client waits for them to finish before
the push completes. `-hook-timeout` bounds that wait.

A hook **cannot reject a push**. It runs after the refs are updated and after
report-status is sent, which is post-receive semantics. Deleted branches are
skipped.

For the environment variables, the stdin format, and example scripts, see
[../usage/hooks.md](../usage/hooks.md).

## Why go-git is forked here

Live streaming needs a seam that go-git does not expose.
`transport.ReceivePack` builds the sideband `Muxer` internally, and it sends
the closing flush-pkt before it returns.

`cmd/objgitd/receivepack.go` holds a small fork, `receivePackStreaming`. The
fork adds one callback, `onUpdated(progress io.Writer, updates []refUpdate, acceptedAt time.Time)`,
which runs after report-status is attempted and before that final flush.

`progress` writes to the sideband `ProgressMessage` channel, which is band 2,
when the client negotiated sideband. Otherwise `progress` is `nil`. Hook
stdout and stderr are then buffered and logged through `slog` instead. The
exit status goes to `slog` in both cases.

## Which refs changed

All three transports call `d.receivePack` in `hooks.go`, which drives the
fork.

`transport.ReceivePack` does not report which refs it changed. The fork passes
the successful ref commands from the current request to `onUpdated`, with a
timestamp captured immediately after the ref update. Packed-ref writes check
each advertised old hash at the atomic commit point, including after a CAS
retry. If legacy loose-ref cleanup fails after the packed write, receive-pack
checks which updates are visible before dispatch. Shell hooks
run for updated branches, and push webhooks run for every updated ref with a
configured destination. Both run synchronously after the ref update and cannot
reject it. See [../usage/webhooks.md](../usage/webhooks.md) for delivery details.

HTTP needs one extra piece. It wraps its `ResponseWriter` in `flushWriter`
(`http.go`), a flush-on-write writer, so `net/http` buffering does not hold
the `remote:` lines back. git:// and SSH write to a live socket, so they need
no such wrapper.

## The sandbox (kefka)

The script is read from the tree of the pushed commit. A branch therefore
carries its own hook.

The script runs in a **kefka** virtual shell
(`tangled.org/xeiaso.net/kefka`). kefka is _not_ an OS sandbox. It is an
`mvdan.cc/sh` interpreter wired to a `billy.Filesystem`, plus a fixed registry
of commands, which here is coreutils only.

The sandbox filesystem is an `internal/mountfs` composite of two mounts:

| Mount  | Contents                                                                 |
| ------ | ------------------------------------------------------------------------ |
| `/src` | A lazy read-only `internal/treefs` view of the commit tree. Blobs are fetched on open, with no checkout to disk. |
| `/tmp` | A writable `memfs` for scratch. `HOME` and `TMPDIR` point here.           |

A write outside `/tmp` fails, and a redirect into `/src` aborts the script.

`internal/kefkash` vendors the unexported `billysh` handler wiring from kefka.
Its `OpenHandler` is adapted to permit writes, so `/tmp` redirections work.
The filesystem is what enforces the read-only `/src`.
