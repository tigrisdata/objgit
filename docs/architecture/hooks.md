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
(`github.com/Xe/kefka`). kefka is _not_ an OS sandbox. It is an
`mvdan.cc/sh` interpreter wired to a `billy.Filesystem`, plus a fixed registry
of Go coreutils, WASM programs, and WASM-backed uutils.

The sandbox filesystem is an `internal/mountfs` composite of two mounts:

| Mount  | Contents                                                                                                         |
| ------ | ---------------------------------------------------------------------------------------------------------------- |
| `/src` | A lazy read-only `internal/treefs` view of the commit tree. Blobs are fetched on open, with no checkout to disk. |
| `/tmp` | A writable `memfs` for scratch. `HOME` and `TMPDIR` point here.                                                  |

A write outside `/tmp` fails, and a redirect into `/src` aborts the script.

`internal/kefkash` mirrors kefka's `billysh` handler wiring, which was
internal when it was vendored. Its `OpenHandler` is adapted to permit writes,
so `/tmp` redirections work. The filesystem is what enforces the read-only
`/src`.
`internal/mountfs` also provides read-only directory handles for the WASI
programs, which need to open `/` and directories below it to resolve paths.
Open files report their full mounted path so WASI can stat them after open.

`newHookShell` builds this sandbox, and `hookEnv` builds its environment.
`loadHookChanges` gives both of them the file changes of the update.
`newHookShell` writes these changes to `/tmp/objgit-changes.json`. It also
sets the interpreter's `Dir` to `/src`. Without this setting, interp
copies the host working directory of the daemon into `$PWD`.

## Commands outside kefka

`newHookShell` registers three commands that kefka does not have:

| Command                            | Package              | Notes                                                                                          |
| ---------------------------------- | -------------------- | ---------------------------------------------------------------------------------------------- |
| `kustomize`                        | `internal/kustomize` | An embedded WASI kustomize, tracked with Git LFS. Always registered.                           |
| `kube:apply`, `tekton:pipelinerun` | `internal/kube`      | Real commands when `d.kube` is set by `-allow-kubernetes`. Otherwise stubs that name the flag. |

The kustomize adapter is not kefka's generic `wasmcommand`, for two
reasons. It sets the guest `PWD` to the shell directory, because Go's
wasip1 port takes its working directory from `PWD`. Its wazero runtime also
closes a running instance when the context ends, so `-hook-timeout` stops a
long build. The module compiles once for each process, in about 4 seconds.
If the build embedded a Git LFS pointer, `Exec` returns an error that says so.

`internal/kube` is a small REST client, not client-go. `InCluster` reads
the ServiceAccount mount and reads the token again for each request, because
bound tokens rotate. Each command run takes one `Session`, which caches
discovery for each `apiVersion` until the run ends. The commands get a `kube.Origin`
from `hookOrigin`, and not from the `OBJGIT_*` variables, because a script
can change those. The audit log (`kube: applied`, `kube: created`) and the
PipelineRun metadata use this origin. `splitAPIVersion` refuses an
`apiVersion` that is not a plain group and version, because both go into
request paths. `kubetest` is a fake
API server for the tests. `TestCluster` runs the same calls against a real
cluster when `OBJGIT_TEST_KUBE_*` is set. See
[../usage/kubernetes-hooks.md](../usage/kubernetes-hooks.md).

## The interactive shell (`shell.go`)

The SSH command `sh <repo> [branch]` opens the same sandbox as an interactive
shell. It uses `newHookShell` and `hookEnv`, so the two environments cannot
drift apart. `shellTarget` fills the environment from the tip of the branch.
`OBJGIT_OLD_SHA` is the first parent of that commit, as if the commit was
just pushed.

`golang.org/x/term` gives line editing and history over the session.
`syntax.Parser.InteractiveSeq` reads statements from it. Each statement gets a
new copy of the hook stdin line and its own `-hook-timeout`.

Two points are easy to get wrong:

- x/term returns `io.EOF` for Ctrl-C, the same as for Ctrl-D. It also drops
  the partial line and the rest of the read. `shellInput` therefore changes
  each Ctrl-C byte to `^C` and Enter before x/term reads it. `shellLines` then
  discards that line.
- A fatal error, such as a write into `/src`, sets `Runner.Exited`. A hook
  script stops at this error. The interactive shell prints the error and
  continues. It stops only for `exit`, Ctrl-D, or a closed session.
