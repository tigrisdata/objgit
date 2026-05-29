# Plan: post-receive hooks for objgitd (sandboxed via kefka)

## Context

`objgitd` serves bare git repos stored as objects in S3 (via `internal/s3fs`,
exposed as a `billy.Filesystem`). We want to run a user-supplied shell script
after a successful push, with a checkout of the repo visible at `/src`, executed
in a safe sandbox.

The execution primitive is `tangled.org/xeiaso.net/kefka` — but kefka is **not an
OS sandbox**. It is a virtual `bash` interpreter (`mvdan.cc/sh/v3`) wired to a
`billy.Filesystem` plus a fixed registry of built-in commands (~50 coreutils).
Safety comes from two facts: a script can only touch the `billy.Filesystem` we
hand it, and it can only run commands we register. There is no arbitrary binary
execution, no network, no host access. This is a clean fit because objgit already
models repos as `billy.Filesystem`, so the `/src` checkout never touches host disk.

### Decisions (confirmed with user)

- **Trigger:** push only (`receive-pack`). Hook file: `.objgit/hooks/receive-pack`.
- **Timing:** post-receive, **async** — refs are already updated, the client gets
  its response immediately, the hook runs in the background and **cannot reject**
  the push.
- **Command set:** coreutils only (no WASM `python3`/`jq`/`qjs`/`rg`).
- **Output:** slog only (key `"err"`), never relayed to the pusher.
- **`/src`:** a **lazy read-only `TreeFS`** (`billy.Filesystem` view over the
  commit tree) — no eager copy, scales to large repos.
- **`/tmp`:** a writable in-memory `memfs` for scratch, so scripts can write
  temp files and use redirections. Mounted alongside `/src` via a composite fs.
- **billysh:** manually vendored into objgit (kefka's `internal/billysh` is not
  importable). User approved.

### Known limitations (document in code + docs/plan)

- `/src` is read-only; only `/tmp` is writable. Writes/redirections outside
  `/tmp` (e.g. into `/src`) fail. Scripts should use `/tmp` (and `$TMPDIR`) for
  scratch. A copy-on-write `/src` is a future enhancement if ever needed.
- Hooks are advisory/async: failures, parse errors, and timeouts are logged only.
- git:// transport is unauthenticated; anyone who can push can run a (sandboxed)
  hook. Acceptable given kefka's confinement, but note it.

## Components

### 1. `internal/kefkash` — vendored shell wiring (new package)

Copy the four handler constructors + `readOnlyFile` shim from kefka's
`internal/billysh/billysh.go` (~81 lines, depends only on public symbols:
`registry.Impl` and its exported `Resolve`/`Chdir`/`Pwd`, `billy.Filesystem`,
`mvdan.cc/sh/v3/interp`). Add a header comment crediting the kefka source
(mirror the `internal/s3fs` "vendored from" convention). Exports:

- `CallHandler(reg, fsys, stdout, stderr) interp.CallHandlerFunc`
- `FsysStatHandler(reg, fsys) interp.StatHandlerFunc`
- `FsysOpenHandler(reg, fsys) interp.OpenHandlerFunc`
- `FsysReadDirHandler(reg, fsys) interp.ReadDirHandlerFunc2`

### 2. `internal/treefs` — lazy read-only git-tree filesystem (new package)

A `billy.Filesystem` backed by `(*object.Tree, storer.EncodedObjectStorer)`.
Reads resolve paths on demand via `tree.File(path)` (blob) / `tree.Tree(path)`
(subdir); blob contents stream from `(*object.Blob).Reader()`. All write methods
return a sentinel read-only error (`billy`'s `ErrReadOnly` or a local one).

Implements the full `billy.Filesystem` interface:

- Read: `Open`, `OpenFile` (read flags only; reject `O_CREATE`/`O_WRONLY`),
  `Stat`, `Lstat`, `ReadDir`, `Join`, `Root`.
- `Chroot(path)`: resolve the subtree and return a `TreeFS` rooted there (cheap,
  no copy); or return read-only error if path missing.
- Write/mutate (`Create`, `Rename`, `Remove`, `MkdirAll`, `Symlink`, `TempFile`):
  return read-only error.
- File handle: a `billy.File` wrapping an `io.ReadCloser` + `bytes`/seek over the
  blob; `Write`/`Truncate` return read-only error. Map `filemode.Executable`/
  `Regular`/`Symlink` to `os.FileMode` for `Stat`.

Verified go-git v6 APIs (`go-git/v6 v6.0.0-alpha.4`):
`object.GetCommit(st, hash) (*Commit, error)`, `(*Commit).Tree()`,
`(*Tree).File(path)`, `(*Tree).Tree(path)`, `(*Tree).Files() *FileIter`,
`tree.Entries`, `(*Blob).Reader()`, `filemode` constants.

### 2b. `internal/mountfs` — path-prefix composite filesystem (new package)

A `billy.Filesystem` that dispatches by leading path component to one of several
mounted filesystems, so the kefka sandbox sees both `/src` and `/tmp`:

- `/src/...` → the read-only `TreeFS` (§2).
- `/tmp/...` → a writable `memfs.New()`.
- The root listing (`ReadDir("/")`) reports `src` and `tmp` as dirs; other paths
  return not-exist.

Implementation: a small struct holding `map[string]billy.Filesystem` keyed by
top-level mount name. Each method strips the mount prefix, delegates to the
matching fs (translating the path), and re-prefixes results (e.g. `ReadDir`,
`Stat` names, `Join`). Write methods on `/tmp` succeed (memfs); on `/src` they
return the TreeFS read-only error. `Chroot` into a mount delegates to that fs.
This keeps `TreeFS` and `memfs` simple and isolates the routing concern.

### 3. `cmd/objgitd/hooks.go` — orchestration (new file)

- `type refUpdate struct { Name plumbing.ReferenceName; Old, New plumbing.Hash }`
- `snapshotRefs(st storage.Storer) (map[plumbing.ReferenceName]plumbing.Hash, error)`
  — `st.IterReferences()` (Storer embeds `ReferenceStorer`), keep
  `HashReference && Name().IsBranch()`.
- `diffRefs(before, after) []refUpdate` — created (Old=zero), updated (hash
  differs), deleted (New=zero).
- `(d *daemon) runHooks(repoPath, service string, st storage.Storer, updates []refUpdate)`:
  for each update where `!New.IsZero()` (skip deletions):
  1. `c := object.GetCommit(st, u.New)`; `tree := c.Tree()`.
  2. `hookFile, err := tree.File(".objgit/hooks/receive-pack")`; if not found →
     debug log, continue (no-op).
  3. Read hook script bytes from `hookFile`.
  4. Build the sandbox fs: `mountfs{ "/src": treefs.New(tree, st), "/tmp":
memfs.New() }` and mount it as the kefka root. cwd = `/src` via
     `interp.Dir("/src")` + `reg.Chdir(fsys, "/src")`.
  5. Construct kefka runner (see §4), per-hook `ctx` =
     `context.WithTimeout(context.Background(), d.hookTimeout)` (independent of the
     request ctx so it isn't cancelled when the response finishes).
  6. Parse with `syntax.NewParser(syntax.Variant(syntax.LangBash))`, `sh.Run`.
  7. Capture stdout/stderr to buffers; log via slog (`"err"` key, exit code from
     `interp.ExitStatus`). Feed git-style `old new ref\n` on stdin for
     compatibility, and inject env vars.

  Env vars: `OBJGIT_REPO`, `OBJGIT_SERVICE=receive-pack`, `OBJGIT_REF`
  (`refs/heads/...`), `OBJGIT_BRANCH` (short), `OBJGIT_OLD_SHA`, `OBJGIT_NEW_SHA`,
  plus kefka base env (`HOME=/tmp`, `PWD=/src`, `TMPDIR=/tmp`,
  `PATH=/usr/bin:/bin`, `IFS`).

### 4. kefka runner construction (in `hooks.go`)

```go
reg := registry.New()
coreutils.Register(reg)
_ = reg.Chdir(fsys, "/src")
var sh *interp.Runner
mw := func(next interp.ExecHandlerFunc) interp.ExecHandlerFunc {
    return func(ctx context.Context, args []string) error { return reg.Exec(ctx, fsys, sh, args) }
}
sh, err = interp.New(
    interp.Env(expand.ListEnviron(envPairs...)),
    interp.StdIO(stdin, &outBuf, &errBuf),
    interp.ExecHandlers(mw),
    interp.CallHandler(kefkash.CallHandler(reg, fsys, &outBuf, &errBuf)),
    interp.StatHandler(kefkash.FsysStatHandler(reg, fsys)),
    interp.OpenHandler(kefkash.FsysOpenHandler(reg, fsys)),
    interp.ReadDirHandler2(kefkash.FsysReadDirHandler(reg, fsys)),
    interp.Dir("/src"),
)
```

### 5. Integration into the two receive-pack call sites

Add a `daemon` method that wraps `transport.ReceivePack`, and route the **two
real receive call sites** through it — NOT the advertisement phases:

- `cmd/objgitd/git_protocol.go` ~L138 (git://, currently
  `transport.ReceivePack(ctx, streamingStorer{Storer: st}, r, conn, req)`).
- `cmd/objgitd/http.go` ~L128 (`handleRPC`). **Do not** touch `handleInfoRefs`
  (AdvertiseRefs) or the git:// advertise path.

```go
func (d *daemon) receivePack(ctx context.Context, rpStorer, readStorer storage.Storer,
    repoPath string, r io.ReadCloser, w io.Writer, req *transport.ReceivePackRequest) error {
    var before map[plumbing.ReferenceName]plumbing.Hash
    if d.allowHooks { before, _ = snapshotRefs(readStorer) }
    err := transport.ReceivePack(ctx, rpStorer, r, w, req)
    if err != nil || !d.allowHooks { return err }
    after, serr := snapshotRefs(readStorer)
    if serr != nil { slog.Error("hook: snapshot after push failed", "err", serr); return nil }
    if updates := diffRefs(before, after); len(updates) > 0 {
        d.hookWG.Add(1)
        go func() { defer d.hookWG.Done(); d.runHooks(repoPath, "receive-pack", readStorer, updates) }()
    }
    return nil
}
```

- git:// call site passes `rpStorer = streamingStorer{Storer: st}` (preserves the
  deadlock fix) and `readStorer = st`.
- HTTP call site passes `rpStorer = st`, `readStorer = st`.

### 6. `daemon` struct + flags

`cmd/objgitd/git_protocol.go` — add fields:

```go
allowHooks  bool
hookTimeout time.Duration
hookWG      sync.WaitGroup
```

`cmd/objgitd/main.go` — new flags (kebab-case + flagenv, mirroring `-allow-push`):

- `-allow-hooks` (bool, default false) → `ALLOW_HOOKS`
- `-hook-timeout` (duration, default `60s`) → `HOOK_TIMEOUT`

Wire into the `daemon`. In the shutdown path (after `g.Wait()` / in the shutdown
goroutine), drain in-flight hooks: `select` on `d.hookWG` done vs a bounded
deadline so SIGTERM lets running hooks finish rather than killing them.

## Files

- `internal/kefkash/kefkash.go` — new (vendored billysh wiring).
- `internal/treefs/treefs.go` (+ `file.go`) — new (lazy read-only tree FS).
- `internal/mountfs/mountfs.go` — new (path-prefix composite fs: `/src` + `/tmp`).
- `cmd/objgitd/hooks.go` — new (snapshot/diff/runHooks + runner).
- `cmd/objgitd/git_protocol.go` — daemon fields; route git:// receive through `receivePack`.
- `cmd/objgitd/http.go` — route `handleRPC` receive through `receivePack`.
- `cmd/objgitd/main.go` — flags + daemon wiring + shutdown drain.
- `go.mod` — promote `tangled.org/xeiaso.net/kefka` to a direct require; `go mod
tidy` pulls `mvdan.cc/sh/v3` (network needed on first build).
- `docs/plans/git-hooks.md` — short design doc per repo convention.

## Verification

1. `go build ./...` and `go vet ./...`.
2. Unit tests:
   - `internal/treefs`: build an in-memory repo (go-git memfs storer), commit a
     tree with nested dirs + an executable file, assert `Open`/`ReadDir`/`Stat`
     return correct content/modes and writes return read-only errors.
   - `cmd/objgitd`: `diffRefs`/`snapshotRefs` table tests (created/updated/deleted).
3. End-to-end (gated by `exec.LookPath("git")`, reuse `seedRepo`/`runGit` helpers
   from `git_protocol_test.go`):
   - Start a `daemon` with `allowHooks=true` over an in-memory/test S3FS.
   - Push a repo containing `.objgit/hooks/receive-pack` that runs a coreutils
     command reading `/src` and writing scratch to `/tmp` (e.g.
     `cat /src/README.md && echo built > /tmp/out && cat /tmp/out`); assert the
     `/tmp` write succeeds and a write into `/src` fails.
   - Assert the push succeeds immediately, then (synchronize on `hookWG`) assert
     the hook ran by capturing slog output (inject a `*slog.Logger` writing to a
     buffer) and checking the logged stdout/exit code.
   - Negative: push without a hook file → no-op; push a hook that exits non-zero
     → push still succeeds, error logged.
4. Manual: run `./objgitd -bucket $BUCKET -allow-push -allow-hooks`, push a repo
   with a hook, confirm structured logs show hook stdout and exit status.
