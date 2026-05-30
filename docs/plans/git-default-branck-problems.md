# Fix: clone leaves `warning: remote HEAD refers to nonexistent ref, unable to checkout`

## Context

Cloning a mirror of `golang/go` from objgitd downloads every object and resolves all
deltas, then aborts the checkout with:

```
warning: remote HEAD refers to nonexistent ref, unable to checkout
```

**Root cause — a `main` vs `master` HEAD mismatch that is never healed.**
`loadOrInit` (`cmd/objgitd/git_protocol.go:198`) initializes _every_ new repository
with `git.Init(..., git.WithDefaultBranch(refs/heads/main))`, so its `HEAD` file is
the symbolic ref `ref: refs/heads/main`. Receive-pack only writes the branch refs the
client pushed (`updateReferences` in `receivepack.go:287`) and **never touches HEAD**.
When you push a project whose default branch is not `main` — `golang/go` uses
`master` — the repo ends up with `refs/heads/master` (plus release branches) but a
`HEAD` that still points at the nonexistent `refs/heads/main`. HEAD dangles forever.

Confirmed empirically (both reproduced this turn):

- go-git v6.0.0-alpha.4's advertiser (`plumbing/transport/serve.go:96` `addReferences`)
  resolves HEAD's symbolic target and, on `ErrReferenceNotFound`, **drops HEAD from the
  advertisement entirely** (no `symref=HEAD:...` capability). Pointing HEAD at an
  existing branch instead produces the correct `symref=HEAD:refs/heads/master`.
- A real bare repo (`git init --bare -b main`, then push only `master`) reproduces the
  exact client warning over `file://`. So the trigger is purely the dangling HEAD;
  it is not a go-git wire bug.

This is a generic correctness gap, not specific to `golang/go`: any pushed repo whose
default branch ≠ `main` is unclonable-to-worktree. Real git hosts (GitHub/GitLab/Gitea)
repoint HEAD on push; objgitd does not.

**Intended outcome:** after a push, and for repos already sitting in the bucket, HEAD
resolves to a branch that exists, so `git clone` checks out a worktree with no warning.

## Approach

Add an idempotent **heal HEAD** step and call it (a) on every load and (b) after every
push. Healing only acts when HEAD is symbolic _and_ its target is missing; a detached
HEAD, an already-valid HEAD, or a branch-less repo is left untouched. This fixes future
pushes immediately and lets repos already broken in the bucket (e.g. `golang/go`)
recover on their next clone — one `HEAD` rewrite, then self-correcting — without a
re-push.

### New helpers (in `cmd/objgitd/git_protocol.go`, next to `loadOrInit`)

```go
// ensureHEAD repoints a repo's HEAD at an existing branch when its symbolic target
// is missing. objgitd inits every repo with HEAD -> refs/heads/main, but pushing a
// project whose default branch differs (golang/go uses master) leaves HEAD dangling:
// clients fetch every object yet cannot check out a worktree. Idempotent and best-
// effort; a detached/valid HEAD or a branch-less repo is left alone.
func ensureHEAD(st storage.Storer) error {
    head, err := st.Reference(plumbing.HEAD)
    if err != nil { return err }
    if head.Type() != plumbing.SymbolicReference { return nil }       // detached: leave
    if _, err := st.Reference(head.Target()); err == nil { return nil } // already valid
    else if !errors.Is(err, plumbing.ErrReferenceNotFound) { return err }
    target, err := pickDefaultBranch(st)
    if err != nil || target == "" { return err }                       // no branches: leave
    return st.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, target))
}

// pickDefaultBranch chooses a branch to point HEAD at: prefer refs/heads/main, then
// master, then trunk; otherwise the lexicographically smallest branch (deterministic).
func pickDefaultBranch(st storage.Storer) (plumbing.ReferenceName, error) { ... IterReferences, IsBranch ... }
```

### Wiring (minimal, centralized)

1. **New method `(d *daemon) load(repoPath string) (storage.Storer, error)`** — wraps the
   existing `d.loader.Load(&url.URL{Path: repoPath})` and, on success, calls
   `ensureHEAD`, logging (not failing) on heal error so a clone is never broken by a
   transient write failure. Returns the loader's error verbatim (preserves
   `transport.ErrRepositoryNotFound` → 404 semantics).

2. **Replace the five direct read-path `d.loader.Load` calls with `d.load`:**
   - `git_protocol.go:147` (git:// upload-pack), `:157` (upload-archive)
   - `ssh.go:192` (ssh upload-pack), `:204` (upload-archive)
   - `http.go:190` (`resolve`, the shared read path for both info/refs advertise and RPC)

3. **`loadOrInit` (`git_protocol.go:184`)** — call `d.load` instead of `d.loader.Load`
   for its found path, so the receive-pack advertise phase and pre-push load also heal.
   The create path is unchanged (fresh repo has no branches → `pickDefaultBranch` returns
   "" → HEAD stays `main` until a branch is pushed).

4. **Post-push heal in `(d *daemon) receivePack` (`hooks.go:88`)** — after
   `receivePackStreaming` returns `nil`, call `ensureHEAD(st)` (log on error). This makes
   HEAD correct the moment a first push creates branches, so the write lands during a
   push (a write op) rather than during the first subsequent clone.

No changes to the vendored go-git fork (`receivepack.go`) are needed.

## Critical files

- `cmd/objgitd/git_protocol.go` — add `ensureHEAD` + `pickDefaultBranch` + `d.load`; route `loadOrInit` and the two git:// read sites through `d.load`.
- `cmd/objgitd/ssh.go` — two read sites → `d.load`.
- `cmd/objgitd/http.go` — `resolve` read site (line 190) → `d.load`.
- `cmd/objgitd/hooks.go` — `receivePack` calls `ensureHEAD` after a successful receive.

## Tests

Follow the **xe-go:go-table-driven-tests** skill (per project memory) and the existing
`tt` table style; reuse `runGit`/`tryGit`/`seedRepo` from `git_protocol_test.go`.

1. **Unit (fast, no git binary)** — `ensureHEAD`/`pickDefaultBranch` over a memfs
   `filesystem.Storage`: dangling `HEAD->main` with only `master` present → HEAD repoints
   to `master`; `main` present → unchanged; detached HEAD → unchanged; no branches →
   unchanged; main+master both present → prefers `main`.
2. **End-to-end (gated on `git` on PATH)** — drive the daemon (HTTP and/or SSH like the
   existing protocol tests): create a repo and push a single `master` branch (no `main`),
   then `git clone` it and assert the worktree checked out (clone exit 0, expected file
   present, no "nonexistent ref" warning) and that `info/refs` advertises
   `symref=HEAD:refs/heads/master`.

## Verification

- `go build ./...` and `go test ./...` (and `go test ./cmd/objgitd/...` with `git` on PATH).
- Manual against the live setup (local Garage at `:3903`, bucket `xe-git-repos`,
  `SSH_BIND=:2222`): with `golang/go` already in the bucket, run
  `git clone ssh://localhost:2222/github.com/golang/go.git` and confirm it checks out
  `master` with no warning. The first clone heals HEAD (one `HEAD` write); subsequent
  clones find HEAD already valid.
