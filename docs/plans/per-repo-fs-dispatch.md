# Per-repo filesystem resolution + `{orgID}/{repoName}` paths (HTTP focus)

## Context

Today the `daemon` holds a single static `fs billy.Filesystem` (the whole bucket)
and a single `loader transport.Loader`, both built once in `main.go`. Every
transport passes a **raw, unvalidated, variable-depth** path straight into
`auth.Request.Repo` and into `load`/`loadOrInit`, which `Chroot`s the one bucket
fs by that path.

We want two coupled changes:

1. **Restrict repo paths to `{orgID}/{repoName}`** — `orgID` is an opaque
   reference a later API call will validate; for now it's accepted as-is. Paths
   that aren't exactly two segments are rejected. The `.git` suffix is stripped
   from the repo name (`org/repo.git` and `org/repo` resolve to the same repo;
   storage key `org/repo/`).
2. **Discover the billy filesystem per-repo via a pluggable hook**, and **pass
   the HTTP Basic-auth username/password into that hook** so a real backend can
   route an org to its own bucket/credentials based on who's calling. The
   default hook preserves today's behavior (chroot the one bucket fs, ignoring
   the credential).

**Scope:** this pass targets the **HTTP** transport. SSH is explicitly out of
scope. The shared resolution layer is transport-agnostic, so git:// and SSH get
only the mechanical edits needed to keep compiling (they pass an empty
credential); their auth semantics are unchanged.

## New package: `internal/repofs`

Transport-neutral, mirroring how `internal/auth` is structured. Imports only
`context`, `errors`, `path`, `strings`, and `go-billy/v6`.

```go
package repofs

var ErrInvalidPath = errors.New("repository path must be of the form {orgID}/{repoName}")

// RepoRef identifies a repository. OrgID is opaque (validated later); Name has
// any trailing ".git" stripped.
type RepoRef struct {
	OrgID string
	Name  string
}

// Path is the canonical storage/identity path "orgID/name".
func (r RepoRef) Path() string { return path.Join(r.OrgID, r.Name) }

// Parse trims surrounding slashes, requires exactly two non-empty segments,
// and strips a trailing ".git" from the name. OrgID is not otherwise validated.
func Parse(raw string) (RepoRef, error)

// Credential carries the HTTP Basic-auth username/password (zero value = none).
// Unvalidated; the Resolver decides what to do with it.
type Credential struct {
	Username string
	Password string
}

// Resolver maps a RepoRef (plus the caller's credential) to the
// billy.Filesystem rooted at that repository. This is the hook a real backend
// implements to route an org to its bucket.
type Resolver interface {
	Resolve(ctx context.Context, ref RepoRef, cred Credential) (billy.Filesystem, error)
}

// BucketResolver is the default Resolver: chroot one base filesystem (the whole
// bucket) to ref.Path(), ignoring the credential. Preserves current behavior.
type BucketResolver struct{ Base billy.Filesystem }
func (b BucketResolver) Resolve(_ context.Context, ref RepoRef, _ Credential) (billy.Filesystem, error) {
	return b.Base.Chroot(ref.Path())
}
```

`Parse` is the single validation path. Add unit tests for valid input,
missing/extra segments, empty segments, trailing slash, and `.git` stripping.

## `daemon` changes (`cmd/objgitd/git_protocol.go`)

Replace the `fs` and `loader` fields:

```go
type daemon struct {
	sysFS    billy.Filesystem // bucket-level storage (SSH host key); NOT repo-scoped
	resolver repofs.Resolver
	authz    auth.Authorizer
	allowHooks  bool
	hookTimeout time.Duration
}
```

Rewrite resolution to go through the hook (threading the credential), building
the storer per resolved fs. Reuse go-git's bare-repo detection
(`FilesystemLoader.load` returns `ErrRepositoryNotFound` when no `config` exists
at the chroot root):

```go
// storerFor returns the bare-repo storer rooted at fs, or
// transport.ErrRepositoryNotFound when none exists there.
func storerFor(fs billy.Filesystem) (storage.Storer, error) {
	return transport.NewFilesystemLoader(fs, false).Load(&url.URL{Path: "/"})
}

func (d *daemon) load(ctx context.Context, ref repofs.RepoRef, cred repofs.Credential) (storage.Storer, error) {
	fs, err := d.resolver.Resolve(ctx, ref, cred)
	if err != nil { return nil, err }
	st, err := storerFor(fs)
	if err != nil { return nil, err }
	if err := ensureHEAD(st); err != nil { slog.Warn("...", "repo", ref.Path(), "err", err) }
	return st, nil
}

func (d *daemon) loadOrInit(ctx context.Context, ref repofs.RepoRef, cred repofs.Credential) (storage.Storer, error) {
	fs, err := d.resolver.Resolve(ctx, ref, cred)
	if err != nil { return nil, err }
	st, err := storerFor(fs)
	if err == nil { ensureHEAD(st); return st, nil }
	if !errors.Is(err, transport.ErrRepositoryNotFound) { return nil, err }
	st = filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
	if _, err := git.Init(st, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main"))); err != nil {
		return nil, fmt.Errorf("init bare repo: %w", err)
	}
	metrics.ReposCreated()
	slog.Info("created repository", "repo", ref.Path())
	return st, nil
}
```

The old `d.fs.Chroot(repoPath)` step is gone — `Resolve` returns the repo-root
fs directly, so resolution happens once per request.

## HTTP transport (`cmd/objgitd/http.go` + `main.go`) — primary work

Replace the suffix-dispatch `ServeHTTP` with an `http.ServeMux` (built by a new
`d.httpHandler()` method, wired in `main.go` as the server `Handler`). With a
fixed two-segment path the wildcards the old code couldn't use now work:

- `GET /{orgID}/{repoName}/info/refs`
- `POST /{orgID}/{repoName}/git-upload-pack`
- `POST /{orgID}/{repoName}/git-receive-pack`

Handlers read `r.PathValue("orgID")`/`r.PathValue("repoName")`, build the ref via
`repofs.Parse(path.Join(orgID, repoName))`, and 400 on `ErrInvalidPath`.
ServeMux 404s anything that isn't exactly two segments before the suffix, so the
shape is enforced for free.

`resolve` extracts the Basic-auth credential and threads it through:

```go
func credFromRequest(r *http.Request) (auth.Credential, repofs.Credential) {
	if u, p, ok := r.BasicAuth(); ok {
		return auth.BasicAuth{Username: u, Password: p}, repofs.Credential{Username: u, Password: p}
	}
	return auth.Anonymous{}, repofs.Credential{}
}
```

(or keep the existing `auth` credential helper and build the `repofs.Credential`
inline). `resolve`, `handleInfoRefs`, `handleRPC`, and `d.receivePack` change
their `repoPath string` parameter to a `repofs.RepoRef`; `resolve` passes the
`repofs.Credential` to `load`/`loadOrInit`. Logging/hook context uses
`ref.Path()`. Remove the variable-depth comment block and the now-unused
`strings` import if it drops out.

## git:// and SSH — mechanical only (out of scope)

`git_protocol.go handle` and `ssh.go handleSSH` must adapt to the new
`load`/`loadOrInit` signatures: parse their raw path with `repofs.Parse`
(rendering `ErrInvalidPath` in their own dialect — pktline error / stderr+exit)
and pass an empty `repofs.Credential{}`. `ssh.go`'s host-key load switches from
`d.fs` to `d.sysFS`. No further redesign of these transports.

## `main.go` changes

- Keep building the base bucket fs (`fsys`) as today.
- `d := &daemon{ sysFS: fsys, resolver: repofs.BucketResolver{Base: fsys}, authz: ..., allowHooks: ..., hookTimeout: ... }` — drop the `loader` field.
- HTTP server `Handler: d.httpHandler()` instead of `Handler: d`.
- Drop the `transport.NewFilesystemLoader` call; remove the `transport` import
  from `main.go` if it becomes unused.

## Behavioral note / migration

Stripping `.git` and requiring an org changes the storage key from `repo.git/`
to `org/repo/`. Repos created under the old layout won't resolve under the new
scheme. Acceptable for the current stage; no migration is in scope.

## Tests

- New `internal/repofs/repofs_test.go` — table-driven `Parse` cases (and a tiny
  `BucketResolver.Resolve` check that it chroots to `ref.Path()`).
- Update `cmd/objgitd/http_test.go` (and the shared helpers in
  `git_protocol_test.go` it reuses): remotes gain an org segment (`/test.git`
  → `/acme/test.git`), and storage-key assertions drop `.git`
  (`/test.git/config` → `/acme/test/config`; `assertPackedRepo(t, fs,
"/acme/test")`). The git:// tests in `git_protocol_test.go` need the same path
  updates to keep passing.
- Optionally add an HTTP test that a single-segment path returns 404 and that a
  Basic-auth credential reaches a stub resolver.

## Verification

```text
go build ./...
go test ./internal/repofs/...
go test -run TestSmartHTTP ./cmd/objgitd/...   # requires git on PATH
go test ./cmd/objgitd/...
```

End-to-end against a real bucket:

```text
./objgitd -bucket $BUCKET -http-bind :8080 -allow-push
git clone http://user:pass@localhost:8080/acme/demo.git   # creates acme/demo/ on first push; user/pass reach the resolver
git clone http://localhost:8080/demo.git                  # single segment -> 404
```
