# Plan: Git over SSH + a generic auth seam for objgitd

> **For agentic workers:** implement task-by-task in build order (bottom of this
> doc). Steps are checkboxes (`- [ ]`) for tracking. TDD where a real `git`
> client can drive it; gate those tests on `exec.LookPath("git")` like the
> existing suites.

**Goal:** add a third Git transport — **SSH** (`ssh://host/repo.git`) — and, in
the same change, introduce a transport-neutral **`internal/auth`** authorization
seam that all three transports (git://, HTTP, SSH) funnel through, replacing the
scattered `-allow-push` checks with one pluggable `Authorizer`.

## Context

`objgitd` serves repositories out of a Tigris/S3-backed billy filesystem over
**git:// (TCP)** (`cmd/objgitd/git_protocol.go`) and **smart-HTTP**
(`cmd/objgitd/http.go`). Both wrap one `*daemon` (`fs`, `loader`, `loadOrInit`,
`receivePack`) and answer the protocol **natively** with go-git's
`transport.UploadPack` / `ReceivePack` / `UploadArchive` — there is **no `git`
binary and no on-disk checkout**.

This is the key difference from charmbracelet/wish's `git/git.go` (the reference
the user started from): wish `exec`s the real `git-upload-pack` binary against
an on-disk repo. We do **not** do that. The SSH handler is a third sibling to
`git_protocol.go` / `http.go`: read the requested command, resolve the repo,
hand the SSH session's stdin/stdout to the same `transport.*` functions. From
wish we keep only the SSH-side mechanics (command → service mapping, path
cleaning, session wiring).

SSH is, like git://, a **persistent bidirectional stream** (not HTTP's
request/response), so the two git:// protocol workarounds apply verbatim and the
HTTP path's assumptions do **not** — see [Two protocol gotchas](#two-protocol-gotchas-ssh--git-not-http).

### Why a generic auth seam

Each transport carries a different credential, and HTTP needs a challenge flow
the others don't:

| Transport | Credential it presents        | "please authenticate" mechanism      |
| --------- | ----------------------------- | ------------------------------------- |
| git://    | none (anonymous)              | n/a                                   |
| SSH       | public key (validated at connect) | handled in the SSH auth handshake |
| HTTP      | `Authorization: Basic …` / none | `401` + `WWW-Authenticate`          |

The transport must **not** validate credentials itself (a password is only
checkable against a user store the *policy* owns). Each transport's job shrinks
to three things: **collect** the raw credential, **map** the git service to a
read/write `Operation`, and **enforce** the returned `Decision` in its own
dialect. All real auth logic lives behind one interface, so the same mechanism
serves HTTP basic auth, SSH keys, and any future scheme (bearer tokens, mTLS).

Decisions from the user:

- **One generic `Authorizer` interface**, credential-agnostic on input, with a
  `Decision` expressive enough that HTTP can render a 401 challenge while
  SSH/git:// just allow-or-deny.
- **Retrofit all three transports now**, replacing the three scattered
  `-allow-push` checks with a single default authorizer
  `auth.AllowAnonymous{AllowWrite: *allowPush}`.
- **Stub permissive.** The only implementation today is `AllowAnonymous`; a real
  authn/authz layer plugs in later without touching transport code.
- **Host key persisted in the bucket** (`.objgit/ssh_host_ed25519_key`), so SSH
  needs no operator config and survives restarts without host-key-changed warnings.
- **Opt-in SSH.** New `-ssh-bind` flag defaults to `""` (disabled), matching git://.

Code/test conventions follow `xe-go:xe-go-style` and `xe-go:go-table-driven-tests`
(flagenv→flag, kebab-case flags, slog with `"err"` key, errgroup server
lifecycle, `t.Helper()` helpers, table-driven subtests with `tt`).

## Files

| Action | Path                          | Purpose                                                |
| ------ | ----------------------------- | ------------------------------------------------------ |
| Create | `internal/auth/auth.go`       | transport-neutral `Authorizer`, `Credential`, `Decision`, `AllowAnonymous` |
| Create | `internal/auth/auth_test.go`  | unit tests for `AllowAnonymous` decisions              |
| Create | `cmd/objgitd/ssh.go`          | SSH server, host key, command dispatch                 |
| Create | `cmd/objgitd/ssh_test.go`     | table-driven clone/push/deny/create/hook tests         |
| Edit   | `cmd/objgitd/git_protocol.go` | `authz` field on `daemon`; git:// goes through authz   |
| Edit   | `cmd/objgitd/http.go`         | HTTP goes through authz (Basic-auth parse, 401/403)    |
| Edit   | `cmd/objgitd/main.go`         | `-ssh-bind` flag, default authz, SSH errgroup listener |
| Edit   | `go.mod` / `go.sum`           | add `github.com/gliderlabs/ssh` (direct)               |
| Edit   | `CLAUDE.md`                   | document the auth seam + third transport               |

The `daemon` struct stays the shared backend. The `allowPush bool` field is
**removed** from `daemon` (the flag survives in `main.go` only, to build the
default authorizer); SSH methods live in `ssh.go`.

## internal/auth package

Transport-neutral on purpose: it imports only `context` and
`golang.org/x/crypto/ssh` (for the public-key wire type), **not** gliderlabs/ssh
or go-git. Transports translate their own world into `auth.Request` and act on
`auth.Decision`.

```go
package auth

import (
	"context"

	gossh "golang.org/x/crypto/ssh"
)

// Operation is the access a request needs. Transports map the git service:
// upload-pack/upload-archive → Read, receive-pack → Write.
type Operation int

const (
	Read Operation = iota
	Write
)

// Credential is what the client presented. Exactly one concrete type per
// scheme; a transport constructs the variant it can produce, or Anonymous.
type Credential interface{ isCredential() }

// Anonymous is "no credential presented" (git://, or HTTP/SSH with none).
type Anonymous struct{}

// PublicKey is an SSH public key. Uses x/crypto/ssh's type (gliderlabs/ssh
// keys satisfy it) so this package stays free of the SSH server library.
type PublicKey struct{ Key gossh.PublicKey }

// BasicAuth is an HTTP Basic credential. Unvalidated — the Authorizer owns the
// user store.
type BasicAuth struct{ Username, Password string }

// (BearerToken{Token string} can be added later without changing the interface.)

func (Anonymous) isCredential() {}
func (PublicKey) isCredential() {}
func (BasicAuth) isCredential() {}

// Request is a transport-neutral authorization request.
type Request struct {
	Repo      string
	Operation Operation
	Cred      Credential
	Transport string // "git", "ssh", "http" — for policy/logging
}

// Decision is the outcome. Unauthenticated is the seam that lets HTTP issue a
// 401 challenge; SSH and git:// treat it as Deny.
type Decision int

const (
	Deny Decision = iota
	Allow
	Unauthenticated
)

// Authorizer decides whether a request may proceed. This is the seam a real
// authn/authz layer plugs into later.
type Authorizer interface {
	Authorize(ctx context.Context, req Request) Decision
}

// AllowAnonymous is the permissive default: read for everyone, write only when
// AllowWrite is set. "Dangerously allow everything the server is configured to
// allow" — never more open than the -allow-push gate. It ignores the credential
// entirely and never returns Unauthenticated.
type AllowAnonymous struct{ AllowWrite bool }

func (a AllowAnonymous) Authorize(_ context.Context, req Request) Decision {
	if req.Operation == Write && !a.AllowWrite {
		return Deny
	}
	return Allow
}
```

`daemon` grows `authz auth.Authorizer` (added in `git_protocol.go`); `main.go`
sets `authz: auth.AllowAnonymous{AllowWrite: *allowPush}`.

A small shared mapper (in `cmd/objgitd`, e.g. top of `git_protocol.go`) keeps the
service→operation rule in one place:

```go
func operationFor(service string) auth.Operation {
	if service == transport.ReceivePackService {
		return auth.Write
	}
	return auth.Read // upload-pack, upload-archive
}
```

## Retrofitting git:// and HTTP

### git:// — `handle` (git_protocol.go)

Replace the receive-pack-only `if !d.allowPush` check with an authz check that
covers **all** services. git:// has no credential, so `Cred: auth.Anonymous{}`.

```go
// before resolving the repo for any service:
op := operationFor(req.RequestCommand)
if d.authz.Authorize(ctx, auth.Request{
	Repo: req.Pathname, Operation: op, Cred: auth.Anonymous{}, Transport: "git",
}) != auth.Allow {
	_, _ = pktline.WriteError(conn, fmt.Errorf("access denied"))
	return fmt.Errorf("access denied for %q (%s)", req.Pathname, req.RequestCommand)
}
```

Then the existing per-service `Load` / `loadOrInit` + dispatch is unchanged.
(`Unauthenticated` is impossible with `AllowAnonymous`; the `!= Allow` test
treats it as denial regardless, which is correct for git://.)

### HTTP — `resolve` (http.go)

`resolve` currently takes `(w, service, repoPath)` and checks `d.allowPush`.
Change it to also take the `*http.Request` so it can read the credential, and to
render the decision in HTTP terms:

```go
func (d *daemon) resolve(w http.ResponseWriter, r *http.Request, service, repoPath string) (storage.Storer, bool) {
	cred := credFromRequest(r) // BasicAuth{} if Authorization: Basic present, else Anonymous{}
	switch d.authz.Authorize(r.Context(), auth.Request{
		Repo: repoPath, Operation: operationFor(service), Cred: cred, Transport: "http",
	}) {
	case auth.Allow:
		// fall through
	case auth.Unauthenticated:
		w.Header().Set("WWW-Authenticate", `Basic realm="objgit"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return nil, false
	default: // Deny
		http.Error(w, "access denied", http.StatusForbidden)
		return nil, false
	}

	if service == transport.ReceivePackService {
		st, err := d.loadOrInit(repoPath) // create-on-first-push, now gated by Allow above
		if err != nil { /* 500, as today */ }
		return st, true
	}
	st, err := d.loader.Load(&url.URL{Path: repoPath}) // 404 on ErrRepositoryNotFound, as today
	...
}

func credFromRequest(r *http.Request) auth.Credential {
	if u, p, ok := r.BasicAuth(); ok {
		return auth.BasicAuth{Username: u, Password: p}
	}
	return auth.Anonymous{}
}
```

Update the two `resolve(w, service, repoPath)` call sites in `handleInfoRefs` and
`handleRPC` to pass `r`. The `403`-on-disabled-push behavior is preserved
(`AllowAnonymous{AllowWrite:false}` → `Deny` → `403`), and create-on-first-push
still happens only after an `Allow`.

## ssh.go design

### Host key (persisted in the bucket)

`loadOrCreateHostKey(fs billy.Filesystem) (gossh.Signer, error)` reads
`.objgit/ssh_host_ed25519_key` (PEM) through `d.fs`. If absent: generate ed25519,
marshal to OpenSSH PEM (`gossh.MarshalPrivateKey` → `pem.EncodeToMemory`), write
it back via the filesystem, log `"created ssh host key"`, parse into a signer
(`gossh.ParsePrivateKey`). Use `billy.Filesystem` open/create — never local disk.

```go
const hostKeyPath = ".objgit/ssh_host_ed25519_key"
```

### Server construction

`newSSHServer(d *daemon, addr string) (*ssh.Server, error)`:

```go
signer, err := loadOrCreateHostKey(d.fs)
if err != nil {
	return nil, fmt.Errorf("ssh host key: %w", err)
}
srv := &ssh.Server{
	Addr:    addr,
	Handler: d.handleSSH, // func(ssh.Session)
	PublicKeyHandler: func(ctx ssh.Context, key ssh.PublicKey) bool {
		// Accept every key at connect; real authorization is per-command in
		// handleSSH via d.authz. Stash the key for the auth.Request.
		ctx.SetValue(pubKeyContextKey{}, key)
		return true
	},
}
srv.AddHostKey(signer)
return srv, nil
```

```go
type pubKeyContextKey struct{}
```

Setting `PublicKeyHandler` is what makes the server offer pubkey auth; returning
`true` unconditionally is the "accept the connection" half — the `Authorizer` is
the half that gates repo access.

### Command dispatch — `handleSSH(s ssh.Session)`

gliderlabs/ssh has already shlex-split the exec command, so `s.Command()` returns
e.g. `["git-upload-pack", "/foo/bar.git"]` — no manual parsing.

1. **Reject interactive / malformed.** If `len(s.Command()) != 2`: friendly line
   to `s.Stderr()` ("this is a git SSH endpoint; interactive shells are not
   supported") + `s.Exit(1)`.
2. **Map `cmd[0]` → service:**

   | `cmd[0]`             | service                          |
   | -------------------- | -------------------------------- |
   | `git-upload-pack`    | `transport.UploadPackService`    |
   | `git-upload-archive` | `transport.UploadArchiveService` |
   | `git-receive-pack`   | `transport.ReceivePackService`   |

   Unknown → stderr error + `s.Exit(1)`.
3. **Clean path:** `repoPath := strings.TrimPrefix(cmd[1], "/")` so
   `ssh://host/foo.git` and scp-style `host:foo.git` resolve identically.
4. **Authorize:**
   ```go
   key, _ := s.Context().Value(pubKeyContextKey{}).(ssh.PublicKey)
   var cred auth.Credential = auth.Anonymous{}
   if key != nil {
   	cred = auth.PublicKey{Key: key} // ssh.PublicKey satisfies gossh.PublicKey
   }
   if d.authz.Authorize(s.Context(), auth.Request{
   	Repo: repoPath, Operation: operationFor(service), Cred: cred, Transport: "ssh",
   }) != auth.Allow {
   	fmt.Fprintln(s.Stderr(), "access denied")
   	_ = s.Exit(1)
   	return
   }
   ```
5. **Resolve + dispatch.** Mirror `handle`; the session is reader and writer,
   wrapped exactly as git:// does (see gotchas):

```go
r := io.NopCloser(s)
w := ioutil.WriteNopCloser(s)

switch service {
case transport.UploadPackService:
	st, err := d.loader.Load(&url.URL{Path: repoPath})
	if err != nil {
		fmt.Fprintf(s.Stderr(), "repository %q not found\n", repoPath)
		_ = s.Exit(1)
		return
	}
	if err := transport.UploadPack(s.Context(), st, r, w, &transport.UploadPackRequest{}); err != nil {
		slog.Error("ssh upload-pack failed", "path", repoPath, "err", err)
	}

case transport.UploadArchiveService:
	st, err := d.loader.Load(&url.URL{Path: repoPath})
	if err != nil {
		fmt.Fprintf(s.Stderr(), "repository %q not found\n", repoPath)
		_ = s.Exit(1)
		return
	}
	if err := transport.UploadArchive(s.Context(), st, r, w, &transport.UploadArchiveRequest{}); err != nil {
		slog.Error("ssh upload-archive failed", "path", repoPath, "err", err)
	}

case transport.ReceivePackService:
	st, err := d.loadOrInit(repoPath)
	if err != nil {
		fmt.Fprintf(s.Stderr(), "cannot open repository %q\n", repoPath)
		_ = s.Exit(1)
		return
	}
	if err := d.receivePack(s.Context(), streamingStorer{Storer: st}, st, repoPath, r, w, &transport.ReceivePackRequest{}); err != nil {
		slog.Error("ssh receive-pack failed", "path", repoPath, "err", err)
	}
}
```

`s.Context()` satisfies `context.Context`, threading cancellation into the
transport calls. (Git protocol v2 over SSH arrives via the `GIT_PROTOCOL`
env in an `setenv` request; gliderlabs/ssh gates env with a `LocalPortForwarding`-
style allowlist — defaulting to v0/v1 negotiation is fine and matches what the
git:// path does when `ExtraParams` is empty.)

### Two protocol gotchas (SSH = git://, NOT HTTP)

SSH is a persistent stream like git://, so **both** git:// workarounds apply —
treating SSH like HTTP here is the failure mode:

1. **Hide `PackfileWriter` for receive-pack** — wrap the storer in the existing
   `streamingStorer{}` (in `git_protocol.go`) so `UpdateObjectStorage` takes the
   `Parser.Parse` path, not the `io.CopyBuffer`-until-`io.EOF` path that
   deadlocks on a live socket waiting for report-status. Same loose-objects
   trade-off as git:// (one S3 PUT per object).
2. **No-op closers** — `transport.*` calls `Close` on the reader (and sometimes
   the writer) between negotiation rounds; an `ssh.Session`'s real `Close` tears
   down the channel. Use `io.NopCloser(s)` and `ioutil.WriteNopCloser(s)`
   (`github.com/go-git/go-git/v6/utils/ioutil`).

Receive-pack dispatches through **`d.receivePack`** (the wrapper), not
`transport.ReceivePack`, so push hooks fire over SSH exactly as over git:// / HTTP.

## main.go changes

- Add flag near the others:
  ```go
  sshBind = flag.String("ssh-bind", "", "TCP address to listen on for the git-over-SSH protocol; empty disables it")
  ```
- Relax the "at least one transport" guard to include `*sshBind`.
- Build the daemon with the default authorizer and **no** `allowPush` field:
  ```go
  d := &daemon{
  	fs:          fsys,
  	loader:      transport.NewFilesystemLoader(fsys, false),
  	authz:       auth.AllowAnonymous{AllowWrite: *allowPush},
  	allowHooks:  *allowHooks,
  	hookTimeout: *hookTimeout,
  }
  ```
- Add `"ssh_bind", *sshBind` to the `"objgitd listening"` slog line.
- In the errgroup, when `*sshBind != ""`:
  ```go
  srv, err := newSSHServer(d, *sshBind)
  if err != nil {
  	slog.Error("can't create ssh server", "ssh_bind", *sshBind, "err", err)
  	os.Exit(1)
  }
  g.Go(func() error {
  	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, ssh.ErrServerClosed) {
  		return err
  	}
  	return nil
  })
  g.Go(func() error {
  	<-gCtx.Done()
  	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
  	defer cancel()
  	return srv.Shutdown(shutdownCtx)
  })
  ```
  (`gliderlabs/ssh` exposes `ErrServerClosed`, `ListenAndServe`, `Shutdown`
  mirroring `net/http`.)

## Tests

### internal/auth/auth_test.go (table-driven, no git needed)

`AllowAnonymous` decisions:

| AllowWrite | Operation | want   |
| ---------- | --------- | ------ |
| false      | Read      | Allow  |
| false      | Write     | Deny   |
| true       | Read      | Allow  |
| true       | Write     | Allow  |

One table, `tt`, asserting `(AllowAnonymous{tt.allowWrite}).Authorize(ctx, auth.Request{Operation: tt.op}) == tt.want`.

### Retrofit parity (existing suites)

The existing git:// and HTTP push-rejected-when-disabled tests must still pass
unchanged — they now exercise the authz path. If `git_protocol_test.go` /
`http_test.go` lack an explicit "anonymous read still works when push disabled"
case, add one to each so the retrofit's read-path is covered.

### ssh_test.go (table-driven, gated on `exec.LookPath("git")`)

Reuse `seedRepo` / `runGit` / `tryGit` from `git_protocol_test.go`. Helper
`startSSHServer(t, allowPush, allowHooks) (addr string, fs billy.Filesystem)`
(`t.Helper()`): memfs-backed `daemon` with `authz: auth.AllowAnonymous{AllowWrite: allowPush}`,
listen on `127.0.0.1:0`, serve `newSSHServer`'s server in a goroutine, return the
resolved addr. Skip the subtest if `ssh`/`ssh-keygen` are missing.

Client identity via env on `runGit`:
```go
// ssh-keygen -t ed25519 -N "" -f <tmp>/id_ed25519
env := append(os.Environ(),
	"GIT_SSH_COMMAND=ssh -i "+key+" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
	"GIT_TERMINAL_PROMPT=0",
)
```
URL: `ssh://git@127.0.0.1:<port>/test.git`.

Cases (one table, `tt`, subtests):

- **push then clone round-trips** (`allowPush: true`): push a `seedRepo` commit,
  assert `/test.git/config` in memfs, clone back, `git rev-parse HEAD` non-empty.
- **push creates repo on demand** (`allowPush: true`): push to a not-yet-existing
  path succeeds and creates the bare repo.
- **push rejected when disabled** (`allowPush: false`): push fails;
  `/test.git/config` never created (authz `Deny` for receive-pack).
- **fetch from missing repo fails**: clone of a nonexistent path exits non-zero.
- **hook fires on push** (`allowPush: true`, `allowHooks: true`): push a branch
  carrying `.objgit/hooks/receive-pack`; assert the hook's effect using the
  existing hook test's assertion pattern.

## Verification

1. `gofmt`/`goimports` clean; `go build ./...`.
2. `go test ./...` — `internal/auth`, git://, HTTP, and SSH suites all pass.
3. `go mod tidy` leaves `github.com/gliderlabs/ssh` (and `golang.org/x/crypto`)
   resolved with no unexpected churn.
4. Manual smoke against a real bucket:
   ```
   objgitd -bucket $BUCKET -ssh-bind :2222 -allow-push
   GIT_SSH_COMMAND='ssh -p 2222 -o StrictHostKeyChecking=no' \
     git clone ssh://git@localhost/smoke.git
   (cd smoke && git push ssh://git@localhost:2222/smoke.git main)  # creates + pushes
   git clone ssh://git@localhost:2222/smoke.git verify             # round-trips
   ```
   Confirm a second run reuses the persisted host key (no host-key-changed warning),
   and that `-http-bind`/`-git-bind` still honor `-allow-push` through the shared authz.

## Build order

- [ ] **Task 1 — `internal/auth` package.** `auth.go` (interface, credentials,
  `Decision`, `AllowAnonymous`) + `auth_test.go` table. `go test ./internal/auth/...`.
- [ ] **Task 2 — `authz` on daemon + git:// retrofit.** Add `authz auth.Authorizer`
  to `daemon`, remove `allowPush` field, add `operationFor`, route `handle`
  through authz. Update `main.go` to construct `AllowAnonymous`. Existing git://
  tests pass.
- [ ] **Task 3 — HTTP retrofit.** `credFromRequest`, `resolve(w, r, service, repoPath)`
  with 401/403/Allow handling; update the two call sites. Existing HTTP tests pass;
  add the anonymous-read parity case.
- [ ] **Task 4 — Host key.** `loadOrCreateHostKey` over `d.fs`; unit test on memfs
  (first call creates+writes, second reads identical bytes).
- [ ] **Task 5 — SSH server + dispatch.** `newSSHServer`, `pubKeyContextKey`,
  `PublicKeyHandler`, `handleSSH` with the git://-style wrapping and authz check.
- [ ] **Task 6 — main.go SSH wiring.** `-ssh-bind` flag, errgroup listener +
  graceful shutdown, slog line. `go build -o objgitd ./cmd/objgitd`.
- [ ] **Task 7 — SSH tests.** `ssh_test.go` table per the cases above; TDD each case.
- [ ] **Task 8 — Docs.** Add the auth seam + SSH transport to `CLAUDE.md`.
