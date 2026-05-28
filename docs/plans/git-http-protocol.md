# Plan: Git Smart HTTP protocol for objgitd

## Context

`objgitd` currently serves repositories out of a Tigris/S3-backed billy filesystem
over the **git:// (TCP)** protocol only (`cmd/objgitd/daemon.go`). git:// is
unauthenticated, hard to put behind a normal HTTP proxy, and not what most hosts
(or CI systems) expect. We want clients to be able to `clone`/`fetch`/`push` over
**HTTP smart protocol** (`https://host/repo.git`), which is the standard transport
and the natural place to bolt on auth later via middleware.

go-git v6's server commands are already HTTP-ready: `transport.UploadPack` and
`transport.ReceivePack` expose `AdvertiseRefs bool` and `StatelessRPC bool` knobs
that map exactly onto the two-endpoint smart-HTTP flow, and `transport.AdvertiseRefs`
emits the `# service=...\n` + flush "smart reply" prefix when told to. So this is
almost entirely HTTP plumbing around the same backend the git:// daemon already uses.

Decisions from the user:

- **No auth now** — expose a plain `http.Handler` so auth can be added as wrapping
  middleware later. Push stays gated by the existing global `-allow-push` flag.
- **Run both transports.** HTTP is the new primary (`-http-bind`, default on);
  git:// becomes opt-in via `-git-bind` (if unset, the git:// listener is not started).
- **Rename** the existing git:// files to `git_protocol.go` / `git_protocol_test.go`.

Code/test conventions follow the `xe-go:xe-go-style` and `xe-go:go-table-driven-tests`
skills (flagenv→flag, kebab-case flags, slog with `"err"` key, errgroup server
lifecycle, `t.Helper()` helpers, table-driven subtests with `tt`).

## Files

| Action | Path                                                              | Purpose                                 |
| ------ | ----------------------------------------------------------------- | --------------------------------------- |
| Rename | `cmd/objgitd/daemon.go` → `cmd/objgitd/git_protocol.go`           | git:// server (unchanged logic)         |
| Rename | `cmd/objgitd/daemon_test.go` → `cmd/objgitd/git_protocol_test.go` | git:// tests (unchanged)                |
| Create | `cmd/objgitd/http.go`                                             | smart-HTTP handler on `*daemon`         |
| Create | `cmd/objgitd/http_test.go`                                        | table-driven HTTP clone/push tests      |
| Edit   | `cmd/objgitd/main.go`                                             | flags + errgroup two-listener lifecycle |

The `daemon` struct stays the shared backend (`fs`, `loader`, `allowPush`,
`loadOrInit`). git:// methods live in `git_protocol.go`; HTTP methods in `http.go`.
Use `git mv` for the renames so history is preserved.

## http.go design

Make `*daemon` implement `http.Handler`. Dispatch on URL **suffix** (the same way
`git-http-backend` does), since repo paths are multi-segment and end in `.git`
(e.g. `/foo/bar.git/info/refs`) — `http.ServeMux` wildcards can't capture a
variable-depth prefix before a fixed suffix.

```go
func (d *daemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    p := r.URL.Path
    switch {
    case r.Method == http.MethodGet && strings.HasSuffix(p, "/info/refs"):
        d.handleInfoRefs(w, r, strings.TrimSuffix(p, "/info/refs"))
    case r.Method == http.MethodPost && strings.HasSuffix(p, "/git-upload-pack"):
        d.handleRPC(w, r, transport.UploadPackService, strings.TrimSuffix(p, "/git-upload-pack"))
    case r.Method == http.MethodPost && strings.HasSuffix(p, "/git-receive-pack"):
        d.handleRPC(w, r, transport.ReceivePackService, strings.TrimSuffix(p, "/git-receive-pack"))
    default:
        http.NotFound(w, r)
    }
}
```

### Reference discovery — `GET /{repo}/info/refs?service=git-(upload|receive)-pack`

- Read `service` from the query; reject anything that isn't the two known services
  with `400`.
- Resolve the repo, mirroring the git:// switch in `git_protocol.go`:
  - `git-upload-pack`: `d.loader.Load(&url.URL{Path: repo})`; on
    `transport.ErrRepositoryNotFound` → `404`.
  - `git-receive-pack`: if `!d.allowPush` → `403`; else `d.loadOrInit(repo)`
    (preserves git://'s "first push creates the repo" behavior — the advertise GET
    happens before the POST, so it must create here too).
- Set headers **before** writing the body:
  - `Content-Type: application/x-<service>-advertisement`
  - `Cache-Control: no-cache`
- Emit the advertisement by calling the matching server command with
  `AdvertiseRefs: true, StatelessRPC: true` (StatelessRPC=true makes
  `AdvertiseRefs` write the `# service=...` smart-reply prefix). Reader is `nil`
  (the advertise path returns before touching it); wrap the writer with
  `ioutil.WriteNopCloser(w)`:
  ```go
  transport.UploadPack(r.Context(), st, nil, ioutil.WriteNopCloser(w),
      &transport.UploadPackRequest{AdvertiseRefs: true, StatelessRPC: true,
          GitProtocol: r.Header.Get("Git-Protocol")})
  ```
  (and the `ReceivePack` equivalent for receive-pack).

### RPC — `POST /{repo}/git-(upload|receive)-pack`

- Resolve repo with the same load / loadOrInit / 403 / 404 rules as above.
- Body handling: if `Content-Encoding: gzip`, wrap `r.Body` in `gzip.NewReader`
  (git clients gzip the upload-pack request). Wrap the (possibly decompressed)
  body in `io.NopCloser` for the `io.ReadCloser` arg.
- Headers before body: `Content-Type: application/x-<service>-result`,
  `Cache-Control: no-cache`.
- Dispatch with `StatelessRPC: true` (and `AdvertiseRefs: false`) so the command
  skips advertisement and reads the negotiation straight from the POST body:
  ```go
  transport.UploadPack(r.Context(), st, body, ioutil.WriteNopCloser(w),
      &transport.UploadPackRequest{StatelessRPC: true,
          GitProtocol: r.Header.Get("Git-Protocol")})
  ```
- Pre-dispatch failures use real status codes; once the command starts writing,
  errors are only `slog.Error(... "err", err)`-logged (status already sent).

### Helpers / notes

- Reuse `ioutil` = `github.com/go-git/go-git/v6/utils/ioutil` for `WriteNopCloser`
  — no local nop-closer type needed.
- `transport.UploadPackService`/`ReceivePackService` are the string constants
  `"git-upload-pack"`/`"git-receive-pack"`, so they match both the `?service=`
  value and the URL suffixes with no conversion friction.
- Log each request with slog (service, repo path, remote) like the git:// `handle`.

## main.go changes

- Replace the `bind` flag:
  - `gitBind := flag.String("git-bind", "", "TCP address for the git:// protocol; empty disables it")`
  - `httpBind := flag.String("http-bind", ":8080", "TCP address for the git smart-HTTP protocol; empty disables it")`
  - Keep `-bucket`, `-allow-push`, `-slog-level`.
- Validate at least one of `gitBind`/`httpBind` is non-empty, else `slog.Error` + exit.
- Replace the single `d.Serve(ctx, ln)` call with an `errgroup.WithContext(ctx)`
  (`golang.org/x/sync/errgroup`, already in go.mod — promote to direct via
  `go mod tidy`) running, conditionally:
  - **git://**: if `*gitBind != ""`, `net.Listen` then `d.Serve(gCtx, ln)`.
  - **HTTP**: if `*httpBind != ""`, build `srv := &http.Server{Handler: d}`, run
    `srv.Serve(ln)` in one goroutine and a second goroutine that waits on
    `gCtx.Done()` then `srv.Shutdown(context.WithTimeout(...10s))`. Treat
    `http.ErrServerClosed` as clean shutdown.
- `slog.Info("objgitd listening", "git_bind", *gitBind, "http_bind", *httpBind, "bucket", ...)`.

## Tests — http_test.go (table-driven)

Same package, so reuse `runGit`/`tryGit` from `git_protocol_test.go`. Drive a real
git client against `httptest.NewServer(d)` over a `memfs`-backed `daemon`. Guard
with the existing `exec.LookPath("git")` skip.

Cases (one table, `tt`, subtests via `t.Run(tt.name, ...)`):

- **push then clone round-trips** (`allowPush: true`): push a seed commit to
  `<ts.URL>/test.git`, assert `/test.git/config` exists in the memfs, clone it
  back, `rev-parse HEAD` is non-empty.
- **push creates repo on demand** (`allowPush: true`): push to a path that does
  not exist yet succeeds and creates the bare repo (HTTP parity with
  `TestDaemonPushCreatesRepo`).
- **push rejected when disabled** (`allowPush: false`): push fails and
  `/test.git/config` is never created.
- **fetch from missing repo 404s**: clone of a nonexistent path fails.

Per-case fields: `name`, `allowPush bool`, plus a `wantErr`-style expectation for
the push/clone outcome. Helper (e.g. `newHTTPServer(t, allowPush) (*httptest.Server, billy.Filesystem)`)
marked `t.Helper()`. Note: `httptest` serves plain HTTP, so the git client needs
no TLS config; `GIT_TERMINAL_PROMPT=0` is already set by `tryGit`.

## Verification

1. `gofmt`/`goimports` clean; `go build ./...`.
2. `go test ./cmd/objgitd/...` — both git:// and HTTP suites pass (git:// tests
   unchanged after rename).
3. `go mod tidy` leaves `golang.org/x/sync` as a direct dep, no other churn.
4. Manual smoke against a real bucket:
   ```
   objgitd -bucket $BUCKET -http-bind :8080 -allow-push
   git clone http://localhost:8080/smoke.git   # 404/empty until first push
   (cd repo && git push http://localhost:8080/smoke.git main)  # creates + pushes
   git clone http://localhost:8080/smoke.git verify            # round-trips
   ```
   Optionally add `-git-bind :9418` and confirm both transports serve the same repo.

```

```
