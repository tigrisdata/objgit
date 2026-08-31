# Repository guidelines

`objgitd` is a single-binary git server. It stores repositories as objects in
a Tigris bucket, and not on a local filesystem.

Module path: `github.com/tigrisdata/objgit`. Go 1.26.

## Transports

One backend answers three transports:

| Transport  | Flag         | Default | Notes                                             |
| ---------- | ------------ | ------- | ------------------------------------------------- |
| Smart HTTP | `-http-bind` | `:8080` | The primary transport. Carries HTTP Basic.        |
| git://     | `-git-bind`  | `:9418` | Unauthenticated TCP. Opt-in.                      |
| SSH        | `-ssh-bind`  | off     | Public-key. Opt-in. Host key lives in the bucket. |

All three funnel authorization through one pluggable
`internal/auth.Authorizer`.

A fourth listener serves Prometheus metrics at `/metrics` (`-metrics-bind`,
default `:9090`). An empty value disables it.

## Commands

```text
go build ./...                  # build everything
go build -o objgitd ./cmd/objgitd
go test ./...                   # full test suite
go test ./cmd/objgitd/...       # protocol tests
go test -run TestSmartHTTP ./cmd/objgitd/...
go test -run TestSSH ./cmd/objgitd/...

# Run locally.
./objgitd -bucket $BUCKET -http-bind :8080 -allow-push
./objgitd -bucket $BUCKET -ssh-bind :2222 -allow-push   # git clone ssh://git@host:2222/repo.git
```

Notes on the tests and on configuration:

- Protocol tests need `git` on PATH. They skip themselves without it.
- SSH tests also need `ssh` and `ssh-keygen` on PATH. They skip themselves
  without them.
- Flags can also come from the environment through `flagenv`, which maps each
  flag to UPPER_SNAKE. `-allow-push` becomes `ALLOW_PUSH`, and `-bucket`
  becomes `BUCKET`.
- `godotenv` loads a `.env` file from the working directory at startup.
- Tigris client credentials come from the standard AWS SDK chain, such as
  `AWS_PROFILE`.

## Where the code lives

| Path                          | Purpose                                                                |
| ----------------------------- | ---------------------------------------------------------------------- |
| `cmd/objgitd/main.go`         | Builds the one `*daemon` and starts every listener.                    |
| `cmd/objgitd/git_protocol.go` | The git:// server. Also holds `operationFor` and `(*daemon).authorize`. |
| `cmd/objgitd/http.go`         | Smart HTTP. `*daemon` is the `http.Handler` itself.                    |
| `cmd/objgitd/ssh.go`          | The SSH server and its per-session dispatch.                           |
| `cmd/objgitd/receivepack.go`  | The go-git fork that streams hook output, plus `writePack`.            |
| `cmd/objgitd/hooks.go`        | Ref diffing and the sandboxed hook run.                                |
| `internal/auth`               | The one authorization interface.                                       |
| `internal/repofs`             | Maps a repository path to a `storage.Storer`.                          |
| `internal/storage/tigris`     | Repository storage. A `storage.Storer` on the bucket.                  |
| `internal/bundler`            | The async upload queue behind that storer.                             |
| `internal/s3fs`               | Daemon-level state only, which is the SSH host key.                    |
| `internal/mountfs`, `internal/treefs`, `internal/kefkash` | The hook sandbox filesystem and shell wiring. |
| `internal/metrics`            | Every Prometheus vector, plus thin helpers.                            |
| `internal/slog.go`            | JSON handler init.                                                     |

## Architecture

Read [docs/architecture/README.md](docs/architecture/README.md) first. It
describes the daemon and links to one page for each subsystem.

| Page                                                   | Read it before you change...                                           |
| ------------------------------------------------------ | ---------------------------------------------------------------------- |
| [transports.md](docs/architecture/transports.md)       | Any transport. It holds two protocol points that are easy to get wrong. |
| [auth.md](docs/architecture/auth.md)                   | Credentials, decisions, or a new `Authorizer`.                          |
| [hooks.md](docs/architecture/hooks.md)                 | Push hooks, output streaming, or the sandbox.                           |
| [metrics.md](docs/architecture/metrics.md)             | Any metric or instrumentation seam.                                     |
| [tigris-storer.md](docs/architecture/tigris-storer.md) | Object layout, refs, packs, the pack cache, or the upload path.               |
| [s3fs.md](docs/architecture/s3fs.md)                   | The `billy.Filesystem` over the bucket.                                 |

Two more directories carry detail:

- `docs/reference/` — the `.cue` binary layout, the failure modes, and the
  build order.
- `docs/usage/` — how to use a feature, such as a hook script.

## Conventions

- Flags use **kebab-case**, paired with `flagenv` for the environment
  fallback.
- `slog` uses `"err"` as the error key, and never `"error"`.
- Errgroup owns the server lifecycle. `signal.NotifyContext` provides
  cancellation. HTTP shutdown uses a 10s `context.WithTimeout`.
- Tests are **table-driven with `tt`**. Gate a test with
  `exec.LookPath("git")` when it shells out to a real git client. See
  `http_test.go` and `git_protocol_test.go`.
- Reuse the shared test helpers `runGit`, `tryGit`, and `seedRepo`. They live
  in `git_protocol_test.go`.
- Put a plan for non-trivial work in `docs/plans/`. `git-http-protocol.md`
  shows the style.
- Put architecture notes in `docs/architecture/`, and not in this file. Keep
  this file an index.

## Tigris and object storage

When you work with Tigris buckets, access keys, or IAM, use the
`tigris-storage` skills.

Tigris is S3-compatible. The client here is
`github.com/tigrisdata/storage-go`, which is a thin Tigris-aware wrapper.
