# Plan: Prometheus metrics for objgitd

## Context

`objgitd` currently emits only `slog` logs; an operator has no quantitative view
of what the server is doing. This adds Prometheus instrumentation across the
three things the user named — **S3/Tigris ops**, **git repo operations**, and
**authorization decisions** — plus a few extra operator-visibility metrics, and
serves them on a dedicated `/metrics` HTTP listener.

Metrics use `github.com/prometheus/client_golang/prometheus/promauto` against the
**default registry**, so Go-runtime and process collectors come for free and
`promhttp.Handler()` exposes everything.

### Decisions locked in with the user

- **No `repo` label.** Unbounded cardinality. Repo operations are labeled by
  `protocol` + `service` + `status` only (the original "per repo" ask is dropped).
- **`/metrics` on by default at `:9090`** (`-metrics-bind`, empty string disables).
- **s3fs measured at the S3-API-call level** (GetObject, PutObject, …), not the
  billy-method level.
- **Extras:** push-hook runs, repos auto-created, in-flight requests gauge,
  Go/process collectors.

## New package: `internal/metrics/metrics.go`

Central definitions so names stay consistent and packages don't each import
promauto. All vectors via `promauto.NewCounterVec`/`NewHistogramVec`/`NewGauge`
(default registerer). Prefix `objgit_`.

| Metric                                 | Type      | Labels                                                                                   |
| -------------------------------------- | --------- | ---------------------------------------------------------------------------------------- |
| `objgit_s3_requests_total`             | counter   | `operation`, `status` (`ok`/`error`)                                                     |
| `objgit_s3_request_duration_seconds`   | histogram | `operation`                                                                              |
| `objgit_git_requests_total`            | counter   | `protocol`, `service`, `status` (`ok`/`error`/`denied`)                                  |
| `objgit_git_request_duration_seconds`  | histogram | `protocol`, `service`                                                                    |
| `objgit_git_requests_in_flight`        | gauge     | `protocol`                                                                               |
| `objgit_auth_requests_total`           | counter   | `transport`, `operation` (`read`/`write`), `decision` (`allow`/`deny`/`unauthenticated`) |
| `objgit_auth_request_duration_seconds` | histogram | `transport`                                                                              |
| `objgit_hook_runs_total`               | counter   | `status` (`ok`/`error`/`timeout`)                                                        |
| `objgit_hook_run_duration_seconds`     | histogram | (none)                                                                                   |
| `objgit_repos_created_total`           | counter   | (none)                                                                                   |

Exported helpers (keep call sites tiny):

- `ObserveS3(operation string, dur time.Duration, err error)` — the s3fs observer.
- `ObserveGitOp(protocol, service, status string, start time.Time)`.
- `TrackInFlight(protocol string) func()` — inc gauge, return a dec closure for `defer`.
- `ObserveAuth(transport string, op auth.Operation, d auth.Decision, start time.Time)`
  (maps the enums to label strings here so transports stay clean).
- `ObserveHook(status string, dur time.Duration)` and `ReposCreated()`.

## Instrumentation seams

### 1. s3fs — S3-API granularity, kept dependency-free

The S3 calls live in standalone constructors (`newS3ReadFile`, etc.) that hold a
`*storage.Client` but not the `S3FS`, so an instance `Option` can't reach them
without threading a field through every constructor. Instead use a process-level
observer (`internal/s3fs/metrics.go`) — honest, since the S3 client and the
Prometheus registry it feeds are both process-global:

```go
func SetMetricsObserver(fn func(operation string, dur time.Duration, err error))
func observeS3(operation string, start time.Time, err error) // package-private helper
```

`SetMetricsObserver` installs the observer once at startup; `observeS3` times one
call and reports it (no-op when no observer is set). Wrap each `client.*` call
site with a `start := time.Now()` / `observeS3("Op", start, err)` pair.
Representative sites (from exploration):

- `file.go` — HeadObject (`:68`), GetObject (`:77`), PutObject (`:249`),
  CreateMultipartUpload (`:304`), UploadPart (`:342`), CompleteMultipartUpload (`:394`).
- `basic.go` — HeadObject (`:137`), PutObject (`:186`), RenameObject (`:201`),
  ListObjectsV2 (`:97`,`:158`), DeleteObject (`:226`).
- `dir.go` — ListObjectsV2 (`:32`), PutObject (`:83`).

`operation` label = the S3 API name (string constant per call site). Keeps s3fs
free of any prometheus import — the observer is an opaque callback the project
wires in `main.go` via `s3fs.SetMetricsObserver(metrics.ObserveS3)`.

### 2. Auth — collapse three call sites into one daemon chokepoint

Today each transport calls `d.authz.Authorize(...)` directly
(`ssh.go:144`, `git_protocol.go:123`, `http.go:151`). Add:

```go
func (d *daemon) authorize(ctx context.Context, req auth.Request) auth.Decision
```

in `git_protocol.go` (next to `operationFor`, `git_protocol.go:48`). It times the
inner `d.authz.Authorize`, records `ObserveAuth(...)`, and returns the decision.
Replace all three call sites to call `d.authorize(...)`. This is the single auth
seam the metrics hook lives on.

### 3. Repo operations — one wrap per transport handler

Each handler knows `(protocol, service)` early. Pattern at each entry point:

```go
done := metrics.TrackInFlight(protocol); defer done()
start := time.Now()
// ... handle; determine status ...
metrics.ObserveGitOp(protocol, service, status, start)
```

- **HTTP** (`http.go`): wrap in `handleRPC`/`resolve` (`http.go:89`,`:150`), `protocol="http"`.
- **git://** (`git_protocol.go`): in `handle` after `req.Decode` (`git_protocol.go:109`), `protocol="git"`.
- **SSH** (`ssh.go`): in `handleSSH` after `gitServiceFor` (`ssh.go:130`), `protocol="ssh"`.

`service` = `transport.UploadPackService` / `ReceivePackService` /
`UploadArchiveService`. `status` = `denied` when `authorize` rejects, `error` on a
non-nil handler error, else `ok`.

### 4. Extras

- **Push hooks** (`hooks.go`, `runHooks` ~`:117`): time each hook run, classify
  `timeout` (context deadline) vs `error` vs `ok`, call `metrics.ObserveHook`.
- **Repos auto-created** (`git_protocol.go` `loadOrInit`, the create branch ~`:185`,
  right by the existing `slog.Info("created repository", …)`): `metrics.ReposCreated()`.
- **In-flight gauge** — already handled by `TrackInFlight` in seam #3.
- **Go/process collectors** — automatic via default registry; no code beyond serving.

### 5. main.go — flag, wiring, metrics server

- Add flag after `hookTimeout` (`main.go:37`):
  `metricsBind = flag.String("metrics-bind", ":9090", "TCP address for the Prometheus /metrics endpoint; empty disables it")`.
  `flagenv` auto-maps it to `METRICS_BIND`.
- Wire the s3fs observer before constructing the filesystem:
  `s3fs.SetMetricsObserver(metrics.ObserveS3)` (`main.go`, just before `storage.New`).
- Add `"metrics_bind", *metricsBind` to the startup `slog.Info` (`main.go:84`).
- Add a metrics listener in the errgroup, cloning the existing HTTP
  Serve/Shutdown idiom (`main.go:104-123`):
  ```go
  if *metricsBind != "" {
      ln, err := net.Listen("tcp", *metricsBind) // os.Exit(1) on error, like the others
      mux := http.NewServeMux(); mux.Handle("/metrics", promhttp.Handler())
      srv := &http.Server{Handler: mux}
      g.Go(func() error { /* srv.Serve, ignore http.ErrServerClosed */ })
      g.Go(func() error { /* <-gCtx.Done(); 10s Shutdown */ })
  }
  ```

## Dependencies

`github.com/prometheus/client_golang` is already in `go.sum` (indirect). After
coding, run `go mod tidy` to promote it to a direct require and pull
`promauto`/`promhttp`. No other new deps.

## Files touched

- **New:** `internal/metrics/metrics.go`
- `internal/s3fs/filesystem.go` (Option + `observe` helper), `internal/s3fs/file.go`, `basic.go`, `dir.go` (wrap call sites)
- `cmd/objgitd/git_protocol.go` (`authorize` helper, `handle` wrap, `loadOrInit` counter)
- `cmd/objgitd/http.go`, `cmd/objgitd/ssh.go` (auth call-site swap + repo-op wrap)
- `cmd/objgitd/hooks.go` (hook metrics)
- `cmd/objgitd/main.go` (flag, s3fs wiring, metrics server, startup log)
- `go.mod` / `go.sum` (tidy)
- `CLAUDE.md` (document the metrics surface + `-metrics-bind`, brief)

## Verification

1. `go build ./...` and `go vet ./...`.
2. `go test ./...` — existing protocol/SSH tests must still pass (metrics calls are
   additive and the observer is nil-safe when unset). The metric helpers
   themselves are not unit-tested.
3. Manual: `./objgitd -bucket $BUCKET -allow-push` then
   `curl -s localhost:9090/metrics | grep objgit_` after a clone/push; confirm
   s3, git, auth, hook, and repos-created series plus Go/process collectors.
