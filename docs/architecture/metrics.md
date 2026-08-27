# Metrics (`internal/metrics`)

`internal/metrics` defines every Prometheus vector with `promauto`, against
the **default registry**. The Go-runtime and process collectors from
`client_golang` are therefore exported next to them.

`main.go` serves `promhttp.Handler()` on its own listener. The flag is
`-metrics-bind`, the default is `:9090`, and an empty value disables the
listener. It uses the same errgroup Serve and Shutdown idiom as the HTTP
transport.

All series carry the prefix `objgit_`. There is no `repo` label, because
repository names have unbounded cardinality. Git operations are keyed by
`protocol`, `service`, and `status` only.

## Helpers

The package exposes thin helpers, so no call site carries label plumbing:

| Helper          | Use                                    |
| --------------- | -------------------------------------- |
| `ObserveS3`     | The s3fs observer.                     |
| `ObserveGitOp`  | One git operation.                     |
| `TrackInFlight` | Returns a deferred decrement.          |
| `ObserveAuth`   | Maps the `auth` enums to labels.       |
| `ObserveHook`   | One hook run.                          |
| `ReposCreated`  | A new repository.                      |

## Three instrumentation seams

### s3fs

s3fs reports each S3 round-trip at the API-call level, such as `GetObject` and
`PutObject`. s3fs itself holds no Prometheus code. It keeps a process-level
observer of type `func(op, dur, err)`, set through `s3fs.SetMetricsObserver`,
and `main` wires that observer to `metrics.ObserveS3`.

The setter is package-level, and not an instance option. The S3 calls live in
standalone constructors that hold no `S3FS` value.

### auth

Authorization routes through one chokepoint, which is `(*daemon).authorize` in
`git_protocol.go`. It times `d.authz.Authorize` and records the decision
before it returns. All three transports call it, instead of `d.authz`
directly.

### repo ops

Each transport handler wraps its work once: `handleRPC`, `handle`, and
`handleSSH`. Each wrap adds an in-flight gauge, plus a duration and a count
keyed by protocol and service.

HTTP folds an authorization denial into the `error` git status. The exact
denial stays visible in `objgit_auth_requests_total`. git:// and SSH record
`denied` directly.
