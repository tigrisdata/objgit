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

| Helper                 | Use                              |
| ---------------------- | -------------------------------- |
| `ObserveS3`            | The s3fs observer.               |
| `ObserveGitOp`         | One git operation.               |
| `TrackInFlight`        | Returns a deferred decrement.    |
| `ObserveAuth`          | Maps the `auth` enums to labels. |
| `ObserveHook`          | One hook run.                    |
| `ReposCreated`         | A new repository.                |
| `TrackPushWait`        | Returns a deferred decrement.    |
| `TrackPushSlot`        | Returns a deferred decrement.    |
| `ObservePushWait`      | One wait for a push slot.        |
| `ObserveSnapshot`      | One snapshot Ensure call.        |
| `ObserveSnapshotCache` | One snapshot cache event.        |

## The push queue

`-max-concurrent-pushes` changes the failure mode under load. The daemon no
longer grows its heap without limit. It makes pushes queue instead. This is an
improvement only when the queue is visible, so the cap comes with four series:

| Series                             | Type      | Meaning                                  |
| ---------------------------------- | --------- | ---------------------------------------- |
| `objgit_push_slots_held`           | gauge     | Pushes that unpack a packfile right now. |
| `objgit_push_queue_waiting`        | gauge     | Pushes that wait for a slot.             |
| `objgit_push_queue_wait_seconds`   | histogram | Time one push spent in the queue.        |
| `objgit_push_queue_outcomes_total` | counter   | Slot requests by outcome.                |

`objgit_push_queue_waiting` is the one to alert on. A value that stays above
zero means the cap is below the offered load. There is no other way to see
that from outside the process.

The outcome label separates the two ways a wait ends badly. `timeout` means the
client waited out `-push-queue-timeout`, which is the signal that the cap is too
low. `canceled` means the client hung up while queued, which is not.

## Snapshots

Snapshot Ensure calls and cache events have five series:

| Series                                   | Type      | Labels | Meaning                              |
| ---------------------------------------- | --------- | ------ | ------------------------------------ |
| `objgit_snapshot_builds_total`           | counter   | result | Ensure calls by result.              |
| `objgit_snapshot_build_duration_seconds` | histogram | (none) | Time for builds.                     |
| `objgit_snapshot_image_bytes`            | histogram | (none) | Built image size after compression.  |
| `objgit_snapshot_cache_opens_total`      | counter   | result | Cache lookups by result (hit, miss). |
| `objgit_snapshot_cache_evictions_total`  | counter   | reason | Evictions by reason (budget, idle).  |

The size histogram is data for a later decision about a size limit.

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
