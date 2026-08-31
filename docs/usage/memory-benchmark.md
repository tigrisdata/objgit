# Measuring push memory

`cmd/membench` answers one question: how much memory does `objgitd` need while
it is taking pushes, and does that memory come back afterwards.

It starts a daemon it owns, pushes one real repository into many fresh ones,
samples the daemon's resident set and Go heap throughout, and captures a pprof
heap profile every time memory sets a new high-water mark. Nothing in `objgitd`
had to change for this: `cmd/objgitd/main.go` already serves `net/http/pprof`
and the default Prometheus registry (which carries `go_memstats_*` and
`process_resident_memory_bytes`) on the metrics listener.

## Running it

```text
go run ./cmd/membench -out ./bench-out
```

Run it from the objgit checkout. The harness finds `go.mod`, builds
`./cmd/objgitd`, and starts the daemon with that directory as its working
directory, so the daemon picks up `BUCKET` and the AWS credentials from `.env`
exactly as it would normally.

Pushes go to a real Tigris bucket. There is no local S3 fake in this repo, and a
fake would hide the allocation behaviour of the Tigris storer, which is the
thing being measured.

## What a run does

1. **Mirror.** `git clone --mirror` of the source repository into the run
   directory, once. Every push reads from that copy, so the repository you
   pointed at is never written to and concurrent pushes share an immutable
   source.
2. **Baseline.** Sample the idle daemon for `-baseline`, then capture
   `heap-baseline.pb.gz` with a forced GC. Every later number is read against
   this.
3. **Sequential.** `-seq-pushes` pushes, one at a time, each to a fresh
   repository, with `-idle-gap` between them. This isolates the per-push cost
   and shows whether memory returns after a push.
4. **Concurrency sweep.** For each K in `-conc-steps`, K pushes at once to K
   fresh repositories, with an idle gap between steps. The rise per concurrent
   push is the slope you size a machine with.
5. **Settle.** One more idle gap, then `heap-final.pb.gz` (forced GC) and
   `allocs-final.pb.gz`.

Every repository is created fresh under a UUID, so no push is ever measured
against a repository that already holds its objects. With the defaults that is
20 repositories.

## Repository names

Repositories are `{-org}/{uuidv4}`, defaulting to `benchtest/<uuid>`. That is
the exact `{orgID}/{repoName}` shape `internal/repofs.Parse` requires, and it
means every benchmark repository shares one key prefix in the bucket.

The harness never deletes anything. It writes `repos.txt` listing every
repository it created, and cleanup is yours to run.

## Output

Each run gets its own timestamped directory under `-out`:

| File                     | What it holds                                                        |
| ------------------------ | -------------------------------------------------------------------- |
| `report.md`              | The tables. Start here.                                              |
| `samples.csv`            | One row per sample, tagged with phase and repository. Plot-ready.    |
| `heap-baseline.pb.gz`    | Settled heap before any push.                                        |
| `heap-peak-*.pb.gz`      | Heap at each new resident-set high-water mark.                       |
| `heap-final.pb.gz`       | Settled heap after every push.                                       |
| `allocs-final.pb.gz`     | Total allocation over the run.                                       |
| `daemon.log`             | The daemon's own JSON log.                                           |
| `repos.txt`              | Every repository created, for cleanup.                               |
| `source.git`, `objgitd`  | The mirror and the binary under test.                                |

The two commands that get the most out of a run:

```sh
# What the pushes left behind.
go tool pprof -http=: -base <run>/heap-baseline.pb.gz <run>/heap-final.pb.gz

# What was on the heap at the worst moment.
go tool pprof -http=: <run>/heap-peak-<largest>.pb.gz
```

## Reading the numbers

- **`VmHWM` is the peak**, not `VmRSS`. It is the kernel's own high-water mark,
  so it cannot be missed between two samples the way a spike in `VmRSS` can.
- **Resident set is not the heap.** Go returns freed pages to the kernel lazily,
  so `VmRSS` staying high after a push is not by itself a leak. The column that
  settles it is `heap_inuse` in the CSV, and the baseline-diffed heap profile.
  If `heap_inuse` also stays high while the daemon is idle, that is retention.
- **Peak profiles are taken with `gc=0`.** Forcing a collection would change the
  number that triggered the capture.
- **Every figure is relative to `GOGC` and `GOMEMLIMIT`.** The report records
  both, because a peak-RSS number without them cannot be compared to anything.
- **Wall clock includes network time** to the bucket and varies between runs.
  Memory does not depend on it; throughput comparisons across runs do.

## Flags worth knowing

| Flag                    | Default              | Meaning                                                                  |
| ----------------------- | -------------------- | ------------------------------------------------------------------------ |
| `-repo`                 | `$HOME/Code/Xe/x`    | Repository to push. Mirror-cloned once, never written to.                |
| `-org`                  | `benchtest`          | Org segment every benchmark repository is created under.                 |
| `-seq-pushes`           | `5`                  | Sequential pushes, each to a fresh repository.                           |
| `-conc-steps`           | `1,2,4,8`            | Concurrency levels to sweep. Empty skips the sweep.                      |
| `-sample-interval`      | `250ms`              | How often `/proc` and `/metrics` are read.                               |
| `-idle-gap`             | `5s`                 | Idle time between pushes, so memory can settle.                          |
| `-window-slack`         | `0` (uses idle gap)  | How far outside a push's wall clock memory is still attributed to it.    |
| `-peak-growth`          | `0.05`               | Fractional rise in resident set that triggers a heap capture.            |
| `-peak-cooldown`        | `2s`                 | Minimum time between two peak captures.                                  |
| `-daemon-binary`        | build it             | Prebuilt `objgitd` to test instead of building `./cmd/objgitd`.          |
| `-daemon-allow-hooks`   | `false`              | Off so hook cost is not mistaken for push cost.                          |

All flags take an environment fallback through `flagenv`, in UPPER_SNAKE. The
daemon-facing flags are prefixed `-daemon-` so they do not collide with
`objgitd`'s own `HTTP_BIND` and `METRICS_BIND`.

A cheap smoke run, three repositories and well under a minute:

```text
go run ./cmd/membench -seq-pushes 1 -conc-steps 2 -baseline 3s -idle-gap 3s
```
