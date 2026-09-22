# Benchmark objgit before and after the bin/cue format change

Status: Implemented as `cmd/formatbench`. See
[docs/usage/format-benchmark.md](../usage/format-benchmark.md) to run it.

## Context

`var/bin-cue-git.md:65-66` is the outline for the blog post "Object storage is
not a filesystem". One section is still a placeholder:

```
* Latency/storage improvements
  * TODO(Xe): get before/after comparisons of a few repos
```

This plan fills that placeholder. It builds a harness that pushes three real
repositories into objgitd twice, once on each side of the storage format
change, and records numbers that go straight into the post.

The change itself already shipped. Two commits made it, both on 2026-08-27:

- `f4a1419` `feat(storage/tigris): bin/cue pack containers, async uploads, pack cache`
- `ebfe4e3` `refactor(objgitd)!: serve every repository from one bucket via the storer`

Before those commits, objgitd stored git's native bare-repository layout as
bucket keys through `internal/s3fs`. After them, it stores `packs/<id>.bin`
plus `packs/<id>.cue` containers through `internal/storage/tigris`.

## What "before" and "after" mean

| Column | Ref | What it runs |
| --- | --- | --- |
| before | `v1.0.2` | `internal/s3fs` + go-git dotgit. One bucket. Listing cache and pack cache on. |
| after | `main` (`ce5a7ca`, v1.5.0) | `internal/storage/tigris`. bin/cue containers, zstd, cue v3. |

`v1.0.2` is the last release where repositories live in the single `-bucket`
flag, which is also how HEAD works. That keeps the setup identical on both
sides. The releases after it (`v1.1.0`) moved repositories to per-keypair
buckets and turned repository-side caching off, which would move two variables
at once.

One thing about this pair goes in the post as a caveat, not as a hidden fact:

1. `v1.0.2` is missing two later `internal/s3fs` corrections, `bf81a1d`
   (opt out of default request checksums) and `5bb834b` (directory markers).
   RESOLVED: `v1.0.2` builds with Go 1.26.5 and completes a push and a clone
   against Tigris today, so no cherry-pick is needed. The two corrections
   stay unapplied, which keeps the "before" side exactly as it shipped.
2. HEAD is not only the format. It also carries zstd (`7bea058`), cue v3 with
   delta reuse (`ea4d362`), pack prefetch (`eea720d`), and bounded pushes
   (`877cd16`). The post must say the comparison is "the lazy thing" against
   "today", not a controlled single-variable experiment.

## Corpus

Mirror-clone each repository once. Never write to the mirror.

| Name | URL | Shape |
| --- | --- | --- |
| objgit | https://github.com/tigrisdata/objgit | Small. Mostly text. |
| x | https://github.com/Xe/x | Medium. Large object count. |
| tigris-blog | https://github.com/tigrisdata/tigris-blog | Blob-heavy. Images do not compress. |

Confirm the `tigris-blog` clone URL before the first run. Record the resolved
commit, the object count from `git rev-list --all --count`, and the pack size
of the mirror for each one.

## What gets measured

Both builds report the same Prometheus counter with the same label values.
`internal/metrics/metrics.go:23` defines
`objgit_s3_requests_total{operation,status}`. `v1.0.2` feeds it from
`internal/s3fs/metrics.go` through `s3fs.SetMetricsObserver(metrics.ObserveS3)`
at `cmd/objgitd/main.go:81`. HEAD feeds it from `Storer.observe`
(`internal/storage/tigris/tigris.go:323`). Both use `GetObject`, `PutObject`,
`HeadObject`, `ListObjectsV2`, and `DeleteObject`. No proxy is needed.

Per cell, that is per build and per repository and per workload:

| Field | Source |
| --- | --- |
| Wall seconds | Driver timer around the `git` subprocess. |
| S3 requests by verb | Delta of `objgit_s3_requests_total{operation}`. |
| S3 time | Delta of `objgit_s3_request_duration_seconds_sum`. |
| Keys written | `ListObjectsV2` over the repository prefix after the push. |
| Bytes stored | Sum of the sizes from that same listing. |
| Bytes on the wire | Clone only. The `Receiving objects` line from git stderr. |

Both eras key a repository under its `orgID/name` path. `v1.0.2` gets there
through the go-git loader (`cmd/objgitd/git_protocol.go:61`). HEAD gets there
through `repofs.BucketResolver.Resolve` (`internal/repofs/repofs.go:93-95`).

CAUTION: they disagree about the `.git` suffix. HEAD parses the transport path
through `repofs.Parse`, which strips it (`internal/repofs/repofs.go:47`), so
its keys sit under `<org>/<name>/`. `v1.0.2` hands the raw URL path to the
loader, which keeps it, so its keys sit under `<org>/<name>.git/`. A harness
that lists only one spelling reports an empty bucket for the other side. The
accounting and the cleanup both walk the two prefixes and sum them.

## The harness

Add `cmd/formatbench/`. It is a development tool, not part of the shipped
binary. `cmd/membench/` is the precedent, and `AGENTS.md:69` records that
membench is not shipped.

Do not extend `cmd/membench` itself. It samples `/proc/<pid>/status`
(`cmd/membench/proc.go:18`), so it drops every sample on macOS. This plan
needs wall time, request counts, and bucket size, and all three are portable.

Reuse these patterns from membench rather than inventing new ones:

- `moduleRoot()` (`cmd/membench/main.go:489`) to find the checkout root.
- `buildDaemon` (`main.go:335`) and `startDaemon` (`main.go:352`), which set
  `cmd.Dir = root` so the daemon reads `BUCKET` and AWS credentials from `.env`.
- The `/metrics` scrape and parser in `cmd/membench/metrics.go:25-39`, widened
  to read `CounterVec` series with labels instead of five bare gauges.
- The timestamped output directory and `repos.txt` cleanup list
  (`report.go:342`). Bucket data is never deleted on its own.

### Setup, once per session

1. Run `git worktree add var/bench/src/before v1.0.2`.
2. Build `var/bench/bin/objgitd-before` from inside that worktree. Its
   `go.mod` already declares `go 1.26.3`, so the current toolchain builds it.
3. Build `var/bench/bin/objgitd-after` from the checkout root.
4. Mirror-clone each corpus repository into `var/bench/mirrors/<name>.git`.

### One measurement cell

1. Make a fresh repository path, `bench/<build>-<repo>-<uuid>`. Never reuse
   one. Every push is a cold push into an empty prefix.
2. Make a fresh empty directory for `-pack-cache-dir`.
3. Start the daemon and wait for it to answer. Flags for both builds:
   `-bucket $BUCKET -http-bind 127.0.0.1:8080 -metrics-bind 127.0.0.1:9090
   -ssh-bind= -allow-push -pack-cache-dir <tmp>`. Leave every other flag at
   its default, so each build runs the way it shipped.
4. Scrape `/metrics` and record the counters.
5. Run `git push --mirror http://127.0.0.1:8080/bench/<repo>.git` from the
   mirror. Record the wall time. A mirror push, and not a heads-and-tags
   refspec: a mirror clone of a GitHub project also carries `refs/notes/*` and
   `refs/pull/*`, whose commits a heads-and-tags push leaves behind.
6. Scrape `/metrics` again. Record the deltas.
7. List the bucket under the repository prefix. Record the key count and the
   total bytes.
8. Stop the daemon. Delete the pack cache directory.
9. Start the daemon again with a new empty pack cache directory. This makes
   the clone cold in the process, on disk, and in the pack index.
10. Scrape, run `git clone`, scrape again. Record the wall time and the
    `Receiving objects` byte count from git stderr.
11. Stop the daemon.

### Correctness gate

A benchmark that measures a corrupt push is worthless. After the clone in
step 10, compare the clone against the mirror:

- `git rev-list --all --count` must match.
- `git show-ref` must produce the same set of refs and hashes.
- `git fsck --full` must pass on the clone.

If any of these fail, mark the cell `INVALID`. Do not report its timings.

### Fairness controls

- **Interleave the builds.** Run `(before, objgit)`, `(after, objgit)`,
  `(before, x)`, `(after, x)`, and so on, then repeat the whole cycle. Wi-Fi
  and Tigris both drift over an afternoon. If all the "before" runs happen
  first, that drift looks like a result.
- Three repetitions for `objgit` and `tigris-blog`. Two for `x`. Report the
  median with the minimum and maximum beside it.
- Stop and restart the daemon between every single measurement.
- Record the conditions once per session: `git --version`, `go version`, the
  machine, the bucket region, and whether a VPN is on. Do not change any of
  them mid-session.

### Timeout policy

Set a hard limit of 45 minutes for a push and 30 minutes for a clone. A prior
measurement in `docs/plans/pack-temp-file-cache.md:5-16` recorded 8,500
`GetObject` calls to serve one clone of a 318-object repository under the old
layout, and that clone never finished. The `x` repository can do the same
thing on the "before" build.

On a timeout, record `DNF` with the elapsed time and the request count
reached. Report the cell as `DNF` in the table. Do not extrapolate a finish
time from partial progress.

### Output

Two files in the run directory:

- `results.json`, one record per cell, with every field above.
- `results.md`, the tables below already rendered, ready to paste.

## The tables for the post

Push:

| repo | before wall | after wall | before PUTs | after PUTs | before keys | after keys | before bytes | after bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

Clone:

| repo | before wall | after wall | before GETs | after GETs | wire bytes |
| --- | --- | --- | --- | --- | --- |

End the section with this line, as written:

> Oh yeah, this was with my corporate laptop on wifi.

## Files

| Path | Change |
| --- | --- |
| `cmd/formatbench/main.go` | New. The driver: setup, cell loop, interleaving, timeouts. |
| `cmd/formatbench/metrics.go` | New. Scrape and diff `objgit_s3_requests_total` by label. |
| `cmd/formatbench/bucket.go` | New. List a prefix, sum key count and bytes. |
| `cmd/formatbench/git.go` | New. Timed git subprocesses and the wire-byte parser. |
| `cmd/formatbench/verify.go` | New. The correctness gate. |
| `cmd/formatbench/report.go` | New. `results.json` and `results.md`. |
| `cmd/formatbench/uuid.go` | New. Collision-free repository names. |
| `docs/plans/bin-cue-benchmark.md` | New. This plan, committed where `AGENTS.md` says plans live. |
| `docs/usage/format-benchmark.md` | New. How to run it. Follow `docs/usage/memory-benchmark.md` in shape. |
| `AGENTS.md` | One row in the "Where the code lives" table, next to `cmd/membench/`. |
| `var/bin-cue-git.md` | Replace the `TODO(Xe)` at line 66 with the finished tables. |

## First results

One repetition of `objgit` on each side, run on 2026-09-10 against a real
bucket. These are a smoke result, not the table for the post: one repetition
has no range, and one small repository does not show the loose-object cost the
old layout pays on a big one.

Push:

| Build | Wall | S3 requests | PUT | GET | LIST | Keys | Bucket bytes |
| --- | --: | --: | --: | --: | --: | --: | --: |
| before | 12.6s | 231 | 46 | 47 | 138 | 42 | 829.27 KiB |
| after | 2.1s | 19 | 5 | 11 | 3 | 4 | 902.98 KiB |

Clone:

| Build | Wall | S3 requests | GET | LIST | Wire bytes |
| --- | --: | --: | --: | --: | --: |
| before | 14.3s | 323 | 51 | 272 | 763.63 KiB |
| after | 13.4s | 53 | 49 | 4 | 767.96 KiB |

Two things to read from this before the full run:

1. The push win is already large on the smallest repository in the corpus: 231
   S3 calls against 19, and 42 keys against 4.
2. The clone difference is almost all `ListObjectsV2`, 272 against 4. The
   `GetObject` counts are near equal because `ab60b2d` taught the old layout to
   keep a pushed pack whole. A repository built by many pushes, or one with
   loose objects, is where the old `GetObject` count grows.

## Verification

Done, with the results recorded above:

1. `go build ./...` and `go vet ./...` pass.
2. `go test ./cmd/formatbench/` passes: 58 assertions over the metric parser,
   the report writer, the wire-byte parser, the ref diff, and the prefix set.
3. Smoke run of `-repos objgit -builds after -reps 1` finishes in about a
   minute and writes a complete `results.json`.
4. The `objgit_s3_requests_total` delta is non-zero on both sides, so the
   observer is wired on both. A zero delta would mean the wiring broke, not
   that the build is fast.
5. The correctness gate passes on both sides. It earned its place: it caught a
   heads-and-tags refspec in the first draft that silently dropped
   `refs/notes/*` and `refs/pull/*`.
6. `-cleanup` deletes both prefix spellings and leaves the bucket empty.

Still to do:

1. Full run: `go run ./cmd/formatbench`.
2. Paste the two tables into `var/bin-cue-git.md`, replacing the `TODO(Xe)`.
3. Delete the benchmark repositories with
   `go run ./cmd/formatbench -cleanup var/bench/out/<run>/repos.txt`.
