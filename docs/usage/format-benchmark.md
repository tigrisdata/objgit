# Measuring the storage format change

`cmd/formatbench` answers one question: what did the move from git's native
on-disk layout to the `.bin`/`.cue` containers cost, and what did it buy.

It builds two daemons, pushes the same real repositories into both, and clones
each one back out. Nothing in `objgitd` had to change for this. Both builds
already report `objgit_s3_requests_total` on the metrics listener with the same
operation labels, so the request count each format costs is directly
comparable.

## The two sides

| Side | Ref | What it runs |
| --- | --- | --- |
| before | `v1.0.2` | `internal/s3fs` and go-git's dotgit layout. One bucket. Listing cache and pack cache on. |
| after | current checkout | `internal/storage/tigris`. `.bin`/`.cue` containers, zstd, cue v3. |

The format landed in commits `f4a1419` and `ebfe4e3` on 2026-08-27, released as
v1.2.0. `v1.0.2` is the last release before it where repositories live in the
single `-bucket`, which is also how the current build works. The releases after
it moved repositories to per-keypair buckets and turned repository-side caching
off, so they would move two variables at once.

CAUTION: The current build is not only the format. It also carries zstd
(`7bea058`), cue v3 with delta reuse (`ea4d362`), pack prefetch (`eea720d`),
and bounded pushes (`877cd16`). The comparison is "the first version" against
"today", and not a single-variable experiment. Say so wherever the numbers go.

## Running it

```text
go run ./cmd/formatbench
```

Run it from the objgit checkout. The harness finds `go.mod` and starts each
daemon with that directory as its working directory, so the daemon picks up
`BUCKET` and the AWS credentials from `.env` exactly as it would normally.

Pushes go to a real Tigris bucket. Wall-clock numbers over a real network are
the point, so there is no local fake.

A smoke run of one repository on one side takes about a minute:

```text
go run ./cmd/formatbench -repos objgit -builds after -reps 1
```

## What a run does

1. **Build both sides.** The "before" daemon is compiled from a `git worktree`
   checked out at `-before-ref` under `var/bench/src/`, because its `go.mod`
   and its whole `internal` tree differ from the current checkout. The worktree
   is made once and reused.
2. **Mirror the corpus.** `git clone --mirror` of each repository into
   `var/bench/mirrors/`, once, and reused across runs. Every push reads from
   that copy, so each repetition sends byte-identical history. Pass
   `-refresh-mirrors` to fetch first.
3. **Measure each cell.** One cell is one build, one repository, one
   repetition. Each cell gets a fresh repository prefix under a new UUID, so no
   push is ever measured against a repository that already holds its objects.
4. **Write the tables.** `results.json` holds one record per cell.
   `results.md` holds the rendered tables.

Each cell runs two measurements, and each measurement gets its own daemon
process and its own empty pack cache directory:

1. Start the daemon, scrape `/metrics`, `git push --mirror`, scrape again.
2. List the bucket prefix for the key count and the byte total.
3. Stop the daemon, delete its pack cache, start a new one. This makes the
   clone cold in the process, on disk, and in the pack index that
   `(*Storer).ensurePacksBuilt` rebuilds per request.
4. Scrape, `git clone --mirror`, scrape again.
5. Compare the clone against the mirror.

The push is a mirror push, not a heads-and-tags push. A mirror clone of a
GitHub project also carries `refs/notes/*` and `refs/pull/*`, and those hold
commits that a heads-and-tags push leaves behind.

## Fairness

Builds are interleaved, not batched. The schedule is (repetition 1: every
repository on every build), then (repetition 2: the same). Network conditions
drift over an afternoon, and running every "before" cell first would let that
drift look like a result.

Each row of the report is the median across repetitions, with the minimum and
maximum beside it when they differ. The median is used because two or three
samples over wifi have outliers and no useful mean.

## The correctness gate

A benchmark that measures a corrupt push is worthless, so every clone is
compared against the mirror it came from:

- The same number of reachable commits, from `git rev-list --all --count`.
- The same set of refs pointing at the same hashes, from `git show-ref`.
- An intact object graph, from `git fsck --full`.

A cell that fails any of these is reported as INVALID and its timings are
withheld from the tables. Pass `-verify=false` to skip the gate, which is worth
doing only when you already know the format is sound and you want the run to
finish faster.

## Timeouts

A push stops at `-push-timeout` (45 minutes by default) and a clone at
`-clone-timeout` (30 minutes). An operation that hits its limit is recorded as
DNF, together with the elapsed time, the S3 requests it made, and the bytes it
had written by then.

A DNF is a result and it is reported as one. It is never turned into an
estimated finish time. Under the old layout, one clone of a 318-object
repository made more than 8,500 `GetObject` calls and never finished; see
`docs/plans/pack-temp-file-cache.md`.

## What comes out

`results.md` holds two tables.

Push:

| Repo | Build | Wall | S3 requests | PUT | GET | HEAD | LIST | Keys | Bucket bytes |

Clone:

| Repo | Build | Wall | S3 requests | GET | HEAD | LIST | Wire bytes |

Then the run conditions (host, Go version, git version, bucket, corpus sizes)
and a list of every cell that produced no number, with the reason.

## Cleanup

The harness never deletes bucket data during a run. It appends every prefix it
creates to `repos.txt` after each cell, so an interrupted run still leaves a
complete list. Delete them afterwards:

```text
go run ./cmd/formatbench -cleanup var/bench/out/<run>/repos.txt
```

NOTE: The two builds disagree about the `.git` suffix. The current build parses
the transport path through `repofs.Parse`, which strips it, so its keys sit
under `<org>/<name>/`. The older build hands the raw URL path to go-git's
server loader over `internal/s3fs`, which keeps it, so its keys sit under
`<org>/<name>.git/`. Both the accounting and the cleanup look under both
spellings.

## Flags

| Flag | Default | What it does |
| --- | --- | --- |
| `-repos` | every entry | Comma-separated corpus entries to run. |
| `-builds` | `before,after` | Which columns to run. See below. |
| `-before-ref` | `v1.0.2` | The ref the "before" daemon is built from. |
| `-reps` | per entry | Override the repetition count for every repository. |
| `-out` | `var/bench/out` | Parent directory for run output. |
| `-org` | `formatbench` | Org segment every benchmark repository is created under. |
| `-bucket` | `BUCKET` | The bucket the daemon under test writes to. |
| `-push-timeout` | `45m` | Time limit for one push. |
| `-clone-timeout` | `30m` | Time limit for one clone. |
| `-verify` | `true` | Compare each clone against its mirror. |
| `-refresh-mirrors` | `false` | Fetch each mirror before the run. |
| `-cleanup` | none | Delete every prefix in a `repos.txt` and exit. |

Every flag also reads from the environment through `flagenv`, which maps each
one to UPPER_SNAKE. `-before-ref` becomes `BEFORE_REF`.

### More than two columns

`-builds` takes three spellings, comma-separated:

| Entry | What it builds |
| --- | --- |
| `before` | The ref in `-before-ref`, which defaults to `v1.0.2`. |
| `after` | The current checkout. |
| `<name>=<ref>` | That ref, in a column labelled `<name>`. An empty ref means the current checkout. |

The third form is how you measure a candidate fix. Put the old build, the
current build, and the fix in one run:

```text
go run ./cmd/formatbench -builds "before,main=v1.5.0,fixed=<sha>" -reps 2
```

Run the candidate against a separate earlier run instead, and the difference
between the two numbers includes a day of network drift. One interleaved
session is the only honest way to read three points.

Each ref is built once from its own `git worktree` under `var/bench/src/`.
Slashes in a ref name become dashes in that directory name.

## The corpus

The default corpus is three repositories, chosen for three shapes rather than
three names. Edit the `corpus` table in `cmd/formatbench/main.go` to change it.

| Name | Shape |
| --- | --- |
| objgit | Small. Mostly text. |
| x | Medium. Large object count. |
| tigris-blog | Blob-heavy. Images do not compress. |
