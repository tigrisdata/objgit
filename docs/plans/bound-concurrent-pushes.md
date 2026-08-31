# Plan: bound the number of concurrent pushes

## Context

`cmd/membench` swept concurrent pushes of a 48 MiB pack against one daemon.
Memory is linear in concurrency, with a flat slope and no ceiling of its own:

| K simultaneous pushes | Rise in RSS | Per concurrent push | Failures |
| --------------------- | ----------- | ------------------- | -------- |
| 2                     | +814 MiB    | 407 MiB             | 0        |
| 4                     | +1611 MiB   | 403 MiB             | 0        |
| 8                     | +3431 MiB   | 429 MiB             | 0        |

Nothing in `objgitd` bounds K. Whoever is pushing sets it. A dozen simultaneous
pushes of a large repository is roughly 5 GB of resident set, and the process
has no way to decline.

`GOMEMLIMIT` halves the slope. It measured 215 MiB for each push instead of
429 MiB, with no failures. It does not bound anything either. It makes the
collector work harder as the heap grows. It does not stop the heap from growing.
Under enough concurrency the daemon still gets killed, only later. A limit is the
only thing that turns "linear forever" into a number that can be sized against.

## Where the choke point is

There is exactly one, and it already exists. `(*daemon).receivePack`
(`cmd/objgitd/hooks.go`) is reached from both transports that accept pushes:

- Smart HTTP: `cmd/objgitd/http.go`
- SSH: `cmd/objgitd/ssh.go`

The git:// server does not serve `receive-pack` at all.
`cmd/objgitd/git_protocol.go` only maps the service to a write operation for
authorization. One gate inside `(*daemon).receivePack` therefore covers every
push the daemon can take, and no transport needs to know about it.

Fetches are deliberately out of scope. They allocate very differently, and one
semaphore over both would let a burst of clones block pushes for reasons that
have nothing to do with memory.

## Decisions

- **A counting semaphore on `*daemon`, taken in `(*daemon).receivePack` and
  released on return.** One gate, at the one place both transports already
  funnel through. `golang.org/x/sync/semaphore` comes with the
  `golang.org/x/sync` module that the repo already requires for `errgroup`.

- **Waiting, not rejecting, up to a deadline.** A push that arrives at a busy
  daemon waits for a slot rather than failing at once. Git clients handle a slow
  push far better than a failed one, and a push that has already uploaded its
  pack must not be thrown away because a peer was mid-flight. Past the deadline
  it fails cleanly.

- **The deadline comes from the request context, plus its own flag.** The
  semaphore acquire takes `ctx`, so a client that hangs up while queued gives up
  its place at once. A separate `-push-queue-timeout` bounds how long a
  still-connected client waits, so the queue cannot grow without limit behind
  one slow push.

- **Two flags, kebab-case with a `flagenv` fallback, per the repo convention:**

  | Flag                     | Env                     | Default | Meaning                                           |
  | ------------------------ | ----------------------- | ------- | ------------------------------------------------- |
  | `-max-concurrent-pushes` | `MAX_CONCURRENT_PUSHES` | `4`     | Pushes admitted at once. `0` disables the limit.  |
  | `-push-queue-timeout`    | `PUSH_QUEUE_TIMEOUT`    | `2m`    | How long a push waits for a slot before it fails. |

  The default of 4 is not arbitrary. At the measured 429 MiB for each push it
  puts the steady-state push ceiling near 1.7 GB, which fits a 2 GB container
  once the encoder floor is removed. Set it together with that cap and with
  `GOMEMLIMIT`.

- **`0` means unlimited.** This matches how `-pack-cache-bytes` treats `0` as
  "disable" rather than "zero budget". Anyone who wants today's behavior gets it
  with one flag.

- **The error reaches the client as a push failure with a readable message**,
  through the existing report-status path (`sendReportStatus` in
  `cmd/objgitd/receivepack.go`), so the person pushing sees why instead of
  getting a dropped connection.

## Where the gate landed, and why not at the top

The two decisions above pull against each other. A gate at the very top of
`(*daemon).receivePack` runs before the client's capabilities are decoded, so at
that point the daemon does not know whether the response is plain pkt-line or
multiplexed on a sideband. It cannot write a report-status the client will parse.
All it can do is drop the connection, and over SSH it would also stall before
the reference advertisement.

The gate therefore sits a few lines further in, as an `admitFunc` seam that
`(*daemon).receivePack` passes into `receivePackStreaming`. It fires right after
the capability decode and before the packfile is read. This keeps every property
the plan asked for:

- One gate. Both push transports reach it, and neither knows about it.
- The slot covers the whole memory-heavy region: the unpack, the reference
  update, and the hooks.
- A push that gives up waiting is reported the same way an unpack failure is.
  git prints it as `error: remote unpack failed:` followed by the reason.

The sideband setup in `receivePackStreaming` moved above the packfile read to
make that reporting possible. Nothing is written to the response between the old
position and the new one, so the reorder is not observable on the wire.

## Observability

This changes the failure mode from "the daemon dies" to "pushes queue", which is
an improvement only when the queue is visible. Four series, all under the
`objgit_push_` prefix:

| Series                             | Type      | Meaning                   |
| ---------------------------------- | --------- | ------------------------- |
| `objgit_push_slots_held`           | gauge     | Pushes that hold a slot.  |
| `objgit_push_queue_waiting`        | gauge     | Pushes that wait for one. |
| `objgit_push_queue_wait_seconds`   | histogram | Time spent waiting.       |
| `objgit_push_queue_outcomes_total` | counter   | Slot requests by outcome. |

`objgit_push_queue_waiting` is the one that matters. A value that stays above
zero means the cap is below the offered load, and there is no other way to tell
that from outside the process.

The outcome label separates the two ways a wait ends badly. `timeout` means the
client waited out `-push-queue-timeout`. `canceled` means it hung up while
queued. Only the first says the cap is too low.

## Testing

`cmd/objgitd/pushlimit_test.go`. The limiter's own semantics are tested without
a git client, so no case is decided by how fast a subprocess happens to run:

- **`TestPushLimiterAdmit`.** A table over the cap, the deadline, and the number
  of slots already taken: `0` disables the cap, a push below the cap is admitted,
  a full house gives up at the deadline with an error wrapping
  `errPushQueueTimeout`, and a client that hangs up does not wait out an hour.
- **`TestPushLimiterQueuedClientReleasesItsPlace`.** A push queues, its client
  hangs up, and the held slot is then handed back. A third push must take that
  slot. If the abandoned push kept its place, the third one waits forever.

Then the wiring, with a real `git` client. Each test takes the daemon's only
slot from the test itself, which makes the queue deterministic:

- **`TestPushCapReportsTimeoutToClient`.** Over smart HTTP, a push that finds the
  slot held fails, and its output names the reason. The same push lands once the
  slot is freed, so the gate is not sticky.
- **`TestPushCapReportsTimeoutOverSSH`.** The same assertion over SSH, which is
  a different path: SSH is not a stateless RPC, so the reference advertisement
  goes out from inside `receivePackStreaming` before the gate is reached.
- **`TestPushCapQueuesRatherThanFails`.** Four clients push at once against caps
  of 1, 2, and 0. Every push must land. The cap turns a memory problem into a
  latency problem, not into failures.

Every one of these ends with `assertPushGaugesDrained`. A semaphore released
only on the happy path is the classic bug in this shape of change, and it is
invisible from the outside until the daemon stops taking pushes.

## Risks

- **The release must cover every return path**, including hook failures and
  panics. One `defer` at the acquire point is the only acceptable form.
- **A cap below the number of pushes a CI system fires at once turns a memory
  problem into a latency problem.** That is the intended trade, but it must be a
  deliberate one. This is why the wait-queue gauge is not optional.
- **Interaction with the zstd encoder cap.** A push cap of 4 matches an encoder
  cap of 4. If the push cap is later raised alone, compression becomes the
  bottleneck and pushes get slower for a reason the push metrics do not show.
- **This does not reduce the cost of one push.** A lone push of a very large
  repository still allocates whatever it allocates.

## How this is verified

Re-run the sweep and confirm the slope for each push stops mattering above the
cap:

```sh
go run ./cmd/membench -conc-steps 1,2,4,8,16
```

With `-max-concurrent-pushes 4`, peak RSS at K=8 and K=16 must land close to
peak RSS at K=4 instead of climbing. The failure column must stay at zero, and
the extra pushes must show up as longer wall clock.

**`cmd/membench` is not in this repository.** This step is therefore not yet
run. The tests above cover admission, the deadline, the release, and the
client-visible failure. They do not measure resident set.
