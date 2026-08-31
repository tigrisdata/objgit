# Keep the deltas that a push already computed

## Context

A clone of a large repository spent almost all of its CPU time in one
place. A 30-second profile of `objgitd` while it served a clone of
`tigris-os` gave these numbers:

| Frame                          | Flat  | Cumulative |
| ------------------------------ | ----- | ---------- |
| `(*deltaIndex).findMatch`      | 47.9% | 85.0%      |
| `hashBlock`                    | 34.1% | 34.1%      |
| `diffDelta` and everything under it | —     | 92.8%      |

No code from this repository was in the profile. Every frame belonged to
the delta search of go-git.

The cause was in `packwriter.go`. `PackfileWriter` gave the incoming
pack to a scratch `storage/filesystem.Storage`, which resolved every
delta. The writer then stored each object whole. The client had already
computed a good delta chain, and the server discarded it. Every later
clone computed a new one.

`Storer` did not implement `storer.DeltaObjectStorer`. Because of this,
`deltaSelector.encodedDeltaObject` fell through to the resolved path,
every object entered the packer as a whole object, and no delta could be
reused.

## What the search costs

A full clone of `tigris-os` (59,459 objects) over SSH, measured twice
per configuration, with the delta search on and then off:

| Measurement | Search on | Search off |
| ----------- | --------- | ---------- |
| Wall time   | 108.97s   | 26.71s     |
| Daemon CPU  | 123.70s   | 43.76s     |
| Wire size   | 80.86 MiB | 141.24 MiB |

The search therefore cost 82.3 seconds of wall time and 79.9 seconds of
CPU for each clone.

A second profile with the search off showed a different daemon. 35% of
samples were in `syscall.rawsyscalln` under `bufio.Writer.Flush`, and
under 2% were in `compress/flate`. Without the search, the daemon is
bound by input and output, not by the processor.

That second profile is why this design is better than the same clone
with delta compression turned off. Both reach the same floor. Only this
one still sends the smaller 80.86 MiB pack.

## The design

Keep the delta of the client. Do not compute a new one.

**Format.** `.cue` version 3 is version 2 with one more column: the hash
of the delta base, or all zero bytes. A record holds a delta when that
base is not zero. There is no separate flag, and a run of zero bytes
costs almost nothing after zstd.

Two fields keep their old meaning, which is what keeps the rest of the
package correct with no change:

- `typ` is the real object type, never a git delta type. Every type
  filter continues to work.
- `raw` is the size of the object after the delta is applied. This keeps
  `stored <= raw` true, which the container byte limit depends on.

**Write.** The writer makes two passes. The first reads the hash, the
type, the size, and the delta base of each object, then orders the
objects so that a base always comes before the delta that needs it. This
order is necessary because `IterEncodedObjects` returns hash order.

The second pass writes the payloads. A delta is kept only when its base
is in the container under construction. If a container seals between the
two objects, the writer stores the whole object instead.

This containment rule keeps the older promise that every container
resolves on its own. Without it, an interrupted push can leave a delta
whose base never uploaded. The cost is a few deltas at each container
boundary.

**Read.** `EncodedObject` hides deltas. It fetches the delta, reads the
base, and applies one to the other. `DeltaObject` is the new method that
gives the delta itself, and it is the method that makes `Storer` satisfy
`storer.DeltaObjectStorer`.

A read walks at most 50 links, which is the limit that go-git uses, and
keeps the hashes it has seen. A missing base, a chain that is too long,
and a cycle each give an error. None of them looks like a missing
object.

## What this design delivered

The same clone of `tigris-os`, after the change:

| Measurement | Before    | After     |
| ----------- | --------- | --------- |
| Wall time   | 108.97s   | 84.32s    |
| Daemon CPU  | 123.70s   | 101.39s   |
| Wire size   | 80.86 MiB | 80.86 MiB |

The pack that the server sends holds 27,113 deltas, against 27,110
before. The quality of the pack did not change.

This is a gain of 23% in wall time, not the four times that the
measurement with the search off suggested. The reason is a limit of the
method, not a fault in it. Counters in the writer gave these numbers for
one push of 59,459 objects:

| Result                       | Objects |
| ---------------------------- | ------- |
| Delta kept                   | 26,514  |
| Client sent no delta         | 32,349  |
| Base in another container    | 596     |

The writer keeps 97.8% of the deltas that the client sends. Only 596
objects lose their delta to a container boundary.

But 32,349 objects, or 54% of the push, arrive whole. These are the
delta bases and the objects that are too small to compress against
another. go-git runs its search on every one of them, because it must
decide whether each can become a delta. Delta reuse removes the search
only for objects that are already deltas.

Real git does not have this cost. It copies a whole region of an
existing pack into the output and never examines those objects. go-git
has no equivalent.

## What this design does not do

There is no migration. A version 1 or version 2 container stays readable
and holds no deltas, so a repository written before this change keeps
its old clone cost until something pushes to it.

The first pass costs a second read of most payloads, so a push is
slower. This is deliberate. A push happens once. A clone happens many
times.

The delta chain is only as good as the client that sent it. A client
that pushes an undeltified pack gives the server nothing to reuse.
