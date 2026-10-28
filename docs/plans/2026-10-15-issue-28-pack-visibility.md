# Fresh-reader pack visibility: investigation and implementation plan

> For agentic workers: Use `superpowers:executing-plans` for execution. Complete the evidence gate before selecting a production change.

**Goal:** After a successful push, a fresh reader can fetch the advertised commit and its reachable objects.

**Additional requirement:** All bin uploads must finish before hooks execute. Log exactly `waiting for packfiles to finish pushing` before the upload wait and before hook execution.

**Architecture:** Keep the shared Tigris storer as the publication boundary. Complete each `.bin` upload before its `.cue` upload. Complete all uploads before publishing refs. Require a storage consistency contract that preserves this order for readers.

**Tech stack:** Go 1.26, AWS S3 SDK, Tigris, go-git v6, real Git protocol tests.

**Spec:** [Issue 28](https://github.com/tigrisdata/objgit/issues/28), including the requirement in its first comment.

**Status:** Local implementation and checks complete. Production root cause remains unconfirmed. Source review used commit `b60f728b57b24720389f4fc3c5f8fd59b1b89719`.

## Execution decisions

The user approved this plan, then requested a more ambitious implementation.
The user also stated that the cluster is inaccessible and requested a personal-cluster test procedure.

- A regression test reproduced unsafe ref publication after an earlier upload failure. Upload errors now remain fatal for that writer.
- The requested wait log precedes the ref-publication barrier and hook execution.
- Discovery recovery uses a one-second cooldown after each attempt. Concurrent misses share one refresh, and later misses can retry.
- The refresh adds only unseen immutable packs. It preserves existing tables, snapshots, and local registrations.
- Real HTTP and SSH tests use the AWS SDK and Tigris storer against a gated S3 HTTP fixture.
- Those tests reproduce bin/cue failures and prove that hooks wait for successful publication.
- The new HTTP shallow-clone test exposed an extra NAK in the initial shallow response. A narrow HTTP adapter ends that round at its flush.
- Production bucket inspection and live-cluster runs remain operator tasks. See [the test procedure](../usage/pack-publication-check.md).
- No deployment, bucket migration, or production repair was performed.

## Validation results

- `go test ./...`: passed.
- `go build ./...`: passed.
- `go test -race ./internal/storage/tigris`: passed.
- Focused daemon race tests for publication, HTTP/SSH hooks, and shallow response framing: passed.
- The broader daemon race run reported a race in the unchanged `TestSmartHTTPHealsDanglingHEAD` memory fixture.
- A review found that refresh blocked known-object reads. A failing regression test reproduced it, and the corrected fast path passes.
- The live-bucket and personal-cluster tests were not run. The user will run the supplied procedure.

The task checklists below preserve the original investigation plan. Production-only items remain pending operator evidence.

## Findings

- `packJob.run` in `internal/storage/tigris/packwriter.go` already performs synchronous `.bin` and `.cue` PUTs in that order.
- `commitRefsNotifying` in `refcache.go` flushes all queued uploads before its conditional ref PUT.
- `ensurePacksBuilt` in `packindex.go` caches a successful listing, including an incomplete listing, for the instance lifetime.
- `packLookup` does not refresh that listing after a miss. The object reader then tries the loose-object key.
- Each request gets a scoped storer. A fresh request can recover from temporary visibility lag, but repeated failures need further evidence.
- Existing tests cover cold pack reads and a general upload failure. They do not isolate `.cue` failure or delayed discovery after successful PUTs.
- The architecture document requires Single-region or Multi-region buckets. The affected bucket mode and deployed image digest remain unknown.

A local GET after a PUT cannot prove visibility in every reader region. A retry cannot provide a strict publication guarantee either.

## Global constraints

- Preserve the `.cue` format and existing repository keys.
- Keep pack uploads concurrent across containers. Preserve sequential publication within each container.
- Keep uploads streamed from staging files and retain the current memory bounds.
- Use table-driven tests with `tt`, channel barriers, and bounded waits. Do not use sleeps to control races.
- Keep transport behavior shared through the storer.
- Require successful completion of all bin and cue uploads before publishing branch tips and executing their hooks.
- Use `Signed-off-by: Xe Iaso <xe@tigrisdata.com>` for implementation commits.
- Do not delete or rewrite affected production objects during diagnosis.

## Review focus

1. A failed `.bin` PUT must prevent its `.cue` PUT and every dependent ref update.
2. A failed `.cue` PUT must prevent refs, even when its `.bin` exists.
3. A successful but incomplete LIST must be distinguishable from absent or corrupt stored data.
4. Fresh readers must read complete object bodies without writer overlays or a shared local pack cache.
5. Cancellation, permission errors, and corruption must remain errors rather than become missing-object retries.

## Task 1: Establish the production failure boundary

**Files:** Record evidence in this plan. Read `docs/architecture/tigris-storer.md` and `docs/reference/tigris-backend.md`.

- [ ] Record the deployment image digest and source revision. Compare its upload and discovery code with the reviewed revision.
- [ ] Read the bucket mode, endpoint, and writer/reader regions through the Tigris bucket tools.
- [ ] Read `raidsim/arcadion/packed-refs` and any legacy ref for `refs/heads/main`.
- [ ] List every page of `raidsim/arcadion/packs/`. Parse the cues to locate `cad12a6e25760bbcce5dac516047cfadeb0d78d9`.
- [ ] For a matching cue, GET its bin directly. Check the indexed range, reconstructed object hash, and delta bases.
- [ ] Compare LIST with direct GET for known keys from upload logs. Record timestamps and request identifiers across affected regions.
- [ ] Classify the result using this decision table.

| Evidence                                                   | Next action                                                                                          |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Deployed code differs in publication ordering              | Reproduce that ordering and deploy the corrected sequential path.                                    |
| Cue GET succeeds but LIST omits its known key              | Investigate discovery visibility and the bucket consistency contract.                                |
| Cue exists but bin GET fails                               | Distinguish failed upload, delayed visibility, and missing data from logs and repeated direct reads. |
| Cue contains the commit and bin bytes are valid            | Trace parsing, lookup, prefix, and hash-format handling with that fixture.                           |
| Commit is absent from all available cues and loose storage | Investigate ingestion or lost data. A read retry cannot reconstruct it.                              |
| Unsupported consistency mode explains the failure          | Use a supported bucket configuration and prevent unsupported writable deployments.                   |

**Exit criterion:** Identify the failed boundary or explicitly record which production evidence is unavailable. Do not label listing lag as the root cause without evidence.

## Task 2: Pin publication ordering and failure behavior

**Files:** `internal/storage/tigris/client_test.go`, `pack_test.go`, and `refcommit_test.go`.

**Interface:** Add test-only gates and failures keyed by S3 operation and key. Preserve the existing `s3API` interface.

- [ ] Add a channel gate before a selected PUT becomes visible. Invoke gates outside the fake client's mutex.
- [ ] Record PUT start, completion, and failure events under a mutex.
- [ ] Add `TestPackPublicationOrder` with blocked bin, failed bin, blocked cue, failed cue, and successful upload cases.
- [ ] Start a ref update while the bin is blocked. Assert that neither cue publication nor ref publication occurs.
- [ ] Release the bin and block the cue. Assert that the bin exists and the ref remains unchanged.
- [ ] Inject each PUT failure separately. Assert the returned error and unchanged remote ref through a fresh storer.
- [ ] Repeat with several containers. Require all cue uploads to complete before the ref PUT.
- [ ] Exercise both packed and legacy ref paths where supported by the test constructors.
- [ ] Check a second ref attempt after a failed flush. The uploader currently clears its error on flush, so record any unsafe reuse.

The required event order for every referenced container is:

```text
bin PUT succeeds -> cue PUT starts -> cue PUT succeeds -> ref PUT starts
```

Run:

```sh
go test ./internal/storage/tigris -run 'TestPackPublicationOrder|TestPackUploadFailureBlocksRef|TestCommitRefs'
```

Some ordering cases are expected to pass on current code. They protect the explicit issue requirement without inventing a missing fix.

## Task 3: Reproduce discovery failures and select the smallest correction

**Files:** `internal/storage/tigris/client_test.go`, `pack_test.go`, and, only for a reproduced defect, `packindex.go`, `objects.go`, or `upload.go`.

- [ ] Add independent test controls for LIST omission, cue GET absence, and bin GET absence after successful PUTs.
- [ ] Publish a real pack and ref. Construct a fresh reader with no shared pack cache.
- [ ] Hide its cue from the first successful listing. Restore listing visibility and read the advertised commit again.
- [ ] Assert the current behavior: the same reader retains its incomplete index, while another fresh reader can discover the pack.
- [ ] Add separate cases for permanent absence, permission errors, malformed cues, and canceled contexts.
- [ ] Select the production correction from Task 1 evidence.

**Preferred correction:** Enforce the existing supported storage contract and preserve publication ordering. Do not add a manifest or global retry loop by default.

**If supported storage exposes a reproducible stale-discovery window:** Add one request-bounded index refresh after both pack and loose lookup miss. Coalesce concurrent refreshes. Preserve local registrations and install the new remote tables atomically. Do not append repeated copies of old remote tables. Keep corruption and permission errors outside this path. Add the corresponding cases for `HasEncodedObject`, `EncodedObjectSize`, and `EncodedObject`.

**If failure is in ingestion or upload:** Correct that reproduced defect instead. Preserve a failed transaction state if the second-flush test proves that refs can bypass an earlier upload error.

**If the bucket mode is unsupported:** Determine whether the provider exposes authoritative bucket-mode metadata to the daemon's credentials. Reject known unsupported modes before enabling writes. If metadata is unavailable, document an explicit operator preflight instead of claiming automatic detection. Define that configuration interface in a follow-up design before coding it.

Retries are bounded recovery only. Supporting eventual consistency as a publication guarantee requires a separate design for ref and pack discovery.

## Task 4: Make the pre-hook upload barrier explicit

**Files:** `cmd/objgitd/receivepack.go`, `cmd/objgitd/hooks.go`, the receive-pack tests, and `docs/architecture/hooks.md`.

**Interface:** Preserve the existing `onUpdated` callback and storage interfaces. Use the upload flush inside reference publication as the barrier.

The current sequence is `writePack -> updateReferences -> report-status -> onUpdated -> snapshots -> hooks -> webhooks`.
The Tigris ref mutators already wait for uploads. Hooks must remain after that successful wait.

- [ ] Add the following server log before `updateReferences` for a successfully received pack, before its upload flush can block:

```go
if needPackfile {
    slog.Info("waiting for packfiles to finish pushing")
}
```

- [ ] Keep the exact message text. Add repository context if the existing logger provides it.
- [ ] Do not place the log only in `runHook`: that location is after the upload wait.
- [ ] Keep the ref-publication barrier. Do not add a second post-commit flush that can consume or conceal upload failures.
- [ ] Add a gated upload test through the shared receive-pack path. Block a bin PUT and capture logs and hook execution.
- [ ] Assert that the exact log appears while the upload is blocked and that the hook has not started.
- [ ] Release the bin and block the cue. Assert that the hook still has not started.
- [ ] Release the cue. Assert that the ref update succeeds before the hook starts.
- [ ] Fail either upload. Assert that the ref stays unchanged and the hook never executes.
- [ ] Include multiple containers so one completed bin cannot release the hook while another upload remains pending.
- [ ] Cover HTTP and SSH hook dispatch, plus a deletion-only push that does not emit a pack-upload wait message.
- [ ] Update the hook architecture document to state the upload, ref publication, and hook execution order.

Expected successful sequence:

```text
waiting for packfiles to finish pushing
all bin and cue PUTs complete successfully
refs commit
hook executes
```

The log describes the server upload barrier. It does not claim visibility in every region.

## Task 5: Check the full push-to-fetch contract

**Files:** `internal/storage/tigris/livebucket_test.go`, `cmd/objgitd/http_test.go`, `cmd/objgitd/ssh_test.go`, and `docs/architecture/tigris-storer.md`.

- [ ] Extend the opt-in live pack test to publish a branch ref and resolve it from a new root storer.
- [ ] Read and hash the commit, trees, blobs, and delta bases reachable from that ref.
- [ ] Use a unique test prefix. Limit cleanup to keys created by the test.
- [ ] Run an opt-in daemon smoke test against the supported bucket with a fresh process and empty pack cache.
- [ ] Push, check `ls-remote`, and clone with `--depth=1 --no-checkout` over HTTP and SSH.
- [ ] Check the cloned tip and run `git fsck --full` in each shallow clone.
- [ ] Repeat a fetch after a second push and after a daemon restart.
- [ ] Update the architecture document with the measured failure boundary, publication contract, and supported deployment configuration.

Run the focused tests first, then:

```sh
go test -race ./internal/storage/tigris
go test ./cmd/objgitd -run 'TestSmartHTTP|TestSSH'
go test ./...
go build ./...
```

The full suite needs the Git LFS artifacts described in `AGENTS.md`. Live tests need an explicitly configured test bucket.

## Acceptance criteria

- The diagnosed failure has a regression test that fails before its correction and passes afterward.
- Ordering tests cover both bin and cue failure independently.
- Hooks cannot start while any bin or cue upload remains pending or after an upload failure.
- The exact log `waiting for packfiles to finish pushing` precedes the upload wait and hook execution.
- A successful push is fetchable from a fresh process on supported storage through HTTP and SSH.
- The report identifies the production bucket mode and image revision, or clearly states that those facts remain unavailable.
- No local read probe or finite retry is described as proof of global visibility.
- Existing missing production data, if found, has a separate recovery proposal based on an intact source repository.

## Review follow-up

The review found two limits in the first implementation. This follow-up replaces both mechanisms.

1. Replace the HTTP request decode/re-encode pass with a packet reader. Forward the original bytes and retain at most one packet. Let upstream validate the request once.
2. Replace the lifetime refresh budget with a one-second cooldown after each completed attempt. Concurrent misses share one result. Errors remain visible during the cooldown. Later misses can retry, including after an earlier genuine miss or storage failure.
3. Test byte preservation, fragmented input, shallow-round detection, repeated publications, failure recovery, and concurrent requests. Use virtual time for timing-only tests. Full pack fixtures use the real cooldown because the shared compression pools cannot cross virtual-time boundaries.
4. Run the full suite, build, and focused race checks. Request another skeptical review before completion.

This policy bounds refresh frequency per storer. It does not promise immediate visibility during the cooldown or change the bucket consistency requirement.

Follow-up validation:

- The old refresh implementation fails both new recovery cases: a genuine missing probe and a transient listing error.
- The corrected reader discovers two successive publications through all three object lookup APIs. Concurrent misses share each attempt.
- Virtual-time tests cover a slow refresh, cooldown timing, cancellation during cooldown, and cancellation of an active waiter.
- HTTP tests cover fragmented input, maximum-size packets, malformed packets, read errors, and exact request bytes.
- The adapter returns its packet buffer after the first flush, before pack generation.
- `go test ./...` and `go build ./...` pass.
- The full storage race suite and focused daemon race checks pass. Final focused race checks include all new cases.
- A second skeptical review found no blockers. Its three optional improvements are included.

The adapter benchmark compares packet forwarding with the previous decode/replay pass. In the final local sample, 10,000 wants took 0.25 ms versus 1.55 ms. Allocations were about 4.3 KB versus 3.66 MB. These measurements cover adapter overhead, not total fetch throughput. Run `go test ./cmd/objgitd -run '^$' -bench BenchmarkUploadRequest -benchmem` to repeat them.

Production-cluster validation remains pending. The operator procedure still applies.
