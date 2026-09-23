# Plan: Git LFS support

## Context

`objgitd` stores every git object in a Tigris bucket. Large binary files are the
worst case for that design. `docs/reference/tigris-backend.md:466-474` records
one cost: `EncodedObject` reads a whole body into a `MemoryObject`, so one read
of a large blob costs its full size in resident heap.
`docs/architecture/tigris-storer.md:395-397` records a second: an object larger
than the 128 MiB pack cap gets a container to itself and exceeds the cap.

Git LFS removes both. The repository holds a small pointer file. The large bytes
live under a separate key, and the client transfers them out of band. For a
server whose premise is object storage, the client can talk to the bucket
directly.

There is no LFS code in the repository today. A search for `lfs` across `*.go`
and `*.md` returns nothing.

Decisions from the user:

- **Presign only.** `objgitd` never carries object bytes. The batch response
  hands the client a presigned Tigris URL. The daemon is a control plane for
  LFS, not a data plane.
- **Bucket-global deduplication.** Blob bytes live once at `lfs/blobs/<sha256>`,
  shared by every repository.
- **Full protocol surface.** The batch API, the `verify` endpoint, the file
  locking API, and `git-lfs-authenticate` over SSH.

Code and test conventions follow the `xe-go:xe-go-style` and
`xe-go:go-table-driven-tests` skills. Documentation follows `simple-english`.

Three findings from the design review changed the shape of this plan. Each one
is marked CAUTION below, and each is verified against source, not assumed.

## Files

| Action | Path | Purpose |
| --- | --- | --- |
| Create | `internal/lfs/lfs.go` | Package doc, oid validation, key layout. |
| Create | `internal/lfs/batch.go` | Batch types and the batch resolver. |
| Create | `internal/lfs/store.go` | The bucket surface: `s3API` and the presigner. |
| Create | `internal/lfs/presign.go` | The custom `HTTPPresignerV4` that stops header hoisting. |
| Create | `internal/lfs/locks.go` | The lock document and its compare-and-swap writer. |
| Create | `internal/lfs/errors.go` | The `application/vnd.git-lfs+json` error body. |
| Create | `cmd/objgitd/lfs.go` | The HTTP handlers, on `*daemon`. |
| Edit | `cmd/objgitd/http.go` | New routes, registered only when LFS is on. |
| Edit | `cmd/objgitd/ssh.go` | Intercept `git-lfs-authenticate`. |
| Edit | `cmd/objgitd/git_protocol.go` | One new `daemon` field. |
| Edit | `internal/repofs/repofs.go` | Reject reserved organization names. |
| Edit | `cmd/objgitd/main.go` | Flags, the presign client, and the wiring. |
| Edit | `internal/metrics/metrics.go` | The `objgit_lfs_*` vectors and helpers. |
| Create | `internal/lfs/*_test.go`, `cmd/objgitd/lfs_test.go` | Tests. |
| Create | `docs/architecture/lfs.md`, `docs/usage/lfs.md`, `docs/plans/lfs.md` | Documentation. |
| Edit | `docs/architecture/README.md`, `AGENTS.md`, `README.md` | Index rows. |

## Design

### The package boundary

`internal/lfs` owns the protocol and the bucket. It knows nothing about routing,
`*daemon`, or Prometheus. `cmd/objgitd/lfs.go` owns the handlers: parse the
path, call `d.authorize`, call into `internal/lfs`, render the result. This is
the split that `internal/auth` and `cmd/objgitd/http.go` already use.

The package reaches the bucket through one narrow interface, in the style of
`s3API` in `internal/storage/tigris/tigris.go:86-93`, so tests can supply a fake
bucket. CI runs with no Tigris credentials.

```go
type s3API interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}
```

The presigner is **not** behind a fake. Presigning does no network I/O, so tests
run the real one against static credentials. See Testing.

### The key layout

```
lfs/blobs/<sha256>                     the bytes, once per bucket
acme/test/lfs/objects/<sha256>         a zero-byte membership marker
acme/test/lfs/locks.json               one lock document per repository
acme/test/objects/<hex>
acme/test/packs/<id>.bin
```

Bytes are global. Membership is per repository.

CAUTION: Global bytes alone are a cross-repository read oracle. Without the
marker, any caller can ask `/attacker/scratch/.../batch` for an oid belonging to
a private repository and receive a presigned GET for it. LFS oids leak in
pointer files, forks, and CI logs. Every batch decision therefore reads the
**marker**, never the global blob. An upload writes the marker after `verify`
succeeds. The marker is also the reference count that any future collector
needs.

CAUTION: `repofs.Parse` accepts any two non-empty segments
(`internal/repofs/repofs.go:42-52`), so a repository named `lfs/blobs` takes the
LFS prefix for itself. `repofs.Parse` must reject the reserved organization name
`lfs`, and any segment that starts with `.` (which also protects the existing
`.objgit/ssh_host_ed25519_key`). Put this in `repofs`, so every transport gets
it, and pin it with a test.

### Validation comes first

The oid becomes part of a bucket key, and `repofs.Parse` does not sanitize
anything. Validation runs before any other work:

- The oid is exactly 64 characters.
- Every character is `0-9` or `a-f`. Uppercase is rejected, never normalized. An
  uppercase oid would create a second key for the same bytes and break dedup.
- The test is `hex.DecodeString` plus a length check. It is never a
  `strings.Contains(oid, "..")` check.
- `size` is zero or more, and not larger than `-lfs-max-size`.
- `operation` is exactly `upload` or `download`. Anything else is an error, not
  a default.

A malformed request is 422 with an LFS error body.

### The batch endpoint

`POST /{orgID}/{repoName}/info/lfs/objects/batch` is the whole protocol.
`download` maps to `auth.Read`, `upload` to `auth.Write`. The handler calls
`d.authorize` the way `(*daemon).resolve` does (`cmd/objgitd/http.go:184-223`).

For each object the handler does one `HeadObject` on the per-repository marker.
The calls run under an `errgroup` with `SetLimit`. A batch larger than
`-lfs-max-batch` is 413.

| Operation | Marker present | Response for that object |
| --- | --- | --- |
| `download` | yes | `actions.download`, a presigned GET |
| `download` | no | `error`, code 404 |
| `upload` | yes, size matches | no `actions` key, so the client skips it |
| `upload` | yes, size differs | `error`, code 422 |
| `upload` | no | `actions.upload` and `actions.verify` |

When the marker is absent but the global blob already exists, the upload action
still goes out. The client re-uploads bytes the bucket already holds. The
alternative, trusting a global blob the caller cannot prove it owns, is the read
oracle above. Correctness wins; the cost is one re-upload per new repository.

Every object carries `"authenticated": true`.

CAUTION: Without `authenticated`, git-lfs sends the transfer through
`DoWithAuthNoRetry`, which runs `git credential fill` against the Tigris host
and can attach an `Authorization` header. A SigV4 presigned request rejects a
request that also carries `Authorization`. The user then gets a credential
prompt for `t3.storage.dev`, or a 400 on every transfer.

Every action carries `expires_in`, in seconds, so the client knows the href goes
stale.

### Upload integrity

A presigned PUT means the daemon never reads the body. Two checks are needed,
and only one of them works at PUT time.

The presigned `PutObjectInput` sets `ChecksumSHA256` to the base64 encoding of
the oid's raw 32 bytes. The bucket then rejects a body whose sha256 does not
match the key.

CAUTION: The stock presigner hoists that header into the query string, so it
never reaches `SignedHeader`. This is verified:
`aws-sdk-go-v2@v1.41.7/aws/signer/internal/v4/headers.go:67-70` hoists anything
matching `X-Amz-` that is not in `RequiredSignedHeaders`, and
`X-Amz-Checksum-Sha256` is not in that list. The fix is a custom
`HTTPPresignerV4` (`s3.PresignOptions.Presigner`) that wraps `v4.NewSigner()`
and sets `DisableHeaderHoisting = true`. The checksum then appears in
`SignedHeaders`, and `PresignedHTTPRequest.SignedHeader` carries it back. Put
that map, minus `Host`, into `actions.upload.header`; git-lfs sends every header
in it.

CAUTION: `ContentLength` on a presigned PUT is a no-op. The SDK writes a
`Content-Length` header, but the signer skips it, and it is not hoisted. The
declared `size` is therefore **not** enforced at upload time.

So `verify` is the real gate, not an optional extra. It is the only check that
runs on the daemon's own credentials.

### The verify endpoint

`POST /{orgID}/{repoName}/info/lfs/objects/verify` takes `{oid, size}`. It does
one `HeadObject` on the global blob with `ChecksumMode: ChecksumModeEnabled`,
then compares both the length and the returned sha256.

- Both match: write the per-repository marker, return 200.
- Object missing: 404.
- Length or checksum wrong: delete the blob, return 422.

The marker is written here, and only here. An upload that never verifies leaves
no membership behind.

### Locks

One JSON document per repository at `<orgID>/<repoName>/lfs/locks.json`. Writes
use the compare-and-swap the ref cache already uses
(`internal/storage/tigris/refcache.go:200`): `If-None-Match: *` for the first
write, then `If-Match: <etag>`, re-read and retry on refusal, under a bounded
retry count.

| Route | Operation | Behavior |
| --- | --- | --- |
| `POST .../info/lfs/locks` | Write | 201 with the lock, or 409 with the holder. |
| `GET .../info/lfs/locks` | Read | List, with `path`, `id`, `cursor`, `limit`. |
| `POST .../info/lfs/locks/verify` | Read | Split into `ours` and `theirs`. |
| `POST .../info/lfs/locks/{id}/unlock` | Write | Delete one. `force` takes another owner's lock. |

Lock IDs are generated by the server as lowercase hex, and validated on read. A
client-supplied `{id}` is never used to build a key.

The owner is the HTTP Basic username, or `anonymous` when there is none. The
`ours` and `theirs` split uses that name.

CAUTION: git-lfs halts a push when a file matches a lock in `theirs`, and it
records `lfs.<url>.locksverify=true` after the first 200. With no user store,
every caller that sends no credential is the same owner `anonymous`. All locks
then land in `ours`, so a fully anonymous deployment never blocks a push. A
deployment where some users set a Basic username gets correct behavior. Say this
in `docs/usage/lfs.md`. `-allow-lfs-locks` turns the four routes off, and an
unregistered route is a 404, which the spec states does not halt a push.

### git-lfs-authenticate over SSH

`cmd/objgitd/ssh.go:123-136` rejects any command `gitServiceFor` does not know.
The interception goes before that call. The command arrives as three arguments:
`git-lfs-authenticate <path> <operation>`.

The handler parses the path with `repofs.Parse`, maps `download` to `auth.Read`
and `upload` to `auth.Write`, calls `d.authorize` with `Transport: "ssh"`, then
writes one JSON object to the session, not to stderr, and exits 0:

```json
{"href":"https://git.example.com/acme/test.git/info/lfs","header":{},"expires_in":900}
```

CAUTION: When LFS is off, or `-external-url` is unset, the handler must write
`git-lfs-authenticate: command not found` to stderr and **exit 127**. git-lfs
treats any other non-zero exit as a real error and gives up. With exit 127 it
falls back to guessing the HTTP endpoint, which is the behavior every
non-LFS-aware git server produces today. Exit 1 stays for a bad path or a denied
request.

The `header` map is empty, because the HTTP side accepts anonymous requests.

### Flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `-allow-lfs` | `false` | Register the LFS routes. Opt-in, like `-allow-hooks`. |
| `-allow-lfs-locks` | `true` | Register the four lock routes. |
| `-external-url` | `""` | The public base URL, for `git-lfs-authenticate`. |
| `-lfs-url-ttl` | `15m` | How long a presigned URL stays valid. |
| `-lfs-max-size` | `5368709120` | The largest object. One PUT caps at 5 GiB. |
| `-lfs-max-batch` | `500` | Objects per batch, over which the answer is 413. |

Startup checks, in the style of `main.go:76-84`: `-external-url` must be
absolute, carry a scheme, and end without a slash. It is required when
`-allow-lfs` and `-ssh-bind` are both set. `-allow-lfs` with an empty
`-http-bind` is fatal, because `git-lfs-authenticate` has nothing to point at.

The presign client is built from `rawClient.Client` at `main.go:97`, the
embedded `*s3.Client` in `tstorage.Client`. It is **not** built from the
hardened client at `main.go:106`: `s3fs.Harden` returns an interface value, and
`s3.NewPresignClient` needs the concrete type. Hardening is irrelevant here,
because presigning performs no network I/O.

CAUTION: `tstorage.New` falls back to the AWS credential chain when
`TIGRIS_STORAGE_ACCESS_KEY_ID` is unset, and `AGENTS.md` documents `AWS_PROFILE`
as the normal path. A chain that lands on SSO, IMDS, or assume-role yields a
credential that expires, and the presigned URL dies with it whatever
`-lfs-url-ttl` says. At startup, when `-allow-lfs` is set, retrieve once and
check `aws.Credentials.CanExpire`. If it can expire, clamp every URL's lifetime
to the remaining session time minus a margin, and log that clamp. Under a minute
left, answer the batch with 503 instead of handing out URLs that will fail.

### HTTP status codes

git-lfs reads these precisely. Both directions use
`Content-Type: application/vnd.git-lfs+json`.

- 200 for every batch that parsed, even when individual objects carry an `error`.
- 401 with `LFS-Authenticate`, not only `WWW-Authenticate`.
- 403 when the authorizer denies a write. The git path returns 401 here
  (`http.go:196`); LFS must not.
- 404 for a missing repository. 406 for a wrong `Accept`. 413 for an oversized
  batch. 422 for a body that fails validation.
- Per-object errors are `objects[].error = {code, message}`.
- The top-level error body is `{message, request_id, documentation_url}`.

### Route registration

The patterns go in `httpHandler` inside an `if d.lfs != nil` guard. Existing
tests build bare `&daemon{...}` literals (`http_test.go:106`), so an
unconditional registration would nil-dereference. An unregistered route is a
ServeMux 404, which is exactly the "no LFS here" answer both git-lfs and the
locking spec expect.

The nine patterns have fixed segment counts and no trailing wildcards, so no two
can match the same request and ServeMux cannot panic. Nothing shadows
`GET /{orgID}/{repoName}/info/refs`.

### Metrics

The package takes observer functions. `main` wires `metrics.*` into them, and
the package never imports Prometheus. New vectors under namespace `objgit`:

- `lfs_batch_requests_total{operation,status}`
- `lfs_batch_duration_seconds{operation}`
- `lfs_objects_total{operation,result}`, where `result` is `new`, `present`,
  `missing`, or `error`. The ratio of `present` to `new` is the dedup rate.
- `lfs_verify_total{status}`
- `lfs_lock_requests_total{action,status}`
- `lfs_presign_total{method}`

### Testing

CI runs `go vet ./...` and `go test ./...` with no bucket credentials
(`.github/workflows/go.yml`). Tests run against a fake bucket, in the style of
`fakeS3` in `internal/storage/tigris/client_test.go:43`.

The presigner is tested for real, not faked. Build an `*s3.Client` with
`credentials.NewStaticCredentialsProvider`, a fixed `BaseEndpoint`, and a fixed
signing time, then assert the URL. This is the only test that catches a
regression in header hoisting, and a fake presigner would hide it forever.

`internal/lfs`, table-driven with `tt`:

- An oid of the wrong length, wrong case, or with non-hex bytes is rejected.
- An oid that encodes a path traversal is rejected.
- A negative size, and a size over the cap, are rejected.
- An unknown `operation` is rejected.
- Download of a present marker returns a download action.
- Download of a missing marker returns a per-object 404, and the request is
  still 200.
- Download of an oid whose global blob exists but whose marker does not returns
  404. This is the read-oracle test.
- Upload of a present marker of the same size returns no actions.
- Upload of a present marker of a different size returns 422.
- Upload of a new oid returns an upload action and a verify action.
- The presigned upload URL's `X-Amz-SignedHeaders` contains
  `x-amz-checksum-sha256`, and `SignedHeader` carries that header.
- Every object carries `authenticated`, and every action carries `expires_in`.
- A batch over `-lfs-max-batch` is 413.
- `verify` returns 200, 404, and 422, and writes the marker only on 200.
- `verify` deletes the blob on a checksum mismatch.
- A lock create on a free path returns the lock; on a held path, the holder.
- A refused compare-and-swap on the lock document retries, then succeeds. Use
  the `putHook` pattern to land a competing write mid-loop.
- An unlock by another owner fails without `force`, and succeeds with it.

`internal/repofs`: an organization named `lfs`, and any segment starting with
`.`, are rejected.

`cmd/objgitd/lfs_test.go`, extending `newHTTPServer` with a fake store:

- Every response carries `application/vnd.git-lfs+json`.
- A wrong `Accept` is 406.
- An upload batch with `-allow-push` unset is 403, not 401.
- Every LFS route is 404 when `d.lfs` is nil.
- A malformed body is 422, not 500.

`cmd/objgitd/ssh_test.go`, gated on `exec.LookPath("ssh")` as the file already
does:

- `git-lfs-authenticate acme/test.git download` prints JSON whose `href`
  matches `-external-url`.
- With LFS off, the command exits 127 and prints `command not found`.

One live test, gated on `OBJGIT_TIGRIS_LIVE_BUCKET` and
`exec.LookPath("git-lfs")`, in the style of
`internal/storage/tigris/livebucket_test.go:24`.

CAUTION: Presign-only means `go test ./...` can never prove that LFS works. The
bytes never touch `objgitd`. Six facts are unknowable without a real bucket, and
each needs a named skipped test so the gap stays visible: whether Tigris
enforces `x-amz-checksum-sha256`; what it returns on a mismatch; whether
`HeadObject` with `ChecksumModeEnabled` returns the stored sha256; whether the
presigned host and region resolve and the signature is accepted; the
`If-Match` behavior under contention; and a real `git lfs push` and `pull`.
Treat the live test as the acceptance gate for this feature, not as follow-up
work.

## Verification

1. `go build ./...` and `go vet ./...` pass.
2. `go test ./...` passes with no bucket credentials.
3. Run the live round trip against a real bucket:

```sh
./objgitd -bucket $BUCKET -http-bind :8080 -ssh-bind :2222 \
  -allow-push -allow-lfs -external-url http://localhost:8080 &

git init lfsdemo && cd lfsdemo
git lfs install --local
git lfs track '*.bin'
head -c 50000000 /dev/urandom > big.bin
git add .gitattributes big.bin && git commit -m 'add big file'
git push http://localhost:8080/acme/lfs.git main

tigris ls s3://$BUCKET/lfs/blobs/              # the bytes, once
tigris ls s3://$BUCKET/acme/lfs/lfs/objects/   # the marker

cd .. && git clone http://localhost:8080/acme/lfs.git fresh
cmp lfsdemo/big.bin fresh/big.bin
```

4. Upload a corrupt body to a presigned URL with `curl`. The bucket rejects it,
   or `verify` returns 422 and deletes the blob. Either outcome proves integrity;
   both failing means the feature is not done.
5. Push the same file to a second repository. `objgit_lfs_objects_total` gains a
   `present` count once the marker exists, and a later push of that repository
   transfers nothing.
6. Ask for an oid from repository A through repository B's batch endpoint. The
   answer is 404.
7. Repeat step 3 against `ssh://localhost:2222/acme/lfs.git`. Then run with
   `-allow-lfs` unset and confirm that `git lfs pull` falls back instead of
   failing.
8. Run `git lfs lock big.bin`, `git lfs locks`, and `git lfs unlock big.bin`.

## Remaining gap

- **Deletion.** Nothing deletes an LFS blob, the same gap packs have
  (`docs/reference/tigris-backend.md:490-494`). The per-repository marker gives a
  future collector the reference count it needs, but no collector exists.
- **Objects over 5 GiB.** One presigned PUT caps there. Multipart needs presigned
  part URLs and a client that asks for them.
- **A token for `git-lfs-authenticate`.** The `header` map is empty. Real
  authentication needs an issuer there.
- **The pure-SSH transfer protocol** (`git-lfs-transfer`). An `ssh://` remote
  still needs the HTTP listener.
- **Authorization inside a presigned URL.** The URL works for its full lifetime,
  whatever the Authorizer later decides.
- **One wasted upload per repository** for bytes the bucket already holds, which
  is the price of closing the cross-repository read oracle.

## What implementation changed

The plan above is the design as approved. Three things came out differently, and
the code follows this section where the two disagree.

**Downloads and uploads need different signers.** The plan turned off header
hoisting so an upload's checksum stays inside the signature. That part holds.
But the same setting also signs `x-amz-checksum-mode` on a GetObject, and a URL
signed that way is refused unless the client replays that header. A `git clone`
hid the fault, because git-lfs faithfully sends every header the batch response
names; a plain fetch of the same URL failed with `SignatureDoesNotMatch`. The
presigner now uses the no-hoist signer for uploads and the stock signer for
downloads. `TestPresignGetNeedsNoRequestHeaders` pins it.

**The checksum question is settled.** Tigris does enforce
`x-amz-checksum-sha256` on a presigned PUT. Measured against a live bucket:
wrong bytes answer 400 `XAmzContentSHA256Mismatch`, matching bytes answer 200,
and omitting the header answers 403 `SignatureDoesNotMatch`. That last case is
the important one, because it means a client cannot opt out of the check. The
`-lfs-checksum` flag the plan proposed is therefore unnecessary, and does not
exist.

**Blobs live at `lfs/blobs/`, not `lfs/objects/`.** The per-repository marker
took the `lfs/objects/` name, because that is the name a reader expects to mean
"objects this repository has".

**Uploads go to a per-repository staging key, not straight to the shared blob.**
This is the largest change, and it came out of code review. The plan had the
client upload to `lfs/blobs/<oid>` and had verify grant membership when the blob
existed at the declared size. That check is not a check: an oid and its size sit
side by side in every pointer file, so anyone who could read a pointer could
claim the object in their own repository and then download it. The marker split
was supposed to prevent exactly that, and it did not.

A client now uploads to `<org>/<repo>/lfs/staging/<oid>`. Verify reads that key,
not the shared one, and promotes it with `RenameObject`. Membership therefore
requires having written the bytes, which Tigris checked against the oid as it
accepted them. The same change fixes a second fault the plan had: verify used to
delete the *shared* blob on a size mismatch, so any writer could destroy an
object other repositories depend on. It now deletes only the caller's own staged
key.

`TestVerifyNeedsProofOfPossession` and
`TestLFSVerifyCannotStealAnotherRepositorysObject` pin both.

## Verification performed

Run against a single-region Tigris bucket, with `git-lfs` 3.x:

- A 5 MB file pushed and cloned back byte-for-byte identical. The git pack was
  716 bytes; the blob was the other 5 MB.
- The bucket held one blob under `lfs/blobs/` and one zero-byte marker under
  `acme/lfsdemo/lfs/objects/`.
- The same file pushed to a second repository produced a second marker and no
  second blob.
- An upload batch for an object the repository already held returned no actions,
  so the client transferred nothing.
- A batch request from an unrelated repository for that object answered 404,
  while the owning repository got a download action.
- `git lfs lock`, `git lfs locks`, and `git lfs unlock` all worked.
- The three upload-integrity cases above.
- After a push, the staging prefix was empty: `RenameObject` moved the object
  rather than copying it.
- An unrelated repository calling verify with a known oid and size got 404, and
  still could not download the object.

`internal/lfs/livebucket_test.go` carries the parts of this that can be
automated, behind `OBJGIT_TIGRIS_LIVE_BUCKET`.
