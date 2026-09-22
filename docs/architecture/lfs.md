# Git LFS (`internal/lfs`)

`objgitd` is a control plane for Git LFS, not a data plane. It answers the batch
API with presigned Tigris URLs. The client then transfers to and from the bucket
directly. No LFS object byte passes through the daemon.

This matters because a large blob is the worst case for the git storer.
`EncodedObject` reads a whole body into memory, so one read of a large blob
costs its full size in resident set. An object over the 128 MiB pack cap also
gets a container to itself. LFS keeps both out of the repository: git holds a
small pointer file, and the bytes live under their own key.

The feature is off by default. Without `-allow-lfs` the daemon registers no LFS
route, so every LFS path answers 404.

## Layout in the bucket

| Key | Contents |
| --- | --- |
| `lfs/blobs/<oid>` | The object bytes, stored once for the whole bucket |
| `<orgID>/<repo>/lfs/staging/<oid>` | An upload in flight, before it is verified |
| `<orgID>/<repo>/lfs/objects/<oid>` | A zero-byte marker: this repository holds that object |
| `<orgID>/<repo>/lfs/locks.json` | Every lock held in this repository |

`<oid>` is the SHA-256 of the object, in lowercase hexadecimal.

Bytes are global. Two repositories that hold the same file store it once.
Membership is per repository.

## Membership is separate from storage

Every batch decision reads the marker. No decision reads the blob.

Without that split the server would be a read oracle. A caller who knows an oid
could ask any repository for it and receive a presigned URL for bytes it has no
access to. LFS oids are not secret. They sit in pointer files, in forks, and in
CI logs.

## Uploads land in the repository, not in the shared key

A client uploads to `<orgID>/<repo>/lfs/staging/<oid>`. `Verify` checks those
bytes, moves them to the shared key, and only then writes the marker.

That indirection is what makes a marker mean something. A client that uploaded
straight to the shared key would leave verify able to check only that the bytes
exist and have the right length. A caller who merely knows an oid and a size can
already say both, and a pointer file contains both, side by side. Staging makes
the client prove it holds the content: to get a marker it must first write the
bytes, and Tigris checks them against the oid as it does.

Staging also keeps a failed upload away from anything shared. A rejected object
is deleted from the caller's own staging key. Nothing in the verify path can
delete the blob other repositories are reading.

The promotion uses `RenameObject`, a Tigris extension that moves an object in
place. It copies no data, so promoting a 5 GiB upload costs the same as
promoting an empty one, and it leaves no staged copy to collect.

The cost is one upload per repository. A repository that does not yet hold an
object uploads it again, even when the bucket already has the bytes. The
promotion overwrites the same key, so storage stays at one copy. That is safe
because the key is the hash of the content.

The marker is also the only reference count a collector can use. A blob whose
last marker is gone is unreachable.

CAUTION: `repofs.Parse` rejects the organization ID `lfs`, and any path segment
that starts with a dot. A repository named `lfs/blobs` would otherwise take the
LFS prefix for itself.

## The oid is a bucket key

`ValidateOID` accepts 64 lowercase hexadecimal characters and nothing else. That
admits no separator, no dot, no escape, and no control byte. It runs before any
other work.

Uppercase is rejected and never folded. git-lfs computes lowercase, so an
uppercase oid would store the same bytes under a second key.

## Upload integrity

The daemon never reads an upload body, so it cannot hash it. Two checks cover
that gap, and they run at different times.

**At upload time**, the presigned PUT carries `ChecksumSHA256`. Tigris rejects a
body that does not hash to the key. A client cannot skip this: the checksum is a
signed header, so removing it makes the signature invalid.

CAUTION: The stock AWS presigner moves any unrecognized `X-Amz-*` header into
the query string. `X-Amz-Checksum-Sha256` is one of those, and a hoisted
checksum is a checksum nobody sends and nothing enforces. `noHoistPresigner`
turns hoisting off for uploads. `TestPresignPutSignsTheChecksumHeader` pins it.

**At verify time**, `Verify` heads the *staged* object with the checksum mode
enabled and compares both the length and the digest. This is the only check that
runs on the daemon's own credentials. It is also the only check on size, because
`Content-Length` is not signed on a presigned PUT and cannot be enforced there.

A mismatch deletes the staged object, which belongs to the caller. Nothing here
can reach the shared blob, so a bad verify cannot destroy an object other
repositories depend on.

`Verify` writes the marker, and nothing else does. An upload the client never
verifies leaves no membership behind.

## Uploads and downloads sign differently

A download uses the stock presigner. Turning hoisting off also signs
`x-amz-checksum-mode` on a GetObject, and a URL signed that way fails unless the
client sends that header back. A download URL has to stand on its own, so only
`Host` is signed. `TestPresignGetNeedsNoRequestHeaders` pins it.

## Batch outcomes

| Operation | Marker | Response for that object |
| --- | --- | --- |
| `download` | present | `actions.download`, a presigned GET |
| `download` | absent | `error`, code 404 |
| `upload` | present, size matches | no `actions` key, so the client skips it |
| `upload` | present, size differs | `error`, code 422 |
| `upload` | absent | `actions.upload` and `actions.verify` |

Every object carries `authenticated: true`. Without it git-lfs runs credential
discovery against the storage host and can attach an `Authorization` header,
which a presigned request rejects.

Every action carries `expires_in`, so a client can tell a stale URL from a
denied one.

A per-object failure travels inside a 200. Only a whole-request failure gets a
non-200 status.

## Locks

One JSON document per repository, written under the same compare-and-swap the
ref cache uses: `If-None-Match: *` for the first write, then `If-Match` with the
ETag. A refused write re-reads and re-applies, so a concurrent lock is never
overwritten.

Lock IDs are generated by the server. The ID comes back as a path segment on
unlock, so it is never taken from the client.

CAUTION: The owner is the HTTP Basic username, or `anonymous`. With no user
store every anonymous caller is the same owner, so every lock lands in `ours`
and no push is ever blocked. Locking tells users apart only once callers send a
username. `-allow-lfs-locks=false` removes the four routes, and an unregistered
route is a 404, which the specification says does not halt a push.

## SSH

An `ssh://` remote still needs the HTTP listener. `git-lfs-authenticate` exists
only to name it, and it needs `-external-url`.

CAUTION: When LFS is off, the command must exit **127** and say `command not
found`. git-lfs falls back to guessing the HTTP endpoint on 127 alone. Any other
non-zero exit is reported to the user as a hard failure, so exiting 1 would
break every `git lfs` operation over SSH rather than leaving it alone.

## Credentials and URL lifetime

A presigned URL carries the session token of the credential that signed it. A
temporary credential invalidates every URL it signed when it expires. At
startup `lfsTTL` checks whether the credential can expire and clamps
`-lfs-url-ttl` to the remaining session time. A static Tigris keypair keeps the
configured value.

## Testing

CI has no bucket credentials, so unit tests run against a fake bucket. The
presigner is the exception: presigning makes no network call, so the tests
exercise the real signer against static credentials and assert on the URL. A
fake presigner would hide exactly the hoisting bug these tests exist to catch.

Three facts need a real bucket and live in `livebucket_test.go`, behind
`OBJGIT_TIGRIS_LIVE_BUCKET`: that Tigris enforces the checksum, that the header
cannot be dropped, and that a HEAD returns the stored digest. Run them before
trusting a change to `presign.go` or `store.go`.

## Known gaps

- Nothing deletes a blob. The marker gives a collector its reference count, but
  no collector exists.
- `-lfs-max-size` caps the size a client *declares*, not the size it uploads.
  Content-Length is not signed on a presigned PUT, so the real limit at upload
  time is what the bucket accepts. `Verify` catches the difference, but a client
  that never verifies leaves the staged object behind. Staged objects under
  `<orgID>/<repo>/lfs/staging/` are the one place an abandoned upload
  accumulates; a bucket TTL rule on that prefix is the simplest way to reap
  them until a collector exists.
- One presigned PUT caps at 5 GiB. Larger objects need multipart upload.
- The `git-lfs-authenticate` header map is empty. Real authentication needs a
  token issuer there.
- A presigned URL works for its full lifetime, whatever the Authorizer later
  decides.
