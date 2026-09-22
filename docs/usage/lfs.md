# Git LFS

`objgitd` serves the Git LFS API. Large files stay out of the git repository:
git keeps a small pointer file, and the bytes go straight to the Tigris bucket.

The client transfers those bytes itself, over a presigned URL. They do not pass
through `objgitd`.

## Turn it on

LFS is off by default. Start the daemon with `-allow-lfs`:

```sh
objgitd -bucket $BUCKET -http-bind :8080 -allow-push -allow-lfs
```

To use LFS from an `ssh://` remote, add `-external-url`:

```sh
objgitd -bucket $BUCKET -http-bind :8080 -ssh-bind :2222 \
  -allow-push -allow-lfs -external-url https://git.example.com
```

The LFS API is HTTP, even for an `ssh://` remote. The SSH side only tells the
client where that API is, and it needs `-external-url` to do so. The daemon
refuses to start with `-allow-lfs` and an empty `-http-bind`.

## Flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `-allow-lfs` | `false` | Serve the Git LFS API. |
| `-allow-lfs-locks` | `true` | Serve the file locking API. |
| `-external-url` | `""` | Public base URL, such as `https://git.example.com`. |
| `-lfs-url-ttl` | `15m` | How long a presigned transfer URL stays valid. |
| `-lfs-max-size` | `5368709120` | Largest object accepted, in bytes. |
| `-lfs-max-batch` | `500` | Most objects in one batch request. |

Each flag also reads from an environment variable, in upper snake case.
`-allow-lfs` is `ALLOW_LFS`.

## Push a large file

```sh
git init myrepo && cd myrepo
git lfs install --local
git lfs track '*.bin'

head -c 50000000 /dev/urandom > big.bin
git add .gitattributes big.bin
git commit -m 'add big file'

git push http://localhost:8080/acme/myrepo.git main
```

The push reports the LFS upload separately from the git objects:

```text
Uploading LFS objects: 100% (1/1), 50 MB | 0 B/s, done.
To http://localhost:8080/acme/myrepo.git
 * [new branch]      main -> main
```

A clone retrieves the bytes the same way:

```sh
git clone http://localhost:8080/acme/myrepo.git fresh
```

## Where the bytes go

```text
lfs/blobs/<sha256>                    the bytes, once for the whole bucket
acme/myrepo/lfs/staging/<sha256>      an upload in flight, before it is verified
acme/myrepo/lfs/objects/<sha256>      a marker: this repository holds it
acme/myrepo/packs/<id>.bin            the git objects, which stay small
```

A client uploads into its own repository's staging area. The server checks those
bytes and then moves them to the shared key. A repository has to prove it holds
an object before the server hands out a URL for it, so knowing an object ID is
not enough to read someone else's data.

Object bytes are stored once per bucket. Two repositories that hold the same
file share one copy.

A repository still uploads a file the first time it pushes it, even when another
repository already put those bytes in the bucket. That upload is what proves the
repository holds the object, and it ends at the same shared key, so the bucket
keeps one copy.

## Watch the deduplication

`objgit_lfs_objects_total` counts objects by outcome. On an upload batch, a
`present` result means the repository already held the object and the client
transferred nothing:

```text
objgit_lfs_objects_total{operation="upload",result="new"}
objgit_lfs_objects_total{operation="upload",result="present"}
```

The ratio between those two is the deduplication rate.

`objgit_lfs_verify_total{status="mismatch"}` is the series to alert on. It
counts uploads whose bytes did not match the object ID.

## File locking

```sh
git lfs lock big.bin
git lfs locks
git lfs unlock big.bin
```

CAUTION: Locking needs authentication to be useful. `objgitd` records the HTTP
Basic username as the lock owner. Without a username every caller is the same
owner `anonymous`, so every lock looks like the caller's own and no push is ever
blocked. Set a username, or turn locking off with `-allow-lfs-locks=false`.

## Limits

- One object caps at 5 GiB, because one presigned upload caps there.
- Nothing deletes an LFS object. A file removed from a repository keeps its
  bytes in the bucket.
- An upload that is never verified stays under `<org>/<repo>/lfs/staging/`. Set
  a bucket TTL rule on that prefix to reap abandoned uploads.
- `-lfs-max-size` limits the size a client declares, not the bytes it can send
  to a presigned URL. The verify step catches an object that does not match what
  was declared.
- A presigned URL stays valid for its full lifetime. Removing a user's access
  does not cancel a URL they already hold.
- Temporary credentials shorten that lifetime. If the daemon runs with
  credentials that expire, it clamps `-lfs-url-ttl` to the remaining session
  time and logs the clamp.
