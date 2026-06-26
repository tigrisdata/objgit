# Per-keypair Tigris resolver: a bucket per repo

## Context

The per-repo `repofs.Resolver` hook is already in place: every git request resolves
to a `billy.Filesystem` via `Resolve(ctx, ref, cred)`, and the HTTP Basic-auth
pair already arrives as `repofs.Credential{Username, Password}`. The default
`BucketResolver` chroots one shared bucket.

We now want a real backend: treat the Basic-auth credential as a **Tigris
keypair** (username = access key ID, password = secret access key), build a
`storage.Client` per keypair, and give **each repository its own Tigris bucket**,
created on first push. This replaces the single-shared-bucket model as the
production default.

Decisions (confirmed):

- **Credential = keypair**: `username` → access key ID, `password` → secret.
- **Bucket name**: `objgit-{base36(sha256(orgID/repoName))[:N]}` — deterministic,
  DNS-valid (lowercase alnum), collision-free.
- **Create on push only**: the resolver creates the bucket only on the write
  path; reads of a missing bucket are a 404.
- **Replace default**: `main.go` wires the Tigris resolver. `BucketResolver`
  stays in `repofs` for tests (memfs), just not wired in production.
- **All S3 ops go through `github.com/tigrisdata/storage-go`** (`*storage.Client`,
  which embeds `*s3.Client`); never construct a bare AWS `s3.Client`.

## 1. Gate creation on the Resolver interface (`internal/repofs`)

`Resolve` needs to know read vs. write so it only creates buckets on push. Add a
`create bool`:

```go
type Resolver interface {
	Resolve(ctx context.Context, ref RepoRef, cred Credential, create bool) (billy.Filesystem, error)
}
```

- `BucketResolver.Resolve` ignores `create` (chroot is creation-free).
- `daemon.load` (read) passes `create=false`; `daemon.loadOrInit` (push) passes
  `create=true` (`cmd/objgitd/git_protocol.go`).
- Update the `recordingResolver` test stub in `cmd/objgitd/http_test.go`.

Add a `BucketName()` helper — but put it in the Tigris package (below), since the
`objgit-` prefix and hashing are storage policy, not neutral identity.

## 2. New package: `internal/tigrisfs`

The concrete, Tigris-backed `repofs.Resolver`. Depends on `storage-go`, `s3fs`,
and (for the not-found sentinel) go-git `transport`. Keeping it out of `repofs`
preserves `repofs`'s transport/storage neutrality.

```go
package tigrisfs

// Resolver implements repofs.Resolver against Tigris, one bucket per repo.
type Resolver struct {
	// newClient builds a storage.Client from a keypair. Defaults to
	// storage.New(ctx, storage.WithAccessKeypair(id, secret)); overridable for tests.
	newClient func(ctx context.Context, cred repofs.Credential) (*storage.Client, error)
	fsOpts    []s3fs.Option // listing/pack cache opts applied per-bucket S3FS

	mu      sync.Mutex
	clients map[string]*cachedClient // keyed by access key ID (cred.Username)
}

type cachedClient struct {
	raw      *storage.Client // bucket ops (CreateBucket/HeadBucket) go here
	hardened s3fs.S3Client   // object I/O handed to s3fs (see note on Harden)
}
```

`Resolve`:

1. Reject empty `cred.Username`/`cred.Password` (auth required → surfaces as 401
   at the HTTP layer; see error mapping).
2. Look up / build the cached client for `cred.Username` (build via `newClient`,
   then `hardened = s3fs.Harden(raw)`). Cache under a mutex.
3. `bucket := bucketName(ref)`.
4. If `create`: `ensureBucket(ctx, raw, bucket)` — `CreateBucket`; treat
   `*types.BucketAlreadyOwnedByYou` / `*types.BucketAlreadyExists` (via
   `errors.As`) as success.
   Else: `HeadBucket`; on `*types.NotFound`/`*types.NoSuchBucket` return
   `transport.ErrRepositoryNotFound`.
5. Return `s3fs.NewS3FS(hardened, bucket, r.fsOpts...)` (root `""` — the bucket
   _is_ the repo).

```go
func bucketName(ref repofs.RepoRef) string {
	sum := sha256.Sum256([]byte(ref.Path()))
	b36 := new(big.Int).SetBytes(sum[:]).Text(36) // 0-9a-z
	// left-pad to a fixed width so truncation is deterministic, then take N.
	return "objgit-" + leftPad(b36, 50, '0')[:32] // "objgit-" + 32 = 39 chars, < 63
}
```

**Harden note:** `s3fs.Harden` returns the 9-method object-only `s3Client`
wrapper — it does **not** expose `CreateBucket`/`HeadBucket`. So the resolver
keeps the raw `*storage.Client` for the (rare) bucket calls and hands the
hardened wrapper to `s3fs` for the hot object path. Both are storage-go clients;
no bare AWS client is created.

**s3fs export:** `s3fs.NewS3FS` currently takes an unexported `s3Client`
interface. An external package can still satisfy it (method set is exported), but
to name the field type in `cachedClient` cleanly, export the interface as
`s3fs.S3Client` (alias/rename of the existing `s3Client`). Small, mechanical
change in `internal/s3fs/filesystem.go`.

## 3. `main.go` wiring (replace default)

- Read the keypair-mode resolver instead of `BucketResolver`:
  `resolver: tigrisfs.New(tigrisfs.WithFSOptions(fsOpts...))`.
- The default `newClient` does `storage.New(ctx, storage.WithAccessKeypair(...))`
  (endpoint defaults to the global Tigris endpoint; add a `-tigris-endpoint`
  flag later if needed).
- `sysFS` (SSH host key) still uses the ambient `-bucket` `fsys` built today;
  repos no longer use it. The existing `-bucket` flag becomes "system bucket"
  (host key only). Note this in flag help.
- Per-keypair `ListingCache`/`PackCache`: the caches `main` builds today are
  bound to one bucket and no longer fit a bucket-per-repo world. For this pass,
  pass no per-bucket caches (or a bounded per-(keyID,bucket) cache later);
  **log that repo-side caching is disabled** so it isn't mistaken for working.

## 4. Error mapping

- Empty/invalid credential → resolver returns a sentinel (`tigrisfs.ErrNoCredential`);
  HTTP `resolve` maps it to `401 WWW-Authenticate: Basic` (reuse the existing
  `auth.Unauthenticated` rendering path, or special-case the error).
- Missing bucket on read → `transport.ErrRepositoryNotFound` → existing 404 path.
- A bad keypair surfaces as an S3 `AccessDenied` mid-call; log and return 500
  (acceptable for now — real authz is a later seam).

## 5. Caching & concurrency

- One `cachedClient` per access key ID, guarded by a mutex (or `sync.Map`).
  Building a `storage.Client` loads AWS config (network-free) and is the main
  cost we're avoiding per request.
- Bound the map (simple max or LRU, e.g. 1024 keypairs) as a follow-up; note the
  unbounded-growth risk in a comment for now.

## 6. Tests

- `internal/tigrisfs/tigrisfs_test.go` (unit, no network):
  - `bucketName` is deterministic, `objgit-`-prefixed, ≤ 63 chars, lowercase
    alnum, and differs for different `orgID/repoName`.
  - Empty credential → `ErrNoCredential` (checked before `newClient`).
  - `create` gating: with a fake `newClient` returning a client whose bucket ops
    are observable, assert `CreateBucket` is called iff `create==true` and
    `HeadBucket` otherwise. (Use a small fake satisfying the bucket-op + object
    method set; inject via `newClient`.)
- Integration test gated by real creds (skip when
  `TIGRIS_STORAGE_ACCESS_KEY_ID`/`_SECRET_ACCESS_KEY` unset — see the
  `tigris-storage` skill's `skipIfNoCreds` pattern): push to
  `acme/itest-<unique>.git`, assert the bucket is created and a clone round-trips;
  clean up the bucket after.
- `internal/repofs` and `cmd/objgitd` existing tests keep using `BucketResolver`
  (memfs); update them only for the new `create` parameter.

## Verification

```text
go build ./...
go test ./internal/repofs/... ./internal/tigrisfs/... ./cmd/objgitd/...
```

End-to-end against real Tigris (credentials in the AWS/Tigris env):

```text
./objgitd -bucket $SYS_BUCKET -http-bind :8080 -allow-push
# username = Tigris access key ID, password = secret access key
git clone http://$KEYID:$SECRET@localhost:8080/acme/demo.git   # first push creates bucket objgit-<hash>
# verify the bucket exists:
tigris bucket list | grep objgit-
git clone http://$KEYID:$SECRET@localhost:8080/acme/demo.git   # second clone reuses cached client + bucket
git clone http://localhost:8080/acme/demo.git                   # no creds -> 401
```
