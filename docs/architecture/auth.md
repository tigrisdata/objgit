# The auth seam (`internal/auth`)

Every transport authorizes through one interface. A real authentication and
authorization layer can therefore drop in later, with no change to transport
code.

`internal/auth` is transport-neutral on purpose. It imports `context` and
`golang.org/x/crypto/ssh`, which supplies the public-key wire type. It does
not import gliderlabs/ssh or go-git.

## The interface

The whole seam is one method:

```go
Authorizer.Authorize(ctx, auth.Request) auth.Decision
```

`auth.Request` carries four fields:

| Field        | Meaning                          |
| ------------ | -------------------------------- |
| `Repo`       | The repository path.             |
| `Operation`  | `Read` or `Write`.               |
| `Credential` | What the client presented.       |
| `Transport`  | Which transport asked.           |

## Credential

`Credential` is a sum type. An unexported method seals it, so no other package
can add a case.

| Case                             | Source                                          |
| -------------------------------- | ----------------------------------------------- |
| `Anonymous{}`                    | git://, or HTTP and SSH with nothing presented. |
| `PublicKey{Key}`                 | SSH.                                            |
| `BasicAuth{Username, Password}`  | HTTP.                                           |

CAUTION: `BasicAuth` is unvalidated. The `Authorizer` owns the user store, so
the `Authorizer` must check the password itself.

## Decision

`Decision` is `Allow`, `Deny`, or `Unauthenticated`.

`Unauthenticated` is the seam that lets HTTP send a `401 WWW-Authenticate`
challenge. SSH and git:// treat every value other than `Allow` as a denial.

## What a transport does

Each transport has three jobs and no more:

1. It collects the credential.
2. It maps the git service to an `Operation`, through the shared
   `operationFor` helper.
3. It renders the `Decision` in its own dialect.

The dialects differ: a pktline error for git://, a `401` or a `403` for HTTP,
and stderr plus a non-zero exit status for SSH.

## The one implementation today

`auth.AllowAnonymous{AllowWrite}` allows read for everyone. It allows write
only when `AllowWrite` is set.

`main.go` wires it as `AllowAnonymous{AllowWrite: *allowPush}`. The
`-allow-push` flag is therefore configuration for this default, and not a
field on `daemon`.
