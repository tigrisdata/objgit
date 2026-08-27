# Architecture

`objgitd` is one binary. It stores git repositories as objects in a Tigris
bucket. It does not keep repository data on a local filesystem.

## The daemon is the shared backend

`cmd/objgitd/main.go` builds one `*daemon`. One `errgroup` then serves that
daemon through all three transports at the same time.

The struct holds three fields, plus the hook configuration:

| Field      | Type                 | Purpose                                     |
| ---------- | -------------------- | ------------------------------------------- |
| `sysFS`    | `billy.Filesystem`   | Daemon-level state only, which is the SSH host key. Still `internal/s3fs`. |
| `resolver` | `repofs.Resolver`    | Maps a repository path to its storage.      |
| `authz`    | `auth.Authorizer`    | One decision point for every transport.     |

`resolver.Resolve` returns a go-git `storage.Storer` directly. No
`billy.Filesystem` is involved in repository data. The production resolver is
`repofs.BucketResolver`, and its `Base` field is an
`internal/storage/tigris.Storer` over the single `-bucket`. Each repository
gets its own key prefix of the form `orgID/name` through `Storer.Scoped`.

Three behaviors live on `*daemon`, so every transport acts the same way:

- repository resolution
- authorization
- create-on-first-push, which is `loadOrInit` in `git_protocol.go`

## Pages

| Page                                 | Contents                                                         |
| ------------------------------------ | ---------------------------------------------------------------- |
| [transports.md](transports.md)       | Smart HTTP, git://, and SSH. Two protocol points that are easy to get wrong. |
| [auth.md](auth.md)                   | The one authorization interface that every transport calls.       |
| [hooks.md](hooks.md)                 | Push hooks, live output streaming, and the kefka sandbox.         |
| [metrics.md](metrics.md)             | Prometheus vectors and the three instrumentation seams.           |
| [tigris-storer.md](tigris-storer.md) | Repository storage: a `storage.Storer` on one Tigris bucket.      |
| [s3fs.md](s3fs.md)                   | Daemon-level state: a `billy.Filesystem` on Tigris.               |

Related documents outside this directory:

- [../reference/tigris-backend.md](../reference/tigris-backend.md) — the `.cue`
  binary layout, the failure modes, and the build order.
- [../usage/hooks.md](../usage/hooks.md) — how to write a hook script.
- `../plans/` — plans for work that is not yet complete.
