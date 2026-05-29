# objgit

An experiment in storing git repositories in object storage. You probably don't need this. This is a proof of concept and doesn't really have a lot of uses yet.

This is heavily vibe coded as an exploration of the design space. See [the claude plans folder](./docs/plans/) for an idea of the kinds of things that were being explored and why. If this experiment ends up being shippable, a better implementation will be made to replace this terrible code.

License: MIT

Price: Free as in mattress. If you use this, it's your problem.

## Notable features

- ssh, http, and git protocol support.
- [post-receive hooks](./docs/usage/hooks.md) powered by userspace sandboxed shells and [Kefka](https://xeiaso.net/blog/2026/dancing-mad-sandboxing/).
- basic prometheus metrics.
- no authentication whatsoever, if this ends up being actually useful then authentication will be implemented.

## Design tradeoffs

- All repos are stored in object storage, making the local filesystem irrelevant at the cost of making storage operations a bit more latent compared to the local filesystem.
- Repos are stored in git bare repo format. This theoretically could hamper performance, but also means that in a pinch you can just download the repo from the object storage bucket should you need to get a copy of it RIGHT NOW.
- Post-receive hooks are stored in the repository, meaning that hooks are checked out from the repository on push.
- Right now all s3fs reads are completely buffered in memory before billy can use them, theoretically this means that the size limit for files is 2Gi.
- This is like very new and not tested in the slightest.

## Future goals

- Integration with Tekton for CI pipelines.
- Some kind of web UI using [the Xe Design System](https://design.within.website).
- Any kind of blogpost.
- Deployment to Kubernetes.
- HTTPS support with TLS termination.

## Setup instructions

objgit depends on the following things:

- A system capable of running the Go runtime.
- TLS certificate authorities on the disk.
- A functional IPv4/IPv6 network stack.
- A Tigris account, bucket, and access keypair (part of the core flow of this tool relies on Tigris' [RenameObject](https://www.tigrisdata.com/docs/objects/object-rename/) call which is not part of the standard S3 API).

### Prepare your `.env` file

objgit takes its configuration via flags, environment variables, and `.env` files in this order of precedence:

- envvars
- `.env` file
- flags

Here is a sample `.env` file:

```sh
# Use the AWS credential chain to load Tigris credentials
AWS_PROFILE=tigris

# If true, allow the execution of post-receive hooks
ALLOW_HOOKS=true
# Hook timeout in go time.Duration format
HOOK_TIMEOUT=60s

# If true, allow anonymous pushes
ALLOW_PUSH=true

# The Tigris bucket that all repos are stored in,
# as well as the SSH host key
BUCKET=xe-git-repos

# Per-protocol bindhosts
GIT_BIND=:9418
HTTP_BIND=:8080
SSH_BIND=:2222

# Prometheus metrics
METRICS_BIND=:9090

# Logging detail level
SLOG_LEVEL=INFO
```

### Run the program

```text
go run ./cmd/objgitd
```

### Push a repo

```text
$ git push ssh://localhost:2222/xeiaso.net/objgit.git
Enumerating objects: 9, done.
Counting objects: 100% (9/9), done.
Delta compression using up to 32 threads
Compressing objects: 100% (9/9), done.
Writing objects: 100% (9/9), 2.80 KiB | 2.80 MiB/s, done.
Total 9 (delta 5), reused 0 (delta 0), pack-reused 0 (from 0)
remote: push to xeiaso.net/objgit.git refs/heads/main: a300ea49df20ad5dc82fc4dabb85fbd3261faed7 -> 3d889908eefd60d9a11605f077b7b4560aef8e74
remote: top-level contents:
remote: CLAUDE.md
remote: LICENSE
remote: README.md
remote: cmd
remote: docs
remote: go.mod
remote: go.sum
remote: internal
remote: go module: tangled.org/xeiaso.net/objgit
remote: manifest (/tmp/manifest.txt):
remote: ref refs/heads/main
remote: sha 3d889908eefd60d9a11605f077b7b4560aef8e74
remote: hook done
To ssh://localhost:2222/xeiaso.net/objgit.git
   a300ea4..3d88990  main -> main
```

### Deployment to production

TODO(Xe): re-evaluate performance-based life choices and write.
