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
- Post-receive hooks are stored in the repository, meaning that hooks are checked out from the repository on push.
- Right now all s3fs reads are completely buffered in memory before billy can use them, theoretically this means that the size limit for files is 2Gi.
- This is like very new and not tested in the slightest.

## Future goals

- Integration with Tekton for CI pipelines.
- Some kind of web UI using [the Xe Design System](https://design.within.website).
- Any kind of blogpost.
- Deployment to Kubernetes.
- HTTPS support with TLS termination.
