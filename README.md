# objgit

![GitHub Issues or Pull Requests by label](https://img.shields.io/github/issues/tigrisdata/objgit)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/tigrisdata/objgit)
![language count](https://img.shields.io/github/languages/count/tigrisdata/objgit)
![repo size](https://img.shields.io/github/repo-size/tigrisdata/objgit)

A git server backed by [Tigris](https://www.tigrisdata.com/). This handles Git repository and LFS storage. The vision is that objgit is the backbone of a git server that handles storage in the cloud so your repos don't have to live on your own disks.

Notable features:

- ssh, http, and git protocol support.
- [post-receive hooks](./docs/usage/hooks.md) powered by userspace sandboxed shells and [Kefka](https://xeiaso.net/blog/2026/dancing-mad-sandboxing/).
- SSH sandbox access to the environment hooks run in.
- [Git LFS](./docs/usage/lfs.md) with presigned transfers, so large files move straight between the client and Tigris instead of through the daemon. LFS object bytes are deduplicated across every repository in the bucket.
- EROFS snapshots per tree (including LFS pointer resolution).
- basic prometheus metrics.
- no authentication whatsoever, if this ends up being actually useful then authentication will be implemented.
- [webhooks](./docs/usage/webhooks.md) on a per-repository basis.

## Roadmap

- Integration with Tekton or other services for CI pipelines.
- Some kind of scraper-resistant web UI using [the Xe Design System](https://design.within.website).
- Deployment to Kubernetes (via Helm).
- HTTPS support with TLS termination.
- A robust authentication scheme for agents and users.
- MCP support.
- Integration with Jev / other decision engines in the hook environment.

## Deployment to production

There is a kustomize deployment in [`manifest/`](./manifest/). See
[docs/usage/kubernetes.md](./docs/usage/kubernetes.md) for configuration and
caveats.
