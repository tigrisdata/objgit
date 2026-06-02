# 1.0.0 (2026-06-02)

### Bug Fixes

- further optimize pack caching ([0535d44](https://github.com/tigrisdata/objgit/commit/0535d44d1805cd43d1ba8c97e871d49a31b19082))
- **protocol:** heal dangling HEAD on load and after push ([a488d3c](https://github.com/tigrisdata/objgit/commit/a488d3c752bcc2592f88c9d6aea8b21fdc11150d)), closes [#99](https://github.com/tigrisdata/objgit/issues/99)
- **s3fs:** drop failed pack entry before publishing the error ([ca4f6d6](https://github.com/tigrisdata/objgit/commit/ca4f6d6287d123034b951f0cf33d5576fbf5dca0))
- **s3fs:** harden S3 client transport to fail fast on stale connections ([0e99aa0](https://github.com/tigrisdata/objgit/commit/0e99aa0beb4879dff77c8d942ee0c41f934b1a8c))

### Features

- add git upload-archive support ([645661a](https://github.com/tigrisdata/objgit/commit/645661a6528873b063cd38470d243dfb07c7931c))
- add post-receive hooks sandboxed via kefka ([92bc004](https://github.com/tigrisdata/objgit/commit/92bc004cc64c219f8fce1f86c530ffbb715b37cc))
- **auth:** add transport-neutral authorization seam ([9c4bbba](https://github.com/tigrisdata/objgit/commit/9c4bbba40d9e91412808d5197635061ddffb7a73))
- **git-protocol:** route git:// through the auth.Authorizer ([8d677e1](https://github.com/tigrisdata/objgit/commit/8d677e1320370155f24916b507c63a622c34679b))
- **hooks:** stream push-hook output to client live over sideband ([53aed85](https://github.com/tigrisdata/objgit/commit/53aed85700c55dffac433a8845960f40ed8e034f))
- **http:** route smart-HTTP through the auth.Authorizer; drop allowPush field ([796639f](https://github.com/tigrisdata/objgit/commit/796639fefa99724d0bf74a020e6f6dddd80b5336))
- keep pushed packs whole ([ab60b2d](https://github.com/tigrisdata/objgit/commit/ab60b2dee61aea9eb67610883dabe2f86f7e6135))
- **metrics:** add Prometheus instrumentation and a /metrics endpoint ([a300ea4](https://github.com/tigrisdata/objgit/commit/a300ea49df20ad5dc82fc4dabb85fbd3261faed7))
- **protocol:** add git upload-pack support via git protocol ([ca89e09](https://github.com/tigrisdata/objgit/commit/ca89e0950bcf17e1c16fe307dbf17ef28430473e))
- **s3fs:** add optional Unix-metadata storage as S3 user metadata ([4ae20f9](https://github.com/tigrisdata/objgit/commit/4ae20f98964e43ac6e6be2ea226e1e7c67698891))
- **s3fs:** cache directory listings and object metadata via groupcache ([af8fca3](https://github.com/tigrisdata/objgit/commit/af8fca338ad03ea269bd5e1422d085db45e1cb38))
- **s3fs:** cache immutable pack files on local disk to fix clone hangs ([e114543](https://github.com/tigrisdata/objgit/commit/e11454327c3a1fb9bbcdad263633623d4f15d306))
- **s3fs:** implement lazy, range-based reads with read-ahead window ([76bb55d](https://github.com/tigrisdata/objgit/commit/76bb55d05916b569eb2baccb6f076bc9a0321747))
- **ssh:** add git-over-SSH server and command dispatch ([553532b](https://github.com/tigrisdata/objgit/commit/553532b40b562abcc051df6c547837f0c1b9614c))
- **ssh:** persist ed25519 host key in the bucket filesystem ([a626c13](https://github.com/tigrisdata/objgit/commit/a626c13c4d5ab748d140a4f4fa54ec357c3422ef))
- **ssh:** wire the SSH listener into the server lifecycle ([cbe152b](https://github.com/tigrisdata/objgit/commit/cbe152be4f29a20023bac1aec01b8a4ba8465cef))

### Performance Improvements

- **s3fs:** register chroot roots as recursive subtree prefixes ([e8e749f](https://github.com/tigrisdata/objgit/commit/e8e749fcdff1beb4f1ff294f4d20167db741f641))
