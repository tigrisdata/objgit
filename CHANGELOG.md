# [1.4.0](https://github.com/tigrisdata/objgit/compare/v1.3.0...v1.4.0) (2026-08-31)

### Features

- **objgitd:** bound the number of concurrent pushes ([#7](https://github.com/tigrisdata/objgit/issues/7)) ([877cd16](https://github.com/tigrisdata/objgit/commit/877cd16b39223de2d30f78156e34f0276204839a))

# [1.3.0](https://github.com/tigrisdata/objgit/compare/v1.2.0...v1.3.0) (2026-08-31)

### Bug Fixes

- **storage/tigris:** bound PackfileWriter's push-sized memory ([16cba4b](https://github.com/tigrisdata/objgit/commit/16cba4bb7945865607dc25b03c40eac87ce59ff5))

### Features

- **objgitd:** hand a whole push's ref updates over in one call ([8a3558a](https://github.com/tigrisdata/objgit/commit/8a3558a717085912347cc3986c581caadbe4474a))
- pack refs by default, behind -packed-refs ([e366ed1](https://github.com/tigrisdata/objgit/commit/e366ed1c8d2cfb345bbc79eaf1e92de7a99f5b9e))
- **storage/tigris:** add the packed-refs object format ([5de5668](https://github.com/tigrisdata/objgit/commit/5de56686c33d9a6a4da2563d88a0ad8d7bca847c))
- **storage/tigris:** compress pack payloads and cue indexes with zstd ([7bea058](https://github.com/tigrisdata/objgit/commit/7bea0581ba2358ac49c09a52a72c7b649ad63d25))
- **storage/tigris:** fold legacy loose refs on the first write ([b20935b](https://github.com/tigrisdata/objgit/commit/b20935be4189e41b2dc6dcb550e733946eda965d))
- **storage/tigris:** keep pushed deltas, drop the pack object cap ([ea4d362](https://github.com/tigrisdata/objgit/commit/ea4d362c94a686f7210c8508594c2bf9312fd325))
- **storage/tigris:** read refs through a memoized packed view ([6da6390](https://github.com/tigrisdata/objgit/commit/6da639084b00e4c8809e3bd499b5fa7fc6ec30a0))
- **storage/tigris:** trace pack fetches and cache evictions ([2293f76](https://github.com/tigrisdata/objgit/commit/2293f76cf0e738140bb4e59682f12e16695876d9))
- **storage/tigris:** write refs under a compare-and-swap ([4ca06c4](https://github.com/tigrisdata/objgit/commit/4ca06c4d56fd217ec51131ee94766da91fdfb944))

### Performance Improvements

- **storage/tigris:** prefetch packs in the background ([eea720d](https://github.com/tigrisdata/objgit/commit/eea720dc64d82048b31754cf0f5f9a7f7954dcd3))
- **storage/tigris:** resolve delta bases across scratch storages ([0e86b3f](https://github.com/tigrisdata/objgit/commit/0e86b3fc47948f6fc095f6f808b739e788180c79))

# [1.2.0](https://github.com/tigrisdata/objgit/compare/v1.1.0...v1.2.0) (2026-08-27)

- refactor(objgitd)!: serve every repository from one bucket via the storer ([ebfe4e3](https://github.com/tigrisdata/objgit/commit/ebfe4e315aa387386c98ccef269d44cd98077470))

### Features

- **bundler:** vendor Google's item bundler, generic over item type ([d89b002](https://github.com/tigrisdata/objgit/commit/d89b002c85304c21fa84dcb9c9a9d7a9e58791cf))
- **storage/tigris:** bin/cue pack containers, async uploads, pack cache ([f4a1419](https://github.com/tigrisdata/objgit/commit/f4a1419657cf94680e157e7c181fa856259b0ad5))
- **storage/tigris:** body fetch and type-filtered EncodedObject ([1ccf723](https://github.com/tigrisdata/objgit/commit/1ccf7239fd6ee04812c98f2772d67cb034734758))
- **storage/tigris:** cap pack containers at 128 MiB, overlap uploads ([b4c843d](https://github.com/tigrisdata/objgit/commit/b4c843d0bc691908aa1f9a82348a1b2aceaa9561))
- **storage/tigris:** complete storage.Storer with index, config, module ([4e371c4](https://github.com/tigrisdata/objgit/commit/4e371c472525cab8547f2f554be7873f1bd10a88))
- **storage/tigris:** hash-verifying staging writer for RawObjectWriter ([d3b0fd6](https://github.com/tigrisdata/objgit/commit/d3b0fd60b55edcd3632f7c1c9a71f5f3752b39da))
- **storage/tigris:** HEAD-backed Has and Size lookups ([aac779b](https://github.com/tigrisdata/objgit/commit/aac779b625ed81c6626b36e99dd7f349cbcba00d))
- **storage/tigris:** lazy paginated EncodedObject iteration ([cb28403](https://github.com/tigrisdata/objgit/commit/cb2840320c7db41bdd5f1d3017bcdc6a9bf429fc))
- **storage/tigris:** loose references, CAS updates, shallow marks ([dfd5ae0](https://github.com/tigrisdata/objgit/commit/dfd5ae0c3cd988bdf4ce02a028325556767acb39))
- **storage/tigris:** scaffold Tigris-backed go-git storer seam ([93f1832](https://github.com/tigrisdata/objgit/commit/93f1832988f43711364bb5e8e1750cff332d7e04))
- **storage/tigris:** SetEncodedObject with forged-hash refusal ([c808cda](https://github.com/tigrisdata/objgit/commit/c808cda62cb42b9b47cf3eecaa40a1ba2231d172))

### BREAKING CHANGES

- -bucket now holds every repository, not just daemon
  system state, and repositories are addressed by key prefix rather than
  by per-keypair bucket. Existing per-keypair buckets are not read. The
  flags -s3-cache-ttl, -s3-cache-refresh, -s3-cache-idle,
  -s3-cache-recursive-prefixes, and -s3-cache-max-subtree-keys are
  removed. -pack-cache-dir and -pack-cache-bytes now configure the
  storer's container cache and no longer cache git .pack/.idx files.

Assisted-by: Claude Opus 5 via Claude Code
Signed-off-by: Xe Iaso <xe@tigrisdata.com>

# [1.1.0](https://github.com/tigrisdata/objgit/compare/v1.0.2...v1.1.0) (2026-06-29)

- feat(storage)!: resolve repositories to per-keypair Tigris buckets ([0ee1387](https://github.com/tigrisdata/objgit/commit/0ee13872b4a4ed4aa4f88c5bf0636157b1e578d1))

### Bug Fixes

- **s3fs:** create directory markers as directories, not empty files ([5bb834b](https://github.com/tigrisdata/objgit/commit/5bb834b6b08b037676d07925bb33679594e91c84))
- **s3fs:** opt out of aws-sdk-go-v2 default request checksums ([bf81a1d](https://github.com/tigrisdata/objgit/commit/bf81a1dc0d5c6ae958bdb79e4103ca334a66cdc5))

### BREAKING CHANGES

- repository URLs must be {orgID}/{repoName}; repositories moved from one shared bucket to a bucket per repo, so repositories created under the old layout no longer resolve.

Assisted-by: Claude Opus 4.8 via Claude Code
Signed-off-by: Xe Iaso <xe@tigrisdata.com>

## [1.0.2](https://github.com/tigrisdata/objgit/compare/v1.0.1...v1.0.2) (2026-06-25)

## [1.0.1](https://github.com/tigrisdata/objgit/compare/v1.0.0...v1.0.1) (2026-06-04)

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
