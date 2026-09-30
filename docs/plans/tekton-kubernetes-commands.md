# Plan: Kubernetes and Tekton commands in the hook shell

## Context

Add `kustomize`, `kube:apply`, and `tekton:pipelinerun` to the Kefka shell
that push hooks and the SSH `sh` command use. A new `-allow-kubernetes` flag,
off by default, gates the two commands that talk to the Kubernetes API.

The [Xe/x Tekton example](https://github.com/Xe/x/tree/master/.tekton) keeps
its Pipeline in a Kustomize bundle and its PipelineRun in a separate
`generateName` template. A hook applies the bundle, then creates a new run for
the pushed commit:

```sh
kustomize build /src/.tekton | kube:apply && tekton:pipelinerun /src/.tekton/testrun.yaml
```

The supplied `var/kustomize.wasm` is a WASI module of 24,380,813 bytes, SHA-256
`1724a4e906800e5af0c104e816d56213fdf1d2cb6e2cef417c4e25d4baac061b`. It reports
version `(devel)`, so its source revision is unknown. wazero compiles it in
about 4 s on an Apple M-series machine.

## Decisions

- **No client-go.** `internal/kube` is a small REST client. It covers in-cluster
  config, per-`apiVersion` discovery, Server-Side Apply, and create. client-go
  pulls in k8s.io/api and a large dependency tree. The three calls above are
  about 300 lines.
- **YAML.** `sigs.k8s.io/yaml` converts each document to JSON the way kubectl
  does. A line splitter on `---` separates documents, with the rules of
  apimachinery's `YAMLReader`.
- **`kustomize` is always registered.** It renders files and has no cluster
  access, so `-allow-kubernetes` does not gate it.
- **Disabled stubs.** Without `-allow-kubernetes`, `kube:apply` and
  `tekton:pipelinerun` are registered as stubs. A stub prints how to turn
  the command on and exits 1. "Command not found" would hide the reason.
- **`-allow-kubernetes` needs `-allow-hooks`.** The daemon exits at startup
  when the first is set and the second is not. It also exits when the
  in-cluster config cannot load.
- **Default namespace.** A document without `metadata.namespace` goes to the
  pod's ServiceAccount namespace, read from the mounted `namespace` file.
- **Apply validates first.** `kube:apply` parses and validates the whole stream
  before its first request. Then it applies in stream order, and stops at the
  first API error.
- **PipelineRun `commit` parameter is required.** A missing `commit` parameter
  is an error. Tekton can reject a parameter that the Pipeline does not
  declare, so the command does not add one.
- **Metadata keys.** Annotations `objgit.tigrisdata.com/repo`, `/ref`, and
  `/commit`. Label `objgit.tigrisdata.com/commit-prefix` holds the first 12
  hex characters of the commit.
- **Cancellation.** The kustomize runtime uses `WithCloseOnContextDone(true)`,
  so the hook timeout stops a long build. The generic Kefka adapter does not.

## Tasks

### Task 1: Embed kustomize.wasm and its Kefka adapter

Files: `internal/kustomize/kustomize.go`, `internal/kustomize/kustomize.wasm`
(Git LFS), `internal/kustomize/kustomize_test.go`, `.gitattributes`.

- Move `var/kustomize.wasm` to `internal/kustomize/kustomize.wasm`. Track it with
  `git lfs track`.
- `Command` implements `command.Execer`. It compiles once, and mounts
  `ec.FS` at `/`. It sets guest `PWD` to `ec.GuestPWD()`, and passes `HOME`
  and `TMPDIR` from the shell environment.
- `Exec` returns a clear error when the embedded bytes do not start with the
  WASM magic `\0asm`. That is the result of a build from an unhydrated LFS
  pointer.
- Tests (write first): the embedded bytes have the magic and the recorded
  SHA-256. `kustomize build` of a fixture works from a relative path.
  `-o /src/...` fails and `-o /tmp/...` succeeds. A cancelled context returns
  an error. An Xe-style Pipeline bundle builds and sets `namespace: ci`.

Test command: `go test ./internal/kustomize/`

### Task 2: Kubernetes REST client

Files: `internal/kube/client.go`, `internal/kube/yaml.go`,
`internal/kube/client_test.go`, `internal/kube/yaml_test.go`.

- `InCluster() (*Client, error)` reads `KUBERNETES_SERVICE_HOST` and
  `KUBERNETES_SERVICE_PORT`, plus `ca.crt` and `namespace`. It reads the token
  file again for each request, because bound tokens rotate.
- `New(Config) *Client` is for tests.
- `(*Client).Apply(ctx, obj)` sends a Server-Side Apply PATCH with
  `fieldManager=objgitd` and `force=false`. `(*Client).Create(ctx, obj)` sends a
  POST. Both resolve kind to resource through discovery
  (`/api/v1` or `/apis/<group>/<version>`).
- `*APIError` carries the `Status` code, reason, and message.
- `DecodeYAMLStream(io.Reader) ([]Object, error)` splits the stream and
  converts each document. It skips empty documents.
- Tests (write first): a fake API server covers discovery, create through
  apply, re-apply, a 409 conflict, a 403 denial, an unknown kind, a
  cluster-scoped kind, and the default namespace. The YAML tests cover
  several documents, comments, empty documents, and malformed input.

Test command: `go test ./internal/kube/`

### Task 3: kube:apply and tekton:pipelinerun commands

Files: `internal/kube/commands.go`, `internal/kube/tekton.go`,
`internal/kube/commands_test.go`.

- `Register(reg, client)` registers both commands, or disabled stubs when
  `client` is nil.
- `kube:apply` reads standard input and takes no arguments. An empty stream,
  a document without `apiVersion`, `kind`, or `metadata.name`, an API error,
  and a conflict all exit nonzero, with the API message on stderr. It
  prints `<kind>.<group>/<name> serverside-applied` for each object.
- `tekton:pipelinerun FILE` reads one document from the Kefka filesystem,
  relative to the shell directory. It needs group `tekton.dev`, kind
  `PipelineRun`, `metadata.generateName`, and no `metadata.name`. It sets the
  `commit` parameter (required) and the `branch` parameter (when present)
  from `OBJGIT_NEW_SHA` and `OBJGIT_BRANCH`. It adds the metadata, creates
  the run, and prints the generated name and the namespace.
- Tests (write first): cover validation, substitution, metadata, the
  namespace choice, the printed name, and two calls that make two creates.

Test command: `go test ./internal/kube/`

### Task 4: Wire into objgitd

Files: `cmd/objgitd/main.go`, `cmd/objgitd/git_protocol.go`,
`cmd/objgitd/hooks.go`, `cmd/objgitd/shell.go`, `cmd/objgitd/kube_test.go`.

- `-allow-kubernetes` flag. `daemon.kube *kube.Client`, nil when disabled.
- `newHookShell` takes the client and registers `kustomize` and the kube
  commands.
- Tests (write first): a push hook runs
  `kustomize build .tekton | kube:apply && tekton:pipelinerun .tekton/testrun.yaml`
  against the fake API server, and the push output shows the run name. With no
  client, the stub message reaches the pusher.

Test command: `go test ./cmd/objgitd/`

### Task 5: CI and Docker LFS hydration

Files: `.github/workflows/go.yml`, `.github/workflows/docker.yml`, `Dockerfile`.

- Check out with `lfs: true`. The Dockerfile fails the build when
  `internal/kustomize/kustomize.wasm` does not start with the WASM magic.

Test command: `docker build .` succeeds, and it fails against a pointer file.

### Task 6: Example RBAC and cluster checks

Files: `manifest/tekton-rbac/role.yaml`, `manifest/tekton-rbac/kustomization.yaml`.

- A Role in `ci` with `create, patch` on `pipelines` and `tasks`, and `create`
  on `pipelineruns`. A RoleBinding to ServiceAccount `objgitd` in `objgit`.
- Check in a `kind` cluster with stub Tekton CRDs: `kubectl auth can-i` for
  each allowed verb, and a denial for an unrelated resource. Run `kube:apply`
  and `tekton:pipelinerun` as the ServiceAccount against the real API server.

### Task 7: Documentation

Files: `docs/usage/kubernetes-hooks.md`, `docs/usage/hooks.md`,
`docs/architecture/hooks.md`, `AGENTS.md`, `README.md`.

- Explain the hook recipe, the flag, the RBAC, and the downsides below.

## Downsides

- **Cluster authority.** When hooks and Kubernetes commands are both on,
  anybody who can push a hook, or open the SSH shell, can use the pod
  ServiceAccount's permissions. The example deployment permits anonymous
  pushes when `ALLOW_PUSH=true`.
- **Push latency and duplicates.** Hooks run synchronously after the push is
  accepted. A retry, or a second push of the same commit, creates another
  PipelineRun.
- **Size.** The module adds about 23 MiB to the binary, and about 4 s of CPU on
  first use. LFS adds a hydration step to checkout and CI.
- **`tekton:logs` is deferred.** The commit label makes a later lookup
  possible.
