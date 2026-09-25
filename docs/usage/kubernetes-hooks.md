# Kubernetes and Tekton hook commands

A push hook can render a Kustomize bundle and send it to the Kubernetes
cluster that `objgitd` runs in. Then the hook can start a Tekton
PipelineRun for the pushed commit. Three commands in the hook shell do this
work:

| Command                   | What it does                                                              | Needs                                  |
| ------------------------- | ------------------------------------------------------------------------- | -------------------------------------- |
| `kustomize`               | Runs an embedded kustomize. It reads `/src` and can write only to `/tmp`. | `-allow-hooks`                         |
| `kube:apply`              | Reads YAML on stdin and applies each object with Server-Side Apply.       | `-allow-hooks` and `-allow-kubernetes` |
| `tekton:pipelinerun FILE` | Creates a new PipelineRun from a template, set to the pushed commit.      | `-allow-hooks` and `-allow-kubernetes` |

The commands are also in the SSH `sh` shell, because that shell is the hook
sandbox. Read [hooks.md](hooks.md) first. It explains the sandbox, the
environment variables, and when a hook runs.

## Security

WARNING: Do not use `-allow-kubernetes` on a server that accepts pushes from
people you do not trust.

A hook is a script from the pushed commit. When `-allow-kubernetes` is on,
anybody who can push a hook, or open the SSH shell, can use every permission
of the `objgitd` ServiceAccount. The example deployment in `manifest/`
accepts anonymous pushes when `ALLOW_PUSH=true`. Give the ServiceAccount
only the namespace and the verbs that your pipeline needs.

## Turn the commands on

1. Apply the example RBAC. It is separate from `manifest/`, because the
   `namespace: objgit` field there moves the RoleBinding to `objgit`:

   ```text
   kubectl apply -k manifest/tekton-rbac/
   ```

2. Set these values in the `objgitd-config` ConfigMap in
   `manifest/kustomization.yaml`:

   ```text
   ALLOW_HOOKS=true
   ALLOW_KUBERNETES=true
   ```

3. Apply the deployment again with `kubectl apply -k manifest/`.

| Flag                | Env                | Default | Meaning                                                        |
| ------------------- | ------------------ | ------- | -------------------------------------------------------------- |
| `-allow-kubernetes` | `ALLOW_KUBERNETES` | `false` | Give `kube:apply` and `tekton:pipelinerun` the pod credentials |

`objgitd` stops at startup in two conditions. The first is
`-allow-kubernetes` without `-allow-hooks`. The second is a pod without an
in-cluster ServiceAccount. The log line names the cause.

Without `-allow-kubernetes`, the two Kubernetes commands exist, but they
stop with exit status 1 and this message:

```text
kube:apply: Kubernetes commands are disabled; start objgitd with -allow-kubernetes to enable them
```

## Write the hook

This example uses the layout of the
[Xe/x Tekton example](https://github.com/Xe/x/tree/master/.tekton):

| File                         | Contents                                                       |
| ---------------------------- | -------------------------------------------------------------- |
| `.tekton/kustomization.yaml` | A Kustomize bundle with `namespace: ci` and the Pipeline file. |
| `.tekton/x.yaml`             | The Pipeline. It declares `commit` and `branch` parameters.    |
| `.tekton/testrun.yaml`       | A PipelineRun template with `metadata.generateName`.           |

Put this script at `.objgit/hooks/receive-pack`:

```bash
#!/usr/bin/env bash
# .objgit/hooks/receive-pack

# Start a build only for pushes to main.
if [ "${OBJGIT_BRANCH}" != "main" ]; then
  exit 0
fi

kustomize build /src/.tekton | kube:apply && tekton:pipelinerun /src/.tekton/testrun.yaml
```

The `&&` is necessary. If `kube:apply` fails, the hook does not create a run
against an old Pipeline.

The push output then shows the result:

```text
remote: pipeline.tekton.dev/xe-x-build-test serverside-applied
remote: pipelinerun.tekton.dev/x-m-plms2 created in namespace ci
```

The hook runs after Git updates the ref. A failed command shows its error to
the pusher and in the server log, but the push stays accepted.

## Command reference

### kustomize

`kustomize` is kustomize compiled to WebAssembly. It takes the usual
arguments, for example `kustomize build .tekton`. Relative paths start at the
shell directory.

- `/src` is read-only. Write output with `-o` only to a path in `/tmp`.
- The sandbox has no network. Remote bases, such as a Git URL in
  `resources`, do not load.
- The first run in each `objgitd` process compiles the module. This takes
  about 4 seconds of CPU, and it counts against `-hook-timeout`.
- The hook timeout stops a long build.

The embedded module reports its version as `(devel)`. Its SHA-256 is in
`internal/kustomize/kustomize.go`.

### kube:apply

`kube:apply` reads a stream of YAML documents on stdin and takes no
arguments. It applies the objects in stream order:

1. It reads the full stream. Empty documents and comment-only documents are
   skipped.
2. It makes sure that each object has `apiVersion`, `kind`, and
   `metadata.name`. If one object does not, it stops before it sends a
   request.
3. It sends each object as a Server-Side Apply request, with field manager
   `objgitd`. It does not force conflicts.
4. It stops at the first failed request.

An object without `metadata.namespace` goes to the namespace of the
`objgitd` ServiceAccount. `kube:apply` never deletes objects. An object that
you remove from the bundle stays in the cluster.

`kube:apply` stops with exit status 1 for these conditions:

| Condition                                       | Message contains                               |
| ----------------------------------------------- | ---------------------------------------------- |
| stdin has no objects                            | `no objects on standard input`                 |
| A document is not valid YAML                    | `document N`                                   |
| An object has no name, kind, or apiVersion      | `object N`                                     |
| The cluster does not serve the kind             | `no resource of kind`                          |
| Another field manager owns a field              | `Apply failed with`                            |
| The ServiceAccount does not have the permission | `is forbidden: User "system:serviceaccount:…"` |

A conflict means that another tool, such as `kubectl apply`, owns the field.
To correct it, remove the field from the bundle, or from the other tool.

### tekton:pipelinerun

`tekton:pipelinerun FILE` reads one PipelineRun from `FILE` in the hook
filesystem. A relative path starts at the shell directory. The template must
obey these rules:

- `apiVersion` is in the `tekton.dev` group, and `kind` is `PipelineRun`.
- `metadata.generateName` is set, and `metadata.name` is not set. The API
  server adds a random suffix to the name.
- `spec.params` has a parameter named `commit`.

The command changes the template before it sends it. The values come from
the update that `objgitd` runs the hook for. They are the same values as the
`OBJGIT_*` variables, but a script cannot change them:

| Field                                          | New value                             |
| ---------------------------------------------- | ------------------------------------- |
| The `commit` parameter                         | The commit, as in `OBJGIT_NEW_SHA`    |
| The `branch` parameter, if the template has it | The branch, as in `OBJGIT_BRANCH`     |
| Annotation `objgit.tigrisdata.com/repo`        | The repository, as in `OBJGIT_REPO`   |
| Annotation `objgit.tigrisdata.com/ref`         | The ref, as in `OBJGIT_REF`           |
| Annotation `objgit.tigrisdata.com/commit`      | The commit                            |
| Label `objgit.tigrisdata.com/commit-prefix`    | The first 12 characters of the commit |

In the SSH `sh` shell on a tag or a commit, there is no branch. The `branch`
parameter then keeps its template value.

Other parameters and fields do not change. Then the command sends a create
request and prints the generated name and the namespace. A template without
`metadata.namespace` goes to the namespace of the ServiceAccount.

Each call creates a new run. A second push of the same commit, or a retried
push, therefore creates a second run. To find the runs of one commit, use the
label:

```text
kubectl -n ci get pipelineruns -l objgit.tigrisdata.com/commit-prefix=658195e83ee5
```

## Permissions

The example Role in `manifest/tekton-rbac/role.yaml` gives the ServiceAccount
`objgit/objgitd` these permissions in namespace `ci`:

| Resource                                   | Verbs             | Used by              |
| ------------------------------------------ | ----------------- | -------------------- |
| `pipelines.tekton.dev`, `tasks.tekton.dev` | `create`, `patch` | `kube:apply`         |
| `pipelineruns.tekton.dev`                  | `create`          | `tekton:pipelinerun` |

Server-Side Apply needs `patch` for each object. For an object that does not
exist yet, it also needs `create`. The Role therefore does not let
`kube:apply` send PipelineRuns, because it has no `patch` on them.

Other kinds, or other namespaces, need more RBAC. Without it, the API server
refuses the request with 403 Forbidden, and the hook shows that message.

The Role does not cover these items:

- The ServiceAccount in `spec.taskRunTemplate.serviceAccountName` of a run.
  Tekton runs the tasks as that account, which is a different identity.
- The Tasks, workspace storage, and registry Secrets that the pipeline
  uses. The Xe/x pipeline, for example, needs the `git-clone-naive`, `ko`,
  and `kaniko` Tasks, the `go-mod-cache` claim, and the `ghcr` Secret. Create
  them before the first run.

## Limits

- Hooks run synchronously. A slow API server holds the push connection open
  until `-hook-timeout`.
- There is no `tekton:logs` command yet. Use the commit label to find a run,
  then use `tkn` or `kubectl`.
- The commands use only the in-cluster ServiceAccount. They do not read a
  kubeconfig.

## Test against a real cluster

`TestCluster` in `internal/kube` sends real requests. It runs only when three
variables are set. For a local [kind](https://kind.sigs.k8s.io/) cluster:

1. Create the cluster, the `ci` and `objgit` namespaces, and the `objgitd`
   ServiceAccount.
2. Install CRDs for `pipelines`, `tasks`, and `pipelineruns` in the
   `tekton.dev` group. They must serve `v1`.
3. Apply `manifest/tekton-rbac/`.
4. Set the variables, then run the test:

   ```text
   export OBJGIT_TEST_KUBE_HOST=https://127.0.0.1:6443
   export OBJGIT_TEST_KUBE_CA=/path/to/ca.crt
   export OBJGIT_TEST_KUBE_TOKEN="$(kubectl -n objgit create token objgitd)"
   go test -run TestCluster ./internal/kube/
   ```

To make sure that the Role gives only what it must, use `kubectl auth can-i`:

```text
kubectl auth can-i patch pipelines.tekton.dev -n ci --as=system:serviceaccount:objgit:objgitd
kubectl auth can-i create configmaps -n ci --as=system:serviceaccount:objgit:objgitd
```

The first command prints `yes`, and the second prints `no`.
