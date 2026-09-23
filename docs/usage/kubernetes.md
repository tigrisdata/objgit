# Kubernetes deployment

`manifest/` holds a kustomize deployment for `objgitd`. The pod stores all
repository data in a Tigris bucket. A persistent volume holds the pack cache.
An ephemeral volume holds upload scratch data.

| Object                                     | Purpose                                            |
| ------------------------------------------ | -------------------------------------------------- |
| `Namespace/objgit`                         | Holds every object below.                          |
| `ServiceAccount/objgitd`                   | The pod identity. Users can annotate it for IRSA.  |
| `Deployment/objgitd`                       | The daemon, with all three transports and metrics. |
| `PersistentVolumeClaim/objgitd-pack-cache` | 16Gi from the default StorageClass.                |
| `Service/objgit`                           | HTTP (80), git:// (9418), SSH (22).                |
| `Service/objgit-metrics`                   | Prometheus /metrics (9090), on its own Service.    |

## Configure

The daemon reads all configuration from environment variables. `flagenv` maps
each flag to one variable (see the README section "Setup instructions"). The
literals live in `manifest/kustomization.yaml`:

- `BUCKET` in the `objgitd-config` configMapGenerator is **required**. The
  daemon does not start when this variable does not name a Tigris bucket.
- The `objgitd-tigris` secretGenerator holds a static Tigris keypair
  (`TIGRIS_STORAGE_ACCESS_KEY_ID`, `TIGRIS_STORAGE_SECRET_ACCESS_KEY`).
  Before you apply the configuration, replace both placeholder values.

To avoid a long-lived keypair, use IRSA or workload identity:

1. Delete the `objgitd-tigris` secretGenerator from
   `manifest/kustomization.yaml`.
2. Delete the `secretRef` block from `manifest/deployment.yaml`. The block is
   optional.
3. Annotate the `objgitd` ServiceAccount for EKS IRSA or GKE workload
   identity.

The AWS SDK default credential chain finds the role on its own.

CI builds the image on every commit. The workflow lives in
`.github/workflows/docker.yml`. Every push event publishes these tags to
`ghcr.io/tigrisdata/objgit`:

- `:latest` and `:main` from the main branch.
- `:sha-<commit>` from every branch and every release tag.
- One tag per release, such as `:1.5.1`.

Every tag carries both `linux/amd64` and `linux/arm64` images. Pull requests
from a fork build both platforms and push nothing. The smoke test in the
workflow starts the image and reads `/healthz`.

The Deployment references `:latest` and sets `imagePullPolicy: Always`, so
Kubernetes pulls the image on every pod start. For a fixed deployment, pin an
immutable `:sha-<commit>` tag through an `images:` entry in
`manifest/kustomization.yaml`.

New GHCR packages start private. Make the package public, or configure an
imagePullSecret in the namespace. To use a different registry: build the repo
`Dockerfile`. Push the image to that registry:

```text
docker build -t registry.example.com/objgit:1.5.1 .
docker push registry.example.com/objgit:1.5.1
```

Then update the image reference in `manifest/deployment.yaml`.

## Apply

```text
kubectl apply -k manifest/
```

## Use

Both Services are of type ClusterIP. They are reachable only from inside the
cluster. objgitd has no authentication yet. For this reason, the Services
stay cluster-internal.

The `objgit` Service exposes SSH on port 22 and HTTP on port 80. In the pod,
the daemon still binds port 2222 for SSH and port 8080 for HTTP. The named
target ports in the Service map one to the other:

```text
git clone http://objgit.objgit.svc.cluster.local/org/repo.git
git clone git://objgit.objgit.svc.cluster.local:9418/org/repo.git
git clone ssh://git@objgit.objgit.svc.cluster.local/org/repo.git
```

Scrape metrics from `objgit-metrics.objgit.svc.cluster.local:9090/metrics`.
The same listener serves pprof under `/debug/pprof/`.

## Health check

The metrics listener answers `GET /healthz` with `200 ok`. The smart-HTTP mux
returns 404 for every non-git path. For this reason, the probes use the
metrics listener.

The endpoint reads no bucket state. A Tigris outage therefore does not fail
the probes.

The startup, liveness, and readiness probes in the manifest all send
`GET /healthz` to the metrics port.

## Storage notes

- The **pack cache** (`PACK_CACHE_DIR`) lives on the 16Gi
  `objgitd-pack-cache` claim. `PACK_CACHE_BYTES` caps the cache at 14 GiB.
  The unused space on the claim holds the temporary files of in-flight
  downloads. This claim is the only persistent state. The repositories and
  the SSH host key live in the Tigris bucket. On startup, the daemon sweeps
  cache directories that an earlier run left behind, so a crash does not
  leak disk space across restarts.
- **Upload scratch data** is separate from the pack cache. The tigris storer
  stages pack writes to the OS temp directory through `os.CreateTemp`. The
  root filesystem of the container is read-only, so the OS temp directory
  needs its own `emptyDir` volume (4Gi `sizeLimit`) at `/tmp`. An exceeded
  `sizeLimit` evicts the entire pod, which kills every push in flight. For
  very large pushes, raise `sizeLimit`, or set `TMPDIR` to a path on the
  pack-cache volume.
- The claim has the `ReadWriteOnce` access mode. A rolling update tries to
  mount it on two pods at the same time. For this reason, the Deployment
  uses the `Recreate` strategy and one replica. Scale-out needs
  `ReadOnlyMany` storage or one cache per pod. Reference updates commit
  through compare-and-swap on the bucket, so the ref store does not limit
  the replica count.

## Resources

Each concurrent push costs approximately 400 MiB of resident set for a large
repository. `MAX_CONCURRENT_PUSHES` (4 by default) caps the number of
concurrent pushes. The 4Gi memory limit in the manifest covers four pushes
and fetch traffic. The memory limit and the cap must change together. See
[plans/bound-concurrent-pushes.md](../plans/bound-concurrent-pushes.md) for
the measurements.
