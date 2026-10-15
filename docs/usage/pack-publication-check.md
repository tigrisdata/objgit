# Check pack publication on a personal cluster

This procedure checks issue 28 with the patched image. It uses a new repository in your test bucket.
The local regression tests also check blocked and failed uploads. A successful cluster smoke test alone cannot prove behavior during every failure.

## 1. Record the deployment and bucket

Set these values for your personal cluster:

```sh
export OBJGIT_CONTEXT='your-context'
export OBJGIT_NAMESPACE='objgit'
export OBJGIT_DEPLOYMENT='objgitd'
export OBJGIT_CONTAINER='objgitd'
export OBJGIT_BUCKET='your-test-bucket'
export OBJGIT_IMAGE='your-registry/objgit:issue-28-test'
```

Record the existing image and bucket configuration:

```sh
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" get deployment "$OBJGIT_DEPLOYMENT" \
  -o jsonpath='{.spec.template.spec.containers[*].image}{"\n"}'
tigris buckets get "$OBJGIT_BUCKET" --json
```

The storer requires a Single-region or Multi-region bucket. Record the pod region and bucket locations.
An unsupported bucket mode needs a separate deployment correction. These code changes do not provide global read consistency.

## 2. Build and install the test image

Run these commands from the patched checkout. Select the platform used by your cluster nodes.

```sh
git lfs pull
docker buildx build --platform linux/amd64 -t "$OBJGIT_IMAGE" --push .
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" set image \
  deployment/"$OBJGIT_DEPLOYMENT" "$OBJGIT_CONTAINER=$OBJGIT_IMAGE"
```

For this test deployment, enable hooks and both transports. Disable the pack cache to force reads from the bucket.
These commands change your test deployment's configuration:

```sh
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" set env \
  deployment/"$OBJGIT_DEPLOYMENT" \
  BUCKET="$OBJGIT_BUCKET" ALLOW_PUSH=true ALLOW_HOOKS=true \
  HTTP_BIND=:8080 SSH_BIND=:2222 PACK_CACHE_BYTES=0
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" rollout status \
  deployment/"$OBJGIT_DEPLOYMENT" --timeout=180s
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" get pods \
  -l app.kubernetes.io/name=objgitd \
  -o 'custom-columns=NAME:.metadata.name,IMAGE:.spec.containers[0].image,IMAGEID:.status.containerStatuses[0].imageID'
```

Retain the existing credential configuration. No credential values belong in the test report.

In another terminal, forward both ports:

```sh
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" port-forward \
  deployment/"$OBJGIT_DEPLOYMENT" 18080:8080 12222:2222
```

## 3. Push a repository with a hook

Use Bash or Zsh for these commands:

```sh
export OBJGIT_TEST_ROOT="$(mktemp -d)"
export OBJGIT_TEST_REPO="smoke/pack-publication-$(date +%s)"
export OBJGIT_HTTP="http://127.0.0.1:18080/$OBJGIT_TEST_REPO.git"
export OBJGIT_SSH="ssh://git@127.0.0.1:12222/$OBJGIT_TEST_REPO.git"
export GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$OBJGIT_TEST_ROOT/known_hosts"

git init -b main "$OBJGIT_TEST_ROOT/source"
git -C "$OBJGIT_TEST_ROOT/source" config user.name 'Publication test'
git -C "$OBJGIT_TEST_ROOT/source" config user.email 'publication-test@example.invalid'
mkdir -p "$OBJGIT_TEST_ROOT/source/.objgit/hooks"
printf 'echo publication-hook-ran\n' > "$OBJGIT_TEST_ROOT/source/.objgit/hooks/receive-pack"
dd if=/dev/urandom of="$OBJGIT_TEST_ROOT/source/payload.bin" bs=1048576 count=16

git -C "$OBJGIT_TEST_ROOT/source" add .
git -C "$OBJGIT_TEST_ROOT/source" commit -m 'test pack publication'
git -C "$OBJGIT_TEST_ROOT/source" push "$OBJGIT_HTTP" main
```

The push output must contain `publication-hook-ran`. The server log must contain these messages in order:

```text
waiting for packfiles to finish pushing
hook: running
hook: finished
```

Capture the log before restarting the pod:

```sh
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" logs \
  deployment/"$OBJGIT_DEPLOYMENT" -c "$OBJGIT_CONTAINER" --since=10m \
  > "$OBJGIT_TEST_ROOT/server-before-restart.log"
```

The wait message marks the start of the publication barrier. It does not mark its completion.
The hook starts only after the ref update succeeds, which requires all bin and cue uploads to succeed.

## 4. Fetch through fresh readers

First clone immediately after the push:

```sh
git ls-remote "$OBJGIT_HTTP" refs/heads/main
git clone --depth=1 --no-checkout "$OBJGIT_HTTP" "$OBJGIT_TEST_ROOT/http"
git clone --depth=1 --no-checkout "$OBJGIT_SSH" "$OBJGIT_TEST_ROOT/ssh"
test "$(git -C "$OBJGIT_TEST_ROOT/source" rev-parse HEAD)" = "$(git -C "$OBJGIT_TEST_ROOT/http" rev-parse HEAD)"
test "$(git -C "$OBJGIT_TEST_ROOT/source" rev-parse HEAD)" = "$(git -C "$OBJGIT_TEST_ROOT/ssh" rev-parse HEAD)"
git -C "$OBJGIT_TEST_ROOT/http" fsck --full
git -C "$OBJGIT_TEST_ROOT/ssh" fsck --full
```

Then restart the daemon:

```sh
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" rollout restart deployment/"$OBJGIT_DEPLOYMENT"
kubectl --context "$OBJGIT_CONTEXT" -n "$OBJGIT_NAMESPACE" rollout status deployment/"$OBJGIT_DEPLOYMENT" --timeout=180s
```

Restart the port-forward command after the old pod exits. Repeat both clones into new directories, such as `http-restart` and `ssh-restart`.
Compare their tips and run `git fsck --full` again.

Exercise a second push through SSH:

```sh
printf 'second push\n' > "$OBJGIT_TEST_ROOT/source/second.txt"
git -C "$OBJGIT_TEST_ROOT/source" add second.txt
git -C "$OBJGIT_TEST_ROOT/source" commit -m 'test second publication'
git -C "$OBJGIT_TEST_ROOT/source" push "$OBJGIT_SSH" main
git -C "$OBJGIT_TEST_ROOT/http" fetch --depth=1 origin main
git -C "$OBJGIT_TEST_ROOT/ssh" fetch --depth=1 origin main
test "$(git -C "$OBJGIT_TEST_ROOT/source" rev-parse HEAD)" = "$(git -C "$OBJGIT_TEST_ROOT/http" rev-parse FETCH_HEAD)"
test "$(git -C "$OBJGIT_TEST_ROOT/source" rev-parse HEAD)" = "$(git -C "$OBJGIT_TEST_ROOT/ssh" rev-parse FETCH_HEAD)"
```

## 5. Run the live storage test

From a machine with the test bucket's credentials, run:

```sh
OBJGIT_TIGRIS_LIVE_BUCKET="$OBJGIT_BUCKET" \
  go test ./internal/storage/tigris -run '^TestLiveBucketPackRoundTrip$' -count=1 -v
```

This test writes a unique prefix, publishes a ref, and reads every fixture object through a new root storer.
It hashes the complete object bodies and removes its own keys after the run.
The manual smoke repository remains in the bucket for inspection.

## If a clone fails

Preserve the server logs, image digest, bucket mode, region, advertised SHA, transport, and timestamps.
Record whether a new clone succeeds later without another push.

Use the existing object tools to compare LIST and direct GET for the known pack keys from upload diagnostics:

- A readable cue omitted from LIST indicates discovery visibility trouble.
- A readable cue with an unreadable bin indicates upload or bin visibility trouble.
- A missing commit in all cues requires investigation of ingestion or lost data.
- A valid cue and bin require investigation of lookup, prefix, hash format, or delta resolution.

Do not rewrite the affected ref or delete packs before collecting this evidence.
Restore your previous test image and configuration after the investigation.
