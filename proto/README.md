# Event schemas

`proto/` is the Buf module root. Push webhook messages are defined in
[`tigrisdata/objgit/events/push/v1/push.proto`](tigrisdata/objgit/events/push/v1/push.proto).
The adjacent [`push.example.json`](tigrisdata/objgit/events/push/v1/push.example.json)
is a complete raw ProtoJSON payload with two commits and file changes. Keep it
in sync when changing the schema; future generated documentation can include
it as an example.

Run these commands from the repository root:

```text
buf format --diff --exit-code
buf lint
buf build
buf generate
```

`buf generate` writes Go types under `gen/`, following the Xe/x Buf layout.
Per-repository webhook settings are defined in
[`tigrisdata/objgit/webhooks/v1/settings.proto`](tigrisdata/objgit/webhooks/v1/settings.proto),
with an adjacent raw
[`settings.example.json`](tigrisdata/objgit/webhooks/v1/settings.example.json).
The schema does not yet carry gnostic OpenAPI annotations. Its versioned
package, comments, and example are the starting point for adding those
annotations and generated docs later.

Validate the raw example as ProtoJSON with:

```text
buf convert proto --type=tigrisdata.objgit.events.push.v1.PushEvent --from=proto/tigrisdata/objgit/events/push/v1/push.example.json --to=-#format=binpb > /dev/null
```
