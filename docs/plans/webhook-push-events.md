# Plan: push webhook events

## Contract

The event schema lives at
`proto/tigrisdata/objgit/events/push/v1/push.proto`. The Buf module root is
`proto/`, so later event packages can sit beside `events/push/v1`. A raw
ProtoJSON example lives next to the schema. Message and field comments provide
the starting descriptions for future gnostic OpenAPI annotations and generated
documentation. The schema does not import gnostic until that generation path is
chosen.

One accepted ref update produces one `PushEvent`. Multiple ref updates in one
receive-pack request share a `push_id`; each event has its own `event_id`, which
stays fixed on retries. `commits` contains every commit newly reachable from
the new tip relative to the old tip, ordered with parents before children.
Each commit's file lists compare it with its first parent. The event's `files`
summarizes the net change between old and new trees. Deletes have no commits.

The JSON example includes two commits and all three file-change categories.
Delivery uses ProtoJSON and emits default scalar/list values so receivers see
explicit `false` and empty arrays as in the example.

## Implementation

1. Make receive-pack return the ref commands that actually committed, rather
   than attributing a before/after repository snapshot to a request. Check
   advertised old hashes at the packed-refs commit point. After a loose-ref
   cleanup error, dispatch only updates visible in the effective ref view.
2. Walk each updated ref's commit graph. Exclude commits reachable from the old
   tip, sort the remainder with parents before children, and diff each commit
   against its first parent. Diff the old and new trees once more for `files`.
   Share that net diff with the per-ref shell hook metadata.
3. Resolve operator-owned webhook destinations and secrets. Serialize the
   event with ProtoJSON, sign the exact request bytes, and send with bounded
   timeout and retries. Keep a delivery ID stable across attempts.
   Each repository now reads its settings from
   `.objgit/webhooks/<org>/<repo>/settings.json` in daemon bucket state.
   Raw settings queries on HTTP and SSH use the shared `Authorizer` seam with
   the `Admin` operation. The permissive default denies this operation.
4. Large events are currently sent whole. Event construction has a 30-second
   deadline per ref. A push too large to build within that limit logs a delivery
   failure; it does not silently truncate the commit or path lists. A future
   continuation or retrieval API can provide a better way to handle these
   pushes.
5. Protocol and package tests cover multi-commit pushes, ref changes, commit
   graph traversal, file metadata, signing, and delivery outcomes.

Delivery is currently best effort. It has bounded retries during the Git
request, but no durable queue or replay after daemon shutdown. Operators can
observe failures in logs and `objgit_webhook_deliveries_total`.

Run `buf lint` and `buf build` when editing the schema. Once an earlier schema
exists, run `buf breaking --against '.git#branch=main'`. `WIRE_JSON` is the
compatibility target because webhook deliveries use ProtoJSON.
