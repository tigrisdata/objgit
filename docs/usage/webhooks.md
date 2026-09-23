# Push webhooks

Set `-webhook-config` to a JSON file owned by the daemon operator. The flag
also accepts `WEBHOOK_CONFIG` through the usual environment flag mapping.
An empty value disables delivery. The daemon reads the file at startup, and
rejects invalid entries before it starts serving Git requests.

```json
{
  "repositories": {
    "acme/widgets": {
      "url": "https://events.example.com/objgit",
      "secret": "replace-with-a-long-random-secret"
    }
  }
}
```

Each repository has one destination. The repository key is its canonical
`org/name` path. Configure HTTPS endpoints; HTTP is accepted only for loopback
addresses. Store the config with restricted permissions because it contains
signing secrets. Restart the daemon after changing the file.

## Event body

An accepted ref update sends one `push` event. If one Git push updates several
refs, each ref gets an event with a shared `pushId` and a distinct `eventId`.
Each event contains **all commits newly reachable from that ref** relative to
its previous tip, ordered with parents before children. `commits[].files`
lists additions, changes, and deletions against each commit's first parent.
The top-level `files` lists the net change between the old and new trees.
For example, a file added and deleted across two commits appears in those
commits but not in the top-level list. A deleted ref has an empty commit list.

The schema is [push.proto](../../proto/tigrisdata/objgit/events/push/v1/push.proto),
and [push.example.json](../../proto/tigrisdata/objgit/events/push/v1/push.example.json)
shows the raw JSON sent to receivers. The body is ProtoJSON with default scalar
and list values included. A rename appears as one deletion and one addition.
File contents and patches are not included.
Valid UTF-8 paths are literal. A path containing invalid UTF-8 bytes is
represented as `/objgit/raw-path/base64url/` followed by the unpadded
base64url encoding of its original bytes. Git paths cannot start with `/`,
so receivers can distinguish this representation from a literal path.
Non-UTF-8 ref names use the same representation; Git ref names also cannot
start with `/`.
Commit messages and author or committer names and email addresses use U+FFFD
for invalid UTF-8 bytes so the JSON body remains valid.

## Headers and verification

Each request is an HTTP `POST` with `Content-Type: application/json` and these
headers:

| Header | Value |
| ------ | ----- |
| `X-Objgit-Event` | `push` |
| `X-Objgit-Delivery` | The event's `eventId`, unchanged on retries. |
| `X-Objgit-Signature-256` | `sha256=` followed by the lowercase hex HMAC-SHA256 of the **exact request body bytes**, using the configured secret. |

Verify the HMAC with a constant-time comparison before processing the body.
Use `X-Objgit-Delivery` as an idempotency key: the server can retry the same
event after a network failure. A successful HTTP 2xx response completes the
delivery. The server retries network failures, HTTP 429, and HTTP 5xx up to
three attempts within five seconds. Other status codes end the delivery.

Delivery runs after the ref update, while the Git request is still open.
Delivery failures are logged and counted in
`objgit_webhook_deliveries_total{status="error"}`; they do not reject the
accepted push. The daemon has no durable delivery queue. If it stops after
accepting a ref but before completing delivery, the event may be lost.
