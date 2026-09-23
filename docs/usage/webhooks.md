# Push webhooks

Store one `settings.json` object for each repository in the daemon's Tigris
bucket. For `acme/widgets`, the key is
`.objgit/webhooks/acme/widgets/settings.json`. This is daemon state, alongside
`.objgit/ssh_host_ed25519_key`. It is not a file in the Git repository's tree.
The object contains one ProtoJSON settings message:

```json
{
  "url": "https://events.example.com/objgit",
  "secret": "replace-with-a-long-random-secret"
}
```

Each repository has one destination. Configure HTTPS endpoints; HTTP is
accepted only for loopback addresses. Generate a distinct secret for each
repository with `openssl rand -hex 32`. Restrict bucket access to these
objects because they contain signing secrets. The daemon reads settings for
each push, so the next push uses an updated object without a restart. A
missing object disables webhooks for that repository; invalid settings are
logged and the push continues without webhook delivery.

## Query settings

The Git HTTP and SSH listeners expose admin-only queries for the raw settings,
including the signing secret:

```sh
curl --user 'operator:<admin-password>' \
  https://git.example.com/_objgit/webhooks/acme/widgets/settings
ssh git@git.example.com objgit-webhook-settings acme/widgets
```

The HTTP response uses ProtoJSON and has `Cache-Control: no-store`. The SSH
command prints the same JSON. The HTTP endpoint returns 404 when settings are
absent; the SSH command exits with an error. Use HTTPS for the HTTP query
because it carries a credential and a raw secret.

Both queries ask the configured `internal/auth.Authorizer` for the `Admin`
operation on that repository. The default `AllowAnonymous` authorizer denies
`Admin`, even when `-allow-push` is set. Deploy an authorizer that checks your
admin ACL and grants `Admin` only to eligible HTTP or SSH credentials before
using these queries. Ordinary repository read or write permission does not
grant access to the secret.

The settings schema is
[settings.proto](../../proto/tigrisdata/objgit/webhooks/v1/settings.proto),
with a [raw JSON example](../../proto/tigrisdata/objgit/webhooks/v1/settings.example.json).

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

| Header                   | Value                                                                                                                 |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| `X-Objgit-Event`         | `push`                                                                                                                |
| `X-Objgit-Delivery`      | The event's `eventId`, unchanged on retries.                                                                          |
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
