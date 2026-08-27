# Transports

objgitd speaks three git transports. All three answer the protocol natively
with the same go-git `transport.*` functions. objgitd never runs the `git`
binary, and it never writes a checkout to disk.

| Transport   | Flag           | Default  | Credential                  |
| ----------- | -------------- | -------- | --------------------------- |
| Smart HTTP  | `-http-bind`   | `:8080`  | HTTP Basic, or anonymous.   |
| git://      | `-git-bind`    | `:9418`  | Anonymous only.             |
| SSH         | `-ssh-bind`    | off      | Public key, or anonymous.   |

All three route their decisions through [the auth seam](auth.md).

## git:// (`git_protocol.go`)

`Serve` accepts a TCP connection and calls `handle`. `handle` decodes a
`packp.GitProtoRequest`. Then it dispatches to `transport.UploadPack`,
`transport.UploadArchive`, or `d.receivePack`.

This file also holds the shared `operationFor(service)` helper. It maps
`receive-pack` to `auth.Write`, and every other service to `auth.Read`.

## Smart HTTP (`http.go`)

`*daemon` implements `http.Handler` directly. Dispatch reads the URL suffix,
which is `/info/refs`, `/git-upload-pack`, or `/git-receive-pack`.

`http.ServeMux` does not fit this shape. Repository paths have a variable
depth, and a `ServeMux` wildcard cannot capture a prefix that comes before a
fixed suffix.

Smart HTTP calls the same go-git server commands with `StatelessRPC: true`.
`GET /info/refs` also sets `AdvertiseRefs: true`.

## SSH (`ssh.go`)

`newSSHServer` builds the gliderlabs/ssh server and the host key. `handleSSH`
dispatches one session, as a sibling of `handle`.

wish's `git/git.go` runs the real `git-upload-pack` binary. objgitd does not.

gliderlabs/ssh splits `s.Command()` into words for us. `gitServiceFor(cmd[0])`
selects the service. The repository path is `strings.TrimPrefix(cmd[1], "/")`,
so `ssh://host/foo.git` and the scp-style `host:foo.git` resolve to the same
repository. The session is the protocol stream, because it is both the reader
and the writer.

**Connect and authorize are separate steps.** `PublicKeyHandler` returns
`true` for every key. It must be set, or the server does not offer public-key
authentication at all. Authorization then happens once per command through
`d.authz`, with `Cred: auth.PublicKey{Key: s.PublicKey()}`.

**The host key lives in the bucket** at `.objgit/ssh_host_ed25519_key`.
`loadOrCreateHostKey` generates an ed25519 key on the first start and reads
that same key after that. Clients get no host-key-changed warning across a
restart, and the server needs no local disk.

Receive-pack goes through `d.receivePack`, and not through
`transport.ReceivePack`, so [push hooks](hooks.md) also run over SSH.

Protocol v2 is not forwarded yet. `s.Environ()` carries `GIT_PROTOCOL`, but v0
and v1 are enough for now.

## Two protocol points that are easy to get wrong

### 1. `writePack` keeps the pack whole

`writePack` in `receivepack.go` stores the incoming pack whole on every
transport, when the storer supports it.

The default go-git path does not work here. `WritePackfileToObjectStorage`
calls `io.CopyBufferPool`, which copies until `io.EOF`. Over a persistent
git:// or SSH socket that deadlocks, because the client holds the connection
open and waits for report-status.

`writePack` therefore does not wait for EOF. It drives a `packfile.Scanner`
over an `io.TeeReader(rd, packWriter)`. The scanner knows where the pack ends
from the pack framing, which is the header object count plus the trailer
checksum. The tee mirrors exactly those bytes into the `PackfileWriter`.

The result is a few containers per push, instead of one loose write for each
object. On S3 each loose write costs a `HeadObject` for the dedup `Lstat`,
**and** a `PutObject`.

`internal/storage/tigris.Storer` implements `storer.PackfileWriter`. It stores
a flat `.bin` and `.cue` container, and not the delta-compressed git pack
format. See [tigris-storer.md](tigris-storer.md).

A storer without that interface falls back to `packfile.UpdateObjectStorage`.
That path writes one loose object for each `SetEncodedObject` call.

### 2. No-op closers everywhere

`transport.UploadPack` and `transport.ReceivePack` call `Close` on the reader
between negotiation rounds, and sometimes on the writer. A git:// socket
cannot survive that, and an HTTP `ResponseWriter` does not implement `Close`.

Every transport therefore wraps the reader in `io.NopCloser`, and the writer
in `ioutil.WriteNopCloser` from `go-git/v6/utils/ioutil`.

### SSH shares both points with git://, and HTTP does not

An `ssh.Session` is a persistent two-way stream. `handleSSH` therefore uses
the same no-op closers. It also relies on the same Scanner-bounded
`writePack`, because it has no request-body EOF like the one HTTP enjoys.
