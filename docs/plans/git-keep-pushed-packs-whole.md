# Keep pushed packs whole on git:// and SSH

## Context

Pushing big repositories to `objgitd` is slow. The root cause is not the go-git
dotgit backend itself — it is that two of the three transports explode the
received packfile into **loose objects**, one S3 object per git object.

The git:// and SSH receive-pack paths wrap the storer in `streamingStorer{}`
(`cmd/objgitd/git_protocol.go:43`) to **hide** the storer's `PackfileWriter`
capability. That is a deliberate deadlock workaround: go-git's
`WritePackfileToObjectStorage` copies the pack into the PackfileWriter with
`io.CopyBufferPool`, which only returns on `io.EOF` — fine over HTTP (the request
body has a real EOF) but a hang over a persistent git:// / SSH socket, where the
client holds the connection open waiting for report-status. With PackfileWriter
hidden, `packfile.UpdateObjectStorage` falls through to `Parser.Parse`, which
knows the pack's end from its own framing and writes **loose objects**.

Each loose object then costs **two** S3 round-trips in `internal/s3fs`:

1. go-git's `ObjectWriter.save()` (`dotgit/writers.go:375`) does
   `fs.Lstat(file)` before writing — a content-addressable dedup check — which
   becomes an S3 **HeadObject** (`s3fs/basic.go:200`), plus a ListObjectsV2
   directory probe on a miss.
2. The `Rename` of the temp object then becomes the **PutObject**.

For a 100k-object push that is ~200k S3 round-trips. HTTP avoids all of this
because it keeps the PackfileWriter path and stores the pack as a single object.

**Goal:** make git:// and SSH keep the pack whole too — like HTTP, like real
git's "keep large pack" behavior — so a push is ~2 S3 PutObjects (pack + idx)
plus a handful of ref writes, instead of N. This also removes the
stat-before-every-write entirely, because there are no per-object writes.

## Prerequisite (Step 0): fix the closed-pack-handle panic in s3fs

This must land first — it is a latent crash that Approach B would otherwise newly
expose on git:// and SSH (and which already crashes HTTP+hooks).

go-git's `FSObject.Reader()` (`packfile/fsobject.go:62`) reads packed objects via
`ReadAt` and explicitly recovers from a closed pack descriptor: it probes with a
1-byte `pack.ReadAt(...)` and, if the error matches `os.ErrClosed`, reopens the
pack (`o.fs.Open(o.packPath)`). The billy contract is therefore that
`File.ReadAt` on a closed file returns an `os.ErrClosed`-matching error.

s3fs violates this: `s3ReadFile.Close()` sets `f.reader = nil`
(`internal/s3fs/file.go:150`), but `Read`/`ReadAt`/`Seek`
(`file.go:128/133/138`) dereference `f.reader` unconditionally, so a post-close
call **panics on a nil `*bytes.Reader`** instead of returning `os.ErrClosed`.
After a push the object LRU caches the new commit as an `FSObject` over a pack
handle that index-building later closes; the post-receive hook's `GetCommit` then
calls `Reader()` → probe `ReadAt` on the closed handle → panic
(`hooks.go:136` in the reported trace).

**Fix:** guard the three read methods of `s3ReadFile` to return `os.ErrClosed`
when the file is closed, e.g.:

```go
func (f *s3ReadFile) ReadAt(p []byte, off int64) (int, error) {
	if f.closed || f.reader == nil {
		return 0, os.ErrClosed
	}
	return f.reader.ReadAt(p, off)
}
```

Apply the same guard to `Read` and `Seek`. (`os` is already imported in
`file.go`.) Return `os.ErrClosed` specifically — not the existing `ErrFileClosed`
— unless `ErrFileClosed` is made to wrap it, because go-git keys its reopen on
`errors.Is(err, os.ErrClosed)`.

**Caveat / follow-up (out of scope):** s3fs `Open` reads the _whole_ object into
memory (`file.go:95`), so go-git's reopen-on-closed downloads the entire pack
again per cache-resident `FSObject` read. Acceptable for hooks (a handful of
reads), but a future optimization is to serve `ReadAt` via S3 `GetObject` with a
`Range` header (true random access) and/or keep pack descriptors open, instead of
buffering the full pack. Note it; don't build it here.

## Approach: drive PackfileWriter with a Scanner-bounded TeeReader

The deadlock only exists because `io.CopyBufferPool` waits for the reader's EOF.
We don't need EOF: the packfile is self-delimiting (header carries the object
count; a trailer checksum ends it). go-git's `packfile.Scanner`
(`plumbing/format/packfile/scanner.go`) walks exactly those bytes and stops at
the trailer — this is the same machinery `Parser.Parse` already uses successfully
over the socket today, so it provably terminates without blocking (the client
sends nothing after the pack, so the underlying socket read never waits past the
trailer).

So we get the PackfileWriter's writer, tee the socket reader through it, and let
the Scanner consume exactly the pack — `w.Close()` then builds the index and
uploads one `.pack` + one `.idx`. This depends on framing, not EOF, so it works
identically on HTTP, git://, and SSH. The `streamingStorer` workaround is no
longer needed and is deleted.

### Changes

**1. `cmd/objgitd/receivepack.go` — add `writePack`, replace the unpack call.**

Replace the body at line 118 (`unpackErr = packfile.UpdateObjectStorage(st, rd)`)
with a call to a new helper:

```go
// writePack stores the incoming packfile as a single packfile object via the
// storer's PackfileWriter, delimiting the pack with a Scanner rather than
// waiting for the reader to reach io.EOF. go-git's default PackfileWriter path
// (io.CopyBufferPool until EOF) deadlocks on a persistent git:// / SSH socket
// where the client holds the connection open awaiting report-status; the
// Scanner knows the pack's end from its own framing and stops there, while a
// TeeReader mirrors exactly those bytes into the PackfileWriter. Falls back to
// UpdateObjectStorage (loose objects) if the storer cannot write packs.
func writePack(st storage.Storer, rd io.Reader) error {
	pw, ok := st.(storer.PackfileWriter)
	if !ok {
		return packfile.UpdateObjectStorage(st, rd)
	}

	var sopts []packfile.ScannerOption
	if c, ok := st.(config.ConfigStorer); ok {
		if cfg, err := c.Config(); err == nil &&
			cfg.Extensions.ObjectFormat == formatcfg.SHA256 {
			sopts = append(sopts, packfile.WithSHA256())
		}
	}

	w, err := pw.PackfileWriter()
	if err != nil {
		return err
	}
	sc := packfile.NewScanner(io.TeeReader(rd, w), sopts...)
	for sc.Scan() {
	}
	if err := sc.Error(); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}
```

- Object-format detection mirrors `packfile.UpdateObjectStorage`
  (`common.go:29`): read it from the `ConfigStorer`, default SHA-1; the only
  scanner knob is `packfile.WithSHA256()` (`scanner_options.go:16`).
- New imports: `config`, `formatcfg "…/plumbing/format/config"`,
  and `storer` is already imported. (`packfile`, `storage`, `io` already present.)
- Call site becomes `unpackErr = writePack(st, rd)`.

**2. `cmd/objgitd/git_protocol.go` — delete `streamingStorer`.**

Remove the type and its doc comment (lines 31–45). The `storage` import may
become unused here — drop it if so.

**3. Collapse `receivePack`'s two storer params to one (`hooks.go` + call sites).**

`streamingStorer` was the _only_ reason `rpStorer` and `readStorer` differed; all
callers will now pass the same `st` for both. Simplify
`(*daemon).receivePack` (`hooks.go:87`) to a single `st storage.Storer`
param (used for both the service write and the ref snapshots/hook checkout),
update its doc comment, and update the three call sites:

- `git_protocol.go:186` — `d.receivePack(ctx, st, req.Pathname, …)`
- `ssh.go:224` — `d.receivePack(ctx, st, repoPath, …)`; delete the now-stale
  `streamingStorer` comment at `ssh.go:222–223`.
- `http.go:145` — `d.receivePack(r.Context(), st, repoPath, …)`

**4. Update docs.** `CLAUDE.md` "Two subtle protocol points" #1 and the SSH
section both describe the `streamingStorer` workaround and the loose-object
trade-off — rewrite to describe the Scanner-bounded PackfileWriter that all three
transports now share. (`docs/plans/` may also get a short note.)

### Edge cases / risk

- **No over-read / no hang:** the Scanner's internal `bufio` only issues a read
  when its buffer drains, and a socket read returns the bytes currently present;
  the client sends nothing after the trailer, so it never blocks past the pack.
  This is the exact behavior the current `Parser.Parse` path relies on.
- **Memory:** the pack is buffered in the s3fs in-memory `tempBuffer` before
  upload (`internal/s3fs/tempfs.go`) — identical to today's HTTP path, so git://
  / SSH now match HTTP's memory profile. Spilling large packs to disk / multipart
  upload is a possible follow-up, out of scope here.
- **Empty/delete-only pushes:** unchanged — `needPackfile`
  (`receivepack.go:108`) already gates `writePack` to pushes that carry a pack.
- **Delta bases in existing objects:** unaffected — reads still go through the
  normal storer; we only changed how the incoming pack is _written_.

## Verification

1. `go build ./...`
2. `go test ./...` — existing round-trip push/clone tests for all three
   transports must stay green (`git_protocol_test.go`, `ssh_test.go`,
   `http_test.go`; gated on `git`/`ssh` in PATH).
3. **New assertion proving packs are kept** (tests back the daemon with
   `memfs.New()`, so the stored layout is directly inspectable): after a git://
   push and an SSH push, assert the repo contains `objects/pack/pack-*.pack`
   (+`.idx`) and that **no** loose-object dirs (`objects/<2-hex>/`) exist. Add as
   a table case alongside the existing receive-pack tests; reuse `seedRepo` /
   `runGit` helpers from `git_protocol_test.go`.
4. **Regression test for the Step 0 panic:** with `allowHooks: true` and a repo
   whose pushed tree contains `.objgit/hooks/receive-pack`, push over **HTTP**
   (today's crash) and over **git:// / SSH** (newly pack-backed after Approach B)
   and assert the push succeeds and the hook runs — exercising
   `GetCommit` → `FSObject.Reader()` → `ReadAt` on a cache-resident, closed pack
   handle without panicking. A focused s3fs unit test also helps: open a file,
   `Close()` it, then assert `ReadAt`/`Read`/`Seek` each return an
   `errors.Is(_, os.ErrClosed)` error rather than panicking.
5. **Manual, against a real bucket:** push a large repo over SSH
   (`./objgitd -bucket $BUCKET -ssh-bind :2222 -allow-push`) and watch
   `objgit_s3_requests_total{op="PutObject"}` / `{op="HeadObject"}` on `/metrics`
   — PutObject should drop from O(objects) to ~2 (pack + idx) and HeadObject
   should no longer scale with object count. Compare wall-clock against a
   pre-change push of the same repo.
