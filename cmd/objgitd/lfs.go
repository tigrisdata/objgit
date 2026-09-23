package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	ssh "github.com/gliderlabs/ssh"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/lfs"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/repofs"
)

// lfsService is the Git LFS subsystem. The daemon's field is nil when
// -allow-lfs is unset, and httpHandler then registers no LFS route at all, so
// the whole feature reads as a 404 from outside. That is the answer both
// git-lfs and the locking specification expect from a server without LFS.
type lfsService struct {
	store *lfs.Store
	// ttl answers how long a presigned URL minted right now may live. It is a
	// function and not a value because the answer depends on the credential the
	// daemon holds at that moment, which the SDK refreshes while the process
	// runs. See lfsURLTTLFunc in main.go.
	ttl      func(context.Context) time.Duration
	maxSize  int64
	maxBatch int
	// externalURL is the public base URL, used to build the verify action and
	// the git-lfs-authenticate response.
	externalURL string
	allowLocks  bool
}

// urlTTL is the presigned-URL lifetime for one request.
func (s *lfsService) urlTTL(ctx context.Context) time.Duration {
	return s.ttl(ctx)
}

// maxBodyBytes is the largest LFS request body the daemon reads.
//
// It is derived from the batch cap rather than configured on its own: one
// object entry is an oid, a size, and punctuation, so 256 bytes each is
// generous. The floor keeps the small bodies (verify, locks) workable when
// -lfs-max-batch is tiny.
func (s *lfsService) maxBodyBytes() int64 {
	const (
		perObject = 256
		floor     = 64 << 10
	)
	if n := int64(s.maxBatch) * perObject; n > floor {
		return n
	}
	return floor
}

// Limits on what one repository's lock document may grow to hold.
//
// The document is read whole, decoded, and re-encoded on every lock call, and
// git-lfs calls locks/verify on every push. Nothing else in the LFS surface
// stores caller-supplied text, so these two bounds are what keep one anonymous
// request from permanently slowing every later push to that repository.
const (
	// maxLockPathBytes caps the path a lock may name. 4096 is PATH_MAX on
	// Linux, so it is above anything git can put in a working tree, and no
	// real lock is refused by it.
	maxLockPathBytes = 4096
	// maxLocksPerRepo caps how many locks one repository holds at once. File
	// locking coordinates a team on a handful of binary assets, so a thousand
	// is far above real use while holding the document to a few megabytes even
	// when every path is at the cap above.
	maxLocksPerRepo = 1000
)

// registerLFS adds the LFS routes. It is a no-op when LFS is off.
//
// Every pattern has a fixed segment count and no trailing wildcard, so none can
// match another's requests and none shadows the git routes.
func (d *daemon) registerLFS(mux *http.ServeMux) {
	if d.lfs == nil {
		return
	}
	mux.HandleFunc("POST /{orgID}/{repoName}/info/lfs/objects/batch", d.handleLFSBatch)
	mux.HandleFunc("POST /{orgID}/{repoName}/info/lfs/objects/verify", d.handleLFSVerify)

	if !d.lfs.allowLocks {
		return
	}
	mux.HandleFunc("POST /{orgID}/{repoName}/info/lfs/locks", d.handleLFSLockCreate)
	mux.HandleFunc("GET /{orgID}/{repoName}/info/lfs/locks", d.handleLFSLockList)
	mux.HandleFunc("POST /{orgID}/{repoName}/info/lfs/locks/verify", d.handleLFSLockVerify)
	mux.HandleFunc("POST /{orgID}/{repoName}/info/lfs/locks/{id}/unlock", d.handleLFSUnlock)
}

// lfsBegin runs the checks every LFS route shares: content negotiation, path
// parsing, and authorization. It writes the error itself and returns ok=false
// when the request cannot proceed.
func (d *daemon) lfsBegin(w http.ResponseWriter, r *http.Request, op auth.Operation) (repofs.RepoRef, bool) {
	// Answer as LFS from here on, including for failures.
	w.Header().Set("Content-Type", lfs.MediaType)

	if !acceptsLFS(r) {
		writeLFSError(w, http.StatusNotAcceptable, "this endpoint speaks "+lfs.MediaType)
		return repofs.RepoRef{}, false
	}

	ref, err := repofs.Parse(r.PathValue("orgID") + "/" + r.PathValue("repoName"))
	if err != nil {
		writeLFSError(w, http.StatusNotFound, err.Error())
		return repofs.RepoRef{}, false
	}

	cred, _ := credFromRequest(r)
	decision := d.authorize(r.Context(), auth.Request{
		Repo:      ref.Path(),
		Operation: op,
		Cred:      cred,
		Transport: "http",
	})
	switch decision {
	case auth.Allow:
		return ref, true
	case auth.Unauthenticated:
		// LFS names its own challenge header. git-lfs reads this one, not
		// WWW-Authenticate, when it decides to ask for credentials.
		w.Header().Set("LFS-Authenticate", `Basic realm="objgit"`)
		w.Header().Set("WWW-Authenticate", `Basic realm="objgit"`)
		writeLFSError(w, http.StatusUnauthorized, "authentication required")
		return repofs.RepoRef{}, false
	default:
		// A denied write is 403 and never 401. A 401 tells git-lfs the
		// credentials were wrong, so it asks the user and retries forever.
		slog.Warn("lfs request denied by authorizer",
			"repo", ref.Path(), "operation", op, "remote", r.RemoteAddr)
		writeLFSError(w, http.StatusForbidden, "access denied")
		return repofs.RepoRef{}, false
	}
}

// acceptsLFS reports whether the client will take an LFS answer. An absent or
// wildcard Accept is taken as yes, because curl and older clients send one.
func acceptsLFS(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	if accept == "" {
		return true
	}
	for _, part := range strings.Split(accept, ",") {
		media, _, _ := strings.Cut(strings.TrimSpace(part), ";")
		switch strings.TrimSpace(media) {
		case lfs.MediaType, "*/*", "application/*":
			return true
		}
	}
	return false
}

// writeLFSError renders a top-level failure in the shape git-lfs prints.
func writeLFSError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", lfs.MediaType)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(lfs.ErrorBody{Message: message}); err != nil {
		slog.Error("writing lfs error body", "err", err)
	}
}

// writeLFSJSON renders a successful body.
func writeLFSJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", lfs.MediaType)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("writing lfs response body", "err", err)
	}
}

// capLFSBody bounds a request body before anything reads it.
//
// Every LFS route takes its body from an unauthenticated connection, so none of
// them may buffer an arbitrary amount of JSON. This runs on all of them, and
// not only on the routes that decode before authorizing.
func (d *daemon) capLFSBody(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, d.lfs.maxBodyBytes())
}

// decodeLFSBody reads a JSON request body, answering 422 on anything malformed
// and 413 on a body past the cap.
func decodeLFSBody(w http.ResponseWriter, r *http.Request, into any) bool {
	err := json.NewDecoder(r.Body).Decode(into)
	if err == nil {
		return true
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeLFSError(w, http.StatusRequestEntityTooLarge, "request body is too large")
		return false
	}
	writeLFSError(w, http.StatusUnprocessableEntity, "malformed request body")
	return false
}

// decodeOptionalLFSBody decodes a body the protocol lets a client leave empty.
//
// Only an oversized body is fatal here. Anything else decodes into the zero
// value, which is what an absent body means on these routes.
func decodeOptionalLFSBody(w http.ResponseWriter, r *http.Request, into any) bool {
	err := json.NewDecoder(r.Body).Decode(into)
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeLFSError(w, http.StatusRequestEntityTooLarge, "request body is too large")
		return false
	}
	return true
}

// lfsOwner is the lock owner for a request: the Basic username, or anonymous.
func lfsOwner(r *http.Request) string {
	if user, _, ok := r.BasicAuth(); ok && user != "" {
		return user
	}
	return lfs.AnonymousOwner
}

// verifyURL builds the absolute URL of this repository's verify endpoint, which
// the batch response hands to the client.
func (d *daemon) verifyURL(r *http.Request, ref repofs.RepoRef) string {
	base := strings.TrimSuffix(d.lfs.externalURL, "/")
	if base == "" {
		// Fall back to the host the client reached, which is right for a
		// direct connection and for most reverse proxies.
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
			scheme = fwd
		}
		base = scheme + "://" + r.Host
	}
	return fmt.Sprintf("%s/%s/%s.git/info/lfs/objects/verify", base, ref.OrgID, ref.Name)
}

// handleLFSBatch serves POST .../info/lfs/objects/batch, the whole transfer
// protocol. It answers with presigned URLs; no object byte passes through here.
func (d *daemon) handleLFSBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req lfs.BatchRequest
	// The operation decides the access needed, so the body is read before
	// authorization rather than after. That puts an allocation ahead of the
	// authorizer, so the cap matters most here: an unauthenticated caller must
	// not be able to make the daemon hold an arbitrary amount of JSON before it
	// is turned away.
	d.capLFSBody(w, r)
	if !decodeLFSBody(w, r, &req) {
		metrics.ObserveLFSBatch("unknown", "invalid", start)
		return
	}

	op := auth.Read
	if req.Operation == lfs.OperationUpload {
		op = auth.Write
	}
	ref, ok := d.lfsBegin(w, r, op)
	if !ok {
		metrics.ObserveLFSBatch(req.Operation, "denied", start)
		return
	}

	resp, err := d.lfs.store.Batch(r.Context(), &req, lfs.BatchOptions{
		Repo:      ref.Path(),
		VerifyURL: d.verifyURL(r, ref),
		TTL:       d.lfs.urlTTL(r.Context()),
		MaxSize:   d.lfs.maxSize,
		MaxBatch:  d.lfs.maxBatch,
	})
	if err != nil {
		status, label := lfsBatchStatus(err)
		if status == http.StatusInternalServerError {
			slog.Error("lfs batch failed", "repo", ref.Path(), "operation", req.Operation, "err", err)
			writeLFSError(w, status, "cannot serve batch request")
		} else {
			writeLFSError(w, status, err.Error())
		}
		metrics.ObserveLFSBatch(req.Operation, label, start)
		return
	}

	for _, obj := range resp.Objects {
		metrics.ObserveLFSObject(req.Operation, lfsObjectResult(obj))
	}

	slog.Info("serving lfs batch",
		"repo", ref.Path(),
		"operation", req.Operation,
		"objects", len(resp.Objects),
		"remote", r.RemoteAddr,
	)
	writeLFSJSON(w, http.StatusOK, resp)
	metrics.ObserveLFSBatch(req.Operation, "ok", start)
}

// lfsBatchStatus maps a whole-request failure to its HTTP status.
func lfsBatchStatus(err error) (int, string) {
	switch {
	case errors.Is(err, lfs.ErrTooManyObjects):
		return http.StatusRequestEntityTooLarge, "too_many_objects"
	case errors.Is(err, lfs.ErrInvalidOID),
		errors.Is(err, lfs.ErrInvalidRequest),
		errors.Is(err, lfs.ErrObjectTooLarge),
		errors.Is(err, lfs.ErrNoSupportedTransfer):
		return http.StatusUnprocessableEntity, "invalid"
	default:
		return http.StatusInternalServerError, "error"
	}
}

// lfsObjectResult labels one object's outcome for metrics.
func lfsObjectResult(obj lfs.ObjectResult) string {
	switch {
	case obj.Error != nil && obj.Error.Code == http.StatusNotFound:
		return "missing"
	case obj.Error != nil:
		return "error"
	case obj.Actions == nil:
		// No actions on an upload means the repository already holds it.
		return "present"
	default:
		return "new"
	}
}

// handleLFSVerify serves POST .../info/lfs/objects/verify.
//
// This is the only integrity check that runs on the daemon's own credentials,
// and the only step that records that a repository holds an object. A
// presigned upload the client never verifies leaves no membership behind.
func (d *daemon) handleLFSVerify(w http.ResponseWriter, r *http.Request) {
	ref, ok := d.lfsBegin(w, r, auth.Write)
	if !ok {
		return
	}

	var p lfs.Pointer
	d.capLFSBody(w, r)
	if !decodeLFSBody(w, r, &p) {
		return
	}

	status, err := d.lfs.store.Verify(r.Context(), ref.Path(), p.OID, p.Size)
	if err != nil {
		if errors.Is(err, lfs.ErrInvalidOID) {
			writeLFSError(w, http.StatusUnprocessableEntity, err.Error())
			metrics.ObserveLFSVerify("invalid")
			return
		}
		slog.Error("lfs verify failed", "repo", ref.Path(), "oid", p.OID, "err", err)
		writeLFSError(w, http.StatusInternalServerError, "cannot verify object")
		metrics.ObserveLFSVerify("error")
		return
	}

	metrics.ObserveLFSVerify(status.String())
	switch status {
	case lfs.VerifyOK:
		writeLFSJSON(w, http.StatusOK, lfs.Pointer{OID: p.OID, Size: p.Size})
	case lfs.VerifyMissing:
		writeLFSError(w, http.StatusNotFound, "object was not uploaded")
	default:
		writeLFSError(w, http.StatusUnprocessableEntity, "uploaded bytes do not match the object id")
	}
}

// lockRequest is the body of a lock create.
type lockRequest struct {
	Path string   `json:"path"`
	Ref  *lfs.Ref `json:"ref,omitempty"`
}

// lockResponse wraps a single lock, which is how the protocol returns both a
// creation and a conflict.
type lockResponse struct {
	Lock    lfs.Lock `json:"lock"`
	Message string   `json:"message,omitempty"`
}

// handleLFSLockCreate serves POST .../info/lfs/locks.
func (d *daemon) handleLFSLockCreate(w http.ResponseWriter, r *http.Request) {
	ref, ok := d.lfsBegin(w, r, auth.Write)
	if !ok {
		return
	}

	var req lockRequest
	d.capLFSBody(w, r)
	if !decodeLFSBody(w, r, &req) {
		return
	}
	if len(req.Path) > maxLockPathBytes {
		metrics.ObserveLFSLock("create", "invalid")
		writeLFSError(w, http.StatusUnprocessableEntity, "lock path is too long")
		return
	}
	if !d.lockRoomLeft(w, r, ref, req.Path) {
		return
	}

	lock, created, err := d.lfs.store.CreateLock(r.Context(), ref.Path(), req.Path, lfsOwner(r))
	if err != nil {
		d.writeLockError(w, ref, "create", err)
		return
	}
	if !created {
		metrics.ObserveLFSLock("create", "conflict")
		writeLFSJSON(w, http.StatusConflict, lockResponse{
			Lock:    lock,
			Message: "already created lock",
		})
		return
	}
	metrics.ObserveLFSLock("create", "ok")
	writeLFSJSON(w, http.StatusCreated, lockResponse{Lock: lock})
}

// lockRoomLeft reports whether repository ref may take another lock on path,
// writing the refusal itself when it may not. A lock that already holds path is
// always allowed through, so a repeated `git lfs lock` at the cap still gets the
// 409 that names the holder rather than a confusing refusal.
//
// This costs one extra read of the lock document per create, which is cheap
// next to how rare a lock is. It is a soft cap: two creates racing can both
// pass it. That is enough, because the point is to bound growth, not to make
// the thousandth lock exact.
func (d *daemon) lockRoomLeft(w http.ResponseWriter, r *http.Request, ref repofs.RepoRef, path string) bool {
	locks, _, err := d.lfs.store.ListLocks(r.Context(), ref.Path(), lfs.LockFilter{})
	if err != nil {
		d.writeLockError(w, ref, "create", err)
		return false
	}
	if len(locks) < maxLocksPerRepo {
		return true
	}
	if slices.ContainsFunc(locks, func(l lfs.Lock) bool { return l.Path == path }) {
		return true
	}

	slog.Warn("lfs lock refused: repository is at the lock cap",
		"repo", ref.Path(), "locks", len(locks), "cap", maxLocksPerRepo)
	metrics.ObserveLFSLock("create", "too_many")
	writeLFSError(w, http.StatusRequestEntityTooLarge,
		fmt.Sprintf("this repository already holds the most locks allowed (%d)", maxLocksPerRepo))
	return false
}

// lockListResponse is the body of a lock listing.
type lockListResponse struct {
	Locks      []lfs.Lock `json:"locks"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// handleLFSLockList serves GET .../info/lfs/locks.
func (d *daemon) handleLFSLockList(w http.ResponseWriter, r *http.Request) {
	ref, ok := d.lfsBegin(w, r, auth.Read)
	if !ok {
		return
	}

	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	locks, next, err := d.lfs.store.ListLocks(r.Context(), ref.Path(), lfs.LockFilter{
		Path:   q.Get("path"),
		ID:     q.Get("id"),
		Cursor: q.Get("cursor"),
		Limit:  limit,
	})
	if err != nil {
		d.writeLockError(w, ref, "list", err)
		return
	}

	metrics.ObserveLFSLock("list", "ok")
	writeLFSJSON(w, http.StatusOK, lockListResponse{Locks: orEmpty(locks), NextCursor: next})
}

// lockVerifyRequest is the body git-lfs sends before a push.
type lockVerifyRequest struct {
	Ref    *lfs.Ref `json:"ref,omitempty"`
	Cursor string   `json:"cursor,omitempty"`
	Limit  int      `json:"limit,omitempty"`
}

// lockVerifyResponse splits the locks by holder. git-lfs halts a push when a
// file it is pushing appears in "theirs".
type lockVerifyResponse struct {
	Ours       []lfs.Lock `json:"ours"`
	Theirs     []lfs.Lock `json:"theirs"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// handleLFSLockVerify serves POST .../info/lfs/locks/verify, which git-lfs
// calls on every push once this endpoint answers.
func (d *daemon) handleLFSLockVerify(w http.ResponseWriter, r *http.Request) {
	ref, ok := d.lfsBegin(w, r, auth.Read)
	if !ok {
		return
	}

	var req lockVerifyRequest
	// An empty body is legal here, so a decode failure is not fatal.
	d.capLFSBody(w, r)
	if !decodeOptionalLFSBody(w, r, &req) {
		return
	}

	locks, next, err := d.lfs.store.ListLocks(r.Context(), ref.Path(), lfs.LockFilter{
		Cursor: req.Cursor,
		Limit:  req.Limit,
	})
	if err != nil {
		d.writeLockError(w, ref, "verify", err)
		return
	}

	ours, theirs := lfs.SplitLocks(locks, lfsOwner(r))
	metrics.ObserveLFSLock("verify", "ok")
	writeLFSJSON(w, http.StatusOK, lockVerifyResponse{
		Ours:       orEmpty(ours),
		Theirs:     orEmpty(theirs),
		NextCursor: next,
	})
}

// unlockRequest is the body of an unlock.
type unlockRequest struct {
	Force bool `json:"force,omitempty"`
}

// handleLFSUnlock serves POST .../info/lfs/locks/{id}/unlock.
func (d *daemon) handleLFSUnlock(w http.ResponseWriter, r *http.Request) {
	ref, ok := d.lfsBegin(w, r, auth.Write)
	if !ok {
		return
	}

	var req unlockRequest
	// An empty body is legal here too: `force` is the only field.
	d.capLFSBody(w, r)
	if !decodeOptionalLFSBody(w, r, &req) {
		return
	}

	lock, err := d.lfs.store.DeleteLock(r.Context(), ref.Path(), r.PathValue("id"), lfsOwner(r), req.Force)
	if err != nil {
		d.writeLockError(w, ref, "unlock", err)
		return
	}

	metrics.ObserveLFSLock("unlock", "ok")
	writeLFSJSON(w, http.StatusOK, lockResponse{Lock: lock})
}

// writeLockError renders a lock failure in HTTP terms. action is the short
// metric verb ("create", "list", "verify", "unlock"), so a failure lands under
// the same label as the success it failed to be.
func (d *daemon) writeLockError(w http.ResponseWriter, ref repofs.RepoRef, action string, err error) {
	switch {
	case errors.Is(err, lfs.ErrLockNotFound):
		metrics.ObserveLFSLock(action, "not_found")
		writeLFSError(w, http.StatusNotFound, "no such lock")
	case errors.Is(err, lfs.ErrLockHeldByAnother):
		metrics.ObserveLFSLock(action, "forbidden")
		writeLFSError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, lfs.ErrInvalidRequest):
		metrics.ObserveLFSLock(action, "invalid")
		writeLFSError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, lfs.ErrLockContention):
		metrics.ObserveLFSLock(action, "contended")
		writeLFSError(w, http.StatusConflict, "the lock list is busy, retry")
	default:
		slog.Error("lfs lock request failed", "action", action, "repo", ref.Path(), "err", err)
		metrics.ObserveLFSLock(action, "error")
		writeLFSError(w, http.StatusInternalServerError, "cannot serve lock request")
	}
}

// orEmpty renders a nil slice as [] rather than null, which some LFS clients
// refuse to decode.
func orEmpty(locks []lfs.Lock) []lfs.Lock {
	if locks == nil {
		return []lfs.Lock{}
	}
	return locks
}

// lfsAuthenticateCommand is the SSH command git-lfs runs to discover where a
// repository's LFS HTTP API lives. An ssh:// remote cannot use LFS without it.
const lfsAuthenticateCommand = "git-lfs-authenticate"

// commandNotFoundExit is the exit status git-lfs reads as "this server has no
// LFS", falling back to guessing the HTTP endpoint.
//
// Any other non-zero exit is reported to the user as a hard failure, so a
// server without LFS must answer 127 here. Exiting 1 would break every `git
// lfs` operation over ssh:// instead of leaving it alone.
const commandNotFoundExit = 127

// authenticateResponse tells git-lfs where to send its batch requests.
type authenticateResponse struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header"`
	// ExpiresIn is how long the client may reuse this answer, in seconds.
	ExpiresIn int `json:"expires_in"`
}

// handleLFSAuthenticate serves `git-lfs-authenticate <path> <operation>`.
//
// It writes the JSON answer to the session, which is the command's stdout.
// Diagnostics go to stderr, where they do not corrupt the answer.
func (d *daemon) handleLFSAuthenticate(s ssh.Session, cmd []string) {
	// Without LFS, or without a public URL to point at, the honest answer is
	// that the command does not exist.
	if d.lfs == nil || d.lfs.externalURL == "" {
		fmt.Fprintf(s.Stderr(), "%s: command not found\n", lfsAuthenticateCommand)
		_ = s.Exit(commandNotFoundExit)
		return
	}

	if len(cmd) < 3 {
		fmt.Fprintf(s.Stderr(), "objgitd: usage: %s <path> <upload|download>\n", lfsAuthenticateCommand)
		_ = s.Exit(1)
		return
	}

	ref, err := repofs.Parse(cmd[1])
	if err != nil {
		fmt.Fprintf(s.Stderr(), "objgitd: %v\n", err)
		_ = s.Exit(1)
		return
	}

	op := auth.Read
	switch cmd[2] {
	case lfs.OperationDownload:
	case lfs.OperationUpload:
		op = auth.Write
	default:
		fmt.Fprintf(s.Stderr(), "objgitd: unknown operation %q\n", cmd[2])
		_ = s.Exit(1)
		return
	}

	var cred auth.Credential = auth.Anonymous{}
	if key := s.PublicKey(); key != nil {
		cred = auth.PublicKey{Key: key}
	}
	if d.authorize(s.Context(), auth.Request{
		Repo:      ref.Path(),
		Operation: op,
		Cred:      cred,
		Transport: "ssh",
	}) != auth.Allow {
		fmt.Fprintln(s.Stderr(), "objgitd: access denied")
		_ = s.Exit(1)
		return
	}

	slog.Info("serving lfs authenticate",
		"repo", ref.Path(),
		"operation", cmd[2],
		"remote", s.RemoteAddr().String(),
	)

	// The header map is empty because the HTTP side accepts anonymous
	// requests. A deployment with real authentication mints a token here.
	resp := authenticateResponse{
		Href: fmt.Sprintf("%s/%s/%s.git/info/lfs",
			strings.TrimSuffix(d.lfs.externalURL, "/"), ref.OrgID, ref.Name),
		Header:    map[string]string{},
		ExpiresIn: int(d.lfs.urlTTL(s.Context()).Seconds()),
	}
	if err := json.NewEncoder(s).Encode(resp); err != nil {
		slog.Error("writing lfs authenticate response", "repo", ref.Path(), "err", err)
		_ = s.Exit(1)
		return
	}
	_ = s.Exit(0)
}
