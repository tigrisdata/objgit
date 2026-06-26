package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"time"

	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/utils/ioutil"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/repofs"
)

// httpHandler builds the smart-HTTP router. Repository paths are now a fixed
// {orgID}/{repoName} depth, so http.ServeMux wildcards express the routes
// directly (the captured repoName still carries a ".git" the ref parser strips).
// Anything that is not exactly two segments before the endpoint suffix never
// matches a pattern and falls through to ServeMux's 404.
func (d *daemon) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{orgID}/{repoName}/info/refs", d.handleInfoRefs)
	mux.HandleFunc("POST /{orgID}/{repoName}/git-upload-pack", func(w http.ResponseWriter, r *http.Request) {
		d.handleRPC(w, r, transport.UploadPackService)
	})
	mux.HandleFunc("POST /{orgID}/{repoName}/git-receive-pack", func(w http.ResponseWriter, r *http.Request) {
		d.handleRPC(w, r, transport.ReceivePackService)
	})
	return mux
}

// repoRef builds a RepoRef from the {orgID}/{repoName} path wildcards. It writes
// a 400 and returns ok=false when the pair is not a valid repository path.
func repoRef(w http.ResponseWriter, r *http.Request) (repofs.RepoRef, bool) {
	ref, err := repofs.Parse(path.Join(r.PathValue("orgID"), r.PathValue("repoName")))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return repofs.RepoRef{}, false
	}
	return ref, true
}

// handleInfoRefs serves the reference-discovery phase:
// GET /{orgID}/{repoName}/info/refs?service=git-(upload|receive)-pack.
func (d *daemon) handleInfoRefs(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	switch service {
	case transport.UploadPackService, transport.ReceivePackService:
	default:
		http.Error(w, fmt.Sprintf("unsupported service %q", service), http.StatusBadRequest)
		return
	}

	ref, ok := repoRef(w, r)
	if !ok {
		return
	}

	st, ok := d.resolve(w, r, service, ref)
	if !ok {
		return
	}

	slog.Info("serving smart-http advertisement",
		"service", service,
		"repo", ref.Path(),
		"remote", r.RemoteAddr,
	)

	w.Header().Set("Content-Type", "application/x-"+service+"-advertisement")
	w.Header().Set("Cache-Control", "no-cache")

	gitProtocol := r.Header.Get("Git-Protocol")
	out := ioutil.WriteNopCloser(w)

	// AdvertiseRefs+StatelessRPC emits the "# service=...\n" smart-reply prefix
	// followed by the ref advertisement, then returns without touching a reader.
	var err error
	switch service {
	case transport.UploadPackService:
		err = transport.UploadPack(r.Context(), st, nil, out, &transport.UploadPackRequest{
			AdvertiseRefs: true,
			StatelessRPC:  true,
			GitProtocol:   gitProtocol,
		})
	case transport.ReceivePackService:
		err = transport.ReceivePack(r.Context(), st, nil, out, &transport.ReceivePackRequest{
			AdvertiseRefs: true,
			StatelessRPC:  true,
			GitProtocol:   gitProtocol,
		})
	}
	if err != nil {
		slog.Error("smart-http advertisement failed", "service", service, "repo", ref.Path(), "err", err)
	}
}

// handleRPC serves a stateless negotiation round:
// POST /{orgID}/{repoName}/git-(upload|receive)-pack.
func (d *daemon) handleRPC(w http.ResponseWriter, r *http.Request, service string) {
	defer metrics.TrackInFlight("http")()
	start := time.Now()

	ref, ok := repoRef(w, r)
	if !ok {
		metrics.ObserveGitOp("http", service, "error", start)
		return
	}

	st, ok := d.resolve(w, r, service, ref)
	if !ok {
		// resolve has already written the HTTP error; a denied authorization is
		// recorded by d.authorize in auth_requests_total. Count the failed op
		// here so request totals stay consistent across transports.
		metrics.ObserveGitOp("http", service, "error", start)
		return
	}

	body := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "invalid gzip body", http.StatusBadRequest)
			return
		}
		defer gz.Close()
		body = gz
	}

	slog.Info("serving smart-http rpc",
		"service", service,
		"repo", ref.Path(),
		"remote", r.RemoteAddr,
	)

	w.Header().Set("Content-Type", "application/x-"+service+"-result")
	w.Header().Set("Cache-Control", "no-cache")

	// The server commands call Close between negotiation steps; the body and the
	// response writer must survive that, so both are wrapped as no-op closers.
	in := io.NopCloser(body)
	out := ioutil.WriteNopCloser(w)
	// receive-pack streams hook output over the sideband; net/http buffers
	// writes, so flush after each one to deliver "remote:" lines live.
	if service == transport.ReceivePackService {
		if fl, ok := w.(http.Flusher); ok {
			out = ioutil.WriteNopCloser(flushWriter{w: w, f: fl})
		}
	}
	gitProtocol := r.Header.Get("Git-Protocol")

	var err error
	switch service {
	case transport.UploadPackService:
		err = transport.UploadPack(r.Context(), st, in, out, &transport.UploadPackRequest{
			StatelessRPC: true,
			GitProtocol:  gitProtocol,
		})
	case transport.ReceivePackService:
		err = d.receivePack(r.Context(), st, ref.Path(), in, out, &transport.ReceivePackRequest{
			StatelessRPC: true,
			GitProtocol:  gitProtocol,
		})
	}
	status := "ok"
	if err != nil {
		// The status line is already sent, so this can only be logged.
		slog.Error("smart-http rpc failed", "service", service, "repo", ref.Path(), "err", err)
		status = "error"
	}
	metrics.ObserveGitOp("http", service, status, start)
}

// resolve loads the storer for an HTTP request, authorizing via the daemon's
// Authorizer before touching the repository. It writes an HTTP error and
// returns ok=false when the request cannot proceed. The Basic-auth credential
// is threaded into the filesystem resolver so a backend can route per caller.
func (d *daemon) resolve(w http.ResponseWriter, r *http.Request, service string, ref repofs.RepoRef) (storage.Storer, bool) {
	authCred, fsCred := credFromRequest(r)
	op := operationFor(service)
	user, _, hasBasicAuth := r.BasicAuth()
	decision := d.authorize(r.Context(), auth.Request{
		Repo:      ref.Path(),
		Operation: op,
		Cred:      authCred,
		Transport: "http",
	})
	slog.Debug("authorizing smart-http request",
		"repo", ref.Path(),
		"service", service,
		"operation", op,
		"decision", decision,
		"basic_auth", hasBasicAuth,
		"user", user,
		"remote", r.RemoteAddr,
	)
	switch decision {
	case auth.Allow:
		// authorized; fall through to repo resolution
	case auth.Unauthenticated:
		slog.Info("smart-http request needs authentication",
			"repo", ref.Path(), "service", service, "operation", op,
			"basic_auth", hasBasicAuth, "remote", r.RemoteAddr,
		)
		w.Header().Set("WWW-Authenticate", `Basic realm="objgit"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return nil, false
	default: // auth.Deny
		// The most common cause is a push (receive-pack -> Write) while the
		// authorizer's write gate is closed (e.g. -allow-push unset).
		slog.Warn("smart-http request denied by authorizer",
			"repo", ref.Path(), "service", service, "operation", op,
			"basic_auth", hasBasicAuth, "user", user, "remote", r.RemoteAddr,
		)
		http.Error(w, "access denied", http.StatusForbidden)
		return nil, false
	}

	if service == transport.ReceivePackService {
		st, err := d.loadOrInit(r.Context(), ref, fsCred)
		if err != nil {
			return nil, writeResolveError(w, ref, "opening repository for push", err)
		}
		return st, true
	}

	st, err := d.load(r.Context(), ref, fsCred)
	if err != nil {
		return nil, writeResolveError(w, ref, "loading repository", err)
	}
	return st, true
}

// writeResolveError renders a repository-resolution error in HTTP terms: a
// missing credential is a 401 challenge, a missing repository is a 404, and
// anything else is a logged 500. It always returns false (resolution failed).
func writeResolveError(w http.ResponseWriter, ref repofs.RepoRef, action string, err error) bool {
	switch {
	case errors.Is(err, repofs.ErrUnauthenticated):
		w.Header().Set("WWW-Authenticate", `Basic realm="objgit"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
	case errors.Is(err, transport.ErrRepositoryNotFound):
		http.Error(w, "repository not found", http.StatusNotFound)
	default:
		slog.Error(action, "repo", ref.Path(), "err", err)
		http.Error(w, "cannot open repository", http.StatusInternalServerError)
	}
	return false
}

// flushWriter flushes the underlying http.ResponseWriter after every write so
// sideband progress (hook output) reaches the client incrementally instead of
// being buffered until the handler returns.
type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func (fw flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	fw.f.Flush()
	return n, err
}

// credFromRequest extracts the credential from an HTTP request: HTTP Basic if
// present, otherwise anonymous. It returns both the auth.Credential for the
// Authorizer and the repofs.Credential for the filesystem resolver. Neither is
// validated — the Authorizer owns the user store and the Resolver decides what
// to do with the credential.
func credFromRequest(r *http.Request) (auth.Credential, repofs.Credential) {
	if u, p, ok := r.BasicAuth(); ok {
		return auth.BasicAuth{Username: u, Password: p}, repofs.Credential{Username: u, Password: p}
	}
	return auth.Anonymous{}, repofs.Credential{}
}
