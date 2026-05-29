package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/utils/ioutil"
	"tangled.org/xeiaso.net/objgit/internal/auth"
	"tangled.org/xeiaso.net/objgit/internal/metrics"
)

// ServeHTTP speaks the git smart-HTTP protocol. It dispatches on the URL suffix
// the way git-http-backend does: repository paths are variable-depth (e.g.
// /foo/bar.git) and precede a fixed endpoint suffix, which http.ServeMux's
// wildcards cannot express.
func (d *daemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(p, "/info/refs"):
		d.handleInfoRefs(w, r, strings.TrimSuffix(p, "/info/refs"))
	case r.Method == http.MethodPost && strings.HasSuffix(p, "/git-upload-pack"):
		d.handleRPC(w, r, transport.UploadPackService, strings.TrimSuffix(p, "/git-upload-pack"))
	case r.Method == http.MethodPost && strings.HasSuffix(p, "/git-receive-pack"):
		d.handleRPC(w, r, transport.ReceivePackService, strings.TrimSuffix(p, "/git-receive-pack"))
	default:
		http.NotFound(w, r)
	}
}

// handleInfoRefs serves the reference-discovery phase:
// GET /{repo}/info/refs?service=git-(upload|receive)-pack.
func (d *daemon) handleInfoRefs(w http.ResponseWriter, r *http.Request, repoPath string) {
	service := r.URL.Query().Get("service")
	switch service {
	case transport.UploadPackService, transport.ReceivePackService:
	default:
		http.Error(w, fmt.Sprintf("unsupported service %q", service), http.StatusBadRequest)
		return
	}

	st, ok := d.resolve(w, r, service, repoPath)
	if !ok {
		return
	}

	slog.Info("serving smart-http advertisement",
		"service", service,
		"path", repoPath,
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
		slog.Error("smart-http advertisement failed", "service", service, "path", repoPath, "err", err)
	}
}

// handleRPC serves a stateless negotiation round:
// POST /{repo}/git-(upload|receive)-pack.
func (d *daemon) handleRPC(w http.ResponseWriter, r *http.Request, service, repoPath string) {
	defer metrics.TrackInFlight("http")()
	start := time.Now()

	st, ok := d.resolve(w, r, service, repoPath)
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
		"path", repoPath,
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
		err = d.receivePack(r.Context(), st, st, repoPath, in, out, &transport.ReceivePackRequest{
			StatelessRPC: true,
			GitProtocol:  gitProtocol,
		})
	}
	status := "ok"
	if err != nil {
		// The status line is already sent, so this can only be logged.
		slog.Error("smart-http rpc failed", "service", service, "path", repoPath, "err", err)
		status = "error"
	}
	metrics.ObserveGitOp("http", service, status, start)
}

// resolve loads the storer for an HTTP request, authorizing via the daemon's
// Authorizer before touching the repository. It writes an HTTP error and
// returns ok=false when the request cannot proceed.
func (d *daemon) resolve(w http.ResponseWriter, r *http.Request, service, repoPath string) (storage.Storer, bool) {
	switch d.authorize(r.Context(), auth.Request{
		Repo:      repoPath,
		Operation: operationFor(service),
		Cred:      credFromRequest(r),
		Transport: "http",
	}) {
	case auth.Allow:
		// authorized; fall through to repo resolution
	case auth.Unauthenticated:
		w.Header().Set("WWW-Authenticate", `Basic realm="objgit"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return nil, false
	default: // auth.Deny
		http.Error(w, "access denied", http.StatusForbidden)
		return nil, false
	}

	if service == transport.ReceivePackService {
		st, err := d.loadOrInit(repoPath)
		if err != nil {
			slog.Error("opening repository for push", "path", repoPath, "err", err)
			http.Error(w, "cannot open repository", http.StatusInternalServerError)
			return nil, false
		}
		return st, true
	}

	st, err := d.loader.Load(&url.URL{Path: repoPath})
	if err != nil {
		if errors.Is(err, transport.ErrRepositoryNotFound) {
			http.Error(w, "repository not found", http.StatusNotFound)
			return nil, false
		}
		slog.Error("loading repository", "path", repoPath, "err", err)
		http.Error(w, "cannot open repository", http.StatusInternalServerError)
		return nil, false
	}
	return st, true
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

// credFromRequest extracts an auth credential from an HTTP request: HTTP Basic
// if present, otherwise anonymous. It does not validate — the Authorizer owns
// the user store.
func credFromRequest(r *http.Request) auth.Credential {
	if u, p, ok := r.BasicAuth(); ok {
		return auth.BasicAuth{Username: u, Password: p}
	}
	return auth.Anonymous{}
}
