package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	ssh "github.com/gliderlabs/ssh"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/utils/ioutil"
	gossh "golang.org/x/crypto/ssh"
	"tangled.org/xeiaso.net/objgit/internal/auth"
	"tangled.org/xeiaso.net/objgit/internal/metrics"
)

const hostKeyPath = ".objgit/ssh_host_ed25519_key"

// loadOrCreateHostKey loads the server's ed25519 host key from fs, generating
// and persisting one on first use so the key survives restarts.
func loadOrCreateHostKey(fs billy.Filesystem) (gossh.Signer, error) {
	f, err := fs.Open(hostKeyPath)
	if err == nil {
		defer f.Close()
		pemBytes, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("reading ssh host key: %w", err)
		}
		signer, err := gossh.ParsePrivateKey(pemBytes)
		if err != nil {
			return nil, fmt.Errorf("parsing ssh host key: %w", err)
		}
		return signer, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("opening ssh host key: %w", err)
	}

	// Generate a new ed25519 key.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating ssh host key: %w", err)
	}

	block, err := gossh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, fmt.Errorf("marshaling ssh host key: %w", err)
	}
	pemBytes := pem.EncodeToMemory(block)

	// Persist the key. MkdirAll first in case the parent dir doesn't exist.
	if err := fs.MkdirAll(filepath.Dir(hostKeyPath), 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("creating ssh host key directory: %w", err)
	}

	wf, err := fs.Create(hostKeyPath)
	if err != nil {
		return nil, fmt.Errorf("creating ssh host key file: %w", err)
	}
	if _, err := wf.Write(pemBytes); err != nil {
		wf.Close()
		return nil, fmt.Errorf("writing ssh host key: %w", err)
	}
	if err := wf.Close(); err != nil {
		return nil, fmt.Errorf("closing ssh host key file: %w", err)
	}

	signer, err := gossh.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing generated ssh host key: %w", err)
	}

	slog.Info("created ssh host key", "path", hostKeyPath)
	return signer, nil
}

// gitServiceFor maps an SSH exec command to the go-git service it selects. The
// bool is false for anything that is not a git transport command.
func gitServiceFor(command string) (string, bool) {
	switch command {
	case "git-upload-pack":
		return transport.UploadPackService, true
	case "git-upload-archive":
		return transport.UploadArchiveService, true
	case "git-receive-pack":
		return transport.ReceivePackService, true
	default:
		return "", false
	}
}

// newSSHServer builds the git-over-SSH server. It accepts every public key at
// connect time and defers real authorization to handleSSH via daemon.authz.
func newSSHServer(d *daemon, addr string) (*ssh.Server, error) {
	signer, err := loadOrCreateHostKey(d.fs)
	if err != nil {
		return nil, fmt.Errorf("ssh host key: %w", err)
	}
	srv := &ssh.Server{
		Addr:    addr,
		Handler: d.handleSSH,
		// A non-nil handler is required to enable public-key auth at all;
		// gliderlabs stashes the connecting key for Session.PublicKey().
		PublicKeyHandler: func(ctx ssh.Context, key ssh.PublicKey) bool {
			return true
		},
	}
	srv.AddHostKey(signer)
	return srv, nil
}

// handleSSH services one git-over-SSH exec request: parse the command, authorize,
// resolve the repository, and hand the session to the matching go-git transport
// command. The session is the protocol stream (reader and writer).
func (d *daemon) handleSSH(s ssh.Session) {
	cmd := s.Command()
	if len(cmd) < 2 {
		fmt.Fprintln(s.Stderr(), "objgitd: this is a git SSH endpoint; interactive shells are not supported")
		_ = s.Exit(1)
		return
	}

	service, ok := gitServiceFor(cmd[0])
	if !ok {
		fmt.Fprintf(s.Stderr(), "objgitd: unsupported command %q\n", cmd[0])
		_ = s.Exit(1)
		return
	}

	// ssh://host/foo.git sends "/foo.git"; scp-style host:foo.git sends "foo.git".
	repoPath := strings.TrimPrefix(cmd[1], "/")

	var cred auth.Credential = auth.Anonymous{}
	if key := s.PublicKey(); key != nil {
		cred = auth.PublicKey{Key: key}
	}

	defer metrics.TrackInFlight("ssh")()
	start := time.Now()

	if d.authorize(s.Context(), auth.Request{
		Repo:      repoPath,
		Operation: operationFor(service),
		Cred:      cred,
		Transport: "ssh",
	}) != auth.Allow {
		metrics.ObserveGitOp("ssh", service, "denied", start)
		fmt.Fprintln(s.Stderr(), "objgitd: access denied")
		_ = s.Exit(1)
		return
	}

	slog.Info("serving ssh request",
		"service", service,
		"path", repoPath,
		"remote", s.RemoteAddr().String(),
	)

	status := "ok"
	if err := d.serveSSH(s, service, repoPath); err != nil {
		status = "error"
	}
	metrics.ObserveGitOp("ssh", service, status, start)
}

// serveSSH dispatches an authorized git-over-SSH request to the matching go-git
// transport command. It returns an error for metric classification; when a
// repository cannot be opened it also writes a client-facing message and sets a
// non-zero exit status, matching git's behavior. A mid-transfer error is logged
// (the exit status is left to the session default, as before).
func (d *daemon) serveSSH(s ssh.Session, service, repoPath string) error {
	// SSH is a persistent stream like git://: the transport commands call Close
	// between negotiation rounds, which would tear down the channel, so wrap the
	// session in no-op closers.
	r := io.NopCloser(s)
	w := ioutil.WriteNopCloser(s)
	ctx := s.Context()

	// Protocol v2 negotiation (GIT_PROTOCOL via s.Environ()) is intentionally not
	// forwarded yet; v0/v1 is sufficient. See plan.
	switch service {
	case transport.UploadPackService:
		st, err := d.loader.Load(&url.URL{Path: repoPath})
		if err != nil {
			fmt.Fprintf(s.Stderr(), "objgitd: repository %q not found\n", repoPath)
			_ = s.Exit(1)
			return fmt.Errorf("loading %q: %w", repoPath, err)
		}
		if err := transport.UploadPack(ctx, st, r, w, &transport.UploadPackRequest{}); err != nil {
			slog.Error("ssh upload-pack failed", "path", repoPath, "err", err)
			return err
		}

	case transport.UploadArchiveService:
		st, err := d.loader.Load(&url.URL{Path: repoPath})
		if err != nil {
			fmt.Fprintf(s.Stderr(), "objgitd: repository %q not found\n", repoPath)
			_ = s.Exit(1)
			return fmt.Errorf("loading %q: %w", repoPath, err)
		}
		if err := transport.UploadArchive(ctx, st, r, w, &transport.UploadArchiveRequest{}); err != nil {
			slog.Error("ssh upload-archive failed", "path", repoPath, "err", err)
			return err
		}

	case transport.ReceivePackService:
		st, err := d.loadOrInit(repoPath)
		if err != nil {
			fmt.Fprintf(s.Stderr(), "objgitd: cannot open repository %q\n", repoPath)
			_ = s.Exit(1)
			return fmt.Errorf("opening %q for push: %w", repoPath, err)
		}
		// streamingStorer hides PackfileWriter (the io.CopyBuffer-until-EOF path
		// deadlocks on a live socket); d.receivePack runs push hooks afterward.
		if err := d.receivePack(ctx, streamingStorer{Storer: st}, st, repoPath, r, w, &transport.ReceivePackRequest{}); err != nil {
			slog.Error("ssh receive-pack failed", "path", repoPath, "err", err)
			return err
		}
	}
	return nil
}
