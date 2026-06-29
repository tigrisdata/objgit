package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/repofs"
)

// ServeGitProtocol accepts connections on l until ctx is cancelled or Accept fails.
func (d *daemon) ServeGitProtocol(ctx context.Context, l net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("objgitd: accept: %w", err)
		}

		go func() {
			if err := d.handleGitProtocol(ctx, conn); err != nil {
				slog.Error("connection failed",
					"remote", conn.RemoteAddr().String(),
					"err", err,
				)
			}
		}()
	}
}

// handleGitProtocol services a single git:// connection: decode the request line, resolve
// the repository, and hand the socket to the matching server command.
func (d *daemon) handleGitProtocol(ctx context.Context, conn net.Conn) error {
	defer conn.Close()

	// A silent client must not be able to pin a goroutine forever.
	_ = conn.SetReadDeadline(time.Now().Add(handshakeTimeout))

	var req packp.GitProtoRequest
	if err := req.Decode(conn); err != nil {
		return fmt.Errorf("decoding git-proto-request: %w", err)
	}

	// The transfer that follows can take a while; drop the handshake deadline.
	_ = conn.SetReadDeadline(time.Time{})

	slog.Info("serving request",
		"service", req.RequestCommand,
		"path", req.Pathname,
		"remote", conn.RemoteAddr().String(),
	)

	ref, err := repofs.Parse(req.Pathname)
	if err != nil {
		_, _ = pktline.WriteError(conn, err)
		return fmt.Errorf("invalid repo path %q: %w", req.Pathname, err)
	}

	// ExtraParams carries e.g. "version=2"; transport.ProtocolVersion splits on ":".
	gitProtocol := strings.Join(req.ExtraParams, ":")

	// UploadPack/ReceivePack call r.Close() between negotiation rounds, so the
	// reader must be a no-op closer or the socket dies mid-conversation. The
	// writer is the raw conn: its final Close() ends the connection.
	r := io.NopCloser(conn)

	defer metrics.TrackInFlight("git")()
	start := time.Now()

	if d.authorize(ctx, auth.Request{
		Repo:      ref.Path(),
		Operation: operationFor(req.RequestCommand),
		Cred:      auth.Anonymous{},
		Transport: "git",
	}) != auth.Allow {
		metrics.ObserveGitOp("git", req.RequestCommand, "denied", start)
		_, _ = pktline.WriteError(conn, fmt.Errorf("access denied"))
		return fmt.Errorf("access denied for %q (%s)", req.Pathname, req.RequestCommand)
	}

	err = d.serveGit(ctx, conn, r, req, ref, gitProtocol)
	status := "ok"
	if err != nil {
		status = "error"
	}
	metrics.ObserveGitOp("git", req.RequestCommand, status, start)
	return err
}

// serveGit dispatches a parsed, authorized git:// request to the matching
// go-git transport command.
func (d *daemon) serveGit(ctx context.Context, conn net.Conn, r io.ReadCloser, req packp.GitProtoRequest, ref repofs.RepoRef, gitProtocol string) error {
	switch req.RequestCommand {
	case transport.UploadPackService:
		st, err := d.load(ctx, ref, repofs.Credential{})
		if err != nil {
			_, _ = pktline.WriteError(conn, fmt.Errorf("repository %q not found", req.Pathname))
			return fmt.Errorf("loading %q: %w", req.Pathname, err)
		}
		return transport.UploadPack(ctx, st, r, conn, &transport.UploadPackRequest{
			GitProtocol: gitProtocol,
		})

	case transport.UploadArchiveService:
		st, err := d.load(ctx, ref, repofs.Credential{})
		if err != nil {
			_, _ = pktline.WriteError(conn, fmt.Errorf("repository %q not found", req.Pathname))
			return fmt.Errorf("loading %q: %w", req.Pathname, err)
		}
		return transport.UploadArchive(ctx, st, r, conn, &transport.UploadArchiveRequest{})

	case transport.ReceivePackService:
		st, err := d.loadOrInit(ctx, ref, repofs.Credential{})
		if err != nil {
			_, _ = pktline.WriteError(conn, fmt.Errorf("cannot open repository %q", req.Pathname))
			return fmt.Errorf("opening %q for push: %w", req.Pathname, err)
		}
		return d.receivePack(ctx, st, ref.Path(), r, conn, &transport.ReceivePackRequest{
			GitProtocol: gitProtocol,
		})

	default:
		_, _ = pktline.WriteError(conn, fmt.Errorf("unsupported service %q", req.RequestCommand))
		return fmt.Errorf("unsupported service: %s", req.RequestCommand)
	}
}

// TestDaemonPushCreatesRepo reproduces "git push git://host/new.git" against a
// path that does not exist yet. The daemon must create the bare repository on
// demand and the result must clone back cleanly.
func TestDaemonPushCreatesRepo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	fs := memfs.New()
	d := &daemon{
		sysFS:    fs,
		resolver: repofs.BucketResolver{Base: fs},
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srvErr := make(chan error, 1)
	go func() { srvErr <- d.ServeGitProtocol(ctx, ln) }()

	remote := "git://" + ln.Addr().String() + "/acme/test.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "commit", "--allow-empty", "-m", "initial")

	// The repository does not exist yet; the push must create it.
	runGit(t, work, "push", remote, "main")

	if _, err := fs.Stat("/acme/test/config"); err != nil {
		t.Fatalf("expected bare repo to be created on push, but %q is missing: %v", "/acme/test/config", err)
	}

	// Round-trip: a clone must recover the pushed commit.
	dst := t.TempDir()
	runGit(t, dst, "clone", remote, "cloned")
	head := strings.TrimSpace(runGit(t, filepath.Join(dst, "cloned"), "rev-parse", "HEAD"))
	if head == "" {
		t.Fatal("cloned repository has no HEAD")
	}

	cancel()
	_ = ln.Close()
	select {
	case <-srvErr:
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}

// TestDaemonPushDisabled confirms that pushes are rejected (not silently
// creating repos) when allowPush is false.
func TestDaemonPushDisabled(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	fs := memfs.New()
	d := &daemon{
		sysFS:    fs,
		resolver: repofs.BucketResolver{Base: fs},
		authz:    auth.AllowAnonymous{AllowWrite: false},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = d.ServeGitProtocol(ctx, ln) }()

	remote := "git://" + ln.Addr().String() + "/acme/test.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	runGit(t, work, "commit", "--allow-empty", "-m", "initial")

	if out, err := tryGit(work, "push", remote, "main"); err == nil {
		t.Fatalf("expected push to be rejected when allowPush is false, got success:\n%s", out)
	}

	if _, err := fs.Stat("/acme/test/config"); err == nil {
		t.Fatal("repository must not be created when push is disabled")
	}
}

// TestDaemonPushKeepsPack verifies the receive-pack path stores the incoming
// pack whole (objects/pack/pack-*.pack + .idx) rather than exploding it into
// loose objects (objects/<2-hex>/...). git:// used to hide the PackfileWriter
// capability and fall back to loose objects (one S3 PUT + one Lstat per object);
// writePack now delimits the pack with a Scanner and feeds the PackfileWriter on
// every transport.
func TestDaemonPushKeepsPack(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	fs := memfs.New()
	d := &daemon{
		sysFS:    fs,
		resolver: repofs.BucketResolver{Base: fs},
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = d.ServeGitProtocol(ctx, ln) }()

	remote := "git://" + ln.Addr().String() + "/acme/test.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	writeFile(t, filepath.Join(work, "README.md"), "hello\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "initial") // blob + tree + commit
	runGit(t, work, "push", remote, "main")

	assertPackedRepo(t, fs, "/acme/test")
}

// assertPackedRepo fails unless repoPath holds at least one packfile and no loose
// object directories (a 2-hex-char dir under objects/ such as "ab/").
func assertPackedRepo(t *testing.T, fs billy.Filesystem, repoPath string) {
	t.Helper()

	packs, err := fs.ReadDir(repoPath + "/objects/pack")
	if err != nil {
		t.Fatalf("ReadDir objects/pack: %v", err)
	}
	var packCount int
	for _, e := range packs {
		if strings.HasSuffix(e.Name(), ".pack") {
			packCount++
		}
	}
	if packCount == 0 {
		t.Errorf("expected a packfile under %s/objects/pack, found none", repoPath)
	}

	entries, err := fs.ReadDir(repoPath + "/objects")
	if err != nil {
		t.Fatalf("ReadDir objects: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() && isLooseObjectDir(e.Name()) {
			t.Errorf("found loose-object dir objects/%s; pack should have been kept whole", e.Name())
		}
	}
}

// isLooseObjectDir reports whether name is a git loose-object fan-out directory
// (two lowercase hex characters), distinguishing it from "pack" and "info".
func isLooseObjectDir(name string) bool {
	if len(name) != 2 {
		return false
	}
	for _, c := range name {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := tryGit(dir, args...)
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return out
}

func tryGit(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_TERMINAL_PROMPT=0",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
