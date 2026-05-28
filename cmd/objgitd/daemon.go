package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6"
	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
)

// handshakeTimeout bounds how long a client has to send its git-proto-request.
// It is cleared once the (possibly long) transfer begins.
const handshakeTimeout = 30 * time.Second

// daemon serves the git:// (TCP) protocol out of a billy filesystem.
type daemon struct {
	fs        billy.Filesystem
	loader    transport.Loader
	allowPush bool
}

// Serve accepts connections on l until ctx is cancelled or Accept fails.
func (d *daemon) Serve(ctx context.Context, l net.Listener) error {
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
			if err := d.handle(ctx, conn); err != nil {
				slog.Error("connection failed",
					"remote", conn.RemoteAddr().String(),
					"err", err,
				)
			}
		}()
	}
}

// handle services a single git:// connection: decode the request line, resolve
// the repository, and hand the socket to the matching server command.
func (d *daemon) handle(ctx context.Context, conn net.Conn) error {
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

	// ExtraParams carries e.g. "version=2"; transport.ProtocolVersion splits on ":".
	gitProtocol := strings.Join(req.ExtraParams, ":")

	// UploadPack/ReceivePack call r.Close() between negotiation rounds, so the
	// reader must be a no-op closer or the socket dies mid-conversation. The
	// writer is the raw conn: its final Close() ends the connection.
	r := io.NopCloser(conn)

	switch req.RequestCommand {
	case transport.UploadPackService:
		st, err := d.loader.Load(&url.URL{Path: req.Pathname})
		if err != nil {
			_, _ = pktline.WriteError(conn, fmt.Errorf("repository %q not found", req.Pathname))
			return fmt.Errorf("loading %q: %w", req.Pathname, err)
		}
		return transport.UploadPack(ctx, st, r, conn, &transport.UploadPackRequest{
			GitProtocol: gitProtocol,
		})

	case transport.UploadArchiveService:
		st, err := d.loader.Load(&url.URL{Path: req.Pathname})
		if err != nil {
			_, _ = pktline.WriteError(conn, fmt.Errorf("repository %q not found", req.Pathname))
			return fmt.Errorf("loading %q: %w", req.Pathname, err)
		}
		return transport.UploadArchive(ctx, st, r, conn, &transport.UploadArchiveRequest{})

	case transport.ReceivePackService:
		if !d.allowPush {
			_, _ = pktline.WriteError(conn, fmt.Errorf("push is disabled on this server"))
			return fmt.Errorf("push rejected for %q", req.Pathname)
		}
		st, err := d.loadOrInit(req.Pathname)
		if err != nil {
			_, _ = pktline.WriteError(conn, fmt.Errorf("cannot open repository %q", req.Pathname))
			return fmt.Errorf("opening %q for push: %w", req.Pathname, err)
		}
		return transport.ReceivePack(ctx, st, r, conn, &transport.ReceivePackRequest{
			GitProtocol: gitProtocol,
		})

	default:
		_, _ = pktline.WriteError(conn, fmt.Errorf("unsupported service %q", req.RequestCommand))
		return fmt.Errorf("unsupported service: %s", req.RequestCommand)
	}
}

// loadOrInit returns the storer for repoPath, creating an empty bare repository
// on demand. Git's daemon never auto-creates; objgitd does, so a first push to
// a new path just works.
func (d *daemon) loadOrInit(repoPath string) (storage.Storer, error) {
	st, err := d.loader.Load(&url.URL{Path: repoPath})
	if err == nil {
		return st, nil
	}
	if !errors.Is(err, transport.ErrRepositoryNotFound) {
		return nil, err
	}

	fs, err := d.fs.Chroot(repoPath)
	if err != nil {
		return nil, fmt.Errorf("chroot %q: %w", repoPath, err)
	}

	st = filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
	if _, err := git.Init(st, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main"))); err != nil {
		return nil, fmt.Errorf("init bare repo: %w", err)
	}

	slog.Info("created repository", "path", repoPath)
	return st, nil
}
