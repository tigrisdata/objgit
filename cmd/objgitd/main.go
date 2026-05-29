package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/facebookgo/flagenv"
	"github.com/gliderlabs/ssh"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/tigrisdata/storage-go"
	"golang.org/x/sync/errgroup"
	"tangled.org/xeiaso.net/objgit/internal"
	"tangled.org/xeiaso.net/objgit/internal/auth"
	"tangled.org/xeiaso.net/objgit/internal/s3fs"

	_ "github.com/joho/godotenv/autoload"
)

var (
	gitBind   = flag.String("git-bind", ":9418", "TCP address to listen on for the git:// protocol; empty disables it")
	httpBind  = flag.String("http-bind", ":8080", "TCP address to listen on for the git smart-HTTP protocol; empty disables it")
	sshBind   = flag.String("ssh-bind", "", "TCP address to listen on for the git-over-SSH protocol; empty disables it")
	bucket    = flag.String("bucket", "", "Tigris bucket that holds the git repositories")
	allowPush = flag.Bool("allow-push", false, "allow unauthenticated git-receive-pack (push) requests")
	slogLevel = flag.String("slog-level", "INFO", "log level (DEBUG, INFO, WARN, ERROR)")

	allowHooks  = flag.Bool("allow-hooks", false, "run .objgit/hooks/receive-pack in a sandbox after a successful push")
	hookTimeout = flag.Duration("hook-timeout", 60*time.Second, "wall-clock limit for a single hook run")
)

func main() {
	flagenv.Parse()
	flag.Parse()

	logger, err := internal.InitSlog(*slogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error initializing logging stack:", err)
		os.Exit(1)
	}
	slog.SetDefault(logger)

	if *bucket == "" {
		slog.Error("-bucket is required")
		os.Exit(1)
	}

	if *gitBind == "" && *httpBind == "" && *sshBind == "" {
		slog.Error("at least one of -git-bind, -http-bind, or -ssh-bind must be set")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	client, err := storage.New(ctx)
	if err != nil {
		slog.Error("can't create Tigris storage client", "err", err)
		os.Exit(1)
	}

	fsys, err := s3fs.NewS3FS(client, *bucket)
	if err != nil {
		slog.Error("can't create s3fs", "bucket", *bucket, "err", err)
		os.Exit(1)
	}

	d := &daemon{
		fs:          fsys,
		loader:      transport.NewFilesystemLoader(fsys, false),
		authz:       auth.AllowAnonymous{AllowWrite: *allowPush},
		allowHooks:  *allowHooks,
		hookTimeout: *hookTimeout,
	}

	slog.Info("objgitd listening",
		"git_bind", *gitBind,
		"http_bind", *httpBind,
		"ssh_bind", *sshBind,
		"bucket", *bucket,
		"allow_push", *allowPush,
		"allow_hooks", *allowHooks,
	)

	g, gCtx := errgroup.WithContext(ctx)

	if *gitBind != "" {
		ln, err := net.Listen("tcp", *gitBind)
		if err != nil {
			slog.Error("can't listen", "git_bind", *gitBind, "err", err)
			os.Exit(1)
		}
		g.Go(func() error { return d.Serve(gCtx, ln) })
	}

	if *httpBind != "" {
		ln, err := net.Listen("tcp", *httpBind)
		if err != nil {
			slog.Error("can't listen", "http_bind", *httpBind, "err", err)
			os.Exit(1)
		}
		srv := &http.Server{Handler: d}
		g.Go(func() error {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
		g.Go(func() error {
			<-gCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return srv.Shutdown(shutdownCtx)
		})
	}

	if *sshBind != "" {
		srv, err := newSSHServer(d, *sshBind)
		if err != nil {
			slog.Error("can't create ssh server", "ssh_bind", *sshBind, "err", err)
			os.Exit(1)
		}
		g.Go(func() error {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, ssh.ErrServerClosed) {
				return err
			}
			return nil
		})
		g.Go(func() error {
			<-gCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return srv.Shutdown(shutdownCtx)
		})
	}

	err = g.Wait()

	// Let in-flight async hooks finish before exiting, but don't hang forever.
	drained := make(chan struct{})
	go func() { d.hookWG.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		slog.Warn("shutdown: gave up waiting for in-flight hooks")
	}

	if err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
