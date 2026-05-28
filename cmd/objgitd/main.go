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
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/tigrisdata/storage-go"
	"golang.org/x/sync/errgroup"
	"tangled.org/xeiaso.net/objgit/internal"
	"tangled.org/xeiaso.net/objgit/internal/s3fs"

	_ "github.com/joho/godotenv/autoload"
)

var (
	gitBind   = flag.String("git-bind", ":9418", "TCP address to listen on for the git:// protocol; empty disables it")
	httpBind  = flag.String("http-bind", ":8080", "TCP address to listen on for the git smart-HTTP protocol; empty disables it")
	bucket    = flag.String("bucket", "", "Tigris bucket that holds the git repositories")
	allowPush = flag.Bool("allow-push", false, "allow unauthenticated git-receive-pack (push) requests")
	slogLevel = flag.String("slog-level", "INFO", "log level (DEBUG, INFO, WARN, ERROR)")
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

	if *gitBind == "" && *httpBind == "" {
		slog.Error("at least one of -git-bind or -http-bind must be set")
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
		fs:        fsys,
		loader:    transport.NewFilesystemLoader(fsys, false),
		allowPush: *allowPush,
	}

	slog.Info("objgitd listening",
		"git_bind", *gitBind,
		"http_bind", *httpBind,
		"bucket", *bucket,
		"allow_push", *allowPush,
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

	if err := g.Wait(); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
