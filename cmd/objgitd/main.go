package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/facebookgo/flagenv"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/tigrisdata/storage-go"
	"tangled.org/xeiaso.net/objgit/internal/s3fs"

	_ "github.com/joho/godotenv/autoload"
)

var (
	bind      = flag.String("bind", ":9418", "TCP address to listen on for the git:// protocol")
	bucket    = flag.String("bucket", "", "Tigris bucket that holds the git repositories")
	allowPush = flag.Bool("allow-push", false, "allow unauthenticated git-receive-pack (push) requests")
	slogLevel = flag.String("slog-level", "INFO", "log level (DEBUG, INFO, WARN, ERROR)")
)

func main() {
	flagenv.Parse()
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*slogLevel)); err != nil {
		slog.Error("invalid -slog-level", "value", *slogLevel, "err", err)
		os.Exit(1)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))

	if *bucket == "" {
		slog.Error("-bucket is required")
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

	ln, err := net.Listen("tcp", *bind)
	if err != nil {
		slog.Error("can't listen", "bind", *bind, "err", err)
		os.Exit(1)
	}

	slog.Info("objgitd listening",
		"bind", *bind,
		"bucket", *bucket,
		"allow_push", *allowPush,
	)

	if err := d.Serve(ctx, ln); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
