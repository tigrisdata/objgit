package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/facebookgo/flagenv"
	"github.com/gliderlabs/ssh"
	"github.com/go-git/go-git/v6/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tigrisdata/objgit"
	"github.com/tigrisdata/objgit/internal"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/repofs"
	"github.com/tigrisdata/objgit/internal/s3fs"
	"github.com/tigrisdata/objgit/internal/storage/tigris"
	tstorage "github.com/tigrisdata/storage-go"
	"golang.org/x/sync/errgroup"

	_ "github.com/joho/godotenv/autoload"
)

var (
	httpBind    = flag.String("http-bind", ":8080", "TCP address to listen on for the git smart-HTTP protocol; empty disables it")
	sshBind     = flag.String("ssh-bind", "", "TCP address to listen on for the git-over-SSH protocol; empty disables it")
	metricsBind = flag.String("metrics-bind", ":9090", "TCP address to serve the Prometheus /metrics endpoint; empty disables it")
	bucket      = flag.String("bucket", "", "Tigris bucket holding daemon system state and every repository (one bucket, key-prefixed per repo)")
	allowPush   = flag.Bool("allow-push", false, "allow unauthenticated git-receive-pack (push) requests")
	slogLevel   = flag.String("slog-level", "INFO", "log level (DEBUG, INFO, WARN, ERROR)")

	allowHooks  = flag.Bool("allow-hooks", false, "run .objgit/hooks/receive-pack in a sandbox after a successful push")
	hookTimeout = flag.Duration("hook-timeout", 60*time.Second, "wall-clock limit for a single hook run")

	packCacheDir   = flag.String("pack-cache-dir", "", "parent directory for the local pack cache; empty uses the OS temp directory")
	packCacheBytes = flag.Int64("pack-cache-bytes", 2<<30, "disk budget for the local pack cache, least-recently-used eviction; 0 disables caching")

	packCompression = flag.Bool("pack-compression", true, "store zstd-compressed payloads in newly written pack containers; reading compressed containers is always enabled, so this is safe to turn off for one release before a rollback")
)

// tigrisBase adapts *tigris.Storer to repofs.Base: Storer.Scoped returns the
// concrete *tigris.Storer (useful for chaining/tests), but the Base interface
// needs the abstract storage.Storer go-git works with.
type tigrisBase struct{ s *tigris.Storer }

func (b tigrisBase) Scoped(prefix string) storage.Storer { return b.s.Scoped(prefix) }

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

	if *httpBind == "" && *sshBind == "" {
		slog.Error("at least one of -http-bind or -ssh-bind must be set")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Route s3fs S3 round-trips into Prometheus before any filesystem use.
	s3fs.SetMetricsObserver(metrics.ObserveS3)

	rawClient, err := tstorage.New(ctx)
	if err != nil {
		slog.Error("can't create Tigris storage client", "err", err)
		os.Exit(1)
	}
	// Harden the client's HTTP path so stale keep-alive connections to Tigris
	// fail fast and retry on a fresh connection instead of hanging the request
	// forever (see internal/s3fs/resilient.go). Only sysFS (the SSH host key)
	// uses this client; internal/storage/tigris dials its own.
	client := s3fs.Harden(rawClient)

	fsys, err := s3fs.NewS3FS(client, *bucket)
	if err != nil {
		slog.Error("can't create s3fs", "bucket", *bucket, "err", err)
		os.Exit(1)
	}

	// One pack cache for the whole process, shared by every repository's Storer
	// (its keys are content hashes, so sharing is deduplication). Without it,
	// each request that bulk-fetches a pack throws the copy away when it ends.
	storerOpts := []tigris.Option{
		tigris.WithObserver(metrics.ObserveS3),
		tigris.WithPayloadObserver(metrics.ObservePackPayload),
		tigris.WithPackCompression(*packCompression),
	}
	var packCache *tigris.PackCache
	if *packCacheBytes > 0 {
		packCache, err = tigris.NewPackCache(*packCacheDir, *packCacheBytes)
		if err != nil {
			slog.Error("can't create pack cache", "pack_cache_dir", *packCacheDir, "err", err)
			os.Exit(1)
		}
		storerOpts = append(storerOpts, tigris.WithPackCache(packCache))
	}

	// Every repository lives in the same bucket as daemon system state, keyed
	// by an "orgID/name" prefix (repofs.BucketResolver via tigrisBase.Scoped).
	base, err := tigris.New(ctx, *bucket, storerOpts...)
	if err != nil {
		slog.Error("can't create tigris storer", "bucket", *bucket, "err", err)
		os.Exit(1)
	}

	d := &daemon{
		sysFS:       fsys,
		resolver:    repofs.BucketResolver{Base: tigrisBase{s: base}},
		authz:       auth.AllowAnonymous{AllowWrite: *allowPush},
		allowHooks:  *allowHooks,
		hookTimeout: *hookTimeout,
	}

	slog.Info("objgitd listening",
		"version", objgit.Version,
		"http_bind", *httpBind,
		"ssh_bind", *sshBind,
		"metrics_bind", *metricsBind,
		"bucket", *bucket,
		"allow_push", *allowPush,
		"allow_hooks", *allowHooks,
		"pack_cache_bytes", *packCacheBytes,
	)

	g, gCtx := errgroup.WithContext(ctx)

	if *metricsBind != "" {
		ln, err := net.Listen("tcp", *metricsBind)
		if err != nil {
			slog.Error("can't listen", "metrics_bind", *metricsBind, "err", err)
			os.Exit(1)
		}
		runtime.SetBlockProfileRate(100)
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
		srv := &http.Server{Handler: mux}
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

	if *httpBind != "" {
		ln, err := net.Listen("tcp", *httpBind)
		if err != nil {
			slog.Error("can't listen", "http_bind", *httpBind, "err", err)
			os.Exit(1)
		}
		srv := &http.Server{Handler: d.httpHandler()}
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

	// Explicit, not deferred: the exit below skips defers. Descriptors already
	// handed out keep working, so this is safe even mid-request.
	if cerr := packCache.Cleanup(); cerr != nil {
		slog.Warn("can't remove the pack cache directory", "err", cerr)
	}

	if err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
