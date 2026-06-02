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
	"strings"
	"syscall"
	"time"

	"github.com/facebookgo/flagenv"
	"github.com/gliderlabs/ssh"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tigrisdata/storage-go"
	"golang.org/x/sync/errgroup"
	"github.com/tigrisdata/objgit/internal"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/s3fs"

	_ "github.com/joho/godotenv/autoload"
)

var (
	gitBind     = flag.String("git-bind", ":9418", "TCP address to listen on for the git:// protocol; empty disables it")
	httpBind    = flag.String("http-bind", ":8080", "TCP address to listen on for the git smart-HTTP protocol; empty disables it")
	sshBind     = flag.String("ssh-bind", "", "TCP address to listen on for the git-over-SSH protocol; empty disables it")
	metricsBind = flag.String("metrics-bind", ":9090", "TCP address to serve the Prometheus /metrics endpoint; empty disables it")
	bucket      = flag.String("bucket", "", "Tigris bucket that holds the git repositories")
	allowPush   = flag.Bool("allow-push", false, "allow unauthenticated git-receive-pack (push) requests")
	slogLevel   = flag.String("slog-level", "INFO", "log level (DEBUG, INFO, WARN, ERROR)")

	allowHooks  = flag.Bool("allow-hooks", false, "run .objgit/hooks/receive-pack in a sandbox after a successful push")
	hookTimeout = flag.Duration("hook-timeout", 60*time.Second, "wall-clock limit for a single hook run")

	s3CacheTTL     = flag.Duration("s3-cache-ttl", 60*time.Second, "how long a cached S3 directory listing answers Stat/Open before a re-list; 0 disables the listing cache")
	s3CacheRefresh = flag.Duration("s3-cache-refresh", 30*time.Second, "interval at which the listing-cache warmer re-fills hot prefixes; 0 disables the warmer")
	s3CacheIdle    = flag.Duration("s3-cache-idle", 10*time.Minute, "drop a prefix from the listing-cache warmer after this long without access")

	s3CacheRecursive  = flag.String("s3-cache-recursive-prefixes", "refs/", "comma-separated key prefixes served from one recursive subtree scan instead of a listing per folder; empty disables subtree caching")
	s3CacheMaxSubtree = flag.Int("s3-cache-max-subtree-keys", 50000, "abandon a recursive subtree scan past this many keys and fall back to per-folder listing")

	packCacheBytes = flag.Int64("pack-cache-bytes", 2<<30, "local disk budget for cached pack files (.pack/.idx/.rev), downloaded once and served from a temp file so a clone doesn't re-fetch pack objects per access; 0 disables the cache")
	packCacheDir   = flag.String("pack-cache-dir", "", "parent directory for the pack-file cache; empty uses the OS temp dir")
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

	// Route s3fs S3 round-trips into Prometheus before any filesystem use.
	s3fs.SetMetricsObserver(metrics.ObserveS3)

	rawClient, err := storage.New(ctx)
	if err != nil {
		slog.Error("can't create Tigris storage client", "err", err)
		os.Exit(1)
	}
	// Harden the client's HTTP path so stale keep-alive connections to Tigris
	// fail fast and retry on a fresh connection instead of hanging the request
	// forever (see internal/s3fs/resilient.go).
	client := s3fs.Harden(rawClient)

	var cache *s3fs.ListingCache
	var fsOpts []s3fs.Option
	if *s3CacheTTL > 0 {
		// Non-nil (even empty) so an empty flag explicitly disables subtree
		// caching rather than falling back to the {"refs/"} default.
		recursive := []string{}
		if *s3CacheRecursive != "" {
			recursive = strings.Split(*s3CacheRecursive, ",")
		}
		cache = s3fs.NewListingCache(s3fs.CacheConfig{
			TTL:               *s3CacheTTL,
			RefreshInterval:   *s3CacheRefresh,
			IdleTTL:           *s3CacheIdle,
			RecursivePrefixes: recursive,
			MaxSubtreeKeys:    *s3CacheMaxSubtree,
		}, client, *bucket, "/")
		fsOpts = append(fsOpts, s3fs.WithListingCache(cache))
		metrics.RegisterListingCache(func() metrics.ListingCacheStats {
			s := cache.Stats()
			return metrics.ListingCacheStats{
				Hits: s.Hits, Misses: s.Misses,
				ListingItems: s.ListingItems, SubtreeItems: s.SubtreeItems, HeadItems: s.HeadItems,
			}
		})
	}

	if *packCacheBytes != 0 {
		packCache, err := s3fs.NewPackCache(*packCacheDir, *packCacheBytes)
		if err != nil {
			slog.Error("can't create pack cache", "err", err)
			os.Exit(1)
		}
		defer packCache.Cleanup()
		fsOpts = append(fsOpts, s3fs.WithPackCache(packCache))
	}

	fsys, err := s3fs.NewS3FS(client, *bucket, fsOpts...)
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
		"metrics_bind", *metricsBind,
		"bucket", *bucket,
		"allow_push", *allowPush,
		"allow_hooks", *allowHooks,
		"s3_cache_ttl", *s3CacheTTL,
		"s3_cache_refresh", *s3CacheRefresh,
		"s3_cache_recursive_prefixes", *s3CacheRecursive,
	)

	g, gCtx := errgroup.WithContext(ctx)

	if cache != nil {
		g.Go(func() error {
			cache.RunWarmer(gCtx)
			return nil
		})
	}

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

	if err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
