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
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/facebookgo/flagenv"
	"github.com/gliderlabs/ssh"
	"github.com/go-git/go-git/v6/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tigrisdata/objgit"
	"github.com/tigrisdata/objgit/internal"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/lfs"
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
	packedRefs      = flag.Bool("packed-refs", true, "write every ref into one packed-refs object under a compare-and-swap, instead of one object per ref; reading packed refs is always enabled, so this is safe to turn off for one release before a rollback")

	encoderConcurrency = flag.Int("encoder-concurrency", 4, "how many concurrent zstd EncodeAll calls the process-wide pack encoder serves; each state retains a match-history buffer, so this caps encoder heap that would otherwise scale with GOMAXPROCS. Set with the concurrent push cap in mind")

	maxConcurrentPushes = flag.Int("max-concurrent-pushes", 4, "pushes allowed to unpack a packfile at the same time; each one costs roughly 400 MiB of resident set for a large repository, so this is what bounds memory under concurrent pushes; 0 disables the limit")
	pushQueueTimeout    = flag.Duration("push-queue-timeout", 2*time.Minute, "how long a push waits for a slot before it fails")

	allowLFS      = flag.Bool("allow-lfs", false, "serve the Git LFS API; object bytes move directly between the client and the bucket over presigned URLs, never through this daemon")
	allowLFSLocks = flag.Bool("allow-lfs-locks", true, "serve the Git LFS file locking API; without a user store every anonymous caller is the same owner, so locking is advisory only")
	externalURL   = flag.String("external-url", "", "public base URL of this server, such as https://git.example.com; required for Git LFS over ssh:// because git-lfs-authenticate has to name the HTTP API")
	lfsURLTTL     = flag.Duration("lfs-url-ttl", 15*time.Minute, "how long a presigned Git LFS transfer URL stays valid")
	lfsMaxSize    = flag.Int64("lfs-max-size", 5<<30, "largest Git LFS object accepted, in bytes; one presigned PUT caps at 5 GiB")
	lfsMaxBatch   = flag.Int("lfs-max-batch", 500, "most objects accepted in one Git LFS batch request")
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

	if *allowLFS {
		if *httpBind == "" {
			// Every LFS transfer is negotiated over HTTP, even for an ssh://
			// remote: git-lfs-authenticate exists only to name that endpoint.
			slog.Error("-allow-lfs needs -http-bind; the Git LFS API is HTTP even for ssh:// remotes")
			os.Exit(1)
		}
		if err := checkExternalURL(*externalURL); err != nil {
			slog.Error("-external-url is not usable", "err", err)
			os.Exit(1)
		}
		if *externalURL == "" && *sshBind != "" {
			slog.Error("-allow-lfs with -ssh-bind needs -external-url; git-lfs-authenticate has to name a public HTTP URL")
			os.Exit(1)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Route s3fs S3 round-trips into Prometheus before any filesystem use.
	s3fs.SetMetricsObserver(metrics.ObserveS3)

	// Cap the process-wide pack encoder's concurrency before the first encode.
	// Left at its GOMAXPROCS default, each internal state retains a
	// match-history buffer and the retained floor scales with core count.
	tigris.SetEncoderConcurrency(*encoderConcurrency)

	rawClient, err := tstorage.New(ctx)
	if err != nil {
		slog.Error("can't create Tigris storage client", "err", err)
		os.Exit(1)
	}
	// Harden the client's HTTP path so stale keep-alive connections to Tigris
	// fail fast and retry on a fresh connection instead of hanging the request
	// forever (see internal/s3fs/resilient.go). sysFS (the SSH host key) and the
	// LFS store use this client; internal/storage/tigris dials its own.
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
		tigris.WithRefCASObserver(metrics.ObserveRefCASRetry),
		tigris.WithPackCompression(*packCompression),
		tigris.WithPackedRefs(*packedRefs),
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
		pushes:      newPushLimiter(*maxConcurrentPushes, *pushQueueTimeout),
	}

	if *allowLFS {
		// The store talks to the bucket, so it gets the hardened client like
		// every other request path. rawClient survives only for the presigner:
		// s3.NewPresignClient needs the concrete *s3.Client, and hardening buys
		// nothing there because presigning makes no network call.
		d.lfs = &lfsService{
			store: lfs.NewStore(client,
				lfs.NewPresigner(rawClient.Client, *bucket), *bucket),
			ttl:         lfsURLTTLFunc(rawClient, *lfsURLTTL),
			maxSize:     *lfsMaxSize,
			maxBatch:    *lfsMaxBatch,
			externalURL: strings.TrimSuffix(*externalURL, "/"),
			allowLocks:  *allowLFSLocks,
		}
	}

	slog.Info("objgitd listening",
		"version", objgit.Version,
		"http_bind", *httpBind,
		"ssh_bind", *sshBind,
		"metrics_bind", *metricsBind,
		"bucket", *bucket,
		"allow_push", *allowPush,
		"allow_hooks", *allowHooks,
		"allow_lfs", *allowLFS,
		"external_url", *externalURL,
		"pack_cache_bytes", *packCacheBytes,
		"max_concurrent_pushes", *maxConcurrentPushes,
		"push_queue_timeout", *pushQueueTimeout,
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

// checkExternalURL rejects an -external-url that cannot be used to build an
// absolute LFS endpoint. An empty value is allowed: the HTTP side then derives
// the base from the request, and only SSH needs the flag.
func checkExternalURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parsing %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%q needs an http or https scheme", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("%q names no host", raw)
	}
	return nil
}

// lfsURLTTLFunc builds the per-request presigned-URL lifetime.
//
// The answer is computed per request and never frozen at startup. A temporary
// credential is refreshed by the SDK while the process runs, so a daemon that
// happened to start two minutes before an expiry would otherwise clamp every
// URL it ever signs to the floor below, long after the credential behind it was
// replaced. Retrieve reads the SDK's credential cache and only reaches the
// provider when the credential has actually expired, so this is cheap enough to
// run on every LFS request.
//
// The returned function logs a clamp when it starts and when it stops, and not
// on every request: git-lfs asks for a batch on every fetch and every push.
func lfsURLTTLFunc(client *tstorage.Client, want time.Duration) func(context.Context) time.Duration {
	var clamped, unreadable atomic.Bool
	return func(ctx context.Context) time.Duration {
		got, err := lfsTTL(ctx, client, want)
		switch {
		case err != nil && !unreadable.Swap(true):
			slog.Warn("cannot inspect credentials for lfs url lifetime; using the configured value",
				"lfs_url_ttl", want, "err", err)
		case err == nil:
			unreadable.Store(false)
		}
		switch {
		case got < want && !clamped.Swap(true):
			slog.Warn("credentials expire before the configured lfs url lifetime; clamping",
				"lfs_url_ttl", want, "clamped_to", got)
		case got >= want && clamped.Swap(false):
			slog.Info("credentials now outlive the configured lfs url lifetime",
				"lfs_url_ttl", want)
		}
		return got
	}
}

// lfsTTL clamps the presigned-URL lifetime to what the credentials can outlive.
//
// A presigned URL carries the session token of the credential that signed it,
// so a temporary credential (SSO, IMDS, assume-role) invalidates every URL it
// signed the moment it expires, whatever -lfs-url-ttl says. Static Tigris
// keypairs do not expire and keep the configured value.
//
// An error is reported rather than logged, because the caller runs this on every
// request and decides what is worth saying twice.
func lfsTTL(ctx context.Context, client *tstorage.Client, want time.Duration) (time.Duration, error) {
	creds, err := client.Options().Credentials.Retrieve(ctx)
	if err != nil {
		return want, err
	}
	if !creds.CanExpire {
		return want, nil
	}

	// Leave a margin so a URL minted just before the clamp still outlives the
	// round trip that uses it.
	const margin = time.Minute
	// A floor keeps a short-lived credential from producing a zero or negative
	// expiry, which would sign URLs that are dead on arrival. Credentials this
	// close to expiry are refreshed by the SDK well before the floor runs out.
	const floor = time.Minute

	left := time.Until(creds.Expires) - margin
	if left >= want {
		return want, nil
	}
	if left < floor {
		left = floor
	}
	return left, nil
}
