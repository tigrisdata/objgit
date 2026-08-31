// Command membench measures how much memory objgitd uses while it takes
// pushes. It starts a daemon it owns, pushes one real repository into many
// fresh ones, and samples the daemon's resident set and Go heap the whole time,
// grabbing a pprof heap profile every time memory sets a new high-water mark.
//
// The run has three phases. A baseline establishes what the daemon costs
// sitting idle. A sequential phase pushes one repository at a time with idle
// gaps, which isolates the per-push cost and shows whether memory comes back
// down afterwards. A concurrency sweep then runs K pushes at once for a few
// values of K, which gives the slope you actually size a machine with.
//
// Every repository is created fresh under a UUID, so no push is ever measured
// against a repository that already holds its objects. The harness never
// deletes them; it writes repos.txt and leaves cleanup to you.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/facebookgo/flagenv"
	"github.com/tigrisdata/objgit/internal"
	"golang.org/x/sync/errgroup"
)

// Phase names, as they appear in the phase column of samples.csv.
const (
	phaseBaseline = "baseline"
	phaseIdle     = "idle"
	phaseSeq      = "seq"
	phaseConc     = "conc"
)

var (
	repoPath = flag.String("repo", filepath.Join(os.Getenv("HOME"), "Code/Xe/x"), "repository to push; it is mirror-cloned once and never written to")
	outBase  = flag.String("out", "", "parent directory for run output; empty uses the OS temp directory")
	org      = flag.String("org", "benchtest", "org segment every benchmark repository is created under")

	seqPushes = flag.Int("seq-pushes", 5, "number of sequential pushes, each to a fresh repository")
	concSteps = flag.String("conc-steps", "1,2,4,8", "comma-separated concurrency levels to sweep; empty skips the sweep")

	sampleEvery  = flag.Duration("sample-interval", 250*time.Millisecond, "how often to sample /proc and /metrics")
	idleGap      = flag.Duration("idle-gap", 5*time.Second, "idle time between pushes, so memory has a chance to settle")
	baselineFor  = flag.Duration("baseline", 10*time.Second, "how long to sample the idle daemon before pushing anything")
	windowSlack  = flag.Duration("window-slack", 0, "widen each push's measurement window by this much on both sides, so work finishing after git exits still counts against it; zero uses -idle-gap")
	peakGrowth   = flag.Float64("peak-growth", 0.05, "fractional rise in resident set that triggers a heap profile capture")
	peakCooldown = flag.Duration("peak-cooldown", 2*time.Second, "minimum time between two peak captures")

	daemonBinary     = flag.String("daemon-binary", "", "prebuilt objgitd to run; empty builds ./cmd/objgitd")
	daemonHTTPBind   = flag.String("daemon-http-bind", "127.0.0.1:8080", "address the daemon under test serves smart HTTP on")
	daemonMetrics    = flag.String("daemon-metrics-bind", "127.0.0.1:9090", "address the daemon under test serves /metrics and /debug/pprof on")
	daemonAllowHooks = flag.Bool("daemon-allow-hooks", false, "run push hooks in the daemon under test; off by default so hook cost is not mistaken for push cost")
	daemonReadyWait  = flag.Duration("daemon-ready-wait", 60*time.Second, "how long to wait for the daemon to answer /metrics before giving up")

	slogLevel = flag.String("slog-level", "INFO", "log level (DEBUG, INFO, WARN, ERROR)")
)

func main() {
	flagenv.Parse()
	flag.Parse()

	logger, err := internal.InitSlog(*slogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad -slog-level: %v\n", err)
		os.Exit(1)
	}
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("benchmark failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	steps, err := parseSteps(*concSteps)
	if err != nil {
		return err
	}

	root, err := moduleRoot()
	if err != nil {
		return err
	}

	startedAt := time.Now()
	runDir := filepath.Join(outDir(), startedAt.Format("20060102-150405"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("membench: can't create %s: %w", runDir, err)
	}
	slog.Info("run directory", "path", runDir)

	mirror := filepath.Join(runDir, "source.git")
	if err := mirrorClone(ctx, *repoPath, mirror); err != nil {
		return err
	}

	packed, err := packBytes(mirror)
	if err != nil {
		return err
	}
	slog.Info("mirrored source repository", "path", mirror, "pack_bytes", packed)

	bin := *daemonBinary
	if bin == "" {
		bin = filepath.Join(runDir, "objgitd")
		if err := buildDaemon(ctx, root, bin); err != nil {
			return err
		}
	}

	// -pack-cache-dir names the parent the daemon makes its own cache directory
	// under, so it has to exist before the daemon starts.
	packCache := filepath.Join(runDir, "packcache")
	if err := os.MkdirAll(packCache, 0o755); err != nil {
		return fmt.Errorf("membench: can't create %s: %w", packCache, err)
	}

	args := []string{
		"-http-bind", *daemonHTTPBind,
		"-metrics-bind", *daemonMetrics,
		"-ssh-bind=",
		"-allow-push",
		fmt.Sprintf("-allow-hooks=%t", *daemonAllowHooks),
		"-pack-cache-dir", packCache,
	}

	daemon, err := startDaemon(root, bin, filepath.Join(runDir, "daemon.log"), args)
	if err != nil {
		return err
	}
	defer stopDaemon(daemon)

	if err := waitReady(ctx, *daemonMetrics, *daemonReadyWait); err != nil {
		return err
	}
	slog.Info("daemon ready", "pid", daemon.Process.Pid, "http", *daemonHTTPBind, "metrics", *daemonMetrics)

	s := newSampler(daemon.Process.Pid, *daemonMetrics, runDir, *sampleEvery, *peakCooldown, *peakGrowth)
	sampleCtx, stopSampling := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.run(sampleCtx)
	}()

	pushes, runErr := drive(ctx, s, mirror, steps)

	stopSampling()
	<-done

	samples, profiles := s.snapshot()
	meta := runMeta{
		StartedAt:   startedAt,
		FinishedAt:  time.Now(),
		Host:        hostname(),
		GoVersion:   runtime.Version(),
		GOGC:        os.Getenv("GOGC"),
		GOMEMLIMIT:  os.Getenv("GOMEMLIMIT"),
		NumCPU:      runtime.NumCPU(),
		SourceRepo:  *repoPath,
		PackBytes:   packed,
		Org:         *org,
		DaemonArgs:  args,
		SampleEvery: *sampleEvery,
		WindowSlack: slack(),
		RunDir:      runDir,
	}

	if err := writeCSV(filepath.Join(runDir, "samples.csv"), samples); err != nil {
		return err
	}
	if err := writeRepoList(filepath.Join(runDir, "repos.txt"), *org, pushes); err != nil {
		return err
	}
	if err := writeReport(filepath.Join(runDir, "report.md"), meta, samples, profiles, pushes); err != nil {
		return err
	}

	slog.Info("wrote results",
		"report", filepath.Join(runDir, "report.md"),
		"samples", len(samples),
		"profiles", len(profiles),
		"repos", len(pushes),
	)
	fmt.Printf("\n%s\n\nRepositories left in the bucket are listed in %s.\n",
		filepath.Join(runDir, "report.md"), filepath.Join(runDir, "repos.txt"))

	return runErr
}

// drive walks the three phases. It returns whatever it managed to record even
// when a push fails, because a partial run with a heap profile in it is still
// worth reading.
func drive(ctx context.Context, s *sampler, mirror string, steps []int) ([]pushResult, error) {
	var pushes []pushResult

	s.label(phaseBaseline, "")
	slog.Info("sampling idle baseline", "for", *baselineFor)
	if err := idle(ctx, *baselineFor); err != nil {
		return pushes, err
	}
	if _, err := s.captureProfile(ctx, "heap", true, "heap-baseline.pb.gz", "idle baseline before any push", lastRSS(s)); err != nil {
		slog.Warn("can't capture baseline heap profile", "err", err)
	}

	for i := range *seqPushes {
		if err := ctx.Err(); err != nil {
			return pushes, err
		}

		name, err := newUUID()
		if err != nil {
			return pushes, err
		}

		s.label(phaseSeq, name)
		slog.Info("sequential push", "n", i+1, "of", *seqPushes, "repo", *org+"/"+name)
		pushes = append(pushes, doPush(ctx, mirror, phaseSeq, 1, name))

		s.label(phaseIdle, "")
		if err := idle(ctx, *idleGap); err != nil {
			return pushes, err
		}
	}

	for _, k := range steps {
		if err := ctx.Err(); err != nil {
			return pushes, err
		}

		names := make([]string, k)
		for i := range names {
			name, err := newUUID()
			if err != nil {
				return pushes, err
			}
			names[i] = name
		}

		s.label(phaseConc, fmt.Sprintf("k=%d", k))
		slog.Info("concurrent push step", "k", k)

		results := make([]pushResult, k)
		g, gCtx := errgroup.WithContext(ctx)
		for i, name := range names {
			g.Go(func() error {
				results[i] = doPush(gCtx, mirror, phaseConc, k, name)
				return nil
			})
		}
		_ = g.Wait()
		pushes = append(pushes, results...)

		s.label(phaseIdle, "")
		if err := idle(ctx, *idleGap); err != nil {
			return pushes, err
		}
	}

	s.label(phaseIdle, "")
	if err := idle(ctx, *idleGap); err != nil {
		return pushes, err
	}
	if _, err := s.captureProfile(ctx, "heap", true, "heap-final.pb.gz", "settled heap after every push", lastRSS(s)); err != nil {
		slog.Warn("can't capture final heap profile", "err", err)
	}
	if _, err := s.captureProfile(ctx, "allocs", false, "allocs-final.pb.gz", "total allocation over the whole run", lastRSS(s)); err != nil {
		slog.Warn("can't capture allocation profile", "err", err)
	}

	return pushes, nil
}

func doPush(ctx context.Context, mirror, phase string, concurrency int, name string) pushResult {
	url := fmt.Sprintf("http://%s/%s/%s.git", *daemonHTTPBind, *org, name)

	start := time.Now()
	err := push(ctx, mirror, url)
	end := time.Now()

	if err != nil {
		slog.Error("push failed", "repo", *org+"/"+name, "err", err)
	} else {
		slog.Info("push done", "repo", *org+"/"+name, "took", end.Sub(start).Round(time.Millisecond))
	}

	return pushResult{Phase: phase, Concurrency: concurrency, Repo: name, Start: start, End: end, Err: err}
}

// push sends every branch and tag to a repository that does not exist yet.
// The refspecs are forced because the harness does not care about history on
// the receiving side; it only cares what the daemon does with the pack.
func push(ctx context.Context, mirror, url string) error {
	cmd := exec.CommandContext(ctx, "git", "-C", mirror, "push", "--quiet", url,
		"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*")

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git push to %s: %w: %s", url, err, strings.TrimSpace(string(out)))
	}

	return nil
}

// mirrorClone copies the source repository once, so the benchmark never writes
// to the repository it was pointed at and concurrent pushes all read from the
// same immutable copy.
func mirrorClone(ctx context.Context, src, dst string) error {
	cmd := exec.CommandContext(ctx, "git", "clone", "--mirror", src, dst)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("membench: can't mirror %s: %w: %s", src, err, strings.TrimSpace(string(out)))
	}

	return nil
}

func buildDaemon(ctx context.Context, root, out string) error {
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, "./cmd/objgitd")
	cmd.Dir = root

	res, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("membench: can't build objgitd: %w: %s", err, strings.TrimSpace(string(res)))
	}

	slog.Info("built daemon under test", "path", out)
	return nil
}

// startDaemon runs objgitd with its working directory set to the module root,
// because objgitd loads its bucket and credentials from the .env file there.
// The context is deliberately not attached: shutdown goes through stopDaemon so
// the daemon gets the same SIGINT it would in production.
func startDaemon(root, bin, logPath string, args []string) (*exec.Cmd, error) {
	log, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("membench: can't create %s: %w", logPath, err)
	}

	cmd := exec.Command(bin, args...)
	cmd.Dir = root
	cmd.Stdout = log
	cmd.Stderr = log

	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, fmt.Errorf("membench: can't start %s: %w", bin, err)
	}

	return cmd, nil
}

func stopDaemon(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		slog.Warn("can't interrupt daemon", "err", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		slog.Warn("daemon did not exit on SIGINT, killing it")
		_ = cmd.Process.Kill()
		<-done
	}
}

func waitReady(ctx context.Context, metricsAddr string, limit time.Duration) error {
	url := "http://" + metricsAddr + "/metrics"
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(limit)

	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("membench: can't build readiness request: %w", err)
		}

		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		if err := idle(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}

	return fmt.Errorf("membench: daemon did not answer %s within %s", url, limit)
}

// idle sleeps, but gives up as soon as the run is cancelled.
func idle(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func lastRSS(s *sampler) uint64 {
	samples, _ := s.snapshot()
	if len(samples) == 0 {
		return 0
	}
	return samples[len(samples)-1].Proc.VmRSS
}

// parseSteps turns "1,2,4,8" into the concurrency levels to sweep.
func parseSteps(raw string) ([]int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var out []int
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("membench: %q in -conc-steps is not a number: %w", part, err)
		}
		if n < 1 {
			return nil, fmt.Errorf("membench: -conc-steps must be positive, got %d", n)
		}
		out = append(out, n)
	}

	return out, nil
}

// slack is how far outside a push's wall clock the harness still attributes
// memory to it. The daemon keeps uploading after git has exited, so the default
// is the whole idle gap that follows.
func slack() time.Duration {
	if *windowSlack > 0 {
		return *windowSlack
	}
	return *idleGap
}

func outDir() string {
	if *outBase != "" {
		return *outBase
	}
	return filepath.Join(os.TempDir(), "membench")
}

// moduleRoot walks up from the working directory to the directory holding
// go.mod. That is where objgitd's .env lives, and where `go build` has to run.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("membench: can't read working directory: %w", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("membench: no go.mod above the working directory; run this from the objgit checkout")
		}
		dir = parent
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
