// Command formatbench measures what objgit's storage format change cost and
// bought. It builds two daemons — one from the release before the change, one
// from the current checkout — and pushes the same real repositories into both,
// then clones each one back out.
//
// Before commits f4a1419 and ebfe4e3 (2026-08-27), objgitd stored git's native
// bare-repository layout as bucket keys through internal/s3fs. After them it
// stores packs/<id>.bin plus packs/<id>.cue containers through
// internal/storage/tigris. Both builds report the same Prometheus counter,
// objgit_s3_requests_total, with the same operation labels, so the request
// count that each format costs is comparable and no proxy is needed.
//
// Comparable, but not free: the counter is per process, not per request, so
// anything the daemon does in the background lands in it too. The older build
// re-lists hot prefixes on a wall-clock timer, which would charge it for how
// long an operation took rather than for what the operation did. quiesceArgs
// turns that off, and a build only gets a flag its own source defines.
//
// Every measurement gets a fresh repository prefix, a fresh pack cache
// directory, and a fresh daemon process, so nothing is ever measured warm by
// accident. Builds are interleaved rather than batched: network conditions
// drift over an afternoon, and running every "before" cell first would let that
// drift look like a result.
//
// The harness never deletes bucket data during a run. It writes repos.txt and
// leaves cleanup to a later -cleanup invocation.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/facebookgo/flagenv"
	"github.com/tigrisdata/objgit/internal"

	_ "github.com/joho/godotenv/autoload"
)

// corpus is the default set of repositories. The three shapes matter more than
// the three names: a small mostly-text repository, a medium one with a large
// object count, and one dominated by images, which do not compress.
var corpus = []repoSpec{
	{Name: "objgit", URL: "https://github.com/tigrisdata/objgit", Reps: 3},
	{Name: "x", URL: "https://github.com/Xe/x", Reps: 2},
	{Name: "tigris-blog", URL: "https://github.com/tigrisdata/tigris-blog", Reps: 3},
}

// repoSpec is one corpus entry. Reps is how many times it is pushed per build;
// a bigger repository gets fewer repetitions because each one is expensive.
type repoSpec struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	Reps int    `json:"reps"`

	Mirror  string `json:"mirror"`
	Commits string `json:"commits,omitempty"`
	Pack    int64  `json:"pack_bytes,omitempty"`
}

// buildSpec is one side of the comparison. Ref is the git ref its binary is
// built from; an empty Ref means the current checkout.
type buildSpec struct {
	Name string `json:"name"`
	Ref  string `json:"ref"`

	Bin string `json:"bin"`

	// Commit is the SHA the binary was built from, with "-dirty" appended when
	// the source carried uncommitted changes. Ref pins nothing on its own:
	// "after" is whatever the checkout happened to hold, and a branch named
	// through <name>=<ref> moves between runs. Without this a result cannot be
	// checked after the fact.
	Commit string `json:"commit,omitempty"`

	// Args are the extra daemon flags this build's own source defines. See
	// quiesceArgs.
	Args []string `json:"args,omitempty"`
}

var (
	outBase = flag.String("out", "", "parent directory for run output; empty uses var/bench/out under the checkout")
	org     = flag.String("org", "formatbench", "org segment every benchmark repository is created under")
	bucket  = flag.String("bucket", "", "Tigris bucket the daemon under test writes to; read from BUCKET in .env when empty")

	repos     = flag.String("repos", "", "comma-separated corpus entries to run; empty runs all of them")
	builds    = flag.String("builds", "before,after", "comma-separated sides to run; 'before' and 'after'")
	beforeRef = flag.String("before-ref", "v1.0.2", "git ref the 'before' daemon is built from; the last release where repositories live in the single -bucket through internal/s3fs")
	reps      = flag.Int("reps", 0, "override the per-repository repetition count; 0 keeps each entry's own")

	mirrorDir      = flag.String("mirror-dir", "", "directory holding the mirror clones, reused across runs; empty uses var/bench/mirrors")
	worktreeDir    = flag.String("worktree-dir", "", "directory holding the 'before' worktree; empty uses var/bench/src")
	refreshMirrors = flag.Bool("refresh-mirrors", false, "fetch each mirror before the run; off by default so repeated runs measure identical history")

	pushTimeout  = flag.Duration("push-timeout", 45*time.Minute, "time limit for one push; a push that hits it is recorded as DNF")
	cloneTimeout = flag.Duration("clone-timeout", 30*time.Minute, "time limit for one clone; a clone that hits it is recorded as DNF")
	// An hour, longer than the two above, because those cover a transfer
	// against a daemon on localhost while this one pulls a whole repository
	// from a forge. It is the slowest single git call the harness makes.
	mirrorTimeout = flag.Duration("mirror-timeout", 60*time.Minute, "time limit for the one-time mirror clone of a source repository")
	verify        = flag.Bool("verify", true, "compare each clone against its mirror and mark the cell INVALID when they differ")

	httpBind    = flag.String("http-bind", "127.0.0.1:8080", "address the daemon under test serves smart HTTP on")
	metricsBind = flag.String("metrics-bind", "127.0.0.1:9090", "address the daemon under test serves /metrics on")
	readyWait   = flag.Duration("ready-wait", 60*time.Second, "how long to wait for the daemon to answer /metrics before giving up")

	cleanup = flag.String("cleanup", "", "path to a repos.txt from an earlier run; deletes every prefix in it and exits without benchmarking")

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

	if *cleanup != "" {
		if err := runCleanup(ctx, *cleanup); err != nil {
			slog.Error("cleanup failed", "err", err)
			os.Exit(1)
		}
		return
	}

	if err := run(ctx); err != nil {
		slog.Error("benchmark failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}

	bucketName := bucketFromFlagOrEnv()
	bc, err := newBucketClient(ctx, bucketName)
	if err != nil {
		return err
	}

	selected, err := pickRepos(*repos)
	if err != nil {
		return err
	}

	sides, err := pickBuilds(*builds)
	if err != nil {
		return err
	}

	startedAt := time.Now()
	runDir := filepath.Join(outDir(root), startedAt.Format("20060102-150405"))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("formatbench: can't create %s: %w", runDir, err)
	}
	slog.Info("run directory", "path", runDir, "bucket", bucketName)

	for i := range sides {
		if err := buildSide(ctx, root, runDir, &sides[i]); err != nil {
			return err
		}
	}

	for i := range selected {
		if err := prepareMirror(ctx, root, &selected[i]); err != nil {
			return err
		}
	}

	meta := runMeta{
		StartedAt:  startedAt,
		Host:       hostname(),
		GoVersion:  runtime.Version(),
		GitVersion: gitVersion(ctx),
		NumCPU:     runtime.NumCPU(),
		Bucket:     bucketName,
		Org:        *org,
		Builds:     sides,
		Repos:      selected,
		RunDir:     runDir,
	}

	cells, runErr := drive(ctx, bc, runDir, sides, selected)
	meta.FinishedAt = time.Now()

	if err := writeResults(runDir, meta, cells); err != nil {
		return errors.Join(runErr, err)
	}

	fmt.Printf("\n%s\n\nRepositories left in the bucket are listed in %s.\nDelete them with: go run ./cmd/formatbench -cleanup %s\n",
		filepath.Join(runDir, "results.md"),
		filepath.Join(runDir, "repos.txt"),
		filepath.Join(runDir, "repos.txt"))

	return runErr
}

// drive walks the schedule. It returns whatever it recorded even when a cell
// fails, because a partial table is still worth reading.
func drive(ctx context.Context, bc *bucketClient, runDir string, sides []buildSpec, selected []repoSpec) ([]cell, error) {
	var cells []cell

	for _, c := range schedule(sides, selected) {
		if err := ctx.Err(); err != nil {
			return cells, err
		}

		name, err := newUUID()
		if err != nil {
			return cells, err
		}
		c.Prefix = fmt.Sprintf("%s/%s-%s-%s", *org, c.Build, c.Repo, name)

		slog.Info("cell", "build", c.Build, "repo", c.Repo, "rep", c.Rep, "prefix", c.Prefix)
		runCell(ctx, bc, runDir, sides, selected, &c)
		cells = append(cells, c)

		// Write after every cell. A run can take hours, and an interrupted one
		// must still leave a readable table and a complete cleanup list.
		if err := appendRepoList(filepath.Join(runDir, "repos.txt"), c.Prefix); err != nil {
			slog.Warn("can't record prefix for cleanup", "prefix", c.Prefix, "err", err)
		}
	}

	return cells, nil
}

// schedule interleaves the builds. Repetition is the outer loop, so the order
// is (rep 1: every repo on every build), then (rep 2: the same), rather than
// every "before" cell followed by every "after" cell.
func schedule(sides []buildSpec, selected []repoSpec) []cell {
	most := 0
	for _, r := range selected {
		most = max(most, repCount(r))
	}

	var out []cell
	for rep := 1; rep <= most; rep++ {
		for _, r := range selected {
			if rep > repCount(r) {
				continue
			}
			for _, s := range sides {
				out = append(out, cell{Build: s.Name, Repo: r.Name, Rep: rep})
			}
		}
	}

	return out
}

func repCount(r repoSpec) int {
	if *reps > 0 {
		return *reps
	}
	return r.Reps
}

// runCell does one push and one clone, each against its own fresh daemon. The
// daemon restarts between them so the clone is cold in the process, on disk,
// and in the pack index that (*Storer).ensurePacksBuilt rebuilds per request.
func runCell(ctx context.Context, bc *bucketClient, runDir string, sides []buildSpec, selected []repoSpec, c *cell) {
	side, ok := findBuild(sides, c.Build)
	if !ok {
		c.Push.Err = "unknown build " + c.Build
		return
	}

	repo, ok := findRepo(selected, c.Repo)
	if !ok {
		c.Push.Err = "unknown repo " + c.Repo
		return
	}

	root, err := moduleRoot()
	if err != nil {
		c.Push.Err = err.Error()
		return
	}

	url := fmt.Sprintf("http://%s/%s.git", *httpBind, c.Prefix)
	tag := fmt.Sprintf("%s-%s-%d", c.Build, c.Repo, c.Rep)

	c.Push = measure(ctx, root, runDir, side, tag+"-push", func(runCtx context.Context) (time.Duration, int64, bool, error) {
		wall, dnf, err := gitPush(runCtx, *pushTimeout, repo.Mirror, url)
		return wall, 0, dnf, err
	})

	// A push that never finished still left keys behind, and how far it got is
	// the interesting part of a DNF. Measure the bucket either way.
	//
	// usage returns whatever it summed before it failed, so a listing throttled
	// partway through the tens of thousands of loose-object keys the old format
	// writes comes back short and renders as a complete figure. That
	// understates the older build, which is the direction that would turn the
	// comparison around, so a failed listing invalidates the cell instead. The
	// push timing goes with it: cell has one validity flag, and printing a
	// timing beside a key count nobody can stand behind is what this is trying
	// to avoid.
	usage, err := bc.usage(ctx, c.Prefix)
	if err != nil {
		slog.Error("can't measure bucket usage", "prefix", c.Prefix, "err", err)
		c.Usage = bucketUsage{}
		if c.Push.Err == "" {
			c.Push.Err = fmt.Sprintf("the push finished but the bucket listing did not: %v", err)
		}
	} else {
		c.Usage = usage
	}

	if !c.Push.ok() {
		slog.Warn("skipping clone because the push did not finish", "build", c.Build, "repo", c.Repo, "dnf", c.Push.DNF, "err", c.Push.Err)
		return
	}

	dst := filepath.Join(runDir, "clones", tag+".git")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		c.Clone.Err = err.Error()
		return
	}
	defer os.RemoveAll(dst)

	c.Clone = measure(ctx, root, runDir, side, tag+"-clone", func(runCtx context.Context) (time.Duration, int64, bool, error) {
		return gitClone(runCtx, *cloneTimeout, url, dst)
	})

	if *verify && c.Clone.ok() {
		c.Verify = verifyClone(ctx, repo.Mirror, dst)
		if !c.Verify.OK {
			slog.Error("clone does not match the mirror", "build", c.Build, "repo", c.Repo, "problems", c.Verify.Problems)
		}
	}
}

// measure starts a daemon with an empty pack cache, scrapes its S3 counters,
// runs one git operation, and scrapes again. The daemon is stopped and its
// cache deleted before it returns, so nothing carries into the next
// measurement.
func measure(ctx context.Context, root, runDir string, side buildSpec, tag string, op func(context.Context) (time.Duration, int64, bool, error)) opResult {
	var out opResult

	packCache, err := os.MkdirTemp(runDir, "packcache-")
	if err != nil {
		out.Err = fmt.Sprintf("can't create a pack cache directory: %v", err)
		return out
	}
	defer os.RemoveAll(packCache)

	args := []string{
		"-bucket", bucketFromFlagOrEnv(),
		"-http-bind", *httpBind,
		"-metrics-bind", *metricsBind,
		"-ssh-bind=",
		"-allow-push",
		"-pack-cache-dir", packCache,
	}
	args = append(args, side.Args...)

	if err := ensureNoDaemon(ctx, *metricsBind); err != nil {
		out.Err = err.Error()
		return out
	}

	daemon, err := startDaemon(root, side.Bin, filepath.Join(runDir, "daemon-"+tag+".log"), args)
	if err != nil {
		out.Err = err.Error()
		return out
	}
	defer stopDaemon(daemon)

	if err := waitReady(ctx, daemon, *metricsBind, *readyWait); err != nil {
		out.Err = err.Error()
		return out
	}

	before, err := scrapeS3(ctx, *metricsBind)
	if err != nil {
		out.Err = err.Error()
		return out
	}

	wall, wire, dnf, opErr := op(ctx)

	out.Wall = wall
	out.WallStr = wall.Round(time.Millisecond).String()
	out.WireBytes = wire
	out.DNF = dnf
	if opErr != nil {
		out.Err = opErr.Error()
	}

	// The closing reading is what turns two counter values into a request
	// count; without it there is no count. Leaving out.S3 at its zero value
	// would file a measurement of no S3 traffic at all, and ok() and valid()
	// would both stay true, so it would sit in the median with nothing in the
	// report marking it. A scrape that did not answer fails the measurement.
	after, err := scrapeS3(ctx, *metricsBind)
	if err != nil {
		slog.Error("dropping the measurement: can't scrape metrics after the operation", "tag", tag, "err", err)
		if out.Err == "" {
			out.Err = err.Error()
		}
		return out
	}
	out.S3 = after.sub(before)

	slog.Info("measured", "tag", tag,
		"wall", out.WallStr,
		"s3_requests", out.S3.Total(),
		"s3_seconds", fmt.Sprintf("%.1f", out.S3.TotalSeconds()),
		"dnf", dnf,
	)

	return out
}

// buildSide compiles one daemon. The "before" side is built from a worktree
// checked out at its ref, because its go.mod and its whole internal tree differ
// from the current checkout.
func buildSide(ctx context.Context, root, runDir string, side *buildSpec) error {
	src := root
	if side.Ref != "" {
		var err error
		if src, err = ensureWorktree(ctx, root, side.Ref); err != nil {
			return err
		}
	}

	side.Bin = filepath.Join(runDir, "objgitd-"+side.Name)
	side.Commit = resolveCommit(ctx, src)
	side.Args = quiesceArgs(src)

	cmd := exec.CommandContext(ctx, "go", "build", "-o", side.Bin, "./cmd/objgitd")
	cmd.Dir = src

	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("formatbench: can't build the %q daemon from %s: %w: %s",
			side.Name, src, err, strings.TrimSpace(string(out)))
	}

	slog.Info("built daemon", "side", side.Name, "ref", side.Ref, "commit", side.Commit,
		"src", src, "bin", side.Bin, "args", strings.Join(side.Args, " "))
	return nil
}

// quiesceArgs returns the flags that stop the daemon built from src doing S3
// work on a wall-clock timer.
//
// v1.0.2 runs a listing-cache warmer that re-lists every hot prefix each
// -s3-cache-refresh, 30 seconds by default, and those listings land in
// objgit_s3_requests_total, the vector measure brackets. A 45 minute push
// absorbs about ninety warmer cycles the current build never pays, and the
// inflation grows with wall time, which is the axis the report ranks builds on.
//
// Only the warmer is switched off. -s3-cache-ttl is left at its default,
// because the listing cache answered every request that release served: turning
// it off would measure a daemon nobody ran and would charge the old format for
// listings it did not make.
//
// The source is read rather than the ref name matched, because objgitd exits on
// an unknown flag and the current checkout has no -s3-cache-refresh. A build
// only gets a flag its own source defines.
func quiesceArgs(src string) []string {
	if !definesFlag(src, "s3-cache-refresh") {
		return nil
	}
	return []string{"-s3-cache-refresh=0"}
}

// definesFlag reports whether the daemon's source under src declares the named
// flag.
func definesFlag(src, name string) bool {
	dir := filepath.Join(src, "cmd", "objgitd")

	entries, err := os.ReadDir(dir)
	if err != nil {
		slog.Warn("can't read the daemon's source; assuming it defines no extra flags", "dir", dir, "err", err)
		return false
	}

	needle := []byte(strconv.Quote(name))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}

		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			slog.Warn("can't read a daemon source file", "path", filepath.Join(dir, e.Name()), "err", err)
			continue
		}
		if bytes.Contains(raw, needle) {
			return true
		}
	}

	return false
}

// resolveCommit records which commit a build was compiled from, so a number in
// the report can be traced back to a binary months later. It never fails the
// run: a missing SHA makes a result harder to check, but a wrong one would be
// worse and there is no way to produce one here.
func resolveCommit(ctx context.Context, src string) string {
	sha, _, err := runGit(ctx, 30*time.Second, src, "rev-parse", "HEAD")
	if err != nil {
		slog.Warn("can't resolve the commit a build was made from", "src", src, "err", err)
		return "unknown"
	}
	out := strings.TrimSpace(sha)

	// A dirty tree is the normal case for the current checkout, and it means
	// the SHA alone does not describe what was built.
	status, _, err := runGit(ctx, 30*time.Second, src, "status", "--porcelain")
	if err != nil {
		slog.Warn("can't tell whether a build's source was dirty", "src", src, "err", err)
		return out
	}
	if strings.TrimSpace(status) != "" {
		out += "-dirty"
	}

	return out
}

// ensureWorktree checks a ref out beside the working tree. A worktree is used
// rather than a detached checkout so the operator's own tree is never touched
// and the build can run in parallel with editing.
//
// An existing worktree is moved to ref rather than trusted to already be there.
// The <name>=<ref> spelling exists to benchmark a candidate fix, and a fix
// lives on a branch: rerunning after pushing new commits would otherwise build
// the old checkout and print the branch name over it.
func ensureWorktree(ctx context.Context, root, ref string) (string, error) {
	dir := filepath.Join(srcDir(root), worktreeName(ref))

	if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
		if _, _, err := runGit(ctx, 10*time.Minute, dir, "checkout", "--detach", "--force", ref); err != nil {
			return "", fmt.Errorf("formatbench: can't move the worktree at %s to %s: %w", dir, ref, err)
		}
		slog.Info("reusing worktree", "ref", ref, "path", dir)
		return dir, nil
	}

	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", fmt.Errorf("formatbench: can't create %s: %w", filepath.Dir(dir), err)
	}

	cmd := exec.CommandContext(ctx, "git", "worktree", "add", "--detach", dir, ref)
	cmd.Dir = root

	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("formatbench: can't add a worktree for %s: %w: %s", ref, err, strings.TrimSpace(string(out)))
	}

	slog.Info("added worktree", "ref", ref, "path", dir)
	return dir, nil
}

// prepareMirror makes sure the repository is mirrored locally and records what
// went in: the reachable commit count and the size of the mirror's own pack.
// Those two numbers are what the report's ratios are read against.
func prepareMirror(ctx context.Context, root string, r *repoSpec) error {
	r.Mirror = filepath.Join(mirrorsDir(root), r.Name+".git")

	if _, err := os.Stat(filepath.Join(r.Mirror, "HEAD")); err != nil {
		if err := os.MkdirAll(filepath.Dir(r.Mirror), 0o755); err != nil {
			return fmt.Errorf("formatbench: can't create %s: %w", filepath.Dir(r.Mirror), err)
		}
		slog.Info("mirroring", "repo", r.Name, "url", r.URL, "path", r.Mirror)
		if err := mirrorClone(ctx, r.URL, r.Mirror); err != nil {
			return err
		}
	} else if *refreshMirrors {
		slog.Info("refreshing mirror", "repo", r.Name, "path", r.Mirror)
		if _, _, err := runGit(ctx, 30*time.Minute, r.Mirror, "remote", "update", "--prune"); err != nil {
			return fmt.Errorf("formatbench: can't refresh the %s mirror: %w", r.Name, err)
		}
	}

	count, _, err := runGit(ctx, verifyLimit, r.Mirror, "rev-list", "--all", "--count")
	if err != nil {
		return fmt.Errorf("formatbench: can't count commits in the %s mirror: %w", r.Name, err)
	}
	r.Commits = strings.TrimSpace(count)
	r.Pack = packBytes(r.Mirror)

	slog.Info("mirror ready", "repo", r.Name, "commits", r.Commits, "pack_bytes", r.Pack)
	return nil
}

// packBytes sums the mirror's packfiles. It is the closest local stand-in for
// "how big is this repository" and it is what the bucket byte counts get
// compared against.
func packBytes(gitDir string) int64 {
	entries, err := os.ReadDir(filepath.Join(gitDir, "objects", "pack"))
	if err != nil {
		return 0
	}

	var total int64
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".pack") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}

	return total
}

func runCleanup(ctx context.Context, listPath string) error {
	raw, err := os.ReadFile(listPath)
	if err != nil {
		return fmt.Errorf("formatbench: can't read %s: %w", listPath, err)
	}

	bc, err := newBucketClient(ctx, bucketFromFlagOrEnv())
	if err != nil {
		return err
	}

	var failed error
	for line := range strings.SplitSeq(string(raw), "\n") {
		prefix := strings.TrimSpace(line)
		if prefix == "" || strings.HasPrefix(prefix, "#") {
			continue
		}

		n, err := bc.remove(ctx, prefix)
		if err != nil {
			slog.Error("can't delete prefix", "prefix", prefix, "err", err)
			failed = errors.Join(failed, err)
			continue
		}
		slog.Info("deleted prefix", "prefix", prefix, "keys", n)
	}

	return failed
}

// daemonProc is one objgitd under test: the process, the log file it writes to,
// and a channel carrying the single result of waiting on it. The channel is
// what lets waitReady notice a daemon that died instead of polling an address
// something else might answer.
type daemonProc struct {
	cmd     *exec.Cmd
	log     *os.File
	logPath string

	// wait carries cmd.Wait's result once and is then closed, so every later
	// receive returns immediately whether or not the value was taken.
	wait chan error
}

// ensureNoDaemon fails when something already answers on the metrics address.
//
// A daemon left behind by an interrupted run keeps the metrics and HTTP ports.
// The one started next dies on bind within milliseconds, and every request the
// harness makes after that goes to the old process: the wrong binary, with a
// warm pack cache, whose counters sub() quietly absorbs. Nothing downstream
// looks wrong, so it has to be caught before the measurement starts.
func ensureNoDaemon(ctx context.Context, metricsAddr string) error {
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if _, err := scrapeS3(probe, metricsAddr); err == nil {
		return fmt.Errorf("formatbench: something already answers http://%s/metrics; stop it before benchmarking", metricsAddr)
	}

	return nil
}

// startDaemon runs objgitd with its working directory set to the module root,
// because objgitd loads its credentials from the .env file there. The context
// is deliberately not attached: shutdown goes through stopDaemon so the daemon
// gets the same SIGINT it would in production.
func startDaemon(root, bin, logPath string, args []string) (*daemonProc, error) {
	log, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("formatbench: can't create %s: %w", logPath, err)
	}

	cmd := exec.Command(bin, args...)
	cmd.Dir = root
	cmd.Stdout = log
	cmd.Stderr = log

	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, fmt.Errorf("formatbench: can't start %s: %w", bin, err)
	}

	p := &daemonProc{cmd: cmd, log: log, logPath: logPath, wait: make(chan error, 1)}
	go func() {
		p.wait <- cmd.Wait()
		close(p.wait)
	}()

	return p, nil
}

// stopDaemon ends the daemon and closes its log. Closing here is what bounds
// the descriptors: measure runs twice per cell, and the file is only flushed
// when it is closed, so a log left open is both a leak and an unreadable log.
func stopDaemon(p *daemonProc) {
	defer p.log.Close()

	if p.cmd.Process == nil {
		return
	}

	// A daemon that already died, or that waitReady found dead, has nothing
	// left to signal.
	select {
	case <-p.wait:
		return
	default:
	}

	if err := p.cmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		slog.Warn("can't interrupt daemon", "err", err)
	}

	select {
	case <-p.wait:
	case <-time.After(30 * time.Second):
		slog.Warn("daemon did not exit on SIGINT, killing it")
		_ = p.cmd.Process.Kill()
		<-p.wait
	}
}

// waitReady polls /metrics until the daemon answers, and gives up early if the
// daemon it was given is no longer running.
//
// Watching the child is the point. A 200 from the metrics address only says
// that some process holds that port, and objgitd exits on a bind failure, so
// without this a daemon that lost the port would be measured as if it had won
// it.
func waitReady(ctx context.Context, p *daemonProc, metricsAddr string, limit time.Duration) error {
	url := "http://" + metricsAddr + "/metrics"
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(limit)

	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}

		select {
		case err := <-p.wait:
			if err == nil {
				err = errors.New("it exited without an error")
			}
			return fmt.Errorf("formatbench: the daemon stopped before it answered %s: %w; see %s", url, err, p.logPath)
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("formatbench: can't build readiness request: %w", err)
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

	return fmt.Errorf("formatbench: daemon did not answer %s within %s", url, limit)
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

// pickRepos resolves -repos against the corpus table.
func pickRepos(raw string) ([]repoSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return slices.Clone(corpus), nil
	}

	var out []repoSpec
	for want := range strings.SplitSeq(raw, ",") {
		want = strings.TrimSpace(want)
		if want == "" {
			continue
		}

		found := false
		for _, r := range corpus {
			if r.Name == want {
				out = append(out, r)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("formatbench: %q is not in the corpus; known entries are %s", want, corpusNames())
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("formatbench: -repos selected nothing")
	}
	return out, nil
}

// pickBuilds resolves -builds into the sides of the comparison.
//
// Three spellings. "before" and "after" are the two sides of the format change
// itself. Anything else is "<name>=<ref>", which builds that ref and labels its
// column <name>; an empty ref means the current checkout. The third form is
// what lets one interleaved session compare more than two points, which is the
// only honest way to read a candidate fix against both the old and the current
// build: comparing against a run from another day measures the network as much
// as the code.
func pickBuilds(raw string) ([]buildSpec, error) {
	var out []buildSpec

	for want := range strings.SplitSeq(raw, ",") {
		want = strings.TrimSpace(want)

		switch {
		case want == "":
		case want == "before":
			out = append(out, buildSpec{Name: "before", Ref: *beforeRef})
		case want == "after":
			out = append(out, buildSpec{Name: "after"})
		default:
			name, ref, ok := strings.Cut(want, "=")
			name, ref = strings.TrimSpace(name), strings.TrimSpace(ref)
			if !ok || name == "" {
				return nil, fmt.Errorf("formatbench: %q is not a build; use 'before', 'after', or '<name>=<ref>'", want)
			}
			out = append(out, buildSpec{Name: name, Ref: ref})
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("formatbench: -builds selected nothing")
	}

	seen := map[string]bool{}
	for _, s := range out {
		if seen[s.Name] {
			return nil, fmt.Errorf("formatbench: -builds names %q twice; every column needs its own name", s.Name)
		}
		seen[s.Name] = true
	}

	return out, nil
}

// worktreeName turns a ref into one directory name. A branch ref carries
// slashes, and joining those straight onto the worktree parent would bury the
// checkout several directories down and collide with any other ref sharing the
// prefix.
func worktreeName(ref string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r == ':' || r == ' ' {
			return '-'
		}
		return r
	}, ref)
}

func corpusNames() string {
	names := make([]string, 0, len(corpus))
	for _, r := range corpus {
		names = append(names, r.Name)
	}
	return strings.Join(names, ", ")
}

func findBuild(sides []buildSpec, name string) (buildSpec, bool) {
	for _, s := range sides {
		if s.Name == name {
			return s, true
		}
	}
	return buildSpec{}, false
}

func findRepo(selected []repoSpec, name string) (repoSpec, bool) {
	for _, r := range selected {
		if r.Name == name {
			return r, true
		}
	}
	return repoSpec{}, false
}

func bucketFromFlagOrEnv() string {
	if *bucket != "" {
		return *bucket
	}
	return os.Getenv("BUCKET")
}

func outDir(root string) string     { return underRoot(root, *outBase, "out") }
func mirrorsDir(root string) string { return underRoot(root, *mirrorDir, "mirrors") }
func srcDir(root string) string     { return underRoot(root, *worktreeDir, "src") }

// underRoot resolves one directory flag against the checkout, and always
// returns an absolute path.
//
// Absolute is load-bearing, not tidiness. startDaemon sets cmd.Dir to the
// checkout root so the daemon finds .env, and a relative binary path is then
// resolved against that directory rather than against the harness's own
// working directory. A relative -out therefore produces a daemon path that
// does not exist and every cell fails with "no such file or directory".
func underRoot(root, flagVal, name string) string {
	dir := flagVal
	if dir == "" {
		dir = filepath.Join(root, "var", "bench", name)
	}

	abs, err := filepath.Abs(dir)
	if err != nil {
		return dir
	}
	return abs
}

// moduleRoot walks up from the working directory to the directory holding
// go.mod. That is where objgitd's .env lives, and where `go build` has to run.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("formatbench: can't read working directory: %w", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("formatbench: no go.mod above the working directory; run this from the objgit checkout")
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

func gitVersion(ctx context.Context) string {
	out, _, err := runGit(ctx, 10*time.Second, "", "version")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(out)
}
