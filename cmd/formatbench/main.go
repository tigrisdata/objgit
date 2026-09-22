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
// count that each format costs is directly comparable and no proxy is needed.
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
	verify       = flag.Bool("verify", true, "compare each clone against its mirror and mark the cell INVALID when they differ")

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
	usage, err := bc.usage(ctx, c.Prefix)
	if err != nil {
		slog.Warn("can't measure bucket usage", "prefix", c.Prefix, "err", err)
	}
	c.Usage = usage

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

	daemon, err := startDaemon(root, side.Bin, filepath.Join(runDir, "daemon-"+tag+".log"), args)
	if err != nil {
		out.Err = err.Error()
		return out
	}
	defer stopDaemon(daemon)

	if err := waitReady(ctx, *metricsBind, *readyWait); err != nil {
		out.Err = err.Error()
		return out
	}

	before, err := scrapeS3(ctx, *metricsBind)
	if err != nil {
		out.Err = err.Error()
		return out
	}

	wall, wire, dnf, opErr := op(ctx)

	after, err := scrapeS3(ctx, *metricsBind)
	if err != nil {
		slog.Warn("can't scrape metrics after the operation", "tag", tag, "err", err)
		after = before
	}

	out.Wall = wall
	out.WallStr = wall.Round(time.Millisecond).String()
	out.S3 = after.sub(before)
	out.WireBytes = wire
	out.DNF = dnf
	if opErr != nil {
		out.Err = opErr.Error()
	}

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

	cmd := exec.CommandContext(ctx, "go", "build", "-o", side.Bin, "./cmd/objgitd")
	cmd.Dir = src

	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("formatbench: can't build the %q daemon from %s: %w: %s",
			side.Name, src, err, strings.TrimSpace(string(out)))
	}

	slog.Info("built daemon", "side", side.Name, "ref", side.Ref, "src", src, "bin", side.Bin)
	return nil
}

// ensureWorktree checks a ref out beside the working tree, once. A worktree is
// used rather than a detached checkout so the operator's own tree is never
// touched and the build can run in parallel with editing.
func ensureWorktree(ctx context.Context, root, ref string) (string, error) {
	dir := filepath.Join(srcDir(root), worktreeName(ref))

	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
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

// startDaemon runs objgitd with its working directory set to the module root,
// because objgitd loads its credentials from the .env file there. The context
// is deliberately not attached: shutdown goes through stopDaemon so the daemon
// gets the same SIGINT it would in production.
func startDaemon(root, bin, logPath string, args []string) (*exec.Cmd, error) {
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
	case <-time.After(30 * time.Second):
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
