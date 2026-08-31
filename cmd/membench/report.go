package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// pushResult is one git push the harness drove, with the window the sampler
// should be searched over to find what that push cost.
type pushResult struct {
	Phase       string
	Concurrency int
	Repo        string
	Start       time.Time
	End         time.Time
	Err         error
}

func (p pushResult) duration() time.Duration { return p.End.Sub(p.Start) }

// runMeta is everything about the run that is not a measurement, but that the
// numbers are meaningless without. Go's resident set is a function of GOGC and
// GOMEMLIMIT, so a peak-RSS figure recorded without them cannot be compared to
// anything.
type runMeta struct {
	StartedAt   time.Time
	FinishedAt  time.Time
	Host        string
	GoVersion   string
	GOGC        string
	GOMEMLIMIT  string
	NumCPU      int
	SourceRepo  string
	PackBytes   int64
	Org         string
	DaemonArgs  []string
	SampleEvery time.Duration
	WindowSlack time.Duration
	RunDir      string
}

// windowPeak returns the highest resident set and heap-in-use seen between two
// instants. The window is widened by one sample interval on each side because
// a push's cost does not land inside its own wall clock exactly: the daemon is
// still flushing to the bucket when git has already exited.
func windowPeak(samples []sample, start, end time.Time, slack time.Duration) (rss, heap uint64) {
	from, to := start.Add(-slack), end.Add(slack)
	for _, s := range samples {
		if s.At.Before(from) || s.At.After(to) {
			continue
		}
		if s.Proc.VmRSS > rss {
			rss = s.Proc.VmRSS
		}
		if s.GoOK && s.Go.HeapInuse > heap {
			heap = s.Go.HeapInuse
		}
	}
	return rss, heap
}

// rssAt returns the resident set at the most recent sample at or before t, and
// zero when the sampler had not started yet. It is what makes retention legible:
// the reading going into a push, compared with the reading coming out of it.
func rssAt(samples []sample, t time.Time) uint64 {
	var out uint64
	for _, s := range samples {
		if s.At.After(t) {
			break
		}
		out = s.Proc.VmRSS
	}
	return out
}

// phaseTail returns the last sample recorded for a phase, which is the settled
// reading for it: the idle value after everything has drained.
//
// Samples whose metrics scrape failed are skipped. The last tick before the
// daemon exits routinely fails, and taking it would report a settled heap of
// zero rather than the figure that was actually reached.
func phaseTail(samples []sample, phase string) (sample, bool) {
	for i := len(samples) - 1; i >= 0; i-- {
		if samples[i].Phase == phase && samples[i].GoOK {
			return samples[i], true
		}
	}
	return sample{}, false
}

// goCell renders a runtime metric, or an empty cell when that tick's scrape
// failed. Empty is deliberate: a zero here would plot as a cliff to zero.
func goCell(ok bool, v uint64) string {
	if !ok {
		return ""
	}
	return strconv.FormatUint(v, 10)
}

func writeCSV(path string, samples []sample) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("membench: can't create %s: %w", path, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"t_ms", "iso8601", "phase", "repo",
		"vm_rss", "vm_hwm", "pss", "private_dirty",
		"heap_inuse", "heap_sys", "next_gc", "goroutines", "process_rss",
	}
	if err := w.Write(header); err != nil {
		return fmt.Errorf("membench: can't write CSV header: %w", err)
	}

	if len(samples) == 0 {
		return nil
	}

	origin := samples[0].At
	for _, s := range samples {
		row := []string{
			strconv.FormatInt(s.At.Sub(origin).Milliseconds(), 10),
			s.At.Format(time.RFC3339Nano),
			s.Phase,
			s.Repo,
			strconv.FormatUint(s.Proc.VmRSS, 10),
			strconv.FormatUint(s.Proc.VmHWM, 10),
			strconv.FormatUint(s.Proc.Pss, 10),
			strconv.FormatUint(s.Proc.PrivateDirty, 10),
			goCell(s.GoOK, s.Go.HeapInuse),
			goCell(s.GoOK, s.Go.HeapSys),
			goCell(s.GoOK, s.Go.NextGC),
			goCell(s.GoOK, s.Go.Goroutines),
			goCell(s.GoOK, s.Go.ProcessRSS),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("membench: can't write CSV row: %w", err)
		}
	}

	return w.Error()
}

// mib formats bytes as MiB, which is the only unit anyone reads these numbers
// in.
func mib(b uint64) string { return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20)) }

// deltaMiB formats a signed difference in MiB.
func deltaMiB(now, base uint64) string {
	d := (float64(now) - float64(base)) / (1 << 20)
	return fmt.Sprintf("%+.1f MiB", d)
}

func writeReport(path string, meta runMeta, samples []sample, profiles []profileCapture, pushes []pushResult) error {
	var b strings.Builder

	baseRSS, baseHeap := uint64(0), uint64(0)
	if s, ok := phaseTail(samples, phaseBaseline); ok {
		baseRSS, baseHeap = s.Proc.VmRSS, s.Go.HeapInuse
	}

	var hwm uint64
	for _, s := range samples {
		if s.Proc.VmHWM > hwm {
			hwm = s.Proc.VmHWM
		}
	}

	fmt.Fprintf(&b, "# objgitd push memory benchmark\n\n")
	fmt.Fprintf(&b, "Run started %s, finished %s (%s).\n\n",
		meta.StartedAt.Format(time.RFC3339),
		meta.FinishedAt.Format(time.RFC3339),
		meta.FinishedAt.Sub(meta.StartedAt).Round(time.Second))

	fmt.Fprintf(&b, "## Headline\n\n")
	fmt.Fprintf(&b, "- Peak resident set for the whole run (VmHWM): **%s**\n", mib(hwm))
	fmt.Fprintf(&b, "- Idle baseline before any push: %s resident, %s heap in use\n", mib(baseRSS), mib(baseHeap))
	fmt.Fprintf(&b, "- Source pack pushed each time: %s\n", mib(uint64(meta.PackBytes)))
	if s, ok := phaseTail(samples, phaseIdle); ok {
		fmt.Fprintf(&b, "- Settled after the last push, once idle: %s resident, %s heap in use (%s of heap over baseline)\n",
			mib(s.Proc.VmRSS), mib(s.Go.HeapInuse), deltaMiB(s.Go.HeapInuse, baseHeap))
	}
	fmt.Fprintf(&b, "- Repositories created: %d under `%s/`\n\n", len(pushes), meta.Org)

	fmt.Fprintf(&b, "## Run conditions\n\n")
	fmt.Fprintf(&b, "| Setting | Value |\n| --- | --- |\n")
	fmt.Fprintf(&b, "| Host | %s |\n", meta.Host)
	fmt.Fprintf(&b, "| Go | %s |\n", meta.GoVersion)
	fmt.Fprintf(&b, "| GOGC | %s |\n", orDefault(meta.GOGC, "unset (100)"))
	fmt.Fprintf(&b, "| GOMEMLIMIT | %s |\n", orDefault(meta.GOMEMLIMIT, "unset (no limit)"))
	fmt.Fprintf(&b, "| CPUs | %d |\n", meta.NumCPU)
	fmt.Fprintf(&b, "| Source repo | %s |\n", meta.SourceRepo)
	fmt.Fprintf(&b, "| Sample interval | %s |\n", meta.SampleEvery)
	fmt.Fprintf(&b, "| Daemon flags | `%s` |\n", strings.Join(meta.DaemonArgs, " "))
	fmt.Fprintf(&b, "| Run directory | %s |\n\n", meta.RunDir)

	slack := meta.WindowSlack

	fmt.Fprintf(&b, "## Sequential pushes\n\n")
	fmt.Fprintf(&b, "One push at a time, each to a fresh repository, with an idle gap between them. ")
	fmt.Fprintf(&b, "Peaks are taken over the push plus %s of slack, so work the daemon finishes after git exits still counts against the push that caused it.\n\n", slack)
	fmt.Fprintf(&b, "Read the before and after columns together. After is the settled reading once the idle gap has passed: if it tracks upwards push after push instead of falling back to before, the daemon is keeping memory it no longer needs.\n\n")
	fmt.Fprintf(&b, "| # | Repo | Wall clock | RSS before | Peak RSS | Peak heap | RSS after | Kept | Result |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	n := 0
	for _, p := range pushes {
		if p.Phase != phaseSeq {
			continue
		}
		n++
		rss, heap := windowPeak(samples, p.Start, p.End, slack)
		before := rssAt(samples, p.Start)
		after := rssAt(samples, p.End.Add(slack))
		fmt.Fprintf(&b, "| %d | `%s` | %s | %s | %s | %s | %s | %s | %s |\n",
			n, p.Repo, p.duration().Round(time.Millisecond),
			mib(before), mib(rss), mib(heap), mib(after), deltaMiB(after, before), result(p.Err))
	}

	fmt.Fprintf(&b, "\n## Concurrency sweep\n\n")
	fmt.Fprintf(&b, "K simultaneous pushes to K fresh repositories. The rise is measured from the reading going into the step, not from the original idle baseline: ")
	fmt.Fprintf(&b, "if earlier pushes left memory behind, measuring from the baseline would charge that to this step as well.\n\n")
	fmt.Fprintf(&b, "Per concurrent push is the rise divided by K. That is the slope to size a machine with, and it is only meaningful if it stays flat as K grows.\n\n")
	fmt.Fprintf(&b, "| K | Wall clock | RSS before | Peak RSS | Peak heap | Rise | Per concurrent push | Failures |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, step := range concurrencySteps(pushes) {
		group := pushesAt(pushes, step)
		start, end := groupWindow(group)
		rss, heap := windowPeak(samples, start, end, slack)
		before := rssAt(samples, start)
		rise := float64(rss) - float64(before)
		fails := 0
		for _, p := range group {
			if p.Err != nil {
				fails++
			}
		}
		fmt.Fprintf(&b, "| %d | %s | %s | %s | %s | %s | %.1f MiB | %d |\n",
			step, end.Sub(start).Round(time.Millisecond), mib(before), mib(rss), mib(heap),
			deltaMiB(rss, before), rise/float64(step)/(1<<20), fails)
	}

	fmt.Fprintf(&b, "\n## Captured profiles\n\n")
	if len(profiles) == 0 {
		fmt.Fprintf(&b, "None.\n")
	} else {
		fmt.Fprintf(&b, "| File | Taken at | RSS then | Why |\n| --- | --- | --- | --- |\n")
		for _, p := range profiles {
			fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n",
				p.Name, p.At.Format("15:04:05.000"), mib(p.RSS), p.Reason)
		}
	}

	fmt.Fprintf(&b, "\n## Reading the profiles\n\n")
	fmt.Fprintf(&b, "```sh\n")
	fmt.Fprintf(&b, "# What was on the heap at the worst moment.\n")
	fmt.Fprintf(&b, "go tool pprof -http=: %s/heap-peak-*.pb.gz\n\n", meta.RunDir)
	fmt.Fprintf(&b, "# What the pushes left behind: the settled heap minus the idle baseline.\n")
	fmt.Fprintf(&b, "go tool pprof -http=: -base %s/heap-baseline.pb.gz %s/heap-final.pb.gz\n\n", meta.RunDir, meta.RunDir)
	fmt.Fprintf(&b, "# Total allocation over the whole run, which finds the churn GC is working to keep up with.\n")
	fmt.Fprintf(&b, "go tool pprof -sample_index=alloc_space -http=: %s/allocs-final.pb.gz\n", meta.RunDir)
	fmt.Fprintf(&b, "```\n\n")

	fmt.Fprintf(&b, "## Caveats\n\n")
	fmt.Fprintf(&b, "- Peak heap profiles are taken with `gc=0`. Forcing a collection would change the number that triggered the capture, so the profile shows the heap as it was, sampling error included.\n")
	fmt.Fprintf(&b, "- Resident set lags the heap. Go returns freed pages to the kernel lazily, so RSS staying high after a push is not by itself a leak; the baseline-diffed heap profile is what settles that.\n")
	fmt.Fprintf(&b, "- Pushes go to a real Tigris bucket, so wall clock includes network time and varies run to run. Memory does not depend on it, but throughput comparisons across runs do.\n")
	fmt.Fprintf(&b, "- The pack cache is on local disk (`-pack-cache-dir` under the run directory) and is cold at the start of every run. Disk pressure there is not memory pressure.\n")

	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("membench: can't write %s: %w", path, err)
	}

	return nil
}

func orDefault(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func result(err error) string {
	if err != nil {
		return "FAILED: " + err.Error()
	}
	return "ok"
}

// concurrencySteps returns the distinct K values in the concurrency phase, in
// ascending order.
func concurrencySteps(pushes []pushResult) []int {
	seen := map[int]bool{}
	var out []int
	for _, p := range pushes {
		if p.Phase != phaseConc || seen[p.Concurrency] {
			continue
		}
		seen[p.Concurrency] = true
		out = append(out, p.Concurrency)
	}
	sort.Ints(out)
	return out
}

func pushesAt(pushes []pushResult, k int) []pushResult {
	var out []pushResult
	for _, p := range pushes {
		if p.Phase == phaseConc && p.Concurrency == k {
			out = append(out, p)
		}
	}
	return out
}

// groupWindow is the span from the first push starting to the last one
// finishing, which is the window the whole concurrent step occupied.
func groupWindow(group []pushResult) (start, end time.Time) {
	for i, p := range group {
		if i == 0 || p.Start.Before(start) {
			start = p.Start
		}
		if i == 0 || p.End.After(end) {
			end = p.End
		}
	}
	return start, end
}

// writeRepoList records every repository the run created, so the bucket can be
// cleaned up afterwards. The harness never deletes anything itself.
func writeRepoList(path string, org string, pushes []pushResult) error {
	var b strings.Builder
	for _, p := range pushes {
		fmt.Fprintf(&b, "%s/%s\n", org, p.Repo)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("membench: can't write %s: %w", path, err)
	}
	return nil
}

// packBytes totals the pack files in a bare repository, which is how much data
// each push actually moves.
func packBytes(gitDir string) (int64, error) {
	entries, err := os.ReadDir(filepath.Join(gitDir, "objects", "pack"))
	if err != nil {
		return 0, fmt.Errorf("membench: can't read pack directory: %w", err)
	}

	var total int64
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".pack") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, fmt.Errorf("membench: can't stat %s: %w", e.Name(), err)
		}
		total += info.Size()
	}

	return total, nil
}
