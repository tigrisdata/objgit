package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// sample is one row of samples.csv: everything the harness knew about the
// daemon at one instant, tagged with what the daemon was being asked to do.
type sample struct {
	At    time.Time
	Phase string
	Repo  string
	Proc  procStatus
	Go    goMetrics
	// GoOK is false when the /metrics scrape failed for this tick, which
	// happens routinely while the daemon is shutting down. The proc numbers are
	// still good, so the row is kept, but every Go figure in it is a zero value
	// rather than a measurement and must not be read as one.
	GoOK bool
}

// profileCapture records one pprof profile written to disk, so the report can
// say which resident-memory reading each profile belongs to.
type profileCapture struct {
	Name   string
	Path   string
	At     time.Time
	RSS    uint64
	Reason string
}

// sampler polls the daemon's /proc entries and /metrics on a fixed tick, and
// grabs a heap profile whenever resident memory sets a new high-water mark.
//
// The phase and repo labels are set by the driver as it walks the run, so every
// row says what the daemon was doing when it was taken. That is what turns the
// CSV from a wall of numbers into something you can attribute.
type sampler struct {
	pid        int
	metricsURL string
	pprofURL   string
	outDir     string
	interval   time.Duration
	growth     float64
	cooldown   time.Duration
	client     *http.Client

	mu       sync.Mutex
	phase    string
	repo     string
	samples  []sample
	profiles []profileCapture

	peakRSS     uint64
	lastCapture time.Time
}

func newSampler(pid int, metricsAddr, outDir string, interval, cooldown time.Duration, growth float64) *sampler {
	base := "http://" + metricsAddr
	return &sampler{
		pid:        pid,
		metricsURL: base + "/metrics",
		pprofURL:   base + "/debug/pprof",
		outDir:     outDir,
		interval:   interval,
		growth:     growth,
		cooldown:   cooldown,
		client:     &http.Client{Timeout: 30 * time.Second},
	}
}

// label tells the sampler what the driver is about to do. Every sample taken
// until the next call carries these two values.
func (s *sampler) label(phase, repo string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phase, s.repo = phase, repo
}

// run samples until the context is cancelled.
func (s *sampler) run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.collect(ctx)
		}
	}
}

// collect takes one sample. A failed read is logged and dropped rather than
// aborting the run: a single missed tick costs one row, and the benchmark is
// several minutes of pushes we do not want to throw away.
func (s *sampler) collect(ctx context.Context) {
	ps, err := readProcStatus(s.pid)
	if err != nil {
		slog.Debug("can't read proc status", "pid", s.pid, "err", err)
		return
	}

	gm, err := s.fetchMetrics(ctx)
	goOK := err == nil
	if err != nil {
		slog.Debug("can't read metrics", "url", s.metricsURL, "err", err)
	}

	s.mu.Lock()
	now := time.Now()
	s.samples = append(s.samples, sample{At: now, Phase: s.phase, Repo: s.repo, Proc: ps, Go: gm, GoOK: goOK})

	capture := false
	if float64(ps.VmRSS) > float64(s.peakRSS)*(1+s.growth) && now.Sub(s.lastCapture) > s.cooldown {
		capture = true
		s.lastCapture = now
	}
	if ps.VmRSS > s.peakRSS {
		s.peakRSS = ps.VmRSS
	}
	phase, repo := s.phase, s.repo
	s.mu.Unlock()

	if !capture {
		return
	}

	// gc=0 on purpose. Forcing a collection here would change the very number
	// that triggered the capture; we want the heap as it actually was at the
	// peak, sampling error and all.
	name := fmt.Sprintf("heap-peak-%s-%d.pb.gz", now.Format("150405.000"), ps.VmRSS)
	reason := fmt.Sprintf("new RSS high-water mark during %s %s", phase, repo)
	if _, err := s.captureProfile(ctx, "heap", false, name, reason, ps.VmRSS); err != nil {
		slog.Warn("can't capture peak heap profile", "err", err)
	}
}

func (s *sampler) fetchMetrics(ctx context.Context) (goMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.metricsURL, nil)
	if err != nil {
		return goMetrics{}, fmt.Errorf("membench: can't build metrics request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return goMetrics{}, fmt.Errorf("membench: metrics request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return goMetrics{}, fmt.Errorf("membench: %s returned %s", s.metricsURL, resp.Status)
	}

	return parseMetrics(resp.Body)
}

// captureProfile fetches one pprof profile and writes it into the run
// directory. gc forces a collection before the heap is dumped: use it for the
// baseline and the final capture, where a settled heap is what you want, and
// never mid-push.
func (s *sampler) captureProfile(ctx context.Context, kind string, gc bool, name, reason string, rss uint64) (profileCapture, error) {
	url := fmt.Sprintf("%s/%s?gc=0", s.pprofURL, kind)
	if gc {
		url = fmt.Sprintf("%s/%s?gc=1", s.pprofURL, kind)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return profileCapture{}, fmt.Errorf("membench: can't build pprof request: %w", err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return profileCapture{}, fmt.Errorf("membench: pprof request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return profileCapture{}, fmt.Errorf("membench: %s returned %s", url, resp.Status)
	}

	path := filepath.Join(s.outDir, name)
	f, err := os.Create(path)
	if err != nil {
		return profileCapture{}, fmt.Errorf("membench: can't create %s: %w", path, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return profileCapture{}, fmt.Errorf("membench: can't write %s: %w", path, err)
	}

	pc := profileCapture{Name: name, Path: path, At: time.Now(), RSS: rss, Reason: reason}
	s.mu.Lock()
	s.profiles = append(s.profiles, pc)
	s.mu.Unlock()

	slog.Info("captured profile", "name", name, "reason", reason, "rss_bytes", rss)
	return pc, nil
}

// snapshot returns copies of everything collected so far, so the report writer
// never touches the sampler's state while it is still running.
func (s *sampler) snapshot() ([]sample, []profileCapture) {
	s.mu.Lock()
	defer s.mu.Unlock()

	samples := make([]sample, len(s.samples))
	copy(samples, s.samples)
	profiles := make([]profileCapture, len(s.profiles))
	copy(profiles, s.profiles)

	return samples, profiles
}
