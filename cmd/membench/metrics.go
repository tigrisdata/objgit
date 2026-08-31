package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// goMetrics is the subset of objgitd's /metrics this harness records. The Go
// runtime numbers say what the heap is doing; ProcessRSS is the same figure
// /proc reports, kept as a cross-check that the sampler is watching the pid it
// thinks it is.
type goMetrics struct {
	HeapInuse  uint64
	HeapSys    uint64
	NextGC     uint64
	Goroutines uint64
	ProcessRSS uint64
}

// field returns where a metric name lands, or nil for a metric this harness
// does not record.
func (m *goMetrics) field(name string) *uint64 {
	switch name {
	case "go_memstats_heap_inuse_bytes":
		return &m.HeapInuse
	case "go_memstats_heap_sys_bytes":
		return &m.HeapSys
	case "go_memstats_next_gc_bytes":
		return &m.NextGC
	case "go_goroutines":
		return &m.Goroutines
	case "process_resident_memory_bytes":
		return &m.ProcessRSS
	}
	return nil
}

// parseMetrics reads the Prometheus text exposition format far enough to pull
// out the handful of unlabelled gauges above. It deliberately skips any sample
// carrying labels: every metric this harness wants is a single series, so a
// name with a "{" is one of objgitd's own vectors and not a match.
func parseMetrics(r io.Reader) (goMetrics, error) {
	var m goMetrics

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}

		name, value, ok := strings.Cut(line, " ")
		if !ok || strings.ContainsRune(name, '{') {
			continue
		}

		dst := m.field(name)
		if dst == nil {
			continue
		}

		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return m, fmt.Errorf("membench: metric %s has non-numeric value %q: %w", name, value, err)
		}
		if f < 0 {
			f = 0
		}
		*dst = uint64(f)
	}
	if err := sc.Err(); err != nil {
		return m, fmt.Errorf("membench: reading metrics: %w", err)
	}

	return m, nil
}
