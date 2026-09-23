package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The two series this harness reads. Both builds under test report them with
// the same label values: internal/s3fs feeds metrics.ObserveS3 on the "before"
// side, and (*tigris.Storer).observe feeds the same function on the "after"
// side, so the operation vocabulary (GetObject, PutObject, HeadObject,
// ListObjectsV2, DeleteObject) matches across the format change.
const (
	seriesRequests = "objgit_s3_requests_total"
	seriesSeconds  = "objgit_s3_request_duration_seconds_sum"
)

// s3Counters is one reading of the daemon's S3 counters. Requests is keyed by
// the operation label and sums every status; Errors counts only the samples
// whose status label is not "ok". Seconds is the total time the daemon spent
// inside S3 calls, which separates network cost from local work.
type s3Counters struct {
	Requests map[string]uint64  `json:"requests"`
	Errors   map[string]uint64  `json:"errors,omitempty"`
	Seconds  map[string]float64 `json:"seconds"`
}

func newS3Counters() s3Counters {
	return s3Counters{
		Requests: map[string]uint64{},
		Errors:   map[string]uint64{},
		Seconds:  map[string]float64{},
	}
}

// Total is the request count across every operation.
func (c s3Counters) Total() uint64 {
	var n uint64
	for _, v := range c.Requests {
		n += v
	}
	return n
}

// TotalSeconds is the time spent in S3 calls across every operation.
func (c s3Counters) TotalSeconds() float64 {
	var f float64
	for _, v := range c.Seconds {
		f += v
	}
	return f
}

// Operations lists the operation labels present, sorted, so a report renders
// the same column order every run.
func (c s3Counters) Operations() []string {
	seen := map[string]bool{}
	for k := range c.Requests {
		seen[k] = true
	}
	for k := range c.Seconds {
		seen[k] = true
	}

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)

	return out
}

// sub returns the counters accumulated between two readings. A counter can only
// rise while a process lives, and the harness restarts the daemon between
// measurements, so a negative difference means the reading came from a new
// process and the earlier value is dropped rather than wrapped.
func (c s3Counters) sub(earlier s3Counters) s3Counters {
	out := newS3Counters()

	for k, v := range c.Requests {
		if prev := earlier.Requests[k]; v >= prev {
			out.Requests[k] = v - prev
		} else {
			out.Requests[k] = v
		}
	}
	for k, v := range c.Errors {
		if prev := earlier.Errors[k]; v >= prev {
			if d := v - prev; d > 0 {
				out.Errors[k] = d
			}
		} else {
			out.Errors[k] = v
		}
	}
	for k, v := range c.Seconds {
		if prev := earlier.Seconds[k]; v >= prev {
			out.Seconds[k] = v - prev
		} else {
			out.Seconds[k] = v
		}
	}

	return out
}

// scrapeS3 reads the daemon's /metrics endpoint once.
func scrapeS3(ctx context.Context, metricsAddr string) (s3Counters, error) {
	url := "http://" + metricsAddr + "/metrics"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return s3Counters{}, fmt.Errorf("formatbench: can't build scrape request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return s3Counters{}, fmt.Errorf("formatbench: can't scrape %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return s3Counters{}, fmt.Errorf("formatbench: %s answered %s", url, resp.Status)
	}

	return parseS3Metrics(resp.Body)
}

// parseS3Metrics reads the Prometheus text exposition format and keeps only the
// two S3 series. Every other line is skipped, including the histogram buckets
// and counts that share a prefix with the duration sum.
func parseS3Metrics(r io.Reader) (s3Counters, error) {
	out := newS3Counters()

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}

		name, labels, value, err := parseSample(line)
		if err != nil {
			return out, err
		}

		switch name {
		case seriesRequests:
			op := labels["operation"]
			if op == "" {
				continue
			}
			n := uint64(max(value, 0))
			out.Requests[op] += n
			if labels["status"] != "ok" {
				out.Errors[op] += n
			}
		case seriesSeconds:
			op := labels["operation"]
			if op == "" {
				continue
			}
			out.Seconds[op] += max(value, 0)
		}
	}
	if err := sc.Err(); err != nil {
		return out, fmt.Errorf("formatbench: reading metrics: %w", err)
	}

	return out, nil
}

// parseSample splits one exposition line into its name, its labels, and its
// value.
//
// The line is read left to right, because a sample can carry a trailing
// timestamp: "name{labels} value [timestamp]". Reading from the right would
// take the timestamp as the value, and a timestamp parses as a number, so the
// mistake would be silent.
func parseSample(line string) (string, map[string]string, float64, error) {
	name, labels, rest, err := splitHead(line)
	if err != nil {
		return "", nil, 0, err
	}

	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", nil, 0, fmt.Errorf("formatbench: metric line has no value: %q", line)
	}

	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", nil, 0, fmt.Errorf("formatbench: metric %q has non-numeric value %q", name, fields[0])
	}

	return name, labels, value, nil
}

// splitHead peels the metric name and its label set off the front of a line and
// returns whatever follows. The scan for the closing brace tracks quoting, so a
// label value holding a brace does not end the label set early.
func splitHead(line string) (string, map[string]string, string, error) {
	open := strings.IndexByte(line, '{')
	space := strings.IndexByte(line, ' ')

	if open < 0 || (space >= 0 && space < open) {
		if space < 0 {
			return "", nil, "", fmt.Errorf("formatbench: metric line has no value: %q", line)
		}
		return line[:space], nil, line[space+1:], nil
	}

	inQuote := false
	for i := open + 1; i < len(line); i++ {
		switch {
		case line[i] == '\\' && inQuote:
			i++
		case line[i] == '"':
			inQuote = !inQuote
		case line[i] == '}' && !inQuote:
			return line[:open], parseLabels(line[open+1 : i]), line[i+1:], nil
		}
	}

	return "", nil, "", fmt.Errorf("formatbench: metric line has unbalanced braces: %q", line)
}

// parseLabels reads a comma-separated name="value" list. Backslash escapes
// inside a value are honored, so a label holding a quote or a comma parses.
func parseLabels(raw string) map[string]string {
	out := map[string]string{}

	for i := 0; i < len(raw); {
		eq := strings.IndexByte(raw[i:], '=')
		if eq < 0 {
			break
		}

		key := strings.TrimSpace(raw[i : i+eq])
		i += eq + 1
		if i >= len(raw) || raw[i] != '"' {
			break
		}
		i++

		var val strings.Builder
		for i < len(raw) && raw[i] != '"' {
			if raw[i] == '\\' && i+1 < len(raw) {
				i++
				switch raw[i] {
				case 'n':
					val.WriteByte('\n')
				default:
					val.WriteByte(raw[i])
				}
				i++
				continue
			}
			val.WriteByte(raw[i])
			i++
		}
		i++ // closing quote

		if key != "" {
			out[key] = val.String()
		}

		for i < len(raw) && (raw[i] == ',' || raw[i] == ' ') {
			i++
		}
	}

	return out
}
