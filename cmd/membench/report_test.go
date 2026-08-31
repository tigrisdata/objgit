package main

import (
	"regexp"
	"testing"
	"time"
)

// at builds a sample n seconds after a fixed origin, so window arithmetic in
// these tests reads as plainly as it can.
func at(origin time.Time, sec int, phase string, rss, heap uint64) sample {
	return sample{
		At:    origin.Add(time.Duration(sec) * time.Second),
		Phase: phase,
		Proc:  procStatus{VmRSS: rss, VmHWM: rss},
		Go:    goMetrics{HeapInuse: heap},
		GoOK:  true,
	}
}

// atFailedScrape is a tick where /proc was read but the metrics endpoint did
// not answer, which is what the last sample before shutdown looks like.
func atFailedScrape(origin time.Time, sec int, phase string, rss uint64) sample {
	s := at(origin, sec, phase, rss, 0)
	s.Go = goMetrics{}
	s.GoOK = false
	return s
}

func TestPhaseTailSkipsFailedScrapes(t *testing.T) {
	t.Parallel()

	origin := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	samples := []sample{
		at(origin, 0, phaseIdle, 700, 500),
		atFailedScrape(origin, 1, phaseIdle, 690),
	}

	got, ok := phaseTail(samples, phaseIdle)
	if !ok {
		t.Fatal("wanted the last good sample, got none")
	}
	if got.Go.HeapInuse != 500 {
		t.Logf("want: 500")
		t.Logf("got:  %d", got.Go.HeapInuse)
		t.Error("a failed scrape was reported as a settled heap")
	}
}

func TestWindowPeakIgnoresFailedScrapeHeap(t *testing.T) {
	t.Parallel()

	origin := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	samples := []sample{
		at(origin, 1, phaseSeq, 100, 60),
		atFailedScrape(origin, 2, phaseSeq, 900),
	}

	rss, heap := windowPeak(samples, origin.Add(time.Second), origin.Add(2*time.Second), 0)
	if rss != 900 {
		t.Logf("want rss: 900")
		t.Logf("got rss:  %d", rss)
		t.Error("proc numbers should survive a failed scrape")
	}
	if heap != 60 {
		t.Logf("want heap: 60")
		t.Logf("got heap:  %d", heap)
		t.Error("heap from a failed scrape leaked into the peak")
	}
}

func TestWindowPeak(t *testing.T) {
	t.Parallel()

	origin := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	samples := []sample{
		at(origin, 0, phaseBaseline, 10, 5),
		at(origin, 1, phaseSeq, 40, 20),
		at(origin, 2, phaseSeq, 90, 60),
		at(origin, 3, phaseIdle, 30, 10),
		at(origin, 9, phaseSeq, 999, 999),
	}

	for _, tt := range []struct {
		name       string
		start, end int
		slack      time.Duration
		wantRSS    uint64
		wantHeap   uint64
	}{
		{name: "covers the push window", start: 1, end: 2, wantRSS: 90, wantHeap: 60},
		{name: "slack pulls in the settling sample", start: 1, end: 2, slack: time.Second, wantRSS: 90, wantHeap: 60},
		{name: "excludes a later unrelated spike", start: 0, end: 3, wantRSS: 90, wantHeap: 60},
		{name: "empty window", start: 5, end: 6, wantRSS: 0, wantHeap: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			start := origin.Add(time.Duration(tt.start) * time.Second)
			end := origin.Add(time.Duration(tt.end) * time.Second)

			rss, heap := windowPeak(samples, start, end, tt.slack)
			if rss != tt.wantRSS || heap != tt.wantHeap {
				t.Logf("want: rss=%d heap=%d", tt.wantRSS, tt.wantHeap)
				t.Logf("got:  rss=%d heap=%d", rss, heap)
				t.Error("wrong peak")
			}
		})
	}
}

func TestRSSAt(t *testing.T) {
	t.Parallel()

	origin := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	samples := []sample{
		at(origin, 1, phaseBaseline, 10, 5),
		at(origin, 2, phaseSeq, 90, 60),
		at(origin, 3, phaseIdle, 70, 40),
	}

	for _, tt := range []struct {
		name string
		sec  int
		want uint64
	}{
		{name: "before the first sample", sec: 0, want: 0},
		{name: "exactly on a sample", sec: 2, want: 90},
		{name: "between samples takes the earlier one", sec: 2, want: 90},
		{name: "after the last sample", sec: 99, want: 70},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := rssAt(samples, origin.Add(time.Duration(tt.sec)*time.Second))
			if got != tt.want {
				t.Logf("want: %d", tt.want)
				t.Logf("got:  %d", got)
				t.Error("wrong reading")
			}
		})
	}
}

func TestPhaseTail(t *testing.T) {
	t.Parallel()

	origin := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	samples := []sample{
		at(origin, 0, phaseBaseline, 10, 5),
		at(origin, 1, phaseBaseline, 12, 6),
		at(origin, 2, phaseSeq, 90, 60),
	}

	for _, tt := range []struct {
		name    string
		phase   string
		wantRSS uint64
		wantOK  bool
	}{
		{name: "last baseline sample wins", phase: phaseBaseline, wantRSS: 12, wantOK: true},
		{name: "only sample in the phase", phase: phaseSeq, wantRSS: 90, wantOK: true},
		{name: "phase never happened", phase: phaseConc, wantOK: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := phaseTail(samples, tt.phase)
			if ok != tt.wantOK {
				t.Logf("want ok: %v", tt.wantOK)
				t.Logf("got ok:  %v", ok)
				t.Fatal("wrong presence")
			}
			if ok && got.Proc.VmRSS != tt.wantRSS {
				t.Logf("want: %d", tt.wantRSS)
				t.Logf("got:  %d", got.Proc.VmRSS)
				t.Error("wrong sample")
			}
		})
	}
}

func TestConcurrencySteps(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		pushes []pushResult
		want   []int
	}{
		{
			name: "distinct levels, ascending, sequential pushes ignored",
			pushes: []pushResult{
				{Phase: phaseSeq, Concurrency: 1},
				{Phase: phaseConc, Concurrency: 4},
				{Phase: phaseConc, Concurrency: 4},
				{Phase: phaseConc, Concurrency: 1},
				{Phase: phaseConc, Concurrency: 2},
			},
			want: []int{1, 2, 4},
		},
		{name: "no concurrency phase", pushes: []pushResult{{Phase: phaseSeq, Concurrency: 1}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := concurrencySteps(tt.pushes)
			if len(got) != len(tt.want) {
				t.Logf("want: %v", tt.want)
				t.Logf("got:  %v", got)
				t.Fatal("wrong number of steps")
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Logf("want: %v", tt.want)
					t.Logf("got:  %v", got)
					t.Fatal("wrong steps")
				}
			}
		})
	}
}

func TestGroupWindow(t *testing.T) {
	t.Parallel()

	origin := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	group := []pushResult{
		{Start: origin.Add(2 * time.Second), End: origin.Add(5 * time.Second)},
		{Start: origin, End: origin.Add(3 * time.Second)},
		{Start: origin.Add(time.Second), End: origin.Add(9 * time.Second)},
	}

	start, end := groupWindow(group)
	if !start.Equal(origin) {
		t.Logf("want: %v", origin)
		t.Logf("got:  %v", start)
		t.Error("wrong window start")
	}
	if want := origin.Add(9 * time.Second); !end.Equal(want) {
		t.Logf("want: %v", want)
		t.Logf("got:  %v", end)
		t.Error("wrong window end")
	}
}

func TestParseSteps(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		input   string
		want    []int
		wantErr bool
	}{
		{name: "the default sweep", input: "1,2,4,8", want: []int{1, 2, 4, 8}},
		{name: "spaces and a trailing comma", input: " 1, 3 ,", want: []int{1, 3}},
		{name: "empty disables the sweep", input: "  "},
		{name: "not a number", input: "1,two", wantErr: true},
		{name: "zero is not a concurrency", input: "0", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseSteps(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("wanted an error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(got) != len(tt.want) {
				t.Logf("want: %v", tt.want)
				t.Logf("got:  %v", got)
				t.Fatal("wrong number of steps")
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Logf("want: %v", tt.want)
					t.Logf("got:  %v", got)
					t.Fatal("wrong steps")
				}
			}
		})
	}
}

func TestNewUUID(t *testing.T) {
	t.Parallel()

	shape := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

	seen := map[string]bool{}
	for range 100 {
		got, err := newUUID()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !shape.MatchString(got) {
			t.Logf("want: a version 4 UUID")
			t.Logf("got:  %s", got)
			t.Fatal("wrong shape")
		}
		if seen[got] {
			t.Fatalf("%s came back twice", got)
		}
		seen[got] = true
	}
}
