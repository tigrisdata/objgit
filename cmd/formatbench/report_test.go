package main

import (
	"strings"
	"testing"
	"time"
)

// mkCell builds a finished, verified cell so a test only has to state the parts
// it cares about.
func mkCell(build, repo string, rep int, pushSecs float64, gets, puts uint64) cell {
	push := opResult{Wall: time.Duration(pushSecs * float64(time.Second)), S3: newS3Counters()}
	push.S3.Requests["GetObject"] = gets
	push.S3.Requests["PutObject"] = puts

	clone := opResult{Wall: time.Second, S3: newS3Counters(), WireBytes: 1 << 20}
	clone.S3.Requests["GetObject"] = gets

	return cell{
		Build:  build,
		Repo:   repo,
		Rep:    rep,
		Push:   push,
		Clone:  clone,
		Usage:  bucketUsage{Keys: 12, Bytes: 4096},
		Verify: verifyResult{Ran: true, OK: true},
	}
}

func TestWallCellReportsMedianAndRange(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		cells []cell
		want  string
	}{
		{
			name:  "one repetition has no range",
			cells: []cell{mkCell("after", "objgit", 1, 4, 10, 2)},
			want:  "4s",
		},
		{
			name: "three repetitions report the middle one",
			cells: []cell{
				mkCell("after", "objgit", 1, 4, 10, 2),
				mkCell("after", "objgit", 2, 9, 10, 2),
				mkCell("after", "objgit", 3, 6, 10, 2),
			},
			want: "6s (4s-9s)",
		},
		{
			name: "an even count takes the lower middle value",
			cells: []cell{
				mkCell("after", "objgit", 1, 4, 10, 2),
				mkCell("after", "objgit", 2, 8, 10, 2),
			},
			want: "4s (4s-8s)",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := wallCell(tt.cells, func(c cell) opResult { return c.Push })
			if got != tt.want {
				t.Logf("want: %s", tt.want)
				t.Logf("got:  %s", got)
				t.Error("wall time cell is wrong")
			}
		})
	}
}

// A push that ran out of time is reported as DNF. It is never turned into an
// estimated finish, and it never averages with the repetitions that finished.
func TestWallCellReportsDNF(t *testing.T) {
	t.Parallel()

	dnf := mkCell("before", "x", 1, 2700, 500, 40000)
	dnf.Push.DNF = true
	dnf.Push.Err = "git push stopped at the 45m0s limit"

	if got := wallCell([]cell{dnf}, func(c cell) opResult { return c.Push }); got != "DNF" {
		t.Logf("want: DNF")
		t.Logf("got:  %s", got)
		t.Error("a push that hit its time limit was reported as a measurement")
	}

	ok := mkCell("before", "x", 2, 100, 500, 40000)
	got := wallCell([]cell{dnf, ok}, func(c cell) opResult { return c.Push })
	if got != "1m40s" {
		t.Logf("want: 1m40s")
		t.Logf("got:  %s", got)
		t.Error("a DNF repetition polluted the median of the ones that finished")
	}
}

// A clone that does not match its mirror means the push was not a valid
// measurement of storing that repository, so none of the cell's numbers count.
func TestInvalidCellIsExcluded(t *testing.T) {
	t.Parallel()

	bad := mkCell("after", "objgit", 1, 4, 10, 2)
	bad.Verify = verifyResult{Ran: true, OK: false, Problems: []string{"clone is missing ref refs/heads/main"}}

	if bad.valid() {
		t.Fatal("a cell whose clone does not match the mirror reported itself as valid")
	}

	if got := wallCell([]cell{bad}, func(c cell) opResult { return c.Push }); got != "-" {
		t.Logf("want: -")
		t.Logf("got:  %s", got)
		t.Error("an INVALID cell contributed a timing to the table")
	}
}

func TestCountCell(t *testing.T) {
	t.Parallel()

	cells := []cell{
		mkCell("before", "objgit", 1, 4, 8500, 20000),
		mkCell("before", "objgit", 2, 4, 8500, 20000),
	}

	for _, tt := range []struct {
		name string
		op   string
		want string
	}{
		{"one operation", "PutObject", "20,000"},
		{"total across operations", "", "28,500"},
		{"an operation that never ran", "HeadObject", "0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := countCell(cells, func(c cell) opResult { return c.Push }, tt.op)
			if got != tt.want {
				t.Logf("want: %s", tt.want)
				t.Logf("got:  %s", got)
				t.Error("request count cell is wrong")
			}
		})
	}
}

func TestRenderReport(t *testing.T) {
	t.Parallel()

	meta := runMeta{
		StartedAt:  time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC),
		FinishedAt: time.Date(2026, 9, 10, 14, 0, 0, 0, time.UTC),
		Host:       "laptop",
		Builds: []buildSpec{
			{Name: "before", Ref: "v1.0.2"},
			{Name: "after"},
		},
		Repos: []repoSpec{{Name: "objgit", URL: "https://example.invalid/objgit", Commits: "1200", Pack: 15 << 20}},
	}

	dnf := mkCell("before", "objgit", 1, 2700, 400, 9000)
	dnf.Push.DNF = true
	dnf.Push.WallStr = "45m0s"

	got := renderReport(meta, []cell{dnf, mkCell("after", "objgit", 1, 12, 40, 3)})

	for _, want := range []string{
		"## Push",
		"## Clone",
		"| Build \"before\" | v1.0.2 |",
		"| Build \"after\" | current checkout |",
		"## Cells that produced no number",
		"push DNF at 45m0s",
		"Oh yeah, this was with my corporate laptop on wifi.",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report is missing %q", want)
		}
	}
}

func TestHumanFloatBytes(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		in   float64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1 << 10, "1.00 KiB"},
		{15 << 20, "15.00 MiB"},
		{1536 << 20, "1.50 GiB"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			if got := humanFloatBytes(tt.in); got != tt.want {
				t.Logf("want: %s", tt.want)
				t.Logf("got:  %s", got)
				t.Error("byte formatting is wrong")
			}
		})
	}
}

func TestPlainNumber(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{12, "12"},
		{123, "123"},
		{1234, "1,234"},
		{20000, "20,000"},
		{2900000, "2,900,000"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			if got := plainNumber(tt.in); got != tt.want {
				t.Logf("want: %s", tt.want)
				t.Logf("got:  %s", got)
				t.Error("number formatting is wrong")
			}
		})
	}
}
