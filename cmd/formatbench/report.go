package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// cell is one (build, repository, repetition) measurement: a push, a clone, the
// bucket it left behind, and whether that bucket gave the repository back
// unchanged.
type cell struct {
	Build  string `json:"build"`
	Repo   string `json:"repo"`
	Rep    int    `json:"rep"`
	Prefix string `json:"prefix"`

	Push  opResult `json:"push"`
	Clone opResult `json:"clone"`

	Usage  bucketUsage  `json:"usage"`
	Verify verifyResult `json:"verify"`
}

// valid reports whether this cell's timings can be believed. A push that did
// not finish, or a clone that came back different from what went in, is not a
// measurement of anything.
func (c cell) valid() bool {
	return c.Push.ok() && (!c.Verify.Ran || c.Verify.OK)
}

// runMeta records the conditions the numbers were taken under. Wall-clock
// figures from a laptop on wifi mean nothing without them.
type runMeta struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`

	Host       string `json:"host"`
	GoVersion  string `json:"go_version"`
	GitVersion string `json:"git_version"`
	NumCPU     int    `json:"num_cpu"`

	Bucket string `json:"bucket"`
	Org    string `json:"org"`
	RunDir string `json:"run_dir"`

	Builds []buildSpec `json:"builds"`
	Repos  []repoSpec  `json:"repos"`
}

// writeResults saves the raw records and the rendered tables side by side. The
// JSON is what you re-analyze; the Markdown is what goes in the post.
func writeResults(runDir string, meta runMeta, cells []cell) error {
	payload := struct {
		Meta  runMeta `json:"meta"`
		Cells []cell  `json:"cells"`
	}{Meta: meta, Cells: cells}

	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("formatbench: can't encode results: %w", err)
	}

	jsonPath := filepath.Join(runDir, "results.json")
	if err := os.WriteFile(jsonPath, append(raw, '\n'), 0o644); err != nil {
		return fmt.Errorf("formatbench: can't write %s: %w", jsonPath, err)
	}

	mdPath := filepath.Join(runDir, "results.md")
	if err := os.WriteFile(mdPath, []byte(renderReport(meta, cells)), 0o644); err != nil {
		return fmt.Errorf("formatbench: can't write %s: %w", mdPath, err)
	}

	return nil
}

// appendRepoList records one prefix for later cleanup. It appends rather than
// rewrites, so an interrupted run still leaves a complete list.
func appendRepoList(path, prefix string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("formatbench: can't open %s: %w", path, err)
	}
	defer f.Close()

	if _, err := fmt.Fprintln(f, prefix); err != nil {
		return fmt.Errorf("formatbench: can't write %s: %w", path, err)
	}

	return nil
}

// renderReport writes the two tables the blog post needs, plus the conditions
// they were measured under.
func renderReport(meta runMeta, cells []cell) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# objgit storage format: before and after\n\n")
	fmt.Fprintf(&b, "Every row is the median across repetitions. Ranges are min-max where\n")
	fmt.Fprintf(&b, "more than one repetition finished.\n\n")

	writeConditions(&b, meta)
	writePushTable(&b, meta, cells)
	writeCloneTable(&b, meta, cells)
	writeProblems(&b, cells)

	fmt.Fprintf(&b, "\nOh yeah, this was with my corporate laptop on wifi.\n")

	return b.String()
}

func writeConditions(b *strings.Builder, meta runMeta) {
	fmt.Fprintf(b, "## Run conditions\n\n")
	fmt.Fprintf(b, "| Item | Value |\n| --- | --- |\n")
	fmt.Fprintf(b, "| Started | %s |\n", meta.StartedAt.Format(time.RFC3339))
	fmt.Fprintf(b, "| Finished | %s |\n", meta.FinishedAt.Format(time.RFC3339))
	fmt.Fprintf(b, "| Host | %s, %d CPUs |\n", meta.Host, meta.NumCPU)
	fmt.Fprintf(b, "| Go | %s |\n", meta.GoVersion)
	fmt.Fprintf(b, "| Git | %s |\n", meta.GitVersion)
	fmt.Fprintf(b, "| Bucket | %s |\n", meta.Bucket)

	for _, s := range meta.Builds {
		ref := s.Ref
		if ref == "" {
			ref = "current checkout"
		}
		fmt.Fprintf(b, "| Build %q | %s |\n", s.Name, ref)
	}

	fmt.Fprintf(b, "\n### Corpus\n\n")
	fmt.Fprintf(b, "| Repo | URL | Commits | Mirror pack |\n| --- | --- | --: | --: |\n")
	for _, r := range meta.Repos {
		fmt.Fprintf(b, "| %s | %s | %s | %s |\n", r.Name, r.URL, orDash(r.Commits), humanBytes(r.Pack))
	}
	fmt.Fprintln(b)
}

func writePushTable(b *strings.Builder, meta runMeta, cells []cell) {
	fmt.Fprintf(b, "## Push\n\n")
	fmt.Fprintf(b, "| Repo | Build | Wall | S3 requests | PUT | GET | HEAD | LIST | Keys | Bucket bytes |\n")
	fmt.Fprintf(b, "| --- | --- | --: | --: | --: | --: | --: | --: | --: | --: |\n")

	for _, r := range meta.Repos {
		for _, s := range meta.Builds {
			got := pick(cells, s.Name, r.Name)
			if len(got) == 0 {
				continue
			}

			fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				r.Name, s.Name,
				wallCell(got, func(c cell) opResult { return c.Push }),
				countCell(got, func(c cell) opResult { return c.Push }, ""),
				countCell(got, func(c cell) opResult { return c.Push }, "PutObject"),
				countCell(got, func(c cell) opResult { return c.Push }, "GetObject"),
				countCell(got, func(c cell) opResult { return c.Push }, "HeadObject"),
				countCell(got, func(c cell) opResult { return c.Push }, "ListObjectsV2"),
				usageCell(got, func(u bucketUsage) float64 { return float64(u.Keys) }, plainNumber),
				usageCell(got, func(u bucketUsage) float64 { return float64(u.Bytes) }, humanFloatBytes),
			)
		}
	}
	fmt.Fprintln(b)
}

func writeCloneTable(b *strings.Builder, meta runMeta, cells []cell) {
	fmt.Fprintf(b, "## Clone\n\n")
	fmt.Fprintf(b, "| Repo | Build | Wall | S3 requests | GET | HEAD | LIST | Wire bytes |\n")
	fmt.Fprintf(b, "| --- | --- | --: | --: | --: | --: | --: | --: |\n")

	for _, r := range meta.Repos {
		for _, s := range meta.Builds {
			got := pick(cells, s.Name, r.Name)
			if len(got) == 0 {
				continue
			}

			clone := func(c cell) opResult { return c.Clone }
			fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s | %s |\n",
				r.Name, s.Name,
				wallCell(got, clone),
				countCell(got, clone, ""),
				countCell(got, clone, "GetObject"),
				countCell(got, clone, "HeadObject"),
				countCell(got, clone, "ListObjectsV2"),
				wireCell(got),
			)
		}
	}
	fmt.Fprintln(b)
}

// writeProblems lists every cell that did not produce a usable number, with the
// reason. Nothing is silently dropped from the tables above.
func writeProblems(b *strings.Builder, cells []cell) {
	var lines []string

	for _, c := range cells {
		switch {
		case c.Push.DNF:
			lines = append(lines, fmt.Sprintf("- %s/%s rep %d: push DNF at %s, %d S3 requests and %s in the bucket by then",
				c.Build, c.Repo, c.Rep, c.Push.WallStr, c.Push.S3.Total(), humanBytes(c.Usage.Bytes)))
		case c.Push.Err != "":
			lines = append(lines, fmt.Sprintf("- %s/%s rep %d: push failed: %s", c.Build, c.Repo, c.Rep, c.Push.Err))
		case c.Clone.DNF:
			lines = append(lines, fmt.Sprintf("- %s/%s rep %d: clone DNF at %s, %d S3 requests by then",
				c.Build, c.Repo, c.Rep, c.Clone.WallStr, c.Clone.S3.Total()))
		case c.Clone.Err != "":
			lines = append(lines, fmt.Sprintf("- %s/%s rep %d: clone failed: %s", c.Build, c.Repo, c.Rep, c.Clone.Err))
		case c.Verify.Ran && !c.Verify.OK:
			lines = append(lines, fmt.Sprintf("- %s/%s rep %d: INVALID, the clone does not match the mirror: %s",
				c.Build, c.Repo, c.Rep, strings.Join(c.Verify.Problems, "; ")))
		}
	}

	if len(lines) == 0 {
		return
	}

	fmt.Fprintf(b, "## Cells that produced no number\n\n")
	fmt.Fprintf(b, "%s\n", strings.Join(lines, "\n"))
}

// pick returns every cell for one build and repository, in repetition order.
func pick(cells []cell, build, repo string) []cell {
	var out []cell
	for _, c := range cells {
		if c.Build == build && c.Repo == repo {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rep < out[j].Rep })
	return out
}

// wallCell renders the median wall time with its range. A cell where nothing
// finished renders as DNF, never as an extrapolation.
func wallCell(cells []cell, get func(cell) opResult) string {
	var secs []float64
	dnf := false

	for _, c := range cells {
		r := get(c)
		if r.DNF {
			dnf = true
		}
		if !c.valid() || !r.ok() {
			continue
		}
		secs = append(secs, r.Wall.Seconds())
	}

	if len(secs) == 0 {
		if dnf {
			return "DNF"
		}
		return "-"
	}

	return withRange(secs, func(f float64) string {
		return time.Duration(f * float64(time.Second)).Round(100 * time.Millisecond).String()
	})
}

// countCell renders the median request count for one operation. An empty op
// means the total across every operation.
func countCell(cells []cell, get func(cell) opResult, op string) string {
	var counts []float64

	for _, c := range cells {
		r := get(c)
		if !c.valid() || !r.ok() {
			continue
		}
		if op == "" {
			counts = append(counts, float64(r.S3.Total()))
		} else {
			counts = append(counts, float64(r.S3.Requests[op]))
		}
	}

	if len(counts) == 0 {
		return "-"
	}
	return withRange(counts, plainNumber)
}

func usageCell(cells []cell, get func(bucketUsage) float64, render func(float64) string) string {
	var vals []float64
	for _, c := range cells {
		if !c.valid() {
			continue
		}
		vals = append(vals, get(c.Usage))
	}

	if len(vals) == 0 {
		return "-"
	}
	return withRange(vals, render)
}

func wireCell(cells []cell) string {
	var vals []float64
	for _, c := range cells {
		if !c.valid() || !c.Clone.ok() {
			continue
		}
		vals = append(vals, float64(c.Clone.WireBytes))
	}

	if len(vals) == 0 {
		return "-"
	}
	return withRange(vals, humanFloatBytes)
}

// withRange renders the median, and appends the span when the repetitions
// disagree. Wifi is noisy; hiding the spread would misrepresent the number.
func withRange(vals []float64, render func(float64) string) string {
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)

	mid := render(median(sorted))
	lo, hi := render(sorted[0]), render(sorted[len(sorted)-1])
	if len(sorted) < 2 || lo == hi {
		return mid
	}

	return fmt.Sprintf("%s (%s-%s)", mid, lo, hi)
}

// median takes the lower of the two middle values on an even count, so the
// figure printed is always one that was actually measured.
func median(sorted []float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[(len(sorted)-1)/2]
}

func plainNumber(f float64) string {
	n := int64(f)
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return s
	}

	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

func humanBytes(n int64) string { return humanFloatBytes(float64(n)) }

func humanFloatBytes(f float64) string {
	const unit = 1024.0

	if f < unit {
		return fmt.Sprintf("%d B", int64(f))
	}

	div, exp := unit, 0
	for n := f / unit; n >= unit && exp < 3; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %ciB", f/div, "KMGT"[exp])
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
