package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// gitEnv isolates every git subprocess from the operator's own configuration,
// so a stray push.default or http.postBuffer in ~/.gitconfig cannot move a
// measurement. Same treatment tryGit gives the protocol tests.
var gitEnv = []string{
	"GIT_CONFIG_GLOBAL=/dev/null",
	"GIT_CONFIG_SYSTEM=/dev/null",
	"GIT_TERMINAL_PROMPT=0",
	"GIT_ASKPASS=/bin/true",
}

// opResult is one timed git operation against the daemon under test.
type opResult struct {
	Wall    time.Duration `json:"wall_ns"`
	WallStr string        `json:"wall"`

	S3 s3Counters `json:"s3"`

	// WireBytes is what git reported receiving. Clone only; a push does not
	// print a comparable figure.
	WireBytes int64 `json:"wire_bytes,omitempty"`

	// DNF marks an operation the harness stopped at its time limit. Its Wall is
	// the limit, not a completion time, and the S3 counters are however far it
	// got. A DNF is a result and gets reported as one; it is never extrapolated
	// into a finish time.
	DNF bool `json:"dnf,omitempty"`

	Err string `json:"err,omitempty"`
}

func (r opResult) ok() bool { return r.Err == "" && !r.DNF }

// runGit runs one git command under a time limit and returns its combined
// output. The boolean reports whether the limit was what stopped it.
func runGit(ctx context.Context, limit time.Duration, dir string, args ...string) (string, bool, error) {
	runCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(cmd.Environ(), gitEnv...)

	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	err := cmd.Run()
	out := strings.TrimSpace(buf.String())

	// A timeout kills the child, which surfaces as a signal error rather than a
	// deadline error, so ask the context which one happened.
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return out, true, fmt.Errorf("git %s stopped at the %s limit", args[0], limit)
	}
	if err != nil {
		return out, false, fmt.Errorf("git %s: %w: %s", args[0], err, out)
	}

	return out, false, nil
}

// gitPush sends the whole repository to a prefix that does not exist yet.
//
// --mirror rather than a heads-and-tags refspec, because the point is to
// measure storing an entire repository: a mirror clone of a GitHub project
// also carries refs/notes/* and refs/pull/*, and those hold commits that a
// heads-and-tags push leaves behind. Leaving them behind would understate the
// work on both sides of the comparison and would make every clone disagree
// with its mirror in verifyClone.
func gitPush(ctx context.Context, limit time.Duration, mirror, url string) (time.Duration, bool, error) {
	start := time.Now()
	_, dnf, err := runGit(ctx, limit, mirror, "push", "--quiet", "--mirror", url)

	return time.Since(start), dnf, err
}

// gitClone pulls the repository back out. --progress forces the receive
// counters onto stderr even though the harness is not a terminal, which is
// where the wire byte count comes from.
func gitClone(ctx context.Context, limit time.Duration, url, dst string) (time.Duration, int64, bool, error) {
	start := time.Now()
	out, dnf, err := runGit(ctx, limit, "", "clone", "--mirror", "--progress", url, dst)
	wall := time.Since(start)

	return wall, parseReceivedBytes(out), dnf, err
}

// receivedRe matches the byte total git prints once a pack is fully received,
// for example "Receiving objects: 100% (59459/59459), 80.86 MiB | 4.20 MiB/s".
// A small pack is reported in bytes instead ("244 bytes"), so both spellings
// are accepted. Only the 100% line matches, so the partial progress lines that
// precede it on the same stream are ignored.
var receivedRe = regexp.MustCompile(`Receiving objects: *100%[^,]*, *([0-9.]+) *(bytes|[KMG]iB)`)

// parseReceivedBytes pulls the pack size off git's progress output. It returns
// zero when the line is absent, which happens for an empty clone or a clone
// that failed before any data moved.
func parseReceivedBytes(out string) int64 {
	m := receivedRe.FindStringSubmatch(out)
	if m == nil {
		return 0
	}

	n, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0
	}

	switch m[2] {
	case "KiB":
		n *= 1 << 10
	case "MiB":
		n *= 1 << 20
	case "GiB":
		n *= 1 << 30
	}

	return int64(n)
}

// mirrorClone copies a source repository once. Every push in the run reads from
// this immutable copy, so the harness never writes to whatever it was pointed
// at and every repetition sends byte-identical history.
func mirrorClone(ctx context.Context, src, dst string) error {
	cmd := exec.CommandContext(ctx, "git", "clone", "--mirror", src, dst)
	cmd.Env = append(cmd.Environ(), gitEnv...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("formatbench: can't mirror %s: %w: %s", src, err, strings.TrimSpace(string(out)))
	}

	return nil
}
