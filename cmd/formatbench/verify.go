package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// verifyLimit caps each of the three checks. They are local git operations
// against an on-disk clone, so they finish in seconds even for a large
// repository; the limit only stops a wedged subprocess.
const verifyLimit = 10 * time.Minute

// verifyResult says whether the repository that came back out of the bucket is
// the repository that went in. A benchmark that measures a corrupt push is
// worthless, so a cell that fails here is reported as INVALID and its timings
// are withheld rather than printed with a footnote.
type verifyResult struct {
	Ran bool `json:"ran"`
	OK  bool `json:"ok"`

	SourceCommits string `json:"source_commits,omitempty"`
	CloneCommits  string `json:"clone_commits,omitempty"`

	Problems []string `json:"problems,omitempty"`
}

// verifyClone compares a clone against the mirror it came from. It checks three
// things: the same number of reachable commits, the same set of refs pointing
// at the same hashes, and an intact object graph.
func verifyClone(ctx context.Context, mirror, clone string) verifyResult {
	out := verifyResult{Ran: true, OK: true}

	srcCount, _, err := runGit(ctx, verifyLimit, mirror, "rev-list", "--all", "--count")
	if err != nil {
		return out.fail("counting commits in the mirror: %v", err)
	}
	out.SourceCommits = strings.TrimSpace(srcCount)

	dstCount, _, err := runGit(ctx, verifyLimit, clone, "rev-list", "--all", "--count")
	if err != nil {
		return out.fail("counting commits in the clone: %v", err)
	}
	out.CloneCommits = strings.TrimSpace(dstCount)

	if out.SourceCommits != out.CloneCommits {
		out = out.fail("mirror has %s reachable commits, clone has %s", out.SourceCommits, out.CloneCommits)
	}

	srcRefs, err := benchRefs(ctx, mirror)
	if err != nil {
		return out.fail("reading refs from the mirror: %v", err)
	}

	dstRefs, err := benchRefs(ctx, clone)
	if err != nil {
		return out.fail("reading refs from the clone: %v", err)
	}

	for _, diff := range diffRefs(srcRefs, dstRefs) {
		out = out.fail("%s", diff)
	}

	if _, _, err := runGit(ctx, verifyLimit, clone, "fsck", "--full", "--no-progress"); err != nil {
		out = out.fail("fsck on the clone: %v", err)
	}

	return out
}

func (r verifyResult) fail(format string, args ...any) verifyResult {
	r.OK = false
	r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
	return r
}

// benchRefs reads a repository's refs as name to hash. HEAD is left out: the
// mirror carries the upstream default branch and the daemon repoints a dangling
// HEAD on load, so the two disagree for reasons that have nothing to do with
// object storage.
func benchRefs(ctx context.Context, dir string) (map[string]string, error) {
	out, _, err := runGit(ctx, verifyLimit, dir, "show-ref")
	if err != nil {
		return nil, err
	}

	refs := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		hash, name, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			continue
		}
		refs[name] = hash
	}

	return refs, nil
}

// diffRefs reports every ref that is missing, extra, or pointing somewhere
// else. It returns the problems sorted, so two runs of the same fault read the
// same way.
func diffRefs(src, dst map[string]string) []string {
	var problems []string

	for name, want := range src {
		got, ok := dst[name]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("clone is missing ref %s", name))
		case got != want:
			problems = append(problems, fmt.Sprintf("ref %s is %s in the mirror and %s in the clone", name, want, got))
		}
	}

	for name := range dst {
		if _, ok := src[name]; !ok {
			problems = append(problems, fmt.Sprintf("clone has an extra ref %s", name))
		}
	}

	sort.Strings(problems)
	return problems
}
