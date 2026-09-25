package kube

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/Xe/kefka/command"
	"mvdan.cc/sh/v3/interp"
)

// Metadata that tekton:pipelinerun adds to each run, so a later command can
// find the runs of a commit.
const (
	AnnotationRepo    = "objgit.tigrisdata.com/repo"
	AnnotationRef     = "objgit.tigrisdata.com/ref"
	AnnotationCommit  = "objgit.tigrisdata.com/commit"
	LabelCommitPrefix = "objgit.tigrisdata.com/commit-prefix"
)

// commitPrefixLen is how much of the commit hash the label keeps. A label
// value is at most 63 characters, which a SHA-256 hash does not fit.
const commitPrefixLen = 12

// PipelineRunCommand is tekton:pipelinerun FILE. It reads one PipelineRun
// template from the hook filesystem, points it at the pushed commit, and
// creates it. The template must use metadata.generateName, so each call
// creates a new run.
type PipelineRunCommand struct {
	Client *Client
}

// Exec runs tekton:pipelinerun.
func (p PipelineRunCommand) Exec(ctx context.Context, ec *command.ExecContext, args []string) error {
	const name = "tekton:pipelinerun"
	if len(args) != 1 {
		fmt.Fprintln(ec.Stderr, "usage: tekton:pipelinerun FILE")
		return interp.ExitStatus(2)
	}
	file := args[0]
	commit := env(ec, "OBJGIT_NEW_SHA")
	if commit == "" || strings.Trim(commit, "0") == "" {
		return fail(ec, name, "OBJGIT_NEW_SHA is not set to a commit")
	}

	f, err := ec.FS.Open(resolve(ec.Dir, file))
	if err != nil {
		return fail(ec, name, "%s: %v", file, err)
	}
	data, err := readAll(f)
	if err != nil {
		return fail(ec, name, "%s: %v", file, err)
	}
	objs, err := DecodeYAMLStream(bytes.NewReader(data))
	if err != nil {
		return fail(ec, name, "%s: %v", file, err)
	}
	if len(objs) != 1 {
		return fail(ec, name, "%s holds %d objects, want one PipelineRun", file, len(objs))
	}
	run := objs[0]
	if err := preparePipelineRun(run, commit, env(ec, "OBJGIT_BRANCH"), env(ec, "OBJGIT_REPO"), env(ec, "OBJGIT_REF")); err != nil {
		return fail(ec, name, "%s: %v", file, err)
	}

	res, err := p.Client.Create(ctx, run)
	if err != nil {
		return fail(ec, name, "create %s: %v", strings.TrimSuffix(run.GenerateName(), "-"), err)
	}
	audit(ec, "kube: created", res)
	fmt.Fprintf(ec.Stdout, "%s/%s created in namespace %s\n", res.Resource, res.Object.Name(), res.Object.Namespace())
	return nil
}

// resolve maps a shell path to an fsys-relative path, as the Kefka registry
// does: absolute paths start at the filesystem root, and relative paths start
// at dir.
func resolve(dir, p string) string {
	if path.IsAbs(p) {
		p = strings.TrimPrefix(path.Clean(p), "/")
	} else {
		p = path.Join(dir, p)
	}
	if p == "" {
		return "."
	}
	return p
}

// preparePipelineRun checks that run is a generateName PipelineRun, and sets
// its commit and branch parameters and its objgit metadata.
func preparePipelineRun(run Object, commit, branch, repo, ref string) error {
	group, _, _ := strings.Cut(run.APIVersion(), "/")
	if group != "tekton.dev" || run.Kind() != "PipelineRun" {
		return fmt.Errorf("%s %s is not a tekton.dev PipelineRun", run.APIVersion(), run.Kind())
	}
	if run.Name() != "" {
		return fmt.Errorf("a PipelineRun template must not set metadata.name (%q); use metadata.generateName so each push creates a new run", run.Name())
	}
	if run.GenerateName() == "" {
		return fmt.Errorf("a PipelineRun template needs metadata.generateName")
	}

	spec, _ := run["spec"].(map[string]any)
	params, _ := spec["params"].([]any)
	var haveCommit bool
	for _, p := range params {
		p, ok := p.(map[string]any)
		if !ok {
			continue
		}
		switch p["name"] {
		case "commit":
			p["value"] = commit
			haveCommit = true
		case "branch":
			if branch != "" {
				p["value"] = branch
			}
		}
	}
	if !haveCommit {
		return fmt.Errorf("the PipelineRun has no commit parameter; add one to spec.params")
	}

	meta := run["metadata"].(map[string]any) // GenerateName found it
	annotations := subMap(meta, "annotations")
	for key, v := range map[string]string{AnnotationRepo: repo, AnnotationRef: ref, AnnotationCommit: commit} {
		if v != "" {
			annotations[key] = v
		}
	}
	subMap(meta, "labels")[LabelCommitPrefix] = commit[:min(len(commit), commitPrefixLen)]
	return nil
}

// subMap returns m[key] as a map, and creates it when it is missing.
func subMap(m map[string]any, key string) map[string]any {
	sub, ok := m[key].(map[string]any)
	if !ok {
		sub = map[string]any{}
		m[key] = sub
	}
	return sub
}
