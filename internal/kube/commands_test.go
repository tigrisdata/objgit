package kube

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/Xe/kefka/command"
	"github.com/Xe/kefka/command/registry"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/tigrisdata/objgit/internal/kube/kubetest"
	"mvdan.cc/sh/v3/expand"
	"mvdan.cc/sh/v3/interp"
)

const testSHA = "0123456789abcdef0123456789abcdef01234567"

// testOrigin is the update that the daemon gives the commands.
var testOrigin = Origin{Repo: "xe/x", Ref: "refs/heads/main", Branch: "main", Commit: testSHA}

// hookEnv is the part of the hook environment the commands read.
var hookEnv = []string{
	"OBJGIT_REPO=xe/x",
	"OBJGIT_REF=refs/heads/main",
	"OBJGIT_BRANCH=main",
	"OBJGIT_NEW_SHA=" + testSHA,
}

// run executes cmd and returns its stdout, stderr, and exit status.
func run(t *testing.T, cmd command.Execer, ec *command.ExecContext, args ...string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	ec.Stdout, ec.Stderr = &stdout, &stderr
	if ec.Environ == nil {
		ec.Environ = expand.ListEnviron(hookEnv...)
	}
	if ec.FS == nil {
		ec.FS = memfs.New()
	}
	err := cmd.Exec(context.Background(), ec, args)
	code := 0
	if err != nil {
		var exit interp.ExitStatus
		if !errors.As(err, &exit) {
			t.Fatalf("Exec returned %v, want nil or an interp.ExitStatus", err)
		}
		code = int(exit)
	}
	return stdout.String(), stderr.String(), code
}

const twoConfigMaps = `apiVersion: v1
kind: ConfigMap
metadata:
  name: a
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: b
  namespace: ci
`

func TestApplyCommand(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*kubetest.Server)
		stdin      string
		args       []string
		wantCode   int
		wantStdout []string
		wantStderr string
		wantPaths  []string // PATCH paths, in order
	}{
		{
			name:       "applies every document in order",
			stdin:      twoConfigMaps,
			wantStdout: []string{"configmap/a serverside-applied", "configmap/b serverside-applied"},
			wantPaths:  []string{"/api/v1/namespaces/objgit/configmaps/a", "/api/v1/namespaces/ci/configmaps/b"},
		},
		{
			name:       "Tekton kinds use their group",
			stdin:      "apiVersion: tekton.dev/v1beta1\nkind: Pipeline\nmetadata:\n  name: build\n  namespace: ci\n",
			wantStdout: []string{"pipeline.tekton.dev/build serverside-applied"},
			wantPaths:  []string{"/apis/tekton.dev/v1beta1/namespaces/ci/pipelines/build"},
		},
		{
			name:       "empty input fails",
			stdin:      "# nothing here\n",
			wantCode:   1,
			wantStderr: "no objects",
		},
		{
			name:       "malformed input fails before any request",
			stdin:      twoConfigMaps + "---\nkind: [\n",
			wantCode:   1,
			wantStderr: "document 3",
		},
		{
			name:       "an invalid later document fails before any request",
			stdin:      twoConfigMaps + "---\napiVersion: v1\nkind: ConfigMap\n",
			wantCode:   1,
			wantStderr: "object 3: ConfigMap object has no metadata.name",
		},
		{
			name: "field conflict fails",
			setup: func(s *kubetest.Server) {
				s.Put("/api/v1/namespaces/objgit/configmaps/a", "kubectl", map[string]any{})
			},
			stdin:      twoConfigMaps,
			wantCode:   1,
			wantStderr: "configmap/a: Apply failed with 1 conflict",
			wantPaths:  []string{"/api/v1/namespaces/objgit/configmaps/a"},
		},
		{
			name:       "RBAC denial fails and stops the stream",
			setup:      func(s *kubetest.Server) { s.Deny("create", "configmaps") },
			stdin:      twoConfigMaps,
			wantCode:   1,
			wantStderr: `cannot create resource "configmaps"`,
			wantPaths:  []string{"/api/v1/namespaces/objgit/configmaps/a"},
		},
		{
			name:       "unknown kind fails",
			stdin:      "apiVersion: v1\nkind: Widget\nmetadata:\n  name: w\n",
			wantCode:   1,
			wantStderr: `no resource of kind "Widget"`,
		},
		{
			name:       "arguments are a usage error",
			args:       []string{"-f", "x.yaml"},
			wantCode:   2,
			wantStderr: "usage: kube:apply",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := kubetest.New(t)
			if tt.setup != nil {
				tt.setup(srv)
			}
			stdout, stderr, code := run(t, ApplyCommand{Client: testClient(srv), Origin: testOrigin},
				&command.ExecContext{Stdin: strings.NewReader(tt.stdin)}, tt.args...)
			if code != tt.wantCode {
				t.Fatalf("exit = %d, want %d; stderr: %s", code, tt.wantCode, stderr)
			}
			for _, want := range tt.wantStdout {
				if !strings.Contains(stdout, want) {
					t.Errorf("stdout lacks %q:\n%s", want, stdout)
				}
			}
			if !strings.Contains(stderr, tt.wantStderr) {
				t.Errorf("stderr = %q, want it to contain %q", stderr, tt.wantStderr)
			}
			var paths []string
			for _, r := range srv.Requests() {
				paths = append(paths, r.Path)
			}
			if strings.Join(paths, " ") != strings.Join(tt.wantPaths, " ") {
				t.Errorf("requests = %q, want %q", paths, tt.wantPaths)
			}
		})
	}
}

func TestApplyCommandReapply(t *testing.T) {
	srv := kubetest.New(t)
	cmd := ApplyCommand{Client: testClient(srv), Origin: testOrigin}
	for i := range 2 {
		if _, stderr, code := run(t, cmd, &command.ExecContext{Stdin: strings.NewReader(twoConfigMaps)}); code != 0 {
			t.Fatalf("apply %d: exit %d; stderr: %s", i+1, code, stderr)
		}
	}
	if n := len(srv.Requests()); n != 4 {
		t.Errorf("%d requests, want 4", n)
	}
}

const testRun = `apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  generateName: x-m-
  namespace: ci
  labels:
    app: x
spec:
  params:
    - name: commit
      value: master
    - name: branch
      value: master
    - name: actor
      value: did:plc:e5nncb3dr5thdkjir5cfaqfe
  pipelineRef:
    name: xe-x-build-test
`

func TestPipelineRunCommand(t *testing.T) {
	srv := kubetest.New(t)
	fsys := memfs.New()
	if err := util.WriteFile(fsys, "src/.tekton/testrun.yaml", []byte(testRun), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := PipelineRunCommand{Client: testClient(srv), Origin: testOrigin}

	stdout, stderr, code := run(t, cmd, &command.ExecContext{Dir: "src", FS: fsys}, ".tekton/testrun.yaml")
	if code != 0 {
		t.Fatalf("exit %d; stderr: %s", code, stderr)
	}
	if want := "pipelinerun.tekton.dev/x-m-00001 created in namespace ci\n"; stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}

	reqs := srv.Requests()
	if len(reqs) != 1 || reqs[0].Method != "POST" || reqs[0].Path != "/apis/tekton.dev/v1/namespaces/ci/pipelineruns" {
		t.Fatalf("requests = %+v, want one POST to ci pipelineruns", reqs)
	}
	body := Object(reqs[0].Body)
	params := map[string]any{}
	for _, p := range body["spec"].(map[string]any)["params"].([]any) {
		p := p.(map[string]any)
		params[p["name"].(string)] = p["value"]
	}
	for name, want := range map[string]any{"commit": testSHA, "branch": "main", "actor": "did:plc:e5nncb3dr5thdkjir5cfaqfe"} {
		if params[name] != want {
			t.Errorf("param %s = %v, want %v", name, params[name], want)
		}
	}
	meta := body["metadata"].(map[string]any)
	annotations, _ := meta["annotations"].(map[string]any)
	for key, want := range map[string]string{
		"objgit.tigrisdata.com/repo":   "xe/x",
		"objgit.tigrisdata.com/ref":    "refs/heads/main",
		"objgit.tigrisdata.com/commit": testSHA,
	} {
		if annotations[key] != want {
			t.Errorf("annotation %s = %v, want %s", key, annotations[key], want)
		}
	}
	labels, _ := meta["labels"].(map[string]any)
	if labels["objgit.tigrisdata.com/commit-prefix"] != testSHA[:12] || labels["app"] != "x" {
		t.Errorf("labels = %v, want the commit prefix label next to app=x", labels)
	}

	// Each call is a new run, even for the same commit.
	stdout, _, code = run(t, cmd, &command.ExecContext{Dir: "src", FS: fsys}, "/src/.tekton/testrun.yaml")
	if code != 0 || !strings.Contains(stdout, "x-m-00002") {
		t.Errorf("second call: exit %d, stdout %q, want run x-m-00002", code, stdout)
	}
	if n := len(srv.Requests()); n != 2 {
		t.Errorf("%d create requests after two calls, want 2", n)
	}
}

func TestPipelineRunCommandDefaultNamespace(t *testing.T) {
	srv := kubetest.New(t)
	fsys := memfs.New()
	manifest := strings.Replace(testRun, "  namespace: ci\n", "", 1)
	if err := util.WriteFile(fsys, "run.yaml", []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := run(t, PipelineRunCommand{Client: testClient(srv), Origin: testOrigin}, &command.ExecContext{FS: fsys}, "run.yaml")
	if code != 0 {
		t.Fatalf("exit %d; stderr: %s", code, stderr)
	}
	if !strings.HasSuffix(stdout, "created in namespace objgit\n") {
		t.Errorf("stdout = %q, want the default namespace", stdout)
	}
	if reqs := srv.Requests(); len(reqs) != 1 || reqs[0].Path != "/apis/tekton.dev/v1/namespaces/objgit/pipelineruns" {
		t.Errorf("requests = %+v, want one POST to objgit pipelineruns", reqs)
	}
}

func TestPipelineRunCommandWithoutBranchParam(t *testing.T) {
	srv := kubetest.New(t)
	fsys := memfs.New()
	manifest := strings.Replace(testRun, "    - name: branch\n      value: master\n", "", 1)
	if err := util.WriteFile(fsys, "run.yaml", []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, stderr, code := run(t, PipelineRunCommand{Client: testClient(srv), Origin: testOrigin}, &command.ExecContext{FS: fsys}, "run.yaml"); code != 0 {
		t.Fatalf("exit %d; stderr: %s", code, stderr)
	}
	for _, p := range srv.Requests()[0].Body["spec"].(map[string]any)["params"].([]any) {
		if p.(map[string]any)["name"] == "branch" {
			t.Errorf("branch parameter was added: %v", p)
		}
	}
}

func TestPipelineRunCommandErrors(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*kubetest.Server)
		manifest   string   // written to run.yaml; "" writes nothing
		origin     *Origin  // nil means testOrigin
		args       []string // nil means run.yaml
		noArgs     bool
		wantCode   int
		wantStderr string
	}{
		{name: "no file argument", manifest: testRun, noArgs: true, wantCode: 2, wantStderr: "usage: tekton:pipelinerun FILE"},
		{name: "two file arguments", manifest: testRun, args: []string{"run.yaml", "run.yaml"}, wantCode: 2, wantStderr: "usage"},
		{name: "missing file", args: []string{"run.yaml"}, wantCode: 1, wantStderr: "run.yaml"},
		{
			name:       "fixed name",
			manifest:   strings.Replace(testRun, "generateName: x-m-", "name: x-m-fixed", 1),
			wantCode:   1,
			wantStderr: "metadata.generateName",
		},
		{
			name:       "name and generateName",
			manifest:   strings.Replace(testRun, "generateName: x-m-", "generateName: x-m-\n  name: fixed", 1),
			wantCode:   1,
			wantStderr: "must not set metadata.name",
		},
		{
			name:       "not a PipelineRun",
			manifest:   strings.Replace(testRun, "kind: PipelineRun", "kind: Pipeline", 1),
			wantCode:   1,
			wantStderr: "not a tekton.dev PipelineRun",
		},
		{
			name:       "not Tekton",
			manifest:   strings.Replace(testRun, "tekton.dev/v1", "example.com/v1", 1),
			wantCode:   1,
			wantStderr: "not a tekton.dev PipelineRun",
		},
		{
			name:       "no commit parameter",
			manifest:   strings.Replace(testRun, "    - name: commit\n      value: master\n", "", 1),
			wantCode:   1,
			wantStderr: "no commit parameter",
		},
		{
			name:       "two documents",
			manifest:   testRun + "---\n" + testRun,
			wantCode:   1,
			wantStderr: "holds 2 objects",
		},
		{
			name:       "no commit in the origin",
			manifest:   testRun,
			origin:     &Origin{Repo: "xe/x", Ref: "refs/heads/main", Branch: "main"},
			wantCode:   1,
			wantStderr: "no commit",
		},
		{
			name:       "an all-zero commit in the origin",
			manifest:   testRun,
			origin:     &Origin{Repo: "xe/x", Commit: strings.Repeat("0", 40)},
			wantCode:   1,
			wantStderr: "no commit",
		},
		{
			name:       "RBAC denial",
			setup:      func(s *kubetest.Server) { s.Deny("create", "pipelineruns") },
			manifest:   testRun,
			wantCode:   1,
			wantStderr: "forbidden",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := kubetest.New(t)
			if tt.setup != nil {
				tt.setup(srv)
			}
			fsys := memfs.New()
			if tt.manifest != "" {
				if err := util.WriteFile(fsys, "run.yaml", []byte(tt.manifest), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			args := tt.args
			if args == nil && !tt.noArgs {
				args = []string{"run.yaml"}
			}
			origin := testOrigin
			if tt.origin != nil {
				origin = *tt.origin
			}
			_, stderr, code := run(t, PipelineRunCommand{Client: testClient(srv), Origin: origin}, &command.ExecContext{FS: fsys}, args...)
			if code != tt.wantCode {
				t.Fatalf("exit = %d, want %d; stderr: %s", code, tt.wantCode, stderr)
			}
			if !strings.Contains(stderr, tt.wantStderr) {
				t.Errorf("stderr = %q, want it to contain %q", stderr, tt.wantStderr)
			}
			if tt.setup == nil && len(srv.Requests()) != 0 {
				t.Errorf("a request was sent for an invalid run")
			}
		})
	}
}

func TestRegister(t *testing.T) {
	srv := kubetest.New(t)
	tests := []struct {
		name       string
		client     *Client
		wantCode   int
		wantStderr string
	}{
		{name: "disabled", client: nil, wantCode: 1, wantStderr: "start objgitd with -allow-kubernetes"},
		{name: "enabled", client: testClient(srv), wantCode: 1, wantStderr: "no objects"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := registry.New()
			Register(reg, tt.client, testOrigin)
			for _, name := range []string{"kube:apply", "tekton:pipelinerun"} {
				cmd, ok := reg.Get(name)
				if !ok {
					t.Fatalf("%s is not registered", name)
				}
				if name != "kube:apply" && tt.client != nil {
					continue
				}
				_, stderr, code := run(t, cmd, &command.ExecContext{Stdin: strings.NewReader("")})
				if code != tt.wantCode || !strings.Contains(stderr, tt.wantStderr) {
					t.Errorf("%s: exit %d, stderr %q; want exit %d and %q", name, code, stderr, tt.wantCode, tt.wantStderr)
				}
			}
		})
	}
}

// TestCommandsIgnoreSpoofedEnvironment runs both commands with OBJGIT_*
// variables that a script changed. The audit log, the run metadata, and the
// parameters must come from the daemon's origin instead.
func TestCommandsIgnoreSpoofedEnvironment(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)

	spoofed := expand.ListEnviron(
		"OBJGIT_REPO=victim/repo",
		"OBJGIT_REF=refs/heads/prod",
		"OBJGIT_BRANCH=prod",
		"OBJGIT_NEW_SHA=ffffffffffffffffffffffffffffffffffffffff",
	)
	srv := kubetest.New(t)
	fsys := memfs.New()
	if err := util.WriteFile(fsys, "run.yaml", []byte(testRun), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, stderr, code := run(t, ApplyCommand{Client: testClient(srv), Origin: testOrigin},
		&command.ExecContext{Stdin: strings.NewReader(twoConfigMaps), Environ: spoofed}); code != 0 {
		t.Fatalf("kube:apply: exit %d; stderr: %s", code, stderr)
	}
	if _, stderr, code := run(t, PipelineRunCommand{Client: testClient(srv), Origin: testOrigin},
		&command.ExecContext{FS: fsys, Environ: spoofed}, "run.yaml"); code != 0 {
		t.Fatalf("tekton:pipelinerun: exit %d; stderr: %s", code, stderr)
	}

	if strings.Contains(logs.String(), "victim") || strings.Contains(logs.String(), "prod") {
		t.Errorf("audit log uses the spoofed environment:\n%s", logs.String())
	}
	if strings.Count(logs.String(), "repo=xe/x") != 3 {
		t.Errorf("audit log lacks repo=xe/x for each change:\n%s", logs.String())
	}

	reqs := srv.Requests()
	body := Object(reqs[len(reqs)-1].Body)
	annotations := body["metadata"].(map[string]any)["annotations"].(map[string]any)
	if annotations[AnnotationRepo] != "xe/x" || annotations[AnnotationRef] != "refs/heads/main" || annotations[AnnotationCommit] != testSHA {
		t.Errorf("annotations = %v, want the origin", annotations)
	}
	for _, p := range body["spec"].(map[string]any)["params"].([]any) {
		p := p.(map[string]any)
		if (p["name"] == "commit" && p["value"] != testSHA) || (p["name"] == "branch" && p["value"] != "main") {
			t.Errorf("param %v, want the origin value", p)
		}
	}
}

// TestPipelineRunCommandWithoutBranch is the SSH sh shell on a tag or a
// commit: the origin has no branch, so the template's branch stays.
func TestPipelineRunCommandWithoutBranch(t *testing.T) {
	srv := kubetest.New(t)
	fsys := memfs.New()
	if err := util.WriteFile(fsys, "run.yaml", []byte(testRun), 0o644); err != nil {
		t.Fatal(err)
	}
	origin := Origin{Repo: "xe/x", Ref: "refs/tags/v1.0", Commit: testSHA}
	if _, stderr, code := run(t, PipelineRunCommand{Client: testClient(srv), Origin: origin}, &command.ExecContext{FS: fsys}, "run.yaml"); code != 0 {
		t.Fatalf("exit %d; stderr: %s", code, stderr)
	}
	for _, p := range srv.Requests()[0].Body["spec"].(map[string]any)["params"].([]any) {
		p := p.(map[string]any)
		if p["name"] == "branch" && p["value"] != "master" {
			t.Errorf("branch param = %v, want the template value master", p["value"])
		}
	}
}
