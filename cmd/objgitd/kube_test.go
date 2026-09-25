package main

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/kube"
	"github.com/tigrisdata/objgit/internal/kube/kubetest"
	"github.com/tigrisdata/objgit/internal/repofs"
)

// tektonHook is the hook recipe from docs/usage/kubernetes-hooks.md.
const tektonHook = "kustomize build .tekton | kube:apply && tekton:pipelinerun .tekton/testrun.yaml\n"

// TestReceivePackHookKubernetes pushes a repository with an Xe-style .tekton
// bundle and the Tekton hook recipe, against a fake Kubernetes API server.
func TestReceivePackHookKubernetes(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	const pipelinePath = "/apis/tekton.dev/v1beta1/namespaces/ci/pipelines/xe-x-build-test"
	tests := []struct {
		name        string
		enabled     bool
		deny        [2]string // a verb and resource the fake refuses
		wantOutput  []string
		avoidOutput []string
		wantRun     bool
	}{
		{
			name:    "applies the bundle and creates a run for the pushed commit",
			enabled: true,
			wantOutput: []string{
				"pipeline.tekton.dev/xe-x-build-test serverside-applied",
				"pipelinerun.tekton.dev/x-m-00001 created in namespace ci",
			},
			wantRun: true,
		},
		{
			name:        "a failed apply creates no run",
			enabled:     true,
			deny:        [2]string{"create", "pipelines"},
			wantOutput:  []string{`cannot create resource "pipelines"`},
			avoidOutput: []string{"created in namespace"},
		},
		{
			name:        "without -allow-kubernetes the commands explain themselves",
			enabled:     false,
			wantOutput:  []string{"kube:apply: Kubernetes commands are disabled; start objgitd with -allow-kubernetes"},
			avoidOutput: []string{"created in namespace"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := kubetest.New(t)
			if tt.deny[0] != "" {
				srv.Deny(tt.deny[0], tt.deny[1])
			}
			d := &daemon{
				sysFS:       memfs.New(),
				resolver:    repofs.BucketResolver{Base: newMemBase()},
				authz:       auth.AllowAnonymous{AllowWrite: true},
				allowHooks:  true,
				hookTimeout: 2 * time.Minute, // the first kustomize run compiles the module
			}
			if tt.enabled {
				d.kube = kube.New(kube.Config{
					Host:       srv.URL,
					HTTPClient: srv.Client(),
					Token:      func() (string, error) { return srv.Token(), nil },
					Namespace:  "objgit",
				})
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			go func() { _ = d.ServeGitProtocol(ctx, ln) }()

			work := t.TempDir()
			runGit(t, work, "init", "-b", "main")
			runGit(t, work, "config", "user.email", "test@example.com")
			runGit(t, work, "config", "user.name", "Test")
			for _, name := range []string{"kustomization.yaml", "x.yaml", "testrun.yaml"} {
				data, err := os.ReadFile(filepath.Join("..", "..", "internal", "kustomize", "testdata", "tekton", name))
				if err != nil {
					t.Fatal(err)
				}
				writeFile(t, filepath.Join(work, ".tekton", name), string(data))
			}
			writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), tektonHook)
			runGit(t, work, "add", ".")
			runGit(t, work, "commit", "-m", "tekton")
			sha := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))

			out := runGit(t, work, "push", "git://"+ln.Addr().String()+"/xe/x.git", "main")

			for _, want := range tt.wantOutput {
				if !strings.Contains(out, want) {
					t.Errorf("push output lacks %q:\n%s", want, out)
				}
			}
			for _, avoid := range tt.avoidOutput {
				if strings.Contains(out, avoid) {
					t.Errorf("push output has %q:\n%s", avoid, out)
				}
			}

			var runs []kubetest.Request
			for _, r := range srv.Requests() {
				if strings.HasSuffix(r.Path, "/pipelineruns") {
					runs = append(runs, r)
				}
			}
			if !tt.wantRun {
				if len(runs) != 0 {
					t.Errorf("%d PipelineRuns created, want 0", len(runs))
				}
				return
			}
			if _, ok := srv.Object(pipelinePath); !ok {
				t.Errorf("no Pipeline at %s", pipelinePath)
			}
			if len(runs) != 1 {
				t.Fatalf("%d PipelineRuns created, want 1", len(runs))
			}
			meta := runs[0].Body["metadata"].(map[string]any)
			if got := meta["annotations"].(map[string]any)[kube.AnnotationCommit]; got != sha {
				t.Errorf("run commit annotation = %v, want the pushed commit %s", got, sha)
			}
			for _, p := range runs[0].Body["spec"].(map[string]any)["params"].([]any) {
				p := p.(map[string]any)
				if p["name"] == "commit" && p["value"] != sha {
					t.Errorf("commit param = %v, want %s", p["value"], sha)
				}
				if p["name"] == "branch" && p["value"] != "main" {
					t.Errorf("branch param = %v, want main", p["value"])
				}
			}
		})
	}
}
