package kube

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tigrisdata/objgit/internal/kube/kubetest"
)

// testClient returns a client for srv whose default namespace is "objgit".
func testClient(srv *kubetest.Server) *Client {
	return New(Config{
		Host:       srv.URL,
		HTTPClient: srv.Client(),
		Token:      func() (string, error) { return srv.Token(), nil },
		Namespace:  "objgit",
	})
}

func obj(apiVersion, kind, namespace, name string) Object {
	meta := map[string]any{"name": name}
	if namespace != "" {
		meta["namespace"] = namespace
	}
	return Object{"apiVersion": apiVersion, "kind": kind, "metadata": meta, "spec": map[string]any{"x": "y"}}
}

func TestApply(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*kubetest.Server)
		obj      Object
		wantPath string
		wantCode int    // APIError status code; 0 means success
		wantErr  string // error substring when the error is not an APIError
	}{
		{
			name:     "namespaced object without a namespace uses the default",
			obj:      obj("v1", "ConfigMap", "", "greeting"),
			wantPath: "/api/v1/namespaces/objgit/configmaps/greeting",
		},
		{
			name:     "metadata.namespace wins over the default",
			obj:      obj("tekton.dev/v1beta1", "Pipeline", "ci", "build"),
			wantPath: "/apis/tekton.dev/v1beta1/namespaces/ci/pipelines/build",
		},
		{
			name:     "cluster-scoped object has no namespace in its path",
			obj:      obj("v1", "Namespace", "", "ci"),
			wantPath: "/api/v1/namespaces/ci",
		},
		{
			name: "re-apply of an object objgitd owns",
			setup: func(s *kubetest.Server) {
				s.Put("/apis/tekton.dev/v1/namespaces/ci/pipelines/build", FieldManager, map[string]any{})
			},
			obj:      obj("tekton.dev/v1", "Pipeline", "ci", "build"),
			wantPath: "/apis/tekton.dev/v1/namespaces/ci/pipelines/build",
		},
		{
			name: "field conflict with another manager",
			setup: func(s *kubetest.Server) {
				s.Put("/apis/tekton.dev/v1/namespaces/ci/pipelines/build", "kubectl", map[string]any{})
			},
			obj:      obj("tekton.dev/v1", "Pipeline", "ci", "build"),
			wantPath: "/apis/tekton.dev/v1/namespaces/ci/pipelines/build",
			wantCode: http.StatusConflict,
		},
		{
			name:     "RBAC denial",
			setup:    func(s *kubetest.Server) { s.Deny("create", "pipelines") },
			obj:      obj("tekton.dev/v1", "Pipeline", "ci", "build"),
			wantPath: "/apis/tekton.dev/v1/namespaces/ci/pipelines/build",
			wantCode: http.StatusForbidden,
		},
		{
			// A real API server checks patch for every apply, and create as
			// well when the object is new.
			name:     "RBAC denial of patch on a new object",
			setup:    func(s *kubetest.Server) { s.Deny("patch", "pipelines") },
			obj:      obj("tekton.dev/v1", "Pipeline", "ci", "build"),
			wantPath: "/apis/tekton.dev/v1/namespaces/ci/pipelines/build",
			wantCode: http.StatusForbidden,
		},
		{
			name:    "unknown kind",
			obj:     obj("v1", "Widget", "", "w"),
			wantErr: `no resource of kind "Widget" in v1`,
		},
		{
			name:    "apiVersion the server does not serve",
			obj:     obj("example.com/v1", "Widget", "", "w"),
			wantErr: "example.com/v1 is not served",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := kubetest.New(t)
			if tt.setup != nil {
				tt.setup(srv)
			}
			res, err := testClient(srv).Apply(context.Background(), tt.obj)
			switch {
			case tt.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
				}
				if n := len(srv.Requests()); n != 0 {
					t.Errorf("%d mutating requests sent, want 0", n)
				}
				return
			case tt.wantCode != 0:
				apiErr, ok := errors.AsType[*APIError](err)
				if !ok || apiErr.Code != tt.wantCode {
					t.Fatalf("err = %v, want an APIError with code %d", err, tt.wantCode)
				}
				if apiErr.Message == "" || !strings.Contains(err.Error(), apiErr.Message) {
					t.Errorf("err = %q, want it to carry the API message %q", err, apiErr.Message)
				}
			case err != nil:
				t.Fatalf("Apply: %v", err)
			default:
				if res.Object.Name() != tt.obj.Name() {
					t.Errorf("result name = %q, want %q", res.Object.Name(), tt.obj.Name())
				}
				if _, ok := srv.Object(tt.wantPath); !ok {
					t.Errorf("no object stored at %s", tt.wantPath)
				}
			}

			reqs := srv.Requests()
			if len(reqs) != 1 {
				t.Fatalf("%d mutating requests, want 1", len(reqs))
			}
			r := reqs[0]
			if r.Method != http.MethodPatch || r.Path != tt.wantPath {
				t.Errorf("request = %s %s, want PATCH %s", r.Method, r.Path, tt.wantPath)
			}
			if r.ContentType != "application/apply-patch+yaml" {
				t.Errorf("Content-Type = %q, want application/apply-patch+yaml", r.ContentType)
			}
			if r.Query["fieldManager"] != "objgitd" || r.Query["force"] != "false" {
				t.Errorf("query = %v, want fieldManager=objgitd and force=false", r.Query)
			}
		})
	}
}

func TestApplyRejectsMalformedAPIVersion(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion string
	}{
		{"escaped slashes reach another path", "v1%2Fnamespaces%2Fci%2Fsecrets%2Fx"},
		{"escaped slashes in a group", "tekton.dev/v1%2Fnamespaces%2Fci"},
		{"a query string", "tekton.dev/v1?dryRun=All&x="},
		{"a fragment", "v1#x"},
		{"too many slashes", "tekton.dev/v1/pipelines"},
		{"an empty group", "/v1"},
		{"an empty version", "tekton.dev/"},
		{"an upper-case group", "Tekton.dev/v1"},
		{"a dot segment", "../v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := kubetest.New(t)
			_, err := testClient(srv).Apply(context.Background(), obj(tt.apiVersion, "ConfigMap", "", "a"))
			if err == nil || !strings.Contains(err.Error(), "invalid apiVersion") {
				t.Fatalf("err = %v, want an invalid apiVersion error", err)
			}
			if paths := srv.Paths(); len(paths) != 0 {
				t.Errorf("requests sent for a malformed apiVersion: %q", paths)
			}
		})
	}
}

func TestApplyRejectsIncompleteObjects(t *testing.T) {
	tests := []struct {
		name string
		obj  Object
		want string
	}{
		{"no apiVersion", Object{"kind": "ConfigMap", "metadata": map[string]any{"name": "a"}}, "apiVersion"},
		{"no kind", Object{"apiVersion": "v1", "metadata": map[string]any{"name": "a"}}, "kind"},
		{"no name", Object{"apiVersion": "v1", "kind": "ConfigMap"}, "metadata.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := kubetest.New(t)
			_, err := testClient(srv).Apply(context.Background(), tt.obj)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to name %s", err, tt.want)
			}
		})
	}
}

func TestCreate(t *testing.T) {
	srv := kubetest.New(t)
	run := Object{
		"apiVersion": "tekton.dev/v1",
		"kind":       "PipelineRun",
		"metadata":   map[string]any{"generateName": "x-m-", "namespace": "ci"},
	}
	c := testClient(srv)
	var names []string
	for range 2 {
		res, err := c.Create(context.Background(), run)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if !strings.HasPrefix(res.Object.Name(), "x-m-") || res.Object.Namespace() != "ci" {
			t.Errorf("created %s/%s, want ci/x-m-*", res.Object.Namespace(), res.Object.Name())
		}
		names = append(names, res.Object.Name())
	}
	if names[0] == names[1] {
		t.Errorf("two creates returned the same name %q", names[0])
	}
	for _, r := range srv.Requests() {
		if r.Method != http.MethodPost || r.Path != "/apis/tekton.dev/v1/namespaces/ci/pipelineruns" {
			t.Errorf("request = %s %s, want POST to the pipelineruns collection", r.Method, r.Path)
		}
		if r.ContentType != "application/json" || r.Query["fieldManager"] != "objgitd" {
			t.Errorf("Content-Type %q, query %v", r.ContentType, r.Query)
		}
	}

	srv.Deny("create", "pipelineruns")
	_, err := c.Create(context.Background(), run)
	if apiErr, ok := errors.AsType[*APIError](err); !ok || apiErr.Code != http.StatusForbidden {
		t.Fatalf("err = %v, want a 403 APIError", err)
	}
	if !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("err = %q, want the forbidden message", err)
	}
}

func TestCreateNeedsANameOrGenerateName(t *testing.T) {
	srv := kubetest.New(t)
	_, err := testClient(srv).Create(context.Background(), Object{"apiVersion": "tekton.dev/v1", "kind": "PipelineRun"})
	if err == nil || !strings.Contains(err.Error(), "generateName") {
		t.Fatalf("err = %v, want it to name metadata.generateName", err)
	}
}

func TestInCluster(t *testing.T) {
	srv := kubetest.New(t)
	dir := t.TempDir()
	write := func(name, data string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("ca.crt", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})))
	write("token", srv.Token()+"\n")
	write("namespace", "objgit\n")

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	host, port, _ := net.SplitHostPort(u.Host)
	env := map[string]string{"KUBERNETES_SERVICE_HOST": host, "KUBERNETES_SERVICE_PORT": port}

	c, err := inCluster(dir, func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("inCluster: %v", err)
	}
	if c.Namespace() != "objgit" {
		t.Errorf("Namespace() = %q, want objgit", c.Namespace())
	}
	if _, err := c.Apply(context.Background(), obj("v1", "ConfigMap", "", "a")); err != nil {
		t.Fatalf("Apply with in-cluster config: %v", err)
	}

	// Bound ServiceAccount tokens rotate, so each request reads the file.
	srv.SetToken("rotated")
	write("token", "rotated\n")
	if _, err := c.Apply(context.Background(), obj("v1", "ConfigMap", "", "b")); err != nil {
		t.Fatalf("Apply after token rotation: %v", err)
	}

	if _, err := inCluster(dir, func(string) string { return "" }); !errors.Is(err, ErrNotInCluster) {
		t.Errorf("inCluster without service env = %v, want ErrNotInCluster", err)
	}
}
