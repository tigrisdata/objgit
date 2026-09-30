package kube

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// testCluster returns a client for a real cluster, or skips. Set
// OBJGIT_TEST_KUBE_HOST (the API server URL), OBJGIT_TEST_KUBE_CA (a CA file),
// and OBJGIT_TEST_KUBE_TOKEN (a token for ServiceAccount objgit/objgitd with
// manifest/tekton-rbac applied, from kubectl create token). The cluster needs
// the Tekton CRDs and the ci namespace. See docs/usage/kubernetes-hooks.md.
func testCluster(t *testing.T) *Client {
	t.Helper()
	host, caFile, token := os.Getenv("OBJGIT_TEST_KUBE_HOST"), os.Getenv("OBJGIT_TEST_KUBE_CA"), os.Getenv("OBJGIT_TEST_KUBE_TOKEN")
	if host == "" || caFile == "" || token == "" {
		t.Skip("skipping: OBJGIT_TEST_KUBE_HOST, OBJGIT_TEST_KUBE_CA, and OBJGIT_TEST_KUBE_TOKEN are not set")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("%s holds no certificate", caFile)
	}
	return New(Config{
		Host:       host,
		HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}},
		Token:      func() (string, error) { return token, nil },
		Namespace:  "objgit",
	})
}

// TestCluster runs the hook commands' API calls against a real API server as
// the objgitd ServiceAccount, with the example RBAC.
func TestCluster(t *testing.T) {
	c := testCluster(t)
	ctx := context.Background()
	// A new suffix for each run, because the objects stay in the cluster.
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)

	pipeline := Object{
		"apiVersion": "tekton.dev/v1",
		"kind":       "Pipeline",
		"metadata":   map[string]any{"name": "objgit-" + suffix, "namespace": "ci"},
		"spec":       map[string]any{"description": "first"},
	}
	if _, err := c.Apply(ctx, pipeline); err != nil {
		t.Fatalf("apply creates the Pipeline: %v", err)
	}
	pipeline["spec"] = map[string]any{"description": "second"}
	res, err := c.Apply(ctx, pipeline)
	if err != nil {
		t.Fatalf("re-apply patches the Pipeline: %v", err)
	}
	if got := res.Object["spec"].(map[string]any)["description"]; got != "second" {
		t.Errorf("re-applied description = %v, want second", got)
	}

	// Another field manager takes the field, so objgitd's next apply conflicts.
	task := Object{
		"apiVersion": "tekton.dev/v1",
		"kind":       "Task",
		"metadata":   map[string]any{"name": "objgit-" + suffix, "namespace": "ci"},
		"spec":       map[string]any{"description": "owned by objgitd"},
	}
	if _, err := c.Apply(ctx, task); err != nil {
		t.Fatalf("apply the Task: %v", err)
	}
	other := Object{
		"apiVersion": "tekton.dev/v1", "kind": "Task",
		"metadata": map[string]any{"name": "objgit-" + suffix, "namespace": "ci"},
		"spec":     map[string]any{"description": "taken by another manager"},
	}
	s := c.Session()
	tres, err := s.resource(ctx, "tekton.dev/v1", "Task")
	if err != nil {
		t.Fatal(err)
	}
	u := c.cfg.Host + collectionPath(tres, "ci") + "/objgit-" + suffix + "?" +
		url.Values{"fieldManager": {"someone-else"}, "force": {"true"}}.Encode()
	if _, err := c.send(ctx, http.MethodPatch, u, "application/apply-patch+yaml", other); err != nil {
		t.Fatalf("forced apply by another manager: %v", err)
	}
	_, err = c.Apply(ctx, task)
	if apiErr, ok := errors.AsType[*APIError](err); !ok || apiErr.Code != http.StatusConflict {
		t.Errorf("apply over another manager's field = %v, want 409 Conflict", err)
	} else {
		t.Logf("conflict message: %s", apiErr.Message)
	}

	run := Object{
		"apiVersion": "tekton.dev/v1",
		"kind":       "PipelineRun",
		"metadata":   map[string]any{"generateName": "objgit-" + suffix + "-", "namespace": "ci"},
		"spec":       map[string]any{"pipelineRef": map[string]any{"name": "objgit-" + suffix}},
	}
	var names []string
	for range 2 {
		res, err := c.Create(ctx, run)
		if err != nil {
			t.Fatalf("create PipelineRun: %v", err)
		}
		names = append(names, res.Object.Name())
	}
	if names[0] == names[1] || !strings.HasPrefix(names[0], "objgit-"+suffix+"-") {
		t.Errorf("run names = %q, want two different generated names", names)
	}

	denied := []struct {
		name string
		obj  Object
	}{
		{"a ConfigMap in ci", Object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "x", "namespace": "ci"}}},
		{"a Pipeline in default", Object{"apiVersion": "tekton.dev/v1", "kind": "Pipeline", "metadata": map[string]any{"name": "x", "namespace": "default"}}},
	}
	for _, tt := range denied {
		_, err := c.Apply(ctx, tt.obj)
		if apiErr, ok := errors.AsType[*APIError](err); !ok || apiErr.Code != http.StatusForbidden {
			t.Errorf("apply %s = %v, want 403 Forbidden", tt.name, err)
		} else {
			t.Logf("%s: %s", tt.name, apiErr.Message)
		}
	}
}
