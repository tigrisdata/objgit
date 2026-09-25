// Package kube is the small Kubernetes API client behind the kube:apply and
// tekton:pipelinerun hook commands. It covers in-cluster configuration,
// discovery of one apiVersion at a time, Server-Side Apply, and create. It
// is not client-go: it has no typed objects, no watch, and no cache beyond
// one command run.
package kube

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// FieldManager is the Server-Side Apply field manager, and the create field
// manager, of every request objgitd sends.
const FieldManager = "objgitd"

// serviceAccountDir is where Kubernetes mounts a pod's ServiceAccount
// credentials.
const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// ErrNotInCluster means the process does not run in a Kubernetes pod.
var ErrNotInCluster = errors.New("kube: KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT are not set; objgitd is not running in a Kubernetes pod")

// Object is one Kubernetes object in its JSON form.
type Object map[string]any

// APIVersion returns the object's apiVersion, or "".
func (o Object) APIVersion() string { s, _ := o["apiVersion"].(string); return s }

// Kind returns the object's kind, or "".
func (o Object) Kind() string { s, _ := o["kind"].(string); return s }

// Name returns metadata.name, or "".
func (o Object) Name() string { return o.meta("name") }

// GenerateName returns metadata.generateName, or "".
func (o Object) GenerateName() string { return o.meta("generateName") }

// Namespace returns metadata.namespace, or "".
func (o Object) Namespace() string { return o.meta("namespace") }

func (o Object) meta(key string) string {
	m, _ := o["metadata"].(map[string]any)
	s, _ := m[key].(string)
	return s
}

// Resource is a discovered API resource.
type Resource struct {
	Group, Version string
	Name           string // the plural resource name, such as "pipelineruns"
	Kind           string
	Namespaced     bool
}

// String returns the kubectl form of the resource: kind in lower case, then
// the group when it is not the core group, such as "pipeline.tekton.dev".
func (r Resource) String() string {
	s := strings.ToLower(r.Kind)
	if r.Group != "" {
		s += "." + r.Group
	}
	return s
}

// Result is the object the API server returned, and its resource.
type Result struct {
	Resource Resource
	Object   Object
}

// APIError is a failed API request. Message is the server's Status message,
// which names the resource and, for a denial, the missing permission.
type APIError struct {
	Code    int    // the HTTP status code
	Reason  string // the Status reason, such as "Forbidden" or "Conflict"
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("kubernetes API: %d %s", e.Code, http.StatusText(e.Code))
	}
	return e.Message
}

// Config configures a Client.
type Config struct {
	Host       string       // the API server base URL, such as https://10.0.0.1:443
	HTTPClient *http.Client // nil uses http.DefaultClient
	// Token returns the bearer token for one request. Nil sends no token.
	Token func() (string, error)
	// Namespace is the namespace of an object without metadata.namespace.
	Namespace string
}

// Client talks to one Kubernetes API server. It is safe for concurrent use.
type Client struct {
	cfg Config
}

// New returns a Client for cfg.
func New(cfg Config) *Client {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	cfg.Host = strings.TrimSuffix(cfg.Host, "/")
	return &Client{cfg: cfg}
}

// InCluster returns a Client that uses the pod's ServiceAccount: the API
// server address from the environment, the mounted CA, and the mounted
// token. The token file is read for each request, because bound tokens
// rotate. The default namespace is the ServiceAccount's namespace.
func InCluster() (*Client, error) {
	return inCluster(serviceAccountDir, os.Getenv)
}

func inCluster(dir string, getenv func(string) string) (*Client, error) {
	host, port := getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, ErrNotInCluster
	}
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("kube: read ServiceAccount CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("kube: %s holds no PEM certificate", filepath.Join(dir, "ca.crt"))
	}
	ns, err := os.ReadFile(filepath.Join(dir, "namespace"))
	if err != nil {
		return nil, fmt.Errorf("kube: read ServiceAccount namespace: %w", err)
	}
	tokenFile := filepath.Join(dir, "token")
	if _, err := os.Stat(tokenFile); err != nil {
		return nil, fmt.Errorf("kube: ServiceAccount token: %w", err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return New(Config{
		Host:       "https://" + net.JoinHostPort(host, port),
		HTTPClient: &http.Client{Transport: transport, Timeout: 30 * time.Second},
		Token: func() (string, error) {
			data, err := os.ReadFile(tokenFile)
			if err != nil {
				return "", fmt.Errorf("kube: read ServiceAccount token: %w", err)
			}
			return strings.TrimSpace(string(data)), nil
		},
		Namespace: strings.TrimSpace(string(ns)),
	}), nil
}

// Namespace returns the namespace of an object without metadata.namespace.
func (c *Client) Namespace() string { return c.cfg.Namespace }

// Session returns a view of c that caches discovery until it is dropped. A
// command takes one per run, so a stream of objects discovers each
// apiVersion once, and a CRD installed later is still found by the next run.
func (c *Client) Session() *Session {
	return &Session{c: c, lists: map[string][]Resource{}}
}

// Apply is c.Session().Apply.
func (c *Client) Apply(ctx context.Context, obj Object) (Result, error) {
	return c.Session().Apply(ctx, obj)
}

// Create is c.Session().Create.
func (c *Client) Create(ctx context.Context, obj Object) (Result, error) {
	return c.Session().Create(ctx, obj)
}

// Session is a Client with a discovery cache. It is safe for concurrent use.
type Session struct {
	c *Client

	mu    sync.Mutex
	lists map[string][]Resource // by apiVersion
}

// Apply sends obj as a Server-Side Apply patch with field manager objgitd.
// Conflicts are not forced: a field that another manager owns fails the
// request with a 409 APIError. The server creates a missing object, which
// needs the create verb; an existing object needs patch.
func (s *Session) Apply(ctx context.Context, obj Object) (Result, error) {
	if err := check(obj, false); err != nil {
		return Result{}, err
	}
	res, err := s.resource(ctx, obj.APIVersion(), obj.Kind())
	if err != nil {
		return Result{}, err
	}
	u := s.c.cfg.Host + collectionPath(res, s.namespace(res, obj)) + "/" + url.PathEscape(obj.Name()) +
		"?" + url.Values{"fieldManager": {FieldManager}, "force": {"false"}}.Encode()
	out, err := s.c.send(ctx, http.MethodPatch, u, "application/apply-patch+yaml", obj)
	return Result{Resource: res, Object: out}, err
}

// Create sends obj as a create request with field manager objgitd. obj may
// set metadata.generateName instead of metadata.name; the server then picks
// the name, and Result.Object carries it.
func (s *Session) Create(ctx context.Context, obj Object) (Result, error) {
	if err := check(obj, true); err != nil {
		return Result{}, err
	}
	res, err := s.resource(ctx, obj.APIVersion(), obj.Kind())
	if err != nil {
		return Result{}, err
	}
	u := s.c.cfg.Host + collectionPath(res, s.namespace(res, obj)) +
		"?" + url.Values{"fieldManager": {FieldManager}}.Encode()
	out, err := s.c.send(ctx, http.MethodPost, u, "application/json", obj)
	return Result{Resource: res, Object: out}, err
}

// check reports the first identifying field obj lacks.
func check(obj Object, generateNameOK bool) error {
	switch {
	case obj.APIVersion() == "":
		return errors.New("object has no apiVersion")
	case obj.Kind() == "":
		return fmt.Errorf("%s object has no kind", obj.APIVersion())
	case obj.Name() == "" && !generateNameOK:
		return fmt.Errorf("%s object has no metadata.name", obj.Kind())
	case obj.Name() == "" && obj.GenerateName() == "":
		return fmt.Errorf("%s object has no metadata.name or metadata.generateName", obj.Kind())
	}
	return nil
}

func (s *Session) namespace(res Resource, obj Object) string {
	if !res.Namespaced {
		return ""
	}
	if ns := obj.Namespace(); ns != "" {
		return ns
	}
	return s.c.cfg.Namespace
}

// collectionPath returns the URL path of res in namespace ns ("" for a
// cluster-scoped resource).
func collectionPath(res Resource, ns string) string {
	p := "/api/" + res.Version
	if res.Group != "" {
		p = "/apis/" + res.Group + "/" + res.Version
	}
	if ns != "" {
		p += "/namespaces/" + url.PathEscape(ns)
	}
	return p + "/" + res.Name
}

// resource finds kind in apiVersion through discovery.
func (s *Session) resource(ctx context.Context, apiVersion, kind string) (Resource, error) {
	s.mu.Lock()
	list, ok := s.lists[apiVersion]
	s.mu.Unlock()
	if !ok {
		var err error
		if list, err = s.c.discover(ctx, apiVersion); err != nil {
			return Resource{}, err
		}
		s.mu.Lock()
		s.lists[apiVersion] = list
		s.mu.Unlock()
	}
	for _, r := range list {
		if r.Kind == kind {
			return r, nil
		}
	}
	return Resource{}, fmt.Errorf("no resource of kind %q in %s; is its CustomResourceDefinition installed?", kind, apiVersion)
}

// Kubernetes names API groups as DNS-1123 subdomains, and versions as short
// lower-case alphanumerics such as v1 or v1beta1.
var (
	groupPattern   = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
	versionPattern = regexp.MustCompile(`^[a-z0-9]+$`)
)

// splitAPIVersion splits apiVersion into its group ("" for the core group)
// and version. It refuses anything else, because both parts go into request
// paths: an escaped slash or a "?" there would reach a different API path.
func splitAPIVersion(apiVersion string) (group, version string, err error) {
	group, version, grouped := strings.Cut(apiVersion, "/")
	if !grouped {
		group, version = "", apiVersion
	}
	if (grouped && (len(group) > 253 || !groupPattern.MatchString(group))) || !versionPattern.MatchString(version) {
		return "", "", fmt.Errorf("invalid apiVersion %q; want group/version, such as tekton.dev/v1, or v1", apiVersion)
	}
	return group, version, nil
}

// discover lists the top-level resources of apiVersion. Subresources, such
// as pipelineruns/status, are left out.
func (c *Client) discover(ctx context.Context, apiVersion string) ([]Resource, error) {
	group, version, err := splitAPIVersion(apiVersion)
	if err != nil {
		return nil, err
	}
	p := "/api/" + version
	if group != "" {
		p = "/apis/" + group + "/" + version
	}

	var list struct {
		Resources []struct {
			Name       string `json:"name"`
			Kind       string `json:"kind"`
			Namespaced bool   `json:"namespaced"`
		} `json:"resources"`
	}
	data, err := c.do(ctx, http.MethodGet, c.cfg.Host+p, "", nil)
	if apiErr, ok := errors.AsType[*APIError](err); ok && apiErr.Code == http.StatusNotFound {
		return nil, fmt.Errorf("%s is not served by the Kubernetes API server", apiVersion)
	}
	if err != nil {
		return nil, fmt.Errorf("discover %s: %w", apiVersion, err)
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("discover %s: %w", apiVersion, err)
	}
	var out []Resource
	for _, r := range list.Resources {
		if strings.Contains(r.Name, "/") {
			continue
		}
		out = append(out, Resource{Group: group, Version: version, Name: r.Name, Kind: r.Kind, Namespaced: r.Namespaced})
	}
	return out, nil
}

// send encodes obj as JSON, sends it, and decodes the returned object. JSON
// is valid YAML, so it also serves as an apply-patch+yaml body.
func (c *Client) send(ctx context.Context, method, u, contentType string, obj Object) (Object, error) {
	body, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", obj.Kind(), err)
	}
	data, err := c.do(ctx, method, u, contentType, body)
	if err != nil {
		return nil, err
	}
	var out Object
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode API response: %w", err)
	}
	return out, nil
}

// maxResponse caps an API response body. Objects are at most about 1.5 MiB
// in etcd, and a discovery list is far smaller.
const maxResponse = 8 << 20

func (c *Client) do(ctx context.Context, method, u, contentType string, body []byte) ([]byte, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "objgitd")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.cfg.Token != nil {
		token, err := c.cfg.Token()
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return data, nil
	}
	apiErr := &APIError{Code: resp.StatusCode}
	var status struct {
		Reason  string `json:"reason"`
		Message string `json:"message"`
	}
	if json.Unmarshal(data, &status) == nil {
		apiErr.Reason, apiErr.Message = status.Reason, status.Message
	}
	return nil, apiErr
}
