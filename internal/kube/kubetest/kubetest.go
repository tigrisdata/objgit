// Package kubetest is a fake Kubernetes API server for tests. It serves
// discovery for a fixed set of resources, stores objects in memory, and
// answers Server-Side Apply, create, and get. It is not a conformant API
// server: apply replaces the whole object, and a conflict is any apply over
// an object that another field manager owns. Its RBAC checks match a real
// server's verbs, which internal/kube's TestCluster confirms.
package kubetest

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Resource is one discoverable resource of the fake server.
type Resource struct {
	Group, Version, Kind, Name string
	Namespaced                 bool
}

// Resources is the default discovery data: core ConfigMaps and Namespaces,
// and the Tekton kinds that a hook creates.
var Resources = []Resource{
	{"", "v1", "ConfigMap", "configmaps", true},
	{"", "v1", "Namespace", "namespaces", false},
	{"tekton.dev", "v1", "Pipeline", "pipelines", true},
	{"tekton.dev", "v1", "Task", "tasks", true},
	{"tekton.dev", "v1", "PipelineRun", "pipelineruns", true},
	{"tekton.dev", "v1beta1", "Pipeline", "pipelines", true},
	{"tekton.dev", "v1beta1", "PipelineRun", "pipelineruns", true},
}

// Request is one request the server received.
type Request struct {
	Method, Path, ContentType, Authorization string
	Query                                    map[string]string
	Body                                     map[string]any
}

// Server is a running fake API server.
type Server struct {
	*httptest.Server

	mu       sync.Mutex
	token    string            // the bearer token every request must carry
	objects  map[string]stored // by object path
	denied   map[string]bool   // "verb resource", such as "patch pipelines"
	requests []Request
	nextName int
}

type stored struct {
	manager string
	obj     map[string]any
}

// New starts a TLS fake API server that stops when t ends.
func New(t testing.TB) *Server {
	s := &Server{
		token:   "test-token",
		objects: map[string]stored{},
		denied:  map[string]bool{},
	}
	s.Server = httptest.NewTLSServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	return s
}

// Token returns the bearer token the server accepts.
func (s *Server) Token() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

// SetToken changes the bearer token the server accepts, as a rotation does.
func (s *Server) SetToken(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = token
}

// Deny makes the server refuse verb ("create", "patch", "get") on resource,
// such as "pipelineruns", with 403 Forbidden.
func (s *Server) Deny(verb, resource string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.denied[verb+" "+resource] = true
}

// Put stores obj at path as owned by manager, so a later apply by another
// manager conflicts.
func (s *Server) Put(path, manager string, obj map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[path] = stored{manager: manager, obj: obj}
}

// Object returns the object stored at path, such as
// "/apis/tekton.dev/v1/namespaces/ci/pipelines/build".
func (s *Server) Object(path string) (map[string]any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[path]
	return o.obj, ok
}

// Requests returns every mutating request (not discovery or get) so far.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+s.Token() {
		status(w, http.StatusUnauthorized, "Unauthorized", "Unauthorized")
		return
	}
	if list, ok := s.discovery(r.URL.Path); ok {
		if r.Method != http.MethodGet {
			status(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "discovery is read-only")
			return
		}
		writeJSON(w, http.StatusOK, list)
		return
	}
	res, ns, name, ok := s.route(r.URL.Path)
	if !ok {
		status(w, http.StatusNotFound, "NotFound", "the server could not find the requested resource")
		return
	}

	var body map[string]any
	if r.Method == http.MethodPost || r.Method == http.MethodPatch {
		data, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(data, &body); err != nil {
			status(w, http.StatusBadRequest, "BadRequest", "body is not JSON: "+err.Error())
			return
		}
		q := map[string]string{}
		for k := range r.URL.Query() {
			q[k] = r.URL.Query().Get(k)
		}
		s.mu.Lock()
		s.requests = append(s.requests, Request{
			Method: r.Method, Path: r.URL.Path, ContentType: r.Header.Get("Content-Type"),
			Authorization: r.Header.Get("Authorization"), Query: q, Body: body,
		})
		s.mu.Unlock()
	}

	switch {
	case r.Method == http.MethodGet && name != "":
		s.get(w, res, ns, name, r.URL.Path)
	case r.Method == http.MethodPost && name == "":
		s.create(w, r, res, ns, body)
	case r.Method == http.MethodPatch && name != "":
		s.apply(w, r, res, ns, name, body)
	default:
		status(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method+" is not supported here")
	}
}

func (s *Server) forbidden(w http.ResponseWriter, verb string, res Resource, ns, name string) bool {
	s.mu.Lock()
	denied := s.denied[verb+" "+res.Name]
	s.mu.Unlock()
	if !denied {
		return false
	}
	qualified := res.Name
	if res.Group != "" {
		qualified += "." + res.Group
	}
	status(w, http.StatusForbidden, "Forbidden", fmt.Sprintf(
		"%s %q is forbidden: User \"system:serviceaccount:objgit:objgitd\" cannot %s resource %q in API group %q in the namespace %q",
		qualified, name, verb, res.Name, res.Group, ns))
	return true
}

func (s *Server) get(w http.ResponseWriter, res Resource, ns, name, path string) {
	if s.forbidden(w, "get", res, ns, name) {
		return
	}
	obj, ok := s.Object(path)
	if !ok {
		status(w, http.StatusNotFound, "NotFound", fmt.Sprintf("%s %q not found", res.Name, name))
		return
	}
	writeJSON(w, http.StatusOK, obj)
}

func (s *Server) create(w http.ResponseWriter, r *http.Request, res Resource, ns string, body map[string]any) {
	if r.Header.Get("Content-Type") != "application/json" {
		status(w, http.StatusUnsupportedMediaType, "UnsupportedMediaType", "create needs application/json")
		return
	}
	meta, _ := body["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	if name == "" {
		gen, _ := meta["generateName"].(string)
		if gen == "" {
			status(w, http.StatusUnprocessableEntity, "Invalid", "metadata.name or metadata.generateName is required")
			return
		}
		s.mu.Lock()
		s.nextName++
		name = fmt.Sprintf("%s%05d", gen, s.nextName)
		s.mu.Unlock()
	}
	if s.forbidden(w, "create", res, ns, name) {
		return
	}
	path := r.URL.Path + "/" + name
	if _, exists := s.Object(path); exists {
		status(w, http.StatusConflict, "AlreadyExists", fmt.Sprintf("%s %q already exists", res.Name, name))
		return
	}
	obj := withMeta(body, ns, name, res)
	s.Put(path, r.URL.Query().Get("fieldManager"), obj)
	writeJSON(w, http.StatusCreated, obj)
}

func (s *Server) apply(w http.ResponseWriter, r *http.Request, res Resource, ns, name string, body map[string]any) {
	if r.Header.Get("Content-Type") != "application/apply-patch+yaml" {
		status(w, http.StatusUnsupportedMediaType, "UnsupportedMediaType", "this fake only serves Server-Side Apply patches")
		return
	}
	manager := r.URL.Query().Get("fieldManager")
	if manager == "" {
		status(w, http.StatusBadRequest, "BadRequest", "fieldManager is required for apply requests")
		return
	}
	// Like a real API server: every apply needs patch, and an apply that
	// creates the object needs create as well.
	existing, exists := s.lookup(r.URL.Path)
	if s.forbidden(w, "patch", res, ns, name) || (!exists && s.forbidden(w, "create", res, ns, name)) {
		return
	}
	if exists && existing.manager != manager && r.URL.Query().Get("force") != "true" {
		status(w, http.StatusConflict, "Conflict", fmt.Sprintf(
			"Apply failed with 1 conflict: conflict with %q: .spec", existing.manager))
		return
	}
	code := http.StatusOK
	if !exists {
		code = http.StatusCreated
	}
	obj := withMeta(body, ns, name, res)
	s.Put(r.URL.Path, manager, obj)
	writeJSON(w, code, obj)
}

func (s *Server) lookup(path string) (stored, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[path]
	return o, ok
}

// withMeta returns a copy of body with the server-assigned name and namespace.
func withMeta(body map[string]any, ns, name string, res Resource) map[string]any {
	obj := maps.Clone(body)
	meta, _ := obj["metadata"].(map[string]any)
	meta = maps.Clone(meta)
	if meta == nil {
		meta = map[string]any{}
	}
	meta["name"] = name
	if res.Namespaced {
		meta["namespace"] = ns
	}
	obj["metadata"] = meta
	return obj
}

// discovery answers /api/v1 and /apis/<group>/<version>.
func (s *Server) discovery(path string) (map[string]any, bool) {
	var group, version string
	switch parts := strings.Split(strings.Trim(path, "/"), "/"); {
	case len(parts) == 2 && parts[0] == "api":
		version = parts[1]
	case len(parts) == 3 && parts[0] == "apis":
		group, version = parts[1], parts[2]
	default:
		return nil, false
	}
	var resources []map[string]any
	for _, r := range Resources {
		if r.Group == group && r.Version == version {
			// A subresource comes first, so a lookup that does not skip it fails.
			resources = append(resources, map[string]any{
				"name": r.Name + "/status", "kind": r.Kind, "namespaced": r.Namespaced,
				"verbs": []string{"get", "patch"},
			})
			resources = append(resources, map[string]any{
				"name": r.Name, "kind": r.Kind, "namespaced": r.Namespaced,
				"verbs": []string{"create", "get", "patch"},
			})
		}
	}
	if resources == nil {
		return nil, false
	}
	gv := version
	if group != "" {
		gv = group + "/" + version
	}
	return map[string]any{"kind": "APIResourceList", "groupVersion": gv, "resources": resources}, true
}

// route parses an object or collection path into its resource, namespace,
// and name.
func (s *Server) route(path string) (res Resource, ns, name string, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var group, version string
	switch {
	case len(parts) >= 3 && parts[0] == "api":
		version, parts = parts[1], parts[2:]
	case len(parts) >= 4 && parts[0] == "apis":
		group, version, parts = parts[1], parts[2], parts[3:]
	default:
		return Resource{}, "", "", false
	}
	if len(parts) >= 3 && parts[0] == "namespaces" {
		ns, parts = parts[1], parts[2:]
	}
	if len(parts) < 1 || len(parts) > 2 {
		return Resource{}, "", "", false
	}
	for _, r := range Resources {
		if r.Group == group && r.Version == version && r.Name == parts[0] && r.Namespaced == (ns != "") {
			if len(parts) == 2 {
				name = parts[1]
			}
			return r, ns, name, true
		}
	}
	return Resource{}, "", "", false
}

func status(w http.ResponseWriter, code int, reason, message string) {
	writeJSON(w, code, map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure",
		"message": message, "reason": reason, "code": code,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
