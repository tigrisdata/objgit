package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	pushv1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/events/push/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "webhooks.json")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func configFor(t *testing.T, endpoint string) string {
	t.Helper()
	data, err := json.Marshal(config{Repositories: map[string]repositoryConfig{
		"acme/widgets": {URL: endpoint, Secret: "test-secret"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return writeConfig(t, string(data))
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		wantEnabled bool
		wantError   string
	}{
		{name: "valid loopback", config: `{"repositories":{"acme/widgets":{"url":"http://127.0.0.1:1234/hook","secret":"key"}}}`, wantEnabled: true},
		{name: "valid public HTTPS", config: `{"repositories":{"acme/widgets":{"url":"https://example.com/hook","secret":"key"}}}`, wantEnabled: true},
		{name: "bad repository", config: `{"repositories":{"acme/widgets.git":{"url":"https://example.com/hook","secret":"key"}}}`, wantError: "invalid repository"},
		{name: "missing secret", config: `{"repositories":{"acme/widgets":{"url":"https://example.com/hook"}}}`, wantError: "no secret"},
		{name: "plain HTTP", config: `{"repositories":{"acme/widgets":{"url":"http://example.com/hook","secret":"key"}}}`, wantError: "must use HTTPS"},
		{name: "metadata IP", config: `{"repositories":{"acme/widgets":{"url":"https://169.254.169.254/latest","secret":"key"}}}`, wantError: "nonpublic IP"},
		{name: "private IP", config: `{"repositories":{"acme/widgets":{"url":"https://10.0.0.1/hook","secret":"key"}}}`, wantError: "nonpublic IP"},
		{name: "URL user info", config: `{"repositories":{"acme/widgets":{"url":"https://user:pass@example.com/hook","secret":"key"}}}`, wantError: "no user info"},
		{name: "unknown config field", config: `{"repositories":{},"extra":true}`, wantError: "unknown field"},
		{name: "trailing data", config: `{"repositories":{}} {}`, wantError: "trailing data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Load(writeConfig(t, tt.config))
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error = %v, want substring %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := c.Enabled("acme/widgets"); got != tt.wantEnabled {
				t.Errorf("Enabled = %v, want %v", got, tt.wantEnabled)
			}
		})
	}
}

func TestLoadEmptyPath(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Enabled("acme/widgets") {
		t.Error("empty path enabled a webhook")
	}
	if err := c.Deliver(context.Background(), "acme/widgets", nil); err != nil {
		t.Errorf("disabled Deliver = %v, want nil", err)
	}
}

func TestDeliver(t *testing.T) {
	event := &pushv1.PushEvent{
		EventId:    "delivery-1",
		Repository: "acme/widgets",
		Ref:        "refs/heads/main",
		Commits: []*pushv1.Commit{{
			Id:    "abc123",
			Files: &pushv1.FileChanges{Added: []string{"one.txt"}},
		}},
	}
	var mu sync.Mutex
	var gotBodies [][]byte
	var gotHeaders []http.Header
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		attempts++
		gotBodies = append(gotBodies, body)
		gotHeaders = append(gotHeaders, r.Header.Clone())
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	c, err := Load(configFor(t, server.URL+"/hook"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Deliver(context.Background(), "acme/widgets", event); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	for i, body := range gotBodies {
		if got := gotHeaders[i].Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		if got := gotHeaders[i].Get("X-Objgit-Event"); got != "push" {
			t.Errorf("X-Objgit-Event = %q", got)
		}
		if got := gotHeaders[i].Get("X-Objgit-Delivery"); got != event.EventId {
			t.Errorf("X-Objgit-Delivery = %q", got)
		}
		mac := hmac.New(sha256.New, []byte("test-secret"))
		_, _ = mac.Write(body)
		wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if got := gotHeaders[i].Get("X-Objgit-Signature-256"); got != wantSignature {
			t.Errorf("signature = %q, want %q", got, wantSignature)
		}
		var decoded pushv1.PushEvent
		if err := protojson.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&decoded, event) {
			t.Errorf("decoded event = %v, want %v", &decoded, event)
		}
		if !strings.Contains(string(body), `"created":false`) || !strings.Contains(string(body), `"deleted":false`) {
			t.Errorf("default scalar fields missing from body: %s", body)
		}
		if i > 0 && string(body) != string(gotBodies[0]) {
			t.Error("retry body changed")
		}
	}
}

func TestDeliverNoRedirect(t *testing.T) {
	var followed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	c, err := Load(configFor(t, source.URL))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Deliver(context.Background(), "acme/widgets", &pushv1.PushEvent{EventId: "id", Repository: "acme/widgets"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Errorf("Deliver error = %v, want HTTP 307", err)
	}
	if got := followed.Load(); got != 0 {
		t.Errorf("redirect followed %d times", got)
	}
}

func TestDeliverPermanentFailure(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	c, err := Load(configFor(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Deliver(context.Background(), "acme/widgets", &pushv1.PushEvent{EventId: "id", Repository: "acme/widgets"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Errorf("Deliver error = %v, want HTTP 400", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
}

func TestDeliverBadEvent(t *testing.T) {
	c, err := Load(configFor(t, "http://127.0.0.1:1234"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		event     *pushv1.PushEvent
		wantError string
	}{
		{name: "nil", wantError: "nil"},
		{name: "no ID", event: &pushv1.PushEvent{Repository: "acme/widgets"}, wantError: "ID is empty"},
		{name: "wrong repository", event: &pushv1.PushEvent{EventId: "id", Repository: "other/repo"}, wantError: "does not match"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.Deliver(context.Background(), "acme/widgets", tt.event)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Errorf("Deliver error = %v, want substring %q", err, tt.wantError)
			}
		})
	}
}

func TestPublicIP(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		{name: "public IPv4", ip: "8.8.8.8", want: true},
		{name: "public IPv6", ip: "2606:4700:4700::1111", want: true},
		{name: "private IPv4", ip: "172.16.0.1"},
		{name: "link local", ip: "169.254.169.254"},
		{name: "carrier NAT", ip: "100.64.0.1"},
		{name: "benchmark", ip: "198.19.0.1"},
		{name: "mapped loopback", ip: "::ffff:127.0.0.1"},
		{name: "documentation IPv6", ip: "2001:db8::1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := netip.MustParseAddr(tt.ip)
			if got := publicIP(ip); got != tt.want {
				t.Errorf("publicIP(%s) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

func TestDeliverDoesNotExposeURLQuery(t *testing.T) {
	c, err := Load(configFor(t, "http://127.0.0.1:1/hook?token=hidden-token"))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Deliver(context.Background(), "acme/widgets", &pushv1.PushEvent{EventId: "id", Repository: "acme/widgets"})
	if err == nil {
		t.Fatal("Deliver succeeded against a closed port")
	}
	if strings.Contains(fmt.Sprint(err), "hidden-token") {
		t.Errorf("error exposed URL query: %v", err)
	}
}
