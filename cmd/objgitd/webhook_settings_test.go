package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/webhook"
	gossh "golang.org/x/crypto/ssh"
)

type authorizeFunc func(context.Context, auth.Request) auth.Decision

func (f authorizeFunc) Authorize(ctx context.Context, req auth.Request) auth.Decision {
	return f(ctx, req)
}

func TestSSHWebhookSettingsAuthorization(t *testing.T) {
	checkSSHBinaries(t)
	fs := memfs.New()
	writeWebhookSettings(t, fs, "acme/widgets", "https://example.com/events")
	keyPath := filepath.Join(t.TempDir(), "id_ed25519")
	if out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-f", keyPath).CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v: %s", err, out)
	}
	pub, err := os.ReadFile(keyPath + ".pub")
	if err != nil {
		t.Fatal(err)
	}
	adminKey, _, _, _, err := gossh.ParseAuthorizedKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	authz := authorizeFunc(func(_ context.Context, req auth.Request) auth.Decision {
		if req.Operation == auth.Admin && req.Transport == "ssh" {
			if cred, ok := req.Cred.(auth.PublicKey); ok && cred.Key != nil && bytes.Equal(cred.Key.Marshal(), adminKey.Marshal()) {
				return auth.Allow
			}
		}
		return auth.Deny
	})
	tests := []struct {
		name       string
		authz      auth.Authorizer
		wantSecret bool
	}{
		{"default denies", auth.AllowAnonymous{AllowWrite: true}, false},
		{"authorizer allows", authz, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &daemon{sysFS: fs, authz: tt.authz}
			srv, err := newSSHServer(d, "")
			if err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			go srv.Serve(ln) //nolint:errcheck // closed by test cleanup
			t.Cleanup(func() { srv.Close(); ln.Close() })
			host, port, err := net.SplitHostPort(ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("ssh", "-p", port, "-i", keyPath, "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "git@"+host, "objgit-webhook-settings", "acme/widgets")
			out, err := cmd.CombinedOutput()
			if tt.wantSecret && err != nil {
				t.Fatalf("ssh query: %v: %s", err, out)
			}
			if !tt.wantSecret && err == nil {
				t.Fatalf("ssh query unexpectedly succeeded: %s", out)
			}
			if got := bytes.Contains(out, []byte("test-secret")); got != tt.wantSecret {
				t.Errorf("secret present = %v, want %v: %s", got, tt.wantSecret, out)
			}
		})
	}
}

func TestHTTPWebhookSettingsAuthorization(t *testing.T) {
	fs := memfs.New()
	writeWebhookSettings(t, fs, "acme/widgets", "https://example.com/events")
	settingsPath, err := webhook.SettingsPath("acme/widgets")
	if err != nil {
		t.Fatal(err)
	}
	if settingsPath != ".objgit/webhooks/acme/widgets/settings.json" {
		t.Fatalf("settings path = %q", settingsPath)
	}
	allowed := authorizeFunc(func(_ context.Context, req auth.Request) auth.Decision {
		if req.Operation == auth.Admin && req.Transport == "http" {
			if cred, ok := req.Cred.(auth.BasicAuth); ok && cred.Username == "operator" && cred.Password == "admin-password" {
				return auth.Allow
			}
		}
		return auth.Unauthenticated
	})
	tests := []struct {
		name           string
		authz          auth.Authorizer
		user, password string
		path           string
		wantStatus     int
		wantSecret     bool
	}{
		{"anonymous default", auth.AllowAnonymous{AllowWrite: true}, "", "", "/_objgit/webhooks/acme/widgets/settings", http.StatusForbidden, false},
		{"basic default", auth.AllowAnonymous{AllowWrite: true}, "operator", "admin-password", "/_objgit/webhooks/acme/widgets/settings", http.StatusForbidden, false},
		{"wrong credential", allowed, "operator", "wrong", "/_objgit/webhooks/acme/widgets/settings", http.StatusUnauthorized, false},
		{"admin credential", allowed, "operator", "admin-password", "/_objgit/webhooks/acme/widgets/settings", http.StatusOK, true},
		{"missing settings", allowed, "operator", "admin-password", "/_objgit/webhooks/acme/missing/settings", http.StatusNotFound, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &daemon{sysFS: fs, authz: tt.authz}
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.user != "" {
				req.SetBasicAuth(tt.user, tt.password)
			}
			w := httptest.NewRecorder()
			d.httpHandler().ServeHTTP(w, req)
			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tt.wantStatus)
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Error("response must not be cached")
			}
			if got := bytes.Contains(w.Body.Bytes(), []byte("test-secret")); got != tt.wantSecret {
				t.Errorf("secret present = %v, want %v", got, tt.wantSecret)
			}
		})
	}
}
