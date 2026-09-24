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
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"
	settingsv1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/webhooks/v1"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/webhook"
	gossh "golang.org/x/crypto/ssh"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
		{"default allows", auth.AllowAnonymous{AllowWrite: true}, true},
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
		{"anonymous default", auth.AllowAnonymous{AllowWrite: true}, "", "", "/_objgit/webhooks/acme/widgets/settings", http.StatusOK, true},
		{"basic default", auth.AllowAnonymous{AllowWrite: true}, "operator", "admin-password", "/_objgit/webhooks/acme/widgets/settings", http.StatusOK, true},
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

func TestSSHWebhookSet(t *testing.T) {
	checkSSHBinaries(t)
	deny := authorizeFunc(func(context.Context, auth.Request) auth.Decision { return auth.Deny })
	tests := []struct {
		name       string
		existing   string // "", "valid", or "corrupt"
		authz      auth.Authorizer
		args       []string
		wantErr    string // stderr substring; empty means success
		wantURL    string
		keepSecret bool
	}{
		{name: "create", args: []string{"acme/widgets", "-url", "https://example.com/new"}, wantURL: "https://example.com/new"},
		{name: "change URL keeps secret", existing: "valid", args: []string{"acme/widgets", "-url", "https://example.com/new"}, wantURL: "https://example.com/new", keepSecret: true},
		{name: "rotate keeps URL", existing: "valid", args: []string{"acme/widgets", "-rotate-secret"}, wantURL: "https://example.com/events"},
		{name: "replace unreadable", existing: "corrupt", args: []string{"acme/widgets", "-url", "https://example.com/new", "-rotate-secret"}, wantURL: "https://example.com/new"},
		{name: "partial update of unreadable", existing: "corrupt", args: []string{"acme/widgets", "-url", "https://example.com/new"}, wantErr: "cannot read webhook settings"},
		{name: "no URL", args: []string{"acme/widgets"}, wantErr: "no webhook URL"},
		{name: "plain HTTP", existing: "valid", args: []string{"acme/widgets", "-url", "http://example.com/new"}, wantErr: "must use HTTPS"},
		{name: "unknown flag", existing: "valid", args: []string{"acme/widgets", "-secret", "x"}, wantErr: "flag provided but not defined"},
		{name: "extra argument", existing: "valid", args: []string{"acme/widgets", "-url", "https://example.com/new", "extra"}, wantErr: "usage:"},
		{name: "no repository", args: []string{"-url", "https://example.com/new"}, wantErr: "usage:"},
		{name: "invalid repository", args: []string{"../widgets", "-url", "https://example.com/new"}, wantErr: "invalid repository"},
		{name: "denied", existing: "valid", authz: deny, args: []string{"acme/widgets", "-url", "https://example.com/new"}, wantErr: "admin access denied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := memfs.New()
			switch tt.existing {
			case "valid":
				writeWebhookSettings(t, fs, "acme/widgets", "https://example.com/events")
			case "corrupt":
				settingsPath, _ := webhook.SettingsPath("acme/widgets")
				if err := util.WriteFile(fs, settingsPath, []byte("{not json"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := util.ReadFile(fs, ".objgit/webhooks/acme/widgets/settings.json")
			addr, _ := startSSHServer(t, false, false, func(d *daemon) {
				d.sysFS = fs
				if tt.authz != nil {
					d.authz = tt.authz
				}
			})

			stdout, stderr, code := runSSHCommand(t, addr, append([]string{"objgit-webhook-set"}, tt.args...)...)
			if tt.wantErr != "" {
				if code == 0 {
					t.Fatalf("command succeeded, want error %q\nstdout: %s", tt.wantErr, stdout)
				}
				if !strings.Contains(stderr, tt.wantErr) {
					t.Errorf("stderr = %q, want substring %q", stderr, tt.wantErr)
				}
				if after, _ := util.ReadFile(fs, ".objgit/webhooks/acme/widgets/settings.json"); !bytes.Equal(before, after) {
					t.Errorf("failed command changed stored settings to %q", after)
				}
				return
			}
			if code != 0 {
				t.Fatalf("exit %d\nstderr: %s", code, stderr)
			}

			var printed settingsv1.Settings
			if err := protojson.Unmarshal([]byte(stdout), &printed); err != nil {
				t.Fatalf("stdout is not settings JSON: %v\n%s", err, stdout)
			}
			stored, err := webhook.ReadSettings(fs, "acme/widgets")
			if err != nil || stored == nil {
				t.Fatalf("ReadSettings = %v, %v", stored, err)
			}
			if !proto.Equal(&printed, stored) {
				t.Errorf("printed %v, stored %v", &printed, stored)
			}
			if stored.GetUrl() != tt.wantURL {
				t.Errorf("url = %q, want %q", stored.GetUrl(), tt.wantURL)
			}
			if gotKept := stored.GetSecret() == "test-secret"; gotKept != tt.keepSecret {
				t.Errorf("secret = %q, keepSecret = %v", stored.GetSecret(), tt.keepSecret)
			}
			if !tt.keepSecret && len(stored.GetSecret()) != 64 {
				t.Errorf("new secret %q is not 32 hex-encoded bytes", stored.GetSecret())
			}
		})
	}
}
