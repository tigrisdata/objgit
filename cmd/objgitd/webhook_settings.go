package main

import (
	"fmt"
	"log/slog"
	"net/http"

	ssh "github.com/gliderlabs/ssh"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
	"github.com/tigrisdata/objgit/internal/webhook"
	"google.golang.org/protobuf/encoding/protojson"
)

func (d *daemon) handleHTTPWebhookSettings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	ref, ok := repoRef(w, r)
	if !ok {
		return
	}
	cred, _ := credFromRequest(r)
	decision := d.authorize(r.Context(), auth.Request{
		Repo: ref.Path(), Operation: auth.Admin, Cred: cred, Transport: "http",
	})
	switch decision {
	case auth.Allow:
	case auth.Unauthenticated:
		w.Header().Set("WWW-Authenticate", `Basic realm="objgit admin"`)
		http.Error(w, "admin authentication required", http.StatusUnauthorized)
		return
	default:
		http.Error(w, "admin access denied", http.StatusForbidden)
		return
	}
	body, found, err := d.webhookSettingsJSON(ref)
	if err != nil {
		slog.Error("read webhook settings", "repo", ref.Path(), "err", err)
		http.Error(w, "cannot read webhook settings", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func (d *daemon) handleSSHWebhookSettings(s ssh.Session, repo string) {
	ref, err := repofs.Parse(repo)
	if err != nil {
		fmt.Fprintln(s.Stderr(), "objgitd: invalid repository")
		_ = s.Exit(1)
		return
	}
	var cred auth.Credential = auth.Anonymous{}
	if key := s.PublicKey(); key != nil {
		cred = auth.PublicKey{Key: key}
	}
	if d.authorize(s.Context(), auth.Request{
		Repo: ref.Path(), Operation: auth.Admin, Cred: cred, Transport: "ssh",
	}) != auth.Allow {
		fmt.Fprintln(s.Stderr(), "objgitd: admin access denied")
		_ = s.Exit(1)
		return
	}
	body, found, err := d.webhookSettingsJSON(ref)
	if err != nil {
		slog.Error("read webhook settings", "repo", ref.Path(), "err", err)
		fmt.Fprintln(s.Stderr(), "objgitd: cannot read webhook settings")
		_ = s.Exit(1)
		return
	}
	if !found {
		fmt.Fprintln(s.Stderr(), "objgitd: webhook settings not found")
		_ = s.Exit(1)
		return
	}
	_, _ = s.Write(append(body, '\n'))
}

func (d *daemon) webhookSettingsJSON(ref repofs.RepoRef) ([]byte, bool, error) {
	settings, err := webhook.ReadSettings(d.sysFS, ref.Path())
	if err != nil || settings == nil {
		return nil, false, err
	}
	body, err := (protojson.MarshalOptions{EmitDefaultValues: true}).Marshal(settings)
	return body, true, err
}
