package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	ssh "github.com/gliderlabs/ssh"
	settingsv1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/webhooks/v1"
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

const webhookSetUsage = "usage: objgit-webhook-set org/repo [-url URL] [-rotate-secret]"

// handleSSHWebhookSet changes a repository's webhook settings. Each flag sets
// one non-secret setting; an omitted flag keeps the current value. The secret
// is kept, or generated when there is none or -rotate-secret is set. The
// command prints the stored settings, secret included, as ProtoJSON.
func (d *daemon) handleSSHWebhookSet(s ssh.Session, args []string) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		fmt.Fprintln(s.Stderr(), "objgitd: "+webhookSetUsage)
		_ = s.Exit(1)
		return
	}
	fset := flag.NewFlagSet("objgit-webhook-set", flag.ContinueOnError)
	fset.SetOutput(s.Stderr())
	fset.Usage = func() {
		fmt.Fprintln(fset.Output(), webhookSetUsage)
		fset.PrintDefaults()
	}
	url := fset.String("url", "", "webhook destination URL; HTTPS, or HTTP to loopback only")
	rotate := fset.Bool("rotate-secret", false, "replace the signing secret with a new random one")
	if err := fset.Parse(args[1:]); err != nil {
		_ = s.Exit(1)
		return
	}
	if fset.NArg() != 0 {
		fset.Usage()
		_ = s.Exit(1)
		return
	}

	ref, err := repofs.Parse(args[0])
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

	// Unreadable settings block a partial update, because the kept values are
	// unknown. Setting both -url and -rotate-secret replaces them.
	current, err := webhook.ReadSettings(d.sysFS, ref.Path())
	if err != nil {
		if *url == "" || !*rotate {
			slog.Error("read webhook settings", "repo", ref.Path(), "err", err)
			fmt.Fprintln(s.Stderr(), "objgitd: cannot read webhook settings; set -url and -rotate-secret to replace them")
			_ = s.Exit(1)
			return
		}
		current = nil
	}
	settings := &settingsv1.Settings{Url: current.GetUrl(), Secret: current.GetSecret()}
	if *url != "" {
		settings.Url = *url
	}
	if settings.GetUrl() == "" {
		fmt.Fprintln(s.Stderr(), "objgitd: no webhook URL is set; use -url")
		_ = s.Exit(1)
		return
	}
	rotated := *rotate || settings.GetSecret() == ""
	if rotated {
		settings.Secret = webhook.NewSecret()
	}
	if err := webhook.WriteSettings(d.sysFS, ref.Path(), settings); err != nil {
		slog.Error("write webhook settings", "repo", ref.Path(), "err", err)
		fmt.Fprintf(s.Stderr(), "objgitd: cannot write webhook settings: %v\n", err)
		_ = s.Exit(1)
		return
	}
	slog.Info("webhook settings updated",
		"repo", ref.Path(),
		"remote", s.RemoteAddr().String(),
		"secret_rotated", rotated,
	)

	body, found, err := d.webhookSettingsJSON(ref)
	if err != nil || !found {
		if err == nil {
			err = errors.New("settings missing after write")
		}
		slog.Error("read webhook settings", "repo", ref.Path(), "err", err)
		fmt.Fprintln(s.Stderr(), "objgitd: webhook settings were written but cannot be read back")
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
