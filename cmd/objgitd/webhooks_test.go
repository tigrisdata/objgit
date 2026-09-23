package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	pushv1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/events/push/v1"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
	"github.com/tigrisdata/objgit/internal/webhook"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestSmartHTTPPushWebhook(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	type received struct {
		event     *pushv1.PushEvent
		delivery  string
		eventType string
	}
	events := make(chan received, 3)
	nextEvent := func() received {
		t.Helper()
		select {
		case event := <-events:
			return event
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for push webhook")
			return received{}
		}
	}
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read webhook: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var event pushv1.PushEvent
		if err := protojson.Unmarshal(body, &event); err != nil {
			t.Errorf("decode webhook: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		events <- received{&event, r.Header.Get("X-Objgit-Delivery"), r.Header.Get("X-Objgit-Event")}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(destination.Close)

	config, err := json.Marshal(map[string]any{"repositories": map[string]any{
		"acme/webhook": map[string]string{"url": destination.URL, "secret": "test-secret"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "webhooks.json")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	client, err := webhook.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	d := &daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: newMemBase()},
		authz:    auth.AllowAnonymous{AllowWrite: true},
		webhooks: client,
	}
	server := httptest.NewServer(d.httpHandler())
	t.Cleanup(server.Close)
	remote := server.URL + "/acme/webhook.git"

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	writeFile(t, filepath.Join(work, "readme.txt"), "first\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "first")
	first := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(work, "readme.txt"), "second\n")
	writeFile(t, filepath.Join(work, "transient.txt"), "temporary\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "second")
	second := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))

	if out, err := tryGit(work, "push", remote, "main"); err != nil {
		t.Fatalf("first push: %v\n%s", err, out)
	}
	firstEvent := nextEvent()
	if firstEvent.event.GetRef() != "refs/heads/main" || !firstEvent.event.GetCreated() {
		t.Errorf("first event ref/created = %s/%v", firstEvent.event.GetRef(), firstEvent.event.GetCreated())
	}
	if got := commitIDs(firstEvent.event); !slices.Equal(got, []string{first, second}) {
		t.Errorf("first push commits = %v, want [%s %s]", got, first, second)
	}
	if got := firstEvent.event.GetFiles().GetAdded(); !slices.Equal(got, []string{"readme.txt", "transient.txt"}) {
		t.Errorf("first push net added files = %v", got)
	}

	if err := os.Remove(filepath.Join(work, "transient.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(work, "readme.txt"), "third\n")
	runGit(t, work, "add", "-A")
	runGit(t, work, "commit", "-m", "third")
	third := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(work, "new.txt"), "new\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "fourth")
	fourth := strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
	runGit(t, work, "tag", "v1")

	if out, err := tryGit(work, "push", remote, "main", "refs/tags/v1"); err != nil {
		t.Fatalf("second push: %v\n%s", err, out)
	}
	got := map[string]received{}
	for range 2 {
		rec := nextEvent()
		got[rec.event.GetRef()] = rec
		if rec.delivery != rec.event.GetEventId() || rec.eventType != "push" {
			t.Errorf("headers do not match event: delivery=%q event=%q type=%q", rec.delivery, rec.event.GetEventId(), rec.eventType)
		}
	}
	branch := got["refs/heads/main"].event
	tag := got["refs/tags/v1"].event
	if branch == nil || tag == nil {
		t.Fatalf("second push refs = %v", got)
	}
	if branch.GetPushId() == "" || branch.GetPushId() != tag.GetPushId() || branch.GetEventId() == tag.GetEventId() {
		t.Errorf("second push IDs: branch=%s/%s tag=%s/%s", branch.GetPushId(), branch.GetEventId(), tag.GetPushId(), tag.GetEventId())
	}
	if !branch.GetPushedAt().AsTime().Equal(tag.GetPushedAt().AsTime()) {
		t.Errorf("events from one ref update batch have different acceptance times: branch=%v tag=%v", branch.GetPushedAt(), tag.GetPushedAt())
	}
	for _, tt := range []struct {
		name  string
		event *pushv1.PushEvent
		want  []string
	}{
		{"branch update", branch, []string{third, fourth}},
		{"tag creation", tag, []string{first, second, third, fourth}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if ids := commitIDs(tt.event); !slices.Equal(ids, tt.want) {
				t.Errorf("commits = %v, want %v", ids, tt.want)
			}
		})
	}
	if !slices.Equal(branch.GetFiles().GetAdded(), []string{"new.txt"}) ||
		!slices.Equal(branch.GetFiles().GetChanged(), []string{"readme.txt"}) ||
		!slices.Equal(branch.GetFiles().GetDeleted(), []string{"transient.txt"}) {
		t.Errorf("branch net files = %+v", branch.GetFiles())
	}
}

func commitIDs(event *pushv1.PushEvent) []string {
	ids := make([]string, 0, len(event.GetCommits()))
	for _, commit := range event.GetCommits() {
		ids = append(ids, commit.GetId())
	}
	return ids
}

func TestPushWebhookAcceptanceTimePrecedesHook(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	type delivery struct {
		pushedAt time.Time
		received time.Time
	}
	deliveries := make(chan delivery, 1)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var event pushv1.PushEvent
		if err := protojson.Unmarshal(body, &event); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		deliveries <- delivery{pushedAt: event.GetPushedAt().AsTime(), received: time.Now()}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(destination.Close)

	config, err := json.Marshal(map[string]any{"repositories": map[string]any{
		"acme/timed": map[string]string{"url": destination.URL, "secret": "test-secret"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "webhooks.json")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	client, err := webhook.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&daemon{
		sysFS: memfs.New(), resolver: repofs.BucketResolver{Base: newMemBase()},
		authz: auth.AllowAnonymous{AllowWrite: true}, webhooks: client,
		allowHooks: true, hookTimeout: 5 * time.Second,
	}).httpHandler())
	t.Cleanup(server.Close)

	work := t.TempDir()
	runGit(t, work, "init", "-b", "main")
	runGit(t, work, "config", "user.email", "test@example.com")
	runGit(t, work, "config", "user.name", "Test")
	writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), "sleep 1\n")
	writeFile(t, filepath.Join(work, "readme.txt"), "hello\n")
	runGit(t, work, "add", ".")
	runGit(t, work, "commit", "-m", "initial")
	if out, err := tryGit(work, "push", server.URL+"/acme/timed.git", "main"); err != nil {
		t.Fatalf("push: %v\n%s", err, out)
	}
	select {
	case got := <-deliveries:
		if elapsed := got.received.Sub(got.pushedAt); elapsed < 500*time.Millisecond {
			t.Errorf("pushedAt lags hook execution: delivery arrived only %v after acceptance", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for webhook")
	}
}
