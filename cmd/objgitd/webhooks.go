package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/go-git/go-git/v6/storage"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/pushevents"
	"github.com/tigrisdata/objgit/internal/webhook"
)

// emitPushWebhooks builds one event for each ref changed by this receive-pack.
// Delivery runs after the ref update and cannot reject or undo it. A detached,
// bounded context lets an event finish even if the Git client disconnects
// after the server has committed the refs.
func (d *daemon) emitPushWebhooks(ctx context.Context, st storage.Storer, repo string, webhooks *webhook.Client, updates []refUpdate, acceptedAt time.Time) {
	if webhooks == nil || !webhooks.Enabled(repo) || len(updates) == 0 {
		return
	}

	pushID, err := newEventID()
	if err != nil {
		slog.Error("webhook: create push ID", "repo", repo, "err", err)
		return
	}

	for _, u := range updates {
		eventID, err := newEventID()
		if err != nil {
			slog.Error("webhook: create event ID", "repo", repo, "ref", u.Name.String(), "err", err)
			continue
		}

		eventCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		start := time.Now()
		event, err := pushevents.Build(eventCtx, st, repo, pushevents.Update{
			Name: u.Name,
			Old:  u.Old,
			New:  u.New,
		}, pushID, eventID, acceptedAt)
		if err == nil {
			err = webhooks.Deliver(eventCtx, repo, event)
		}
		cancel()

		status := "ok"
		if err != nil {
			status = "error"
			slog.Error("webhook: delivery failed", "repo", repo, "ref", u.Name.String(), "event_id", eventID, "err", err)
		} else {
			slog.Info("webhook: delivered", "repo", repo, "ref", u.Name.String(), "event_id", eventID)
		}
		metrics.ObserveWebhook(status, time.Since(start))
	}
}

func newEventID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}
