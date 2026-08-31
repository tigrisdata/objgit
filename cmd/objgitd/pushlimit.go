package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tigrisdata/objgit/internal/metrics"
	"golang.org/x/sync/semaphore"
)

// errPushQueueTimeout is the sentinel behind every "waited too long for a push
// slot" failure. It is wrapped, not returned directly, so the client-facing
// message can name the deadline while tests and log readers can still match on
// one error.
var errPushQueueTimeout = errors.New("timed out waiting for a push slot")

// admitFunc asks for permission to unpack a packfile. It blocks until a slot is
// free, the deadline passes, or ctx is done, and returns the release closure for
// the slot it acquired. A nil admitFunc means unlimited.
//
// The returned release must be called exactly once, and only when err is nil.
type admitFunc func(ctx context.Context) (release func(), err error)

// pushLimiter bounds how many pushes unpack a packfile at the same time.
//
// Memory during a push is dominated by the packfile the client is sending, and
// it scales with the number of pushes in flight rather than with anything the
// daemon controls: cmd/membench measured a flat ~429 MiB of resident set per
// concurrent push of a 48 MiB pack, with no ceiling. GOMEMLIMIT halves that
// slope but does not bound it, because it makes the collector work harder as
// the heap grows instead of stopping the heap from growing. A count is the only
// thing that turns "linear in whoever is pushing" into a number that can be
// sized against a container.
//
// A push that arrives at a busy daemon waits rather than failing, because a git
// client handles a slow push far better than a failed one, and a push that has
// already uploaded its pack should not be thrown away because a peer was
// mid-flight. Past the deadline it fails cleanly, which stops the queue from
// growing without limit behind one slow push.
type pushLimiter struct {
	// sem is nil when the limit is disabled, which makes admit a no-op.
	sem *semaphore.Weighted
	// wait bounds how long a still-connected client queues for a slot. Zero
	// means it waits as long as its context lives.
	wait time.Duration
}

// newPushLimiter builds the limiter for max simultaneous pushes, each waiting
// at most wait for a slot. max <= 0 disables the limit, matching how
// -pack-cache-bytes treats 0 as "turn the feature off" rather than "zero
// budget", so today's unbounded behavior stays reachable with one flag.
func newPushLimiter(max int, wait time.Duration) *pushLimiter {
	if max <= 0 {
		return &pushLimiter{}
	}
	return &pushLimiter{sem: semaphore.NewWeighted(int64(max)), wait: wait}
}

// admit acquires one push slot. It is safe on a nil receiver, so a daemon built
// without a limiter (every test that does not care) is unlimited.
//
// The wait deadline is layered on top of ctx rather than replacing it, so a
// client that hangs up while queued gives up its place immediately instead of
// holding it for the rest of the deadline.
func (l *pushLimiter) admit(ctx context.Context) (func(), error) {
	if l == nil || l.sem == nil {
		return func() {}, nil
	}

	start := time.Now()
	doneWaiting := metrics.TrackPushWait()

	waitCtx := ctx
	if l.wait > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, l.wait)
		defer cancel()
	}

	err := l.sem.Acquire(waitCtx, 1)
	doneWaiting()

	if err != nil {
		// ctx outlives waitCtx, so a live ctx means the deadline expired and a
		// dead one means the client left. The two are counted apart because
		// only the first says the cap is too low.
		if ctxErr := ctx.Err(); ctxErr != nil {
			metrics.ObservePushWait(metrics.PushCanceled, start)
			return nil, fmt.Errorf("objgitd: push abandoned while waiting for a slot: %w", ctxErr)
		}
		metrics.ObservePushWait(metrics.PushTimeout, start)
		return nil, fmt.Errorf("objgitd: too many concurrent pushes, %w after %s", errPushQueueTimeout, l.wait)
	}

	metrics.ObservePushWait(metrics.PushAdmitted, start)
	releaseSlot := metrics.TrackPushSlot()
	return func() {
		releaseSlot()
		l.sem.Release(1)
	}, nil
}
