package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
)

// TestPushLimiterAdmit covers admission without a git client, so every case is
// decided by the limiter rather than by how fast a subprocess happens to run.
func TestPushLimiterAdmit(t *testing.T) {
	for _, tt := range []struct {
		name    string
		max     int
		wait    time.Duration
		held    int  // slots taken before the admit under test
		hangUp  bool // cancel the caller's context before it admits
		wantErr error
	}{
		{
			name: "zero disables the cap",
			max:  0,
			wait: 50 * time.Millisecond,
			held: 8,
		},
		{
			name: "admits below the cap",
			max:  2,
			wait: 50 * time.Millisecond,
			held: 1,
		},
		{
			name:    "gives up at the deadline once full",
			max:     1,
			wait:    50 * time.Millisecond,
			held:    1,
			wantErr: errPushQueueTimeout,
		},
		{
			name:    "a client that hangs up does not wait out the deadline",
			max:     1,
			wait:    time.Hour,
			held:    1,
			hangUp:  true,
			wantErr: context.Canceled,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newPushLimiter(tt.max, tt.wait)
			for i := range tt.held {
				release, err := l.admit(context.Background())
				if err != nil {
					t.Fatalf("taking slot %d: %v", i, err)
				}
				t.Cleanup(release)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.hangUp {
				cancel()
			}

			release, err := l.admit(ctx)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("admit err = %v, want one wrapping %v", err, tt.wantErr)
				}
				if release != nil {
					t.Error("admit handed back a release alongside an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("admit: %v", err)
			}
			release()
		})
	}
}

// TestPushLimiterQueuedClientReleasesItsPlace covers the leak that would be
// silent until the daemon wedged: a push that queues and then loses its client
// must give up its place immediately. It is verified the only way it can be —
// by a later push taking the freed slot instead of waiting out a deadline it
// would never reach in a test.
func TestPushLimiterQueuedClientReleasesItsPlace(t *testing.T) {
	l := newPushLimiter(1, time.Hour)

	held, err := l.admit(context.Background())
	if err != nil {
		t.Fatalf("taking the only slot: %v", err)
	}

	// A second push queues behind it, then its client hangs up.
	queuedCtx, hangUp := context.WithCancel(context.Background())
	queued := make(chan error, 1)
	go func() {
		release, err := l.admit(queuedCtx)
		if release != nil {
			release()
		}
		queued <- err
	}()
	waitFor(t, "a push to start queueing", func() bool {
		return gaugeValue(t, "objgit_push_queue_waiting") == 1
	})
	hangUp()
	if err := <-queued; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued push err = %v, want one wrapping context.Canceled", err)
	}

	// Hand the slot back. It must reach the third push, not the dead waiter.
	held()
	third := make(chan error, 1)
	go func() {
		release, err := l.admit(context.Background())
		if release != nil {
			release()
		}
		third <- err
	}()
	select {
	case err := <-third:
		if err != nil {
			t.Fatalf("third push: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("third push never admitted; the abandoned push kept its place")
	}

	assertPushGaugesDrained(t)
}

// TestPushCapReportsTimeoutToClient drives a real git client at a daemon whose
// one push slot is already taken, so the push has to queue and then give up. It
// asserts the reason reaches the person pushing as a push failure rather than as
// a dropped connection, and that the gate is not sticky afterwards.
func TestPushCapReportsTimeoutToClient(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	d := &daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: newMemBase()},
		authz:    auth.AllowAnonymous{AllowWrite: true},
		pushes:   newPushLimiter(1, 250*time.Millisecond),
	}
	ts := httptest.NewServer(d.httpHandler())
	t.Cleanup(ts.Close)

	held, err := d.pushes.admit(context.Background())
	if err != nil {
		t.Fatalf("taking the only push slot: %v", err)
	}

	work := seedRepo(t)
	remote := ts.URL + "/acme/queued.git"

	out, err := tryGit(work, "push", remote, "main")
	if err == nil {
		t.Fatalf("push should have failed while the only slot was held:\n%s", out)
	}
	for _, want := range []string{"too many concurrent pushes", "timed out waiting for a push slot"} {
		if !strings.Contains(out, want) {
			t.Errorf("push output does not explain the failure, missing %q:\n%s", want, out)
		}
	}

	held()
	if out, err := tryGit(work, "push", remote, "main"); err != nil {
		t.Fatalf("push after the slot was freed: %v\n%s", err, out)
	}

	// Both the happy path and the timeout path above must have given their
	// counters back.
	assertPushGaugesDrained(t)
}

// TestPushCapReportsTimeoutOverSSH is the same assertion over SSH, which is not
// the same code path: SSH is not a stateless RPC, so the ref advertisement goes
// out from inside receivePackStreaming before the gate is reached, and the
// sideband is framed on a long-lived connection instead of an HTTP response.
func TestPushCapReportsTimeoutOverSSH(t *testing.T) {
	checkSSHBinaries(t)

	d := &daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: newMemBase()},
		authz:    auth.AllowAnonymous{AllowWrite: true},
		pushes:   newPushLimiter(1, 250*time.Millisecond),
	}
	srv, err := newSSHServer(d, "")
	if err != nil {
		t.Fatalf("newSSHServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(ln) //nolint:errcheck // returns when ln closes
	t.Cleanup(func() { srv.Close(); ln.Close() })

	held, err := d.pushes.admit(context.Background())
	if err != nil {
		t.Fatalf("taking the only push slot: %v", err)
	}

	env := gitSSHEnv(t)
	work := seedRepo(t)
	remote := fmt.Sprintf("ssh://git@%s/acme/queued.git", ln.Addr().String())

	out, err := gitWithEnv(work, env, "push", remote, "main")
	if err == nil {
		t.Fatalf("push should have failed while the only slot was held:\n%s", out)
	}
	if !strings.Contains(out, "too many concurrent pushes") {
		t.Errorf("push output does not explain the failure:\n%s", out)
	}

	held()
	if out, err := gitWithEnv(work, env, "push", remote, "main"); err != nil {
		t.Fatalf("push after the slot was freed: %v\n%s", err, out)
	}

	assertPushGaugesDrained(t)
}

// TestPushCapQueuesRatherThanFails pushes from several clients at once. Whatever
// the cap, every push must land: the cap is meant to turn a memory problem into
// a latency one, not into failures.
func TestPushCapQueuesRatherThanFails(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	const pushes = 4

	for _, tt := range []struct {
		name string
		max  int
	}{
		{name: "one at a time", max: 1},
		{name: "two at a time", max: 2},
		{name: "zero disables the cap", max: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mb := newMemBase()
			ts := httptest.NewServer((&daemon{
				sysFS:    memfs.New(),
				resolver: repofs.BucketResolver{Base: mb},
				authz:    auth.AllowAnonymous{AllowWrite: true},
				pushes:   newPushLimiter(tt.max, 60*time.Second),
			}).httpHandler())
			t.Cleanup(ts.Close)

			works := make([]string, pushes)
			for i := range works {
				works[i] = seedRepo(t)
			}

			var wg sync.WaitGroup
			outs := make([]string, pushes)
			errs := make([]error, pushes)
			for i := range pushes {
				wg.Go(func() {
					remote := fmt.Sprintf("%s/acme/repo%d.git", ts.URL, i)
					outs[i], errs[i] = tryGit(works[i], "push", remote, "main")
				})
			}
			wg.Wait()

			for i := range pushes {
				if errs[i] != nil {
					t.Errorf("push %d failed: %v\n%s", i, errs[i], outs[i])
				}
				if repo := fmt.Sprintf("acme/repo%d", i); !mb.exists(repo) {
					t.Errorf("push %d did not land: %s does not exist", i, repo)
				}
			}

			assertPushGaugesDrained(t)
		})
	}
}

// assertPushGaugesDrained checks both push gauges are back at zero. A semaphore
// released only on the happy path is the classic bug in this shape of change,
// and it is invisible from the outside until the daemon stops taking pushes.
func assertPushGaugesDrained(t *testing.T) {
	t.Helper()
	for _, name := range []string{"objgit_push_slots_held", "objgit_push_queue_waiting"} {
		if got := gaugeValue(t, name); got != 0 {
			t.Errorf("%s = %v after every push finished, want 0", name, got)
		}
	}
}

// gaugeValue reads one gauge out of the default registry by name. The metrics
// package keeps its collectors unexported, and gathering by name avoids handing
// tests a hook into them.
func gaugeValue(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			return m.GetGauge().GetValue()
		}
	}
	t.Fatalf("gauge %q not registered", name)
	return 0
}

// waitFor polls cond until it holds, so a test can wait on a goroutine reaching
// a state instead of sleeping long enough to hope it did.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
