package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"mvdan.cc/sh/v3/expand"
	"mvdan.cc/sh/v3/interp"
	"mvdan.cc/sh/v3/syntax"
	"tangled.org/xeiaso.net/kefka/command/registry"
	"tangled.org/xeiaso.net/kefka/command/registry/coreutils"
	"tangled.org/xeiaso.net/objgit/internal/kefkash"
	"tangled.org/xeiaso.net/objgit/internal/metrics"
	"tangled.org/xeiaso.net/objgit/internal/mountfs"
	"tangled.org/xeiaso.net/objgit/internal/treefs"
)

// refUpdate records a single branch ref change observed across a receive-pack.
// A zero Old means the branch was created; a zero New means it was deleted.
type refUpdate struct {
	Name plumbing.ReferenceName
	Old  plumbing.Hash
	New  plumbing.Hash
}

// snapshotRefs returns the current hash of every branch ref in st. go-git's
// transport.ReceivePack does not report which refs it changed, so we diff a
// snapshot taken before the push against one taken after.
func snapshotRefs(st storage.Storer) (map[plumbing.ReferenceName]plumbing.Hash, error) {
	it, err := st.IterReferences()
	if err != nil {
		return nil, err
	}
	defer it.Close()

	out := map[plumbing.ReferenceName]plumbing.Hash{}
	err = it.ForEach(func(r *plumbing.Reference) error {
		if r.Type() == plumbing.HashReference && r.Name().IsBranch() {
			out[r.Name()] = r.Hash()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// diffRefs computes the branch ref changes between two snapshots.
func diffRefs(before, after map[plumbing.ReferenceName]plumbing.Hash) []refUpdate {
	var updates []refUpdate
	for name, newHash := range after {
		oldHash, ok := before[name]
		switch {
		case !ok:
			updates = append(updates, refUpdate{Name: name, Old: plumbing.ZeroHash, New: newHash})
		case oldHash != newHash:
			updates = append(updates, refUpdate{Name: name, Old: oldHash, New: newHash})
		}
	}
	for name, oldHash := range before {
		if _, ok := after[name]; !ok {
			updates = append(updates, refUpdate{Name: name, Old: oldHash, New: plumbing.ZeroHash})
		}
	}
	return updates
}

// receivePack runs the receive-pack service and, when hooks are enabled, fires
// the repository's receive-pack hook for each updated branch once the push
// succeeds — synchronously, streaming hook output to the client over the
// sideband progress channel (rendered as "remote: " lines) before the response
// stream is closed. st is both what the service writes through and the storer
// used for ref snapshots and hook checkouts — all three transports now share the
// same Scanner-bounded PackfileWriter path (see writePack), so no transport needs
// a capability-hiding wrapper.
func (d *daemon) receivePack(ctx context.Context, st storage.Storer, repoPath string, r io.ReadCloser, w io.WriteCloser, req *transport.ReceivePackRequest) error {
	if !d.allowHooks {
		err := receivePackStreaming(ctx, st, r, w, req, nil)
		d.healHEADAfterPush(err, st, repoPath)
		return err
	}

	before, err := snapshotRefs(st)
	if err != nil {
		slog.Warn("hook: ref snapshot before push failed", "path", repoPath, "err", err)
	}

	// onUpdated runs after refs are updated and report-status is sent, but
	// before the response stream closes, so hook output reaches the client live.
	// progress is the sideband band-2 writer, or nil when the client did not
	// negotiate sideband (hooks then fall back to logging only).
	onUpdated := func(progress io.Writer) {
		after, err := snapshotRefs(st)
		if err != nil {
			slog.Error("hook: ref snapshot after push failed", "path", repoPath, "err", err)
			return
		}
		updates := diffRefs(before, after)
		if len(updates) == 0 {
			return
		}
		d.runHooks(repoPath, "receive-pack", st, updates, progress)
	}

	err = receivePackStreaming(ctx, st, r, w, req, onUpdated)
	d.healHEADAfterPush(err, st, repoPath)
	return err
}

// healHEADAfterPush repoints a dangling HEAD once a push succeeds, so the first
// push to a repo whose default branch is not main (e.g. golang/go uses master)
// leaves HEAD resolvable for the next clone. The HEAD write thus lands during the
// push rather than during a later clone. No-op when the receive failed or HEAD is
// already valid (see ensureHEAD).
func (d *daemon) healHEADAfterPush(recvErr error, st storage.Storer, repoPath string) {
	if recvErr != nil {
		return
	}
	if err := ensureHEAD(st); err != nil {
		slog.Warn("could not repoint HEAD after push", "path", repoPath, "err", err)
	}
}

// runHooks executes the receive-pack hook once per non-deleted branch update,
// streaming each hook's output to progress (nil = log only).
func (d *daemon) runHooks(repoPath, service string, st storage.Storer, updates []refUpdate, progress io.Writer) {
	for _, u := range updates {
		if u.New.IsZero() {
			continue // branch deletion: nothing to check out
		}
		d.runHook(repoPath, service, st, u, progress)
	}
}

// runHook looks up .objgit/hooks/<service> in the updated commit's tree and, if
// present, runs it in a kefka shell with /src bound to a read-only view of that
// tree and /tmp to writable scratch. When progress is non-nil, hook stdout and
// stderr stream to it (the client's sideband, rendered as "remote: " lines);
// otherwise output is buffered and logged. Exit status is always logged.
func (d *daemon) runHook(repoPath, service string, st storage.Storer, u refUpdate, progress io.Writer) {
	log := slog.With("repo", repoPath, "service", service, "ref", u.Name.String(), "sha", u.New.String())

	commit, err := object.GetCommit(st, u.New)
	if err != nil {
		log.Error("hook: load commit", "err", err)
		return
	}
	tree, err := commit.Tree()
	if err != nil {
		log.Error("hook: load tree", "err", err)
		return
	}

	hookFile, err := tree.File(".objgit/hooks/" + service)
	if err != nil {
		log.Debug("hook: no hook file in pushed tree")
		return
	}
	script, err := hookFile.Contents()
	if err != nil {
		log.Error("hook: read hook script", "err", err)
		return
	}

	fsys := mountfs.New(map[string]billy.Filesystem{
		"src": treefs.New(tree),
		"tmp": memfs.New(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), d.hookTimeout)
	defer cancel()

	// When the client negotiated sideband, stream stdout+stderr straight to it
	// ("remote: " lines); otherwise buffer for the log. git does not distinguish
	// the two streams on the wire, so both go to the same place.
	var outBuf, errBuf bytes.Buffer
	stdout, stderr := io.Writer(&outBuf), io.Writer(&errBuf)
	streaming := progress != nil
	if streaming {
		stdout, stderr = progress, progress
	}

	reg := registry.New()
	coreutils.Register(reg)
	if err := reg.Chdir(fsys, "/src"); err != nil {
		log.Error("hook: chdir /src", "err", err)
		return
	}

	env := expand.ListEnviron(
		"HOME=/tmp",
		"PWD=/src",
		"TMPDIR=/tmp",
		"IFS= \t\n",
		"PATH=/usr/bin:/bin",
		"KEFKA=1",
		"OBJGIT_REPO="+repoPath,
		"OBJGIT_SERVICE="+service,
		"OBJGIT_REF="+u.Name.String(),
		"OBJGIT_BRANCH="+u.Name.Short(),
		"OBJGIT_OLD_SHA="+u.Old.String(),
		"OBJGIT_NEW_SHA="+u.New.String(),
	)
	// Mirror git's post-receive stdin: "<old> <new> <ref>\n".
	stdin := strings.NewReader(u.Old.String() + " " + u.New.String() + " " + u.Name.String() + "\n")

	var sh *interp.Runner
	middleware := func(next interp.ExecHandlerFunc) interp.ExecHandlerFunc {
		return func(ctx context.Context, args []string) error {
			return reg.Exec(ctx, fsys, sh, args)
		}
	}
	sh, err = interp.New(
		interp.Env(env),
		interp.StdIO(stdin, stdout, stderr),
		interp.ExecHandlers(middleware),
		interp.CallHandler(kefkash.CallHandler(reg, fsys, stdout, stderr)),
		interp.StatHandler(kefkash.FsysStatHandler(reg, fsys)),
		interp.OpenHandler(kefkash.FsysOpenHandler(reg, fsys)),
		interp.ReadDirHandler2(kefkash.FsysReadDirHandler(reg, fsys)),
	)
	if err != nil {
		log.Error("hook: build shell", "err", err)
		return
	}

	prog, err := syntax.NewParser(syntax.Variant(syntax.LangBash)).Parse(strings.NewReader(script), service)
	if err != nil {
		log.Error("hook: parse script", "err", err)
		return
	}

	log.Info("hook: running")
	runStart := time.Now()
	runErr := sh.Run(ctx, prog)
	metrics.ObserveHook(hookStatus(ctx, runErr), time.Since(runStart))

	var exit interp.ExitStatus
	isExit := errors.As(runErr, &exit)
	attrs := []any{"exit", int(exit)}
	if !streaming {
		// When streaming, output already reached the client; don't duplicate it.
		attrs = append(attrs, "stdout", outBuf.String(), "stderr", errBuf.String())
	}
	if runErr != nil {
		if !isExit {
			attrs = append(attrs, "err", runErr)
		}
		log.Error("hook: finished with errors", attrs...)
		return
	}
	log.Info("hook: finished", attrs...)
}

// hookStatus classifies a hook run for metrics: "timeout" when the hook's
// deadline fired, "error" for any other failure (including a non-zero exit), and
// "ok" otherwise.
func hookStatus(ctx context.Context, runErr error) string {
	switch {
	case ctx.Err() != nil:
		return "timeout"
	case runErr != nil:
		return "error"
	default:
		return "ok"
	}
}
