package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/tigrisdata/objgit/internal/kefkash"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/mountfs"
	"github.com/tigrisdata/objgit/internal/pushevents"
	"github.com/tigrisdata/objgit/internal/treefs"
	"github.com/tigrisdata/objgit/internal/webhook"
	"mvdan.cc/sh/v3/expand"
	"mvdan.cc/sh/v3/interp"
	"mvdan.cc/sh/v3/syntax"
	"tangled.org/xeiaso.net/kefka/command/registry"
	"tangled.org/xeiaso.net/kefka/command/registry/coreutils"
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

// receivePack runs the receive-pack service and dispatches post-receive work
// for the commands whose ref updates succeeded. Hook output streams to the
// client before the response closes. Webhook delivery also finishes before the
// response closes, but cannot reject an already accepted push.
//
// This is also the one place every push funnels through — smart HTTP and SSH
// both land here, and git:// never serves receive-pack at all — so it is where
// the push concurrency cap is applied, via the d.pushes.admit seam.
func (d *daemon) receivePack(ctx context.Context, st storage.Storer, repoPath string, r io.ReadCloser, w io.WriteCloser, req *transport.ReceivePackRequest) error {
	webhooks, settingsErr := webhook.Load(d.sysFS, repoPath)
	if settingsErr != nil {
		slog.Error("webhook: load repository settings", "repo", repoPath, "err", settingsErr)
	}
	if !d.allowHooks && (webhooks == nil || !webhooks.Enabled(repoPath)) {
		err := receivePackStreaming(ctx, st, r, w, req, d.pushes.admit, nil)
		d.healHEADAfterPush(err, st, repoPath)
		return err
	}

	// The callback runs after refs change and report-status is attempted. The
	// updates are taken from this request's successful commands, so another
	// concurrent push cannot be attributed to this one.
	onUpdated := func(progress io.Writer, updates []refUpdate, acceptedAt time.Time) {
		if d.allowHooks {
			var branches []refUpdate
			for _, u := range updates {
				if u.Name.IsBranch() {
					branches = append(branches, u)
				}
			}
			d.runHooks(repoPath, "receive-pack", st, branches, progress)
		}
		d.emitPushWebhooks(ctx, st, repoPath, webhooks, updates, acceptedAt)
	}

	err := receivePackStreaming(ctx, st, r, w, req, d.pushes.admit, onUpdated)
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
	changes, err := pushevents.Diff(ctx, st, u.Old, u.New)
	if err != nil {
		log.Error("hook: diff changed files", "err", err)
		return
	}
	// Keep empty lists as JSON arrays. Paths can contain whitespace or newlines,
	// so JSON is the only unambiguous representation in environment variables.
	added := append([]string{}, changes.GetAdded()...)
	changed := append([]string{}, changes.GetChanged()...)
	deleted := append([]string{}, changes.GetDeleted()...)
	addedJSON, err := json.Marshal(added)
	if err != nil {
		log.Error("hook: encode added files", "err", err)
		return
	}
	changedJSON, err := json.Marshal(changed)
	if err != nil {
		log.Error("hook: encode changed files", "err", err)
		return
	}
	deletedJSON, err := json.Marshal(deleted)
	if err != nil {
		log.Error("hook: encode deleted files", "err", err)
		return
	}
	metadata, err := json.Marshal(struct {
		Added   []string `json:"added"`
		Changed []string `json:"changed"`
		Deleted []string `json:"deleted"`
	}{added, changed, deleted})
	if err != nil {
		log.Error("hook: encode changed files", "err", err)
		return
	}
	const changesFile = "/tmp/objgit-changes.json"
	f, err := fsys.Create(changesFile)
	if err != nil {
		log.Error("hook: create changes file", "err", err)
		return
	}
	_, writeErr := io.Copy(f, bytes.NewReader(metadata))
	closeErr := f.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		log.Error("hook: write changes file", "err", err)
		return
	}

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
		"OBJGIT_ADDED_FILES_JSON="+string(addedJSON),
		"OBJGIT_CHANGED_FILES_JSON="+string(changedJSON),
		"OBJGIT_DELETED_FILES_JSON="+string(deletedJSON),
		"OBJGIT_CHANGES_FILE="+changesFile,
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
