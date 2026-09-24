package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/Xe/kefka/command/registry"
	"github.com/Xe/kefka/command/registry/coreutils"
	"github.com/Xe/kefka/command/registry/uutils"
	"github.com/Xe/kefka/command/registry/wasmprog"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"
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
)

// receivePackHook names the hook that a push runs, and so its script at
// .objgit/hooks/receive-pack and its OBJGIT_SERVICE. It is not the transport
// service name, which is "git-receive-pack".
const receivePackHook = "receive-pack"

// refUpdate records a single branch ref change applied by a receive-pack.
// A zero Old means the branch was created; a zero New means it was deleted.
type refUpdate struct {
	Name plumbing.ReferenceName
	Old  plumbing.Hash
	New  plumbing.Hash
}

// receivePack runs the receive-pack service and dispatches post-receive work
// for the commands whose ref updates succeeded: erofs snapshots of the updated
// branch and tag tips (see runSnapshots), then hooks, then webhooks. Snapshot
// and hook output streams to the client before the response closes. Webhook delivery also finishes before the
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
	if !d.allowHooks && !d.snapshots && (webhooks == nil || !webhooks.Enabled(repoPath)) {
		err := receivePackStreaming(ctx, st, r, w, req, d.pushes.admit, nil)
		d.healHEADAfterPush(err, st, repoPath)
		return err
	}

	// The callback runs after refs change and report-status is attempted. The
	// updates are taken from this request's successful commands, so another
	// concurrent push cannot be attributed to this one.
	onUpdated := func(progress io.Writer, updates []refUpdate, acceptedAt time.Time) {
		if d.snapshots {
			d.runSnapshots(repoPath, st, updates, progress)
		}
		if d.allowHooks {
			var branches []refUpdate
			for _, u := range updates {
				if u.Name.IsBranch() {
					branches = append(branches, u)
				}
			}
			d.runHooks(repoPath, receivePackHook, st, branches, progress)
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
		if u.New.IsZero() || !u.Name.IsBranch() {
			continue // a deletion, or a tag: hooks run for branches only
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

	ctx, cancel := context.WithTimeout(context.Background(), d.hookTimeout)
	defer cancel()
	changes, err := loadHookChanges(ctx, st, u)
	if err != nil {
		log.Error("hook: diff changed files", "err", err)
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

	stdin := strings.NewReader(hookStdin(u))
	sh, err := newHookShell(tree, changes, hookEnv(repoPath, service, u, changes), stdin, stdout, stderr)
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

// hookChangesFile is where a hook finds the JSON object of its file changes.
const hookChangesFile = "/tmp/objgit-changes.json"

// hookChanges holds the net file changes of one update, encoded the way a hook
// reads them. Paths can contain whitespace or newlines, so JSON is the only
// unambiguous form in environment variables.
type hookChanges struct {
	added, changed, deleted []byte // JSON arrays, for the environment
	file                    []byte // JSON object, for hookChangesFile
}

// loadHookChanges diffs u.Old against u.New. A zero Old diffs against the
// empty tree, so every file of a new branch is added.
func loadHookChanges(ctx context.Context, st storage.Storer, u refUpdate) (hookChanges, error) {
	changes, err := pushevents.Diff(ctx, st, u.Old, u.New)
	if err != nil {
		return hookChanges{}, err
	}
	// Keep empty lists as JSON arrays, not null.
	lists := struct {
		Added   []string `json:"added"`
		Changed []string `json:"changed"`
		Deleted []string `json:"deleted"`
	}{
		append([]string{}, changes.GetAdded()...),
		append([]string{}, changes.GetChanged()...),
		append([]string{}, changes.GetDeleted()...),
	}
	var c hookChanges
	for _, enc := range []struct {
		dst *[]byte
		v   any
	}{
		{&c.added, lists.Added},
		{&c.changed, lists.Changed},
		{&c.deleted, lists.Deleted},
		{&c.file, lists},
	} {
		if *enc.dst, err = json.Marshal(enc.v); err != nil {
			return hookChanges{}, fmt.Errorf("encode changed files: %w", err)
		}
	}
	return c, nil
}

// hookEnv returns the environment a hook sees for update u, as KEY=value
// pairs.
func hookEnv(repoPath, service string, u refUpdate, c hookChanges) []string {
	return []string{
		"HOME=/tmp",
		"PWD=/src",
		"TMPDIR=/tmp",
		"IFS= \t\n",
		"PATH=/usr/bin:/bin",
		"KEFKA=1",
		"OBJGIT_REPO=" + repoPath,
		"OBJGIT_SERVICE=" + service,
		"OBJGIT_REF=" + u.Name.String(),
		"OBJGIT_BRANCH=" + u.Name.Short(),
		"OBJGIT_OLD_SHA=" + u.Old.String(),
		"OBJGIT_NEW_SHA=" + u.New.String(),
		"OBJGIT_ADDED_FILES_JSON=" + string(c.added),
		"OBJGIT_CHANGED_FILES_JSON=" + string(c.changed),
		"OBJGIT_DELETED_FILES_JSON=" + string(c.deleted),
		"OBJGIT_CHANGES_FILE=" + hookChangesFile,
	}
}

// hookStdin mirrors git's post-receive stdin for update u: "<old> <new> <ref>\n".
func hookStdin(u refUpdate) string {
	return u.Old.String() + " " + u.New.String() + " " + u.Name.String() + "\n"
}

// newHookShell builds the kefka sandbox a hook runs in: /src is a lazy
// read-only view of tree, /tmp is writable scratch that holds hookChangesFile,
// and the shell starts in /src with env. Both push hooks and the SSH sh command
// use it, so the two environments cannot drift apart.
func newHookShell(tree *object.Tree, changes hookChanges, env []string, stdin io.Reader, stdout, stderr io.Writer) (*interp.Runner, error) {
	fsys := mountfs.New(map[string]billy.Filesystem{
		"src": treefs.New(tree),
		"tmp": memfs.New(),
	})
	if err := util.WriteFile(fsys, hookChangesFile, changes.file, 0o644); err != nil {
		return nil, fmt.Errorf("write changes file: %w", err)
	}

	reg := registry.New()
	coreutils.Register(reg)
	wasmprog.Register(reg)
	uutils.Register(reg)
	if err := reg.Chdir(fsys, "/src"); err != nil {
		return nil, fmt.Errorf("chdir /src: %w", err)
	}

	var sh *interp.Runner
	middleware := func(next interp.ExecHandlerFunc) interp.ExecHandlerFunc {
		return func(ctx context.Context, args []string) error {
			return reg.Exec(ctx, fsys, sh, args)
		}
	}
	sh, err := interp.New(
		interp.Env(expand.ListEnviron(env...)),
		interp.StdIO(stdin, stdout, stderr),
		interp.ExecHandlers(middleware),
		interp.CallHandler(kefkash.CallHandler(reg, fsys, stdout, stderr)),
		interp.StatHandler(kefkash.FsysStatHandler(reg, fsys)),
		interp.OpenHandler(kefkash.FsysOpenHandler(reg, fsys)),
		interp.ReadDirHandler2(kefkash.FsysReadDirHandler(reg, fsys)),
	)
	if err != nil {
		return nil, err
	}
	// interp seeds $PWD from Dir, which defaults to the daemon's host working
	// directory. interp.Dir would stat the host, so set the field directly. The
	// handlers above resolve paths through reg, so nothing else reads Dir.
	sh.Dir = "/src"
	return sh, nil
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
