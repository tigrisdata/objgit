package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"

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

// receivePack runs transport.ReceivePack and, when hooks are enabled, fires the
// repository's receive-pack hook for each updated branch once the push succeeds.
// rpStorer is what ReceivePack writes through (the git:// path hides the
// PackfileWriter capability via streamingStorer); readStorer is the underlying
// storer used for ref snapshots and hook checkouts.
func (d *daemon) receivePack(ctx context.Context, rpStorer, readStorer storage.Storer, repoPath string, r io.ReadCloser, w io.WriteCloser, req *transport.ReceivePackRequest) error {
	var before map[plumbing.ReferenceName]plumbing.Hash
	if d.allowHooks {
		var err error
		if before, err = snapshotRefs(readStorer); err != nil {
			slog.Warn("hook: ref snapshot before push failed", "path", repoPath, "err", err)
		}
	}

	if err := transport.ReceivePack(ctx, rpStorer, r, w, req); err != nil {
		return err
	}
	if !d.allowHooks {
		return nil
	}

	after, err := snapshotRefs(readStorer)
	if err != nil {
		slog.Error("hook: ref snapshot after push failed", "path", repoPath, "err", err)
		return nil
	}

	updates := diffRefs(before, after)
	if len(updates) == 0 {
		return nil
	}

	d.hookWG.Add(1)
	go func() {
		defer d.hookWG.Done()
		d.runHooks(repoPath, "receive-pack", readStorer, updates)
	}()
	return nil
}

// runHooks executes the receive-pack hook once per non-deleted branch update.
func (d *daemon) runHooks(repoPath, service string, st storage.Storer, updates []refUpdate) {
	for _, u := range updates {
		if u.New.IsZero() {
			continue // branch deletion: nothing to check out
		}
		d.runHook(repoPath, service, st, u)
	}
}

// runHook looks up .objgit/hooks/<service> in the updated commit's tree and, if
// present, runs it in a kefka shell with /src bound to a read-only view of that
// tree and /tmp to writable scratch. Output and exit status are logged only.
func (d *daemon) runHook(repoPath, service string, st storage.Storer, u refUpdate) {
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

	var outBuf, errBuf bytes.Buffer

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
		interp.StdIO(stdin, &outBuf, &errBuf),
		interp.ExecHandlers(middleware),
		interp.CallHandler(kefkash.CallHandler(reg, fsys, &outBuf, &errBuf)),
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
	runErr := sh.Run(ctx, prog)

	var exit interp.ExitStatus
	isExit := errors.As(runErr, &exit)
	attrs := []any{"exit", int(exit), "stdout", outBuf.String(), "stderr", errBuf.String()}
	if runErr != nil {
		if !isExit {
			attrs = append(attrs, "err", runErr)
		}
		log.Error("hook: finished with errors", attrs...)
		return
	}
	log.Info("hook: finished", attrs...)
}
