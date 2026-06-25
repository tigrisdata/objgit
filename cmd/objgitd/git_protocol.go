package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/go-git/go-billy/v6"
	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/metrics"
)

// handshakeTimeout bounds how long a client has to send its git-proto-request.
// It is cleared once the (possibly long) transfer begins.
const handshakeTimeout = 30 * time.Second

// operationFor maps a git service to the access it needs: receive-pack writes,
// everything else (upload-pack, upload-archive) reads.
func operationFor(service string) auth.Operation {
	if service == transport.ReceivePackService {
		return auth.Write
	}
	return auth.Read
}

// authorize is the single seam every transport routes authorization through: it
// times the underlying Authorizer and records the decision (by transport,
// operation, and outcome) before returning it. Each transport still renders the
// Decision in its own dialect.
func (d *daemon) authorize(ctx context.Context, req auth.Request) auth.Decision {
	start := time.Now()
	dec := d.authz.Authorize(ctx, req)
	metrics.ObserveAuth(req.Transport, req.Operation, dec, start)
	return dec
}

// daemon serves the git:// (TCP) protocol out of a billy filesystem.
type daemon struct {
	fs     billy.Filesystem
	loader transport.Loader
	authz  auth.Authorizer

	// allowHooks gates running .objgit/hooks/receive-pack after a push.
	allowHooks  bool
	hookTimeout time.Duration
}

// load opens the storer for repoPath and heals a dangling HEAD before returning
// it (see ensureHEAD). It preserves the loader's error verbatim — notably
// transport.ErrRepositoryNotFound, which callers map to a 404 — and treats a
// heal failure as non-fatal so a clone is never broken by a transient HEAD write.
func (d *daemon) load(repoPath string) (storage.Storer, error) {
	st, err := d.loader.Load(&url.URL{Path: repoPath})
	if err != nil {
		return nil, err
	}
	if err := ensureHEAD(st); err != nil {
		slog.Warn("could not repoint dangling HEAD", "path", repoPath, "err", err)
	}
	return st, nil
}

// ensureHEAD repoints a repository's HEAD at an existing branch when its symbolic
// target is missing. objgitd initializes every repo with HEAD -> refs/heads/main
// (loadOrInit), but a repo populated by pushing a project whose default branch
// differs — golang/go uses master — leaves HEAD dangling: clients fetch every
// object yet cannot check out a worktree ("remote HEAD refers to nonexistent
// ref"). Git hosts repoint HEAD on push; we heal idempotently on every load and
// after each push, so repos already in the bucket recover on their next clone
// without a re-push. A detached or already-valid HEAD, or a repo with no branches
// yet, is left untouched.
func ensureHEAD(st storage.Storer) error {
	head, err := st.Reference(plumbing.HEAD)
	if err != nil {
		return err
	}
	if head.Type() != plumbing.SymbolicReference {
		return nil // detached HEAD: nothing to repoint
	}
	if _, err := st.Reference(head.Target()); err == nil {
		return nil // target exists: HEAD is already valid
	} else if !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return err
	}
	target, err := pickDefaultBranch(st)
	if err != nil || target == "" {
		return err // no branches yet (target == ""): leave HEAD as-is
	}
	return st.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, target))
}

// pickDefaultBranch chooses a branch for HEAD: prefer refs/heads/main, then
// master, then trunk; otherwise the lexicographically smallest branch so the
// choice is deterministic. Returns "" when the repo has no branches.
func pickDefaultBranch(st storage.Storer) (plumbing.ReferenceName, error) {
	iter, err := st.IterReferences()
	if err != nil {
		return "", err
	}
	defer iter.Close()

	rank := map[plumbing.ReferenceName]int{
		plumbing.Main:                            0,
		plumbing.Master:                          1,
		plumbing.NewBranchReferenceName("trunk"): 2,
	}
	var (
		first    plumbing.ReferenceName
		best     plumbing.ReferenceName
		bestRank = len(rank)
	)
	err = iter.ForEach(func(r *plumbing.Reference) error {
		if r.Type() != plumbing.HashReference || !r.Name().IsBranch() {
			return nil
		}
		name := r.Name()
		if first == "" || name < first {
			first = name
		}
		if rk, ok := rank[name]; ok && rk < bestRank {
			best, bestRank = name, rk
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if best != "" {
		return best, nil
	}
	return first, nil
}

// loadOrInit returns the storer for repoPath, creating an empty bare repository
// on demand. Git's daemon never auto-creates; objgitd does, so a first push to
// a new path just works.
func (d *daemon) loadOrInit(repoPath string) (storage.Storer, error) {
	st, err := d.load(repoPath)
	if err == nil {
		return st, nil
	}
	if !errors.Is(err, transport.ErrRepositoryNotFound) {
		return nil, err
	}

	fs, err := d.fs.Chroot(repoPath)
	if err != nil {
		return nil, fmt.Errorf("chroot %q: %w", repoPath, err)
	}

	st = filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
	if _, err := git.Init(st, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main"))); err != nil {
		return nil, fmt.Errorf("init bare repo: %w", err)
	}

	metrics.ReposCreated()
	slog.Info("created repository", "path", repoPath)
	return st, nil
}
