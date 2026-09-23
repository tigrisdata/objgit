package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/snapshot"
)

// maxTagDepth bounds how many annotated tags peelToTree follows.
const maxTagDepth = 16

// runSnapshots makes sure that an erofs image exists for the tree at the tip
// of each updated branch and tag. It runs synchronously inside onUpdated, so
// the push waits for it, and it writes one progress line per ref. A failure is logged
// and counted and never fails the push: the refs are already committed.
func (d *daemon) runSnapshots(repoPath string, st storage.Storer, updates []refUpdate, progress io.Writer) {
	store, ok := st.(snapshot.Store)
	if !ok {
		slog.Debug("snapshot: storer holds no snapshot store, skipping", "repo", repoPath)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.snapshotTimeout)
	defer cancel()

	// Branches go first, then tags, each by name, so the ref that builds a
	// shared tree (and the progress output) is the same on every push, however
	// the client ordered its commands.
	ordered := slices.Clone(updates)
	slices.SortFunc(ordered, func(a, b refUpdate) int {
		if a.Name.IsBranch() != b.Name.IsBranch() {
			if a.Name.IsBranch() {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name.String(), b.Name.String())
	})

	// A branch and a tag on one commit share one tree, and one build.
	seen := map[plumbing.Hash]string{}
	for _, u := range ordered {
		if u.New.IsZero() || !(u.Name.IsBranch() || u.Name.IsTag()) {
			continue // a deletion, or a ref such as refs/notes/*
		}
		log := slog.With("repo", repoPath, "ref", u.Name.String(), "sha", u.New.String())

		tree, ok, err := peelToTree(st, u.New)
		if err != nil {
			log.Error("snapshot: resolve tree", "err", err)
			metrics.ObserveSnapshot("error", 0, 0)
			continue
		}
		if !ok {
			log.Debug("snapshot: ref does not point at a commit, skipping")
			continue
		}
		if status, dup := seen[tree]; dup {
			if status == snapshot.StatusBuilt {
				status = snapshot.StatusExists
			}
			writeSnapshotLine(progress, u.Name, tree, status)
			continue
		}

		res, err := snapshot.Ensure(ctx, st, store, tree, d.snapshotTmpDir)
		if err != nil {
			log.Error("snapshot: build failed", "tree", tree.String(), "dur", res.Elapsed, "err", err)
			metrics.ObserveSnapshot("error", res.Elapsed, 0)
			seen[tree] = "failed"
			writeSnapshotLine(progress, u.Name, tree, "failed")
			continue
		}
		metrics.ObserveSnapshot(res.Status, res.Elapsed, res.Bytes)
		seen[tree] = res.Status
		if res.Status == snapshot.StatusBuilt {
			log.Info("snapshot: built", "key", res.Key, "files", res.Files, "bytes", res.Bytes, "dur", res.Elapsed)
			writeSnapshotLine(progress, u.Name, tree, fmt.Sprintf("built, %d files, %s, %s",
				res.Files, humanBytes(res.Bytes), res.Elapsed.Round(100*time.Millisecond)))
			continue
		}
		writeSnapshotLine(progress, u.Name, tree, res.Status)
	}
}

// writeSnapshotLine writes one "remote: objgit: snapshot ..." line. It never
// carries error text, which can name bucket internals; the log has that.
func writeSnapshotLine(progress io.Writer, ref plumbing.ReferenceName, tree plumbing.Hash, status string) {
	if progress == nil {
		return
	}
	fmt.Fprintf(progress, "objgit: snapshot %s (tree %s): %s\n", ref, tree.String()[:7], status)
}

// peelToTree returns the tree of the commit that h names, following annotated
// tags. ok is false, with no error, when h does not lead to a commit (a tag of
// a tree or a blob, or a tree itself), since there is then nothing to snapshot.
func peelToTree(st storer.EncodedObjectStorer, h plumbing.Hash) (plumbing.Hash, bool, error) {
	for range maxTagDepth {
		obj, err := st.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			return plumbing.ZeroHash, false, fmt.Errorf("load %s: %w", h, err)
		}
		switch obj.Type() {
		case plumbing.CommitObject:
			c, err := object.DecodeCommit(st, obj)
			if err != nil {
				return plumbing.ZeroHash, false, fmt.Errorf("decode commit %s: %w", h, err)
			}
			return c.TreeHash, true, nil
		case plumbing.TagObject:
			tag, err := object.DecodeTag(st, obj)
			if err != nil {
				return plumbing.ZeroHash, false, fmt.Errorf("decode tag %s: %w", h, err)
			}
			h = tag.Target
		default:
			return plumbing.ZeroHash, false, nil
		}
	}
	return plumbing.ZeroHash, false, errors.New("tag chain is too deep")
}

// humanBytes formats n in binary units with one decimal, such as "5.6 MiB".
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
