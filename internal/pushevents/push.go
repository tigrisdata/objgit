// Package pushevents builds push webhook payloads from accepted Git ref updates.
package pushevents

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	pushv1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/events/push/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Update records one accepted ref change. A zero hash means the ref did not
// exist on that side of the update.
type Update struct {
	Name plumbing.ReferenceName
	Old  plumbing.Hash
	New  plumbing.Hash
}

// Build returns the event for one accepted ref update. Commit order is stable
// and always places parents before their children. For a merge, the commit's
// files are compared with its first parent. The event's files are the net
// difference between the two ref targets. Invalid UTF-8 bytes in commit
// messages and author/committer names and emails are replaced with U+FFFD so
// ProtoJSON can serialize the event. File paths use a reversible encoding;
// see Diff.
func Build(ctx context.Context, st storage.Storer, repo string, update Update, pushID, eventID string, at time.Time) (*pushv1.PushEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	event := &pushv1.PushEvent{
		EventId:    eventID,
		PushId:     pushID,
		Repository: repo,
		Ref:        encodePath(update.Name.String()),
		Before:     update.Old.String(),
		After:      update.New.String(),
		Created:    update.Old.IsZero() && !update.New.IsZero(),
		Deleted:    update.New.IsZero() && !update.Old.IsZero(),
		PushedAt:   timestamppb.New(at),
		Files:      &pushv1.FileChanges{},
	}

	// Annotated tags and other non-commit targets have no commit tree to
	// compare. They retain the ref metadata but have no file metadata.
	oldCommit, oldIsCommit, err := commitAt(st, update.Old)
	if err != nil {
		return nil, fmt.Errorf("load old ref target %s: %w", update.Old, err)
	}
	newCommit, newIsCommit, err := commitAt(st, update.New)
	if err != nil {
		return nil, fmt.Errorf("load new ref target %s: %w", update.New, err)
	}
	if !oldIsCommit || !newIsCommit {
		return event, nil
	}

	event.Files, err = Diff(ctx, st, update.Old, update.New)
	if err != nil {
		return nil, fmt.Errorf("diff ref %s: %w", update.Name, err)
	}
	if newCommit == nil { // A deletion introduces no commits.
		return event, nil
	}

	oldReachable, err := reachable(ctx, st, oldCommit)
	if err != nil {
		return nil, fmt.Errorf("walk old ref %s: %w", update.Name, err)
	}
	commits, oldIsAncestor, err := collectNew(ctx, st, newCommit, update.Old, oldReachable)
	if err != nil {
		return nil, fmt.Errorf("walk new ref %s: %w", update.Name, err)
	}
	event.Forced = oldCommit != nil && !oldIsAncestor
	for _, commit := range commits {
		var firstParent plumbing.Hash
		if len(commit.ParentHashes) != 0 {
			firstParent = commit.ParentHashes[0]
		}
		files, err := Diff(ctx, st, firstParent, commit.Hash)
		if err != nil {
			return nil, fmt.Errorf("diff commit %s: %w", commit.Hash, err)
		}
		parents := make([]string, len(commit.ParentHashes))
		for i, parent := range commit.ParentHashes {
			parents[i] = parent.String()
		}
		event.Commits = append(event.Commits, &pushv1.Commit{
			Id:        commit.Hash.String(),
			TreeId:    commit.TreeHash.String(),
			Message:   strings.ToValidUTF8(commit.Message, "\ufffd"),
			Author:    signature(commit.Author),
			Committer: signature(commit.Committer),
			ParentIds: parents,
			Files:     files,
		})
	}
	return event, nil
}

func signature(sig object.Signature) *pushv1.Signature {
	return &pushv1.Signature{
		Name:  strings.ToValidUTF8(sig.Name, "\ufffd"),
		Email: strings.ToValidUTF8(sig.Email, "\ufffd"),
		When:  timestamppb.New(sig.When),
	}
}

// Diff compares the trees at two commits. A zero hash represents an empty
// tree, so creation lists additions and deletion lists removals. Paths are
// sorted in each category. Renames are an addition and a deletion. Valid UTF-8
// paths are unchanged. Since Protobuf strings require valid UTF-8, a path with
// invalid bytes is encoded as "/objgit/raw-path/base64url/" followed by its
// unpadded base64url bytes. Git paths cannot start with "/", so this encoding
// is reversible without colliding with a valid path.
func Diff(ctx context.Context, st storage.Storer, oldHash, newHash plumbing.Hash) (*pushv1.FileChanges, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	oldTree, err := treeAt(st, oldHash)
	if err != nil {
		return nil, fmt.Errorf("load old tree: %w", err)
	}
	newTree, err := treeAt(st, newHash)
	if err != nil {
		return nil, fmt.Errorf("load new tree: %w", err)
	}
	files := &pushv1.FileChanges{}
	if err := diffTrees(ctx, st, oldTree, newTree, "", files); err != nil {
		return nil, fmt.Errorf("diff trees: %w", err)
	}
	sort.Strings(files.Added)
	sort.Strings(files.Changed)
	sort.Strings(files.Deleted)
	return files, nil
}

func diffTrees(ctx context.Context, st storage.Storer, oldTree, newTree *object.Tree, prefix string, files *pushv1.FileChanges) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if oldTree != nil && newTree != nil && oldTree.Hash == newTree.Hash {
		return nil
	}
	oldEntries := make(map[string]object.TreeEntry)
	newEntries := make(map[string]object.TreeEntry)
	if oldTree != nil {
		for _, entry := range oldTree.Entries {
			oldEntries[entry.Name] = entry
		}
	}
	if newTree != nil {
		for _, entry := range newTree.Entries {
			newEntries[entry.Name] = entry
		}
	}
	for name, oldEntry := range oldEntries {
		newEntry, hasNew := newEntries[name]
		if hasNew && oldEntry.Hash == newEntry.Hash && oldEntry.Mode == newEntry.Mode {
			continue
		}
		path := prefix + name
		if oldEntry.Mode == filemode.Dir || (hasNew && newEntry.Mode == filemode.Dir) {
			if oldEntry.Mode == filemode.Dir {
				var newSubtree *object.Tree
				if hasNew && newEntry.Mode == filemode.Dir {
					var err error
					newSubtree, err = object.GetTree(st, newEntry.Hash)
					if err != nil {
						return fmt.Errorf("load new subtree %q: %w", path, err)
					}
				}
				oldSubtree, err := object.GetTree(st, oldEntry.Hash)
				if err != nil {
					return fmt.Errorf("load old subtree %q: %w", path, err)
				}
				if err := diffTrees(ctx, st, oldSubtree, newSubtree, path+"/", files); err != nil {
					return err
				}
			} else {
				files.Deleted = append(files.Deleted, encodePath(path))
			}
			if hasNew && newEntry.Mode == filemode.Dir && oldEntry.Mode != filemode.Dir {
				newSubtree, err := object.GetTree(st, newEntry.Hash)
				if err != nil {
					return fmt.Errorf("load new subtree %q: %w", path, err)
				}
				if err := diffTrees(ctx, st, nil, newSubtree, path+"/", files); err != nil {
					return err
				}
			} else if hasNew && newEntry.Mode != filemode.Dir {
				files.Added = append(files.Added, encodePath(path))
			}
			continue
		}
		if !hasNew {
			files.Deleted = append(files.Deleted, encodePath(path))
		} else {
			files.Changed = append(files.Changed, encodePath(path))
		}
	}
	for name, newEntry := range newEntries {
		if _, exists := oldEntries[name]; exists {
			continue
		}
		path := prefix + name
		if newEntry.Mode == filemode.Dir {
			newSubtree, err := object.GetTree(st, newEntry.Hash)
			if err != nil {
				return fmt.Errorf("load new subtree %q: %w", path, err)
			}
			if err := diffTrees(ctx, st, nil, newSubtree, path+"/", files); err != nil {
				return err
			}
		} else {
			files.Added = append(files.Added, encodePath(path))
		}
	}
	return nil
}

func encodePath(path string) string {
	if utf8.ValidString(path) {
		return path
	}
	return "/objgit/raw-path/base64url/" + base64.RawURLEncoding.EncodeToString([]byte(path))
}

func treeAt(st storage.Storer, hash plumbing.Hash) (*object.Tree, error) {
	if hash.IsZero() {
		return nil, nil
	}
	commit, err := object.GetCommit(st, hash)
	if err != nil {
		return nil, err
	}
	return commit.Tree()
}

// commitAt distinguishes a non-commit target from a missing object. Zero
// hashes stand for an absent ref and are treated as commit-compatible.
func commitAt(st storage.Storer, hash plumbing.Hash) (*object.Commit, bool, error) {
	if hash.IsZero() {
		return nil, true, nil
	}
	obj, err := st.EncodedObject(plumbing.AnyObject, hash)
	if err != nil {
		return nil, false, err
	}
	if obj.Type() != plumbing.CommitObject {
		return nil, false, nil
	}
	commit, err := object.DecodeCommit(st, obj)
	return commit, true, err
}

func reachable(ctx context.Context, st storage.Storer, tip *object.Commit) (map[plumbing.Hash]struct{}, error) {
	seen := make(map[plumbing.Hash]struct{})
	if tip == nil {
		return seen, nil
	}
	stack := []plumbing.Hash{tip.Hash}
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hash := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}
		commit, err := object.GetCommit(st, hash)
		if err != nil {
			return nil, err
		}
		stack = append(stack, commit.ParentHashes...)
	}
	return seen, nil
}

// collectNew traverses in postorder, which guarantees parent-before-child
// output even with merges. The parent's recorded order is the tie breaker.
func collectNew(ctx context.Context, st storage.Storer, tip *object.Commit, oldHash plumbing.Hash, excluded map[plumbing.Hash]struct{}) ([]*object.Commit, bool, error) {
	if tip.Hash == oldHash {
		return nil, true, nil
	}
	type frame struct {
		commit *object.Commit
		next   int
	}
	var ordered []*object.Commit
	stack := []frame{{commit: tip}}
	seen := make(map[plumbing.Hash]struct{})
	oldIsAncestor := false
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		current := &stack[len(stack)-1]
		if current.commit.Hash == oldHash {
			oldIsAncestor = true
		}
		if current.next < len(current.commit.ParentHashes) {
			parentHash := current.commit.ParentHashes[current.next]
			current.next++
			if parentHash == oldHash {
				oldIsAncestor = true
			}
			if _, ok := excluded[parentHash]; ok {
				continue
			}
			if _, ok := seen[parentHash]; ok {
				continue
			}
			parent, err := object.GetCommit(st, parentHash)
			if err != nil {
				return nil, false, err
			}
			seen[parentHash] = struct{}{}
			stack = append(stack, frame{commit: parent})
			continue
		}
		ordered = append(ordered, current.commit)
		stack = stack[:len(stack)-1]
	}
	return ordered, oldIsAncestor, nil
}
