package pushevents

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	pushv1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/events/push/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

var testTime = time.Date(2026, time.September, 23, 12, 30, 0, 0, time.UTC)

func putBlob(t *testing.T, st storage.Storer, body string) plumbing.Hash {
	t.Helper()
	obj := st.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	w, err := obj.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	hash, err := st.SetEncodedObject(obj)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func putTree(t *testing.T, st storage.Storer, files map[string]string) plumbing.Hash {
	t.Helper()
	entries := make([]object.TreeEntry, 0, len(files))
	for name, body := range files {
		entries = append(entries, object.TreeEntry{Name: name, Mode: filemode.Regular, Hash: putBlob(t, st, body)})
	}
	sort.Sort(object.TreeEntrySorter(entries))
	obj := st.NewEncodedObject()
	if err := (&object.Tree{Entries: entries}).Encode(obj); err != nil {
		t.Fatal(err)
	}
	hash, err := st.SetEncodedObject(obj)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func putCommit(t *testing.T, st storage.Storer, message string, files map[string]string, parents ...plumbing.Hash) plumbing.Hash {
	t.Helper()
	return putCommitWithTree(t, st, message, putTree(t, st, files), parents...)
}

func putCommitWithTree(t *testing.T, st storage.Storer, message string, treeHash plumbing.Hash, parents ...plumbing.Hash) plumbing.Hash {
	t.Helper()
	commit := &object.Commit{
		TreeHash:     treeHash,
		ParentHashes: parents,
		Message:      message,
		Author:       object.Signature{Name: "Author", Email: "author@example.com", When: testTime},
		Committer:    object.Signature{Name: "Committer", Email: "committer@example.com", When: testTime.Add(time.Minute)},
	}
	obj := st.NewEncodedObject()
	if err := commit.Encode(obj); err != nil {
		t.Fatal(err)
	}
	hash, err := st.SetEncodedObject(obj)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

// Go-git's Tree.Encode validates names more narrowly than Git. Write a tree
// object directly to exercise names that are valid in Git but rejected there.
func putRawTree(t *testing.T, st storage.Storer, name, body string) plumbing.Hash {
	t.Helper()
	blob := putBlob(t, st, body)
	obj := st.NewEncodedObject()
	obj.SetType(plumbing.TreeObject)
	w, err := obj.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(w, "%o %s", filemode.Regular, name); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	if _, err := blob.WriteTo(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	hash, err := st.SetEncodedObject(obj)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func TestBuild(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(*testing.T, storage.Storer) (Update, []plumbing.Hash)
		wantForced   bool
		wantCreated  bool
		wantDeleted  bool
		wantFiles    *pushv1.FileChanges
		wantPerFiles []*pushv1.FileChanges
	}{
		{
			name: "new branch contains all ancestors in parent-first order",
			setup: func(t *testing.T, st storage.Storer) (Update, []plumbing.Hash) {
				first := putCommit(t, st, "add", map[string]string{"a.txt": "a"})
				second := putCommit(t, st, "edit", map[string]string{"a.txt": "b", "b.txt": "b"}, first)
				third := putCommit(t, st, "remove", map[string]string{"a.txt": "b"}, second)
				return Update{Name: "refs/heads/main", New: third}, []plumbing.Hash{first, second, third}
			},
			wantCreated: true,
			wantFiles:   &pushv1.FileChanges{Added: []string{"a.txt"}},
			wantPerFiles: []*pushv1.FileChanges{
				{Added: []string{"a.txt"}},
				{Added: []string{"b.txt"}, Changed: []string{"a.txt"}},
				{Deleted: []string{"b.txt"}},
			},
		},
		{
			name: "fast-forward includes only newly reachable commits",
			setup: func(t *testing.T, st storage.Storer) (Update, []plumbing.Hash) {
				first := putCommit(t, st, "first", map[string]string{"a.txt": "a"})
				second := putCommit(t, st, "second", map[string]string{"a.txt": "b"}, first)
				return Update{Name: "refs/heads/main", Old: first, New: second}, []plumbing.Hash{second}
			},
			wantFiles:    &pushv1.FileChanges{Changed: []string{"a.txt"}},
			wantPerFiles: []*pushv1.FileChanges{{Changed: []string{"a.txt"}}},
		},
		{
			name: "merge places both parents before merge and uses first-parent files",
			setup: func(t *testing.T, st storage.Storer) (Update, []plumbing.Hash) {
				root := putCommit(t, st, "root", map[string]string{"root.txt": "r"})
				left := putCommit(t, st, "left", map[string]string{"root.txt": "r", "left.txt": "l"}, root)
				right := putCommit(t, st, "right", map[string]string{"root.txt": "r", "right.txt": "r"}, root)
				merge := putCommit(t, st, "merge", map[string]string{"root.txt": "r", "left.txt": "l", "right.txt": "r"}, left, right)
				return Update{Name: "refs/heads/main", Old: root, New: merge}, []plumbing.Hash{left, right, merge}
			},
			wantFiles: &pushv1.FileChanges{Added: []string{"left.txt", "right.txt"}},
			wantPerFiles: []*pushv1.FileChanges{
				{Added: []string{"left.txt"}},
				{Added: []string{"right.txt"}},
				{Added: []string{"right.txt"}},
			},
		},
		{
			name: "force push excludes old ancestors and reports net changes",
			setup: func(t *testing.T, st storage.Storer) (Update, []plumbing.Hash) {
				root := putCommit(t, st, "root", map[string]string{"root.txt": "r"})
				old := putCommit(t, st, "old", map[string]string{"root.txt": "r", "old.txt": "o"}, root)
				newTip := putCommit(t, st, "new", map[string]string{"root.txt": "r", "new.txt": "n"}, root)
				return Update{Name: "refs/heads/main", Old: old, New: newTip}, []plumbing.Hash{newTip}
			},
			wantForced:   true,
			wantFiles:    &pushv1.FileChanges{Added: []string{"new.txt"}, Deleted: []string{"old.txt"}},
			wantPerFiles: []*pushv1.FileChanges{{Added: []string{"new.txt"}}},
		},
		{
			name: "branch deletion has net removals and no commits",
			setup: func(t *testing.T, st storage.Storer) (Update, []plumbing.Hash) {
				old := putCommit(t, st, "old", map[string]string{"a.txt": "a"})
				return Update{Name: "refs/heads/main", Old: old}, nil
			},
			wantDeleted: true,
			wantFiles:   &pushv1.FileChanges{Deleted: []string{"a.txt"}},
		},
		{
			name: "unchanged ref has no commits or files",
			setup: func(t *testing.T, st storage.Storer) (Update, []plumbing.Hash) {
				tip := putCommit(t, st, "tip", map[string]string{"a.txt": "a"})
				return Update{Name: "refs/heads/main", Old: tip, New: tip}, nil
			},
			wantFiles: &pushv1.FileChanges{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.NewStorage()
			update, wantCommits := tt.setup(t, st)
			got, err := Build(context.Background(), st, "acme/widgets", update, "push-1", "event-1", testTime)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if got.GetRepository() != "acme/widgets" || got.GetPushId() != "push-1" || got.GetEventId() != "event-1" || got.GetRef() != update.Name.String() {
				t.Errorf("event identity = %v", got)
			}
			if got.GetBefore() != update.Old.String() || got.GetAfter() != update.New.String() {
				t.Errorf("event hashes = (%s, %s), want (%s, %s)", got.GetBefore(), got.GetAfter(), update.Old, update.New)
			}
			if got.GetCreated() != tt.wantCreated || got.GetDeleted() != tt.wantDeleted || got.GetForced() != tt.wantForced {
				t.Errorf("flags = (%v, %v, %v), want (%v, %v, %v)", got.GetCreated(), got.GetDeleted(), got.GetForced(), tt.wantCreated, tt.wantDeleted, tt.wantForced)
			}
			if !got.GetPushedAt().AsTime().Equal(testTime) {
				t.Errorf("pushed at = %v, want %v", got.GetPushedAt().AsTime(), testTime)
			}
			if !sameFiles(got.GetFiles(), tt.wantFiles) {
				t.Errorf("net files = %v, want %v", got.GetFiles(), tt.wantFiles)
			}
			if len(got.GetCommits()) != len(wantCommits) {
				t.Fatalf("commit count = %d, want %d", len(got.GetCommits()), len(wantCommits))
			}
			for i, wantHash := range wantCommits {
				commit := got.GetCommits()[i]
				if commit.GetId() != wantHash.String() {
					t.Errorf("commit %d = %s, want %s", i, commit.GetId(), wantHash)
				}
				if !sameFiles(commit.GetFiles(), tt.wantPerFiles[i]) {
					t.Errorf("commit %d files = %v, want %v", i, commit.GetFiles(), tt.wantPerFiles[i])
				}
				if commit.GetAuthor().GetName() != "Author" || !commit.GetAuthor().GetWhen().AsTime().Equal(testTime) {
					t.Errorf("commit %d author = %v", i, commit.GetAuthor())
				}
			}
		})
	}
}

func sameFiles(got, want *pushv1.FileChanges) bool {
	return reflect.DeepEqual(got.GetAdded(), want.GetAdded()) &&
		reflect.DeepEqual(got.GetChanged(), want.GetChanged()) &&
		reflect.DeepEqual(got.GetDeleted(), want.GetDeleted())
}

func TestDiffCancellation(t *testing.T) {
	st := memory.NewStorage()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Diff(ctx, st, plumbing.ZeroHash, plumbing.ZeroHash)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Diff error = %v, want context.Canceled", err)
	}
}

func TestBuildNonCommitTarget(t *testing.T) {
	st := memory.NewStorage()
	blob := putBlob(t, st, "not a commit")
	got, err := Build(context.Background(), st, "acme/widgets", Update{Name: "refs/tags/data", New: blob}, "push-1", "event-1", testTime)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !got.GetCreated() || got.GetAfter() != blob.String() || len(got.GetCommits()) != 0 || !sameFiles(got.GetFiles(), &pushv1.FileChanges{}) {
		t.Errorf("non-commit event = %v", got)
	}
}

func TestDiffModeChange(t *testing.T) {
	st := memory.NewStorage()
	blob := putBlob(t, st, "same bytes")
	putModeCommit := func(mode filemode.FileMode) plumbing.Hash {
		t.Helper()
		treeObj := st.NewEncodedObject()
		if err := (&object.Tree{Entries: []object.TreeEntry{{Name: "script.sh", Mode: mode, Hash: blob}}}).Encode(treeObj); err != nil {
			t.Fatal(err)
		}
		treeHash, err := st.SetEncodedObject(treeObj)
		if err != nil {
			t.Fatal(err)
		}
		commitObj := st.NewEncodedObject()
		if err := (&object.Commit{
			TreeHash:  treeHash,
			Message:   "mode",
			Author:    object.Signature{Name: "Author", Email: "a@example.com", When: testTime},
			Committer: object.Signature{Name: "Author", Email: "a@example.com", When: testTime},
		}).Encode(commitObj); err != nil {
			t.Fatal(err)
		}
		hash, err := st.SetEncodedObject(commitObj)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	oldHash := putModeCommit(filemode.Regular)
	newHash := putModeCommit(filemode.Executable)
	got, err := Diff(context.Background(), st, oldHash, newHash)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if !sameFiles(got, &pushv1.FileChanges{Changed: []string{"script.sh"}}) {
		t.Errorf("mode-only files = %v, want changed script.sh", got)
	}
}

func TestDiffDirectoryToFile(t *testing.T) {
	st := memory.NewStorage()
	innerTree := putTree(t, st, map[string]string{"old.txt": "old"})
	putSingleEntryTree := func(entry object.TreeEntry) plumbing.Hash {
		t.Helper()
		obj := st.NewEncodedObject()
		if err := (&object.Tree{Entries: []object.TreeEntry{entry}}).Encode(obj); err != nil {
			t.Fatal(err)
		}
		hash, err := st.SetEncodedObject(obj)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	oldTree := putSingleEntryTree(object.TreeEntry{Name: "docs", Mode: filemode.Dir, Hash: innerTree})
	newTree := putSingleEntryTree(object.TreeEntry{Name: "docs", Mode: filemode.Regular, Hash: putBlob(t, st, "replacement")})
	oldCommit := putCommitWithTree(t, st, "directory", oldTree)
	newCommit := putCommitWithTree(t, st, "file", newTree, oldCommit)
	got, err := Diff(context.Background(), st, oldCommit, newCommit)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	want := &pushv1.FileChanges{Added: []string{"docs"}, Deleted: []string{"docs/old.txt"}}
	if !sameFiles(got, want) {
		t.Errorf("files = %v, want %v", got, want)
	}
}

func TestBuildGitFilenames(t *testing.T) {
	for _, tt := range []struct {
		name     string
		gitPath  string
		wantPath string
		encoded  bool
	}{
		{name: "newline remains literal in Protobuf string", gitPath: "a b\nc.txt", wantPath: "a b\nc.txt"},
		{name: "invalid UTF-8 uses reversible base64url path", gitPath: "bad-\xff.txt", encoded: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.NewStorage()
			tip := putCommitWithTree(t, st, "add special path", putRawTree(t, st, tt.gitPath, "body"))
			event, err := Build(context.Background(), st, "acme/widgets", Update{Name: "refs/heads/main", New: tip}, "push-1", "event-1", testTime)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if _, err := protojson.Marshal(event); err != nil {
				t.Fatalf("ProtoJSON marshal: %v", err)
			}
			if len(event.GetFiles().GetAdded()) != 1 {
				t.Fatalf("added paths = %v, want one", event.GetFiles().GetAdded())
			}
			path := event.GetFiles().GetAdded()[0]
			if tt.encoded {
				const prefix = "/objgit/raw-path/base64url/"
				if !strings.HasPrefix(path, prefix) {
					t.Fatalf("encoded path = %q, want prefix %q", path, prefix)
				}
				raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(path, prefix))
				if err != nil {
					t.Fatalf("decode path: %v", err)
				}
				if string(raw) != tt.gitPath {
					t.Errorf("decoded path = %q, want %q", raw, tt.gitPath)
				}
			} else if path != tt.wantPath {
				t.Errorf("path = %q, want %q", path, tt.wantPath)
			}
			if got := event.GetCommits()[0].GetFiles().GetAdded()[0]; got != path {
				t.Errorf("commit path = %q, want %q", got, path)
			}
		})
	}
}

func TestBuildInvalidUTF8CommitText(t *testing.T) {
	st := memory.NewStorage()
	treeHash := putTree(t, st, map[string]string{"readme": "hello"})
	obj := st.NewEncodedObject()
	commit := &object.Commit{
		TreeHash:  treeHash,
		Message:   "bad-\xff-message",
		Author:    object.Signature{Name: "bad-\xff-author", Email: "bad-\xff@example.com", When: testTime},
		Committer: object.Signature{Name: "bad-\xff-committer", Email: "bad-\xff@example.com", When: testTime},
	}
	if err := commit.Encode(obj); err != nil {
		t.Fatalf("encode commit: %v", err)
	}
	tip, err := st.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("store commit: %v", err)
	}
	event, err := Build(context.Background(), st, "acme/widgets", Update{Name: "refs/heads/main", New: tip}, "push-1", "event-1", testTime)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, err := protojson.Marshal(event); err != nil {
		t.Fatalf("ProtoJSON marshal: %v", err)
	}
	got := event.GetCommits()[0]
	if got.GetMessage() != "bad-\ufffd-message" || got.GetAuthor().GetName() != "bad-\ufffd-author" || got.GetAuthor().GetEmail() != "bad-\ufffd@example.com" || got.GetCommitter().GetName() != "bad-\ufffd-committer" {
		t.Errorf("normalized commit text = %v", got)
	}
}

func TestBuildRefNameEncoding(t *testing.T) {
	for _, tt := range []struct {
		name string
		ref  plumbing.ReferenceName
	}{
		{name: "UTF-8 ref stays literal", ref: "refs/heads/main"},
		{name: "invalid UTF-8 ref is reversible", ref: "refs/heads/bad-\xff"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st := memory.NewStorage()
			tip := putCommit(t, st, "message", map[string]string{"readme": "hello"})
			event, err := Build(context.Background(), st, "acme/widgets", Update{Name: tt.ref, New: tip}, "push-1", "event-1", testTime)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := protojson.Marshal(event); err != nil {
				t.Fatalf("ProtoJSON marshal: %v", err)
			}
			got := event.GetRef()
			if got == tt.ref.String() {
				return
			}
			const prefix = "/objgit/raw-path/base64url/"
			if !strings.HasPrefix(got, prefix) {
				t.Fatalf("encoded ref %q lacks prefix", got)
			}
			raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(got, prefix))
			if err != nil || string(raw) != tt.ref.String() {
				t.Errorf("decoded ref = %q, err=%v, want %q", raw, err, tt.ref)
			}
		})
	}
}
