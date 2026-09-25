package kustomize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Xe/kefka/command"
	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/tigrisdata/objgit/internal/mountfs"
	"github.com/tigrisdata/objgit/internal/treefs"
	"mvdan.cc/sh/v3/expand"
)

func TestEmbeddedModule(t *testing.T) {
	if !bytes.HasPrefix(wasm, wasmMagic) {
		t.Fatalf("kustomize.wasm starts with %q, not the WASM magic; run git lfs pull", wasm[:min(len(wasm), 16)])
	}
	sum := sha256.Sum256(wasm)
	if got := hex.EncodeToString(sum[:]); got != SHA256 {
		t.Fatalf("kustomize.wasm SHA-256 = %s, want %s; update SHA256 and its provenance when the module changes", got, SHA256)
	}
}

func TestExecRejectsLFSPointer(t *testing.T) {
	pointer := []byte("version https://git-lfs.github.com/spec/v1\noid sha256:1724a4e9\nsize 24380813\n")
	var stderr bytes.Buffer
	err := newModule(pointer).exec(context.Background(), &command.ExecContext{
		Stdout: io.Discard, Stderr: &stderr, FS: memfs.New(),
	}, []string{"version"})
	if err == nil || !strings.Contains(err.Error(), "Git LFS pointer") {
		t.Fatalf("err = %v, want an error that names the Git LFS pointer", err)
	}
}

func TestExec(t *testing.T) {
	tests := []struct {
		name       string
		dir        string // shell working directory, fsys-relative
		args       []string
		wantErr    bool
		wantStdout []string // substrings of stdout
		wantTmp    string   // file under /tmp that must exist afterwards
	}{
		{
			name:       "relative path from the shell directory",
			dir:        "src/overlay",
			args:       []string{"build", "."},
			wantStdout: []string{"kind: ConfigMap", "name: overlay-greeting", "message: hello"},
		},
		{
			name:       "absolute path",
			dir:        "src",
			args:       []string{"build", "/src/base"},
			wantStdout: []string{"name: greeting"},
		},
		{
			name:       "Xe-style Tekton bundle",
			dir:        "src",
			args:       []string{"build", ".tekton"},
			wantStdout: []string{"kind: Pipeline", "name: xe-x-build-test", "namespace: ci", "$(params.commit)"},
		},
		{
			name:    "output into read-only /src fails",
			dir:     "src",
			args:    []string{"build", "base", "-o", "/src/out.yaml"},
			wantErr: true,
		},
		{
			name:    "output into writable /tmp works",
			dir:     "src",
			args:    []string{"build", "base", "-o", "/tmp/out.yaml"},
			wantTmp: "tmp/out.yaml",
		},
		{
			name:    "missing directory fails",
			dir:     "src",
			args:    []string{"build", "nope"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsys := sandbox(t)
			var stdout, stderr bytes.Buffer
			err := Command{}.Exec(context.Background(), &command.ExecContext{
				Stdin:   strings.NewReader(""),
				Stdout:  &stdout,
				Stderr:  &stderr,
				Dir:     tt.dir,
				Environ: expand.ListEnviron("HOME=/tmp", "TMPDIR=/tmp"),
				FS:      fsys,
			}, tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v; stderr:\n%s", err, tt.wantErr, stderr.String())
			}
			if tt.wantErr {
				t.Logf("stderr: %s", stderr.String())
			}
			for _, want := range tt.wantStdout {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("stdout lacks %q:\n%s", want, stdout.String())
				}
			}
			if tt.wantTmp != "" {
				data, err := util.ReadFile(fsys, tt.wantTmp)
				if err != nil {
					t.Fatalf("read %s: %v", tt.wantTmp, err)
				}
				if !strings.Contains(string(data), "kind: ConfigMap") {
					t.Errorf("%s = %q, want the built ConfigMap", tt.wantTmp, data)
				}
			}
		})
	}
}

func TestExecCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Command{}.Exec(ctx, &command.ExecContext{
		Stdout: io.Discard, Stderr: io.Discard, Dir: "src", FS: sandbox(t),
	}, []string{"build", "base"})
	if err == nil {
		t.Fatal("Exec with a cancelled context returned nil")
	}
}

// sandbox builds the hook filesystem: testdata as a read-only git tree at
// /src, with tekton/ renamed to .tekton as a repository keeps it, and an
// empty writable /tmp.
func sandbox(t *testing.T) billy.Filesystem {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir("testdata", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(strings.TrimPrefix(p, "testdata"+string(filepath.Separator)))
		rel = strings.Replace(rel, "tekton/", ".tekton/", 1)
		files[rel] = string(data)
		return nil
	})
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	store := memory.NewStorage()
	tree, err := object.GetTree(store, putDir(t, store, files, ""))
	if err != nil {
		t.Fatalf("get tree: %v", err)
	}
	return mountfs.New(map[string]billy.Filesystem{
		"src": treefs.New(tree),
		"tmp": memfs.New(),
	})
}

// putDir writes the files under dir (a slash path, "" for the root) as git
// objects and returns the tree hash.
func putDir(t *testing.T, store storage.Storer, files map[string]string, dir string) plumbing.Hash {
	t.Helper()
	prefix := dir
	if prefix != "" {
		prefix += "/"
	}
	subdirs := map[string]bool{}
	var entries []object.TreeEntry
	for p, data := range files {
		rest, ok := strings.CutPrefix(p, prefix)
		if !ok {
			continue
		}
		if name, _, nested := strings.Cut(rest, "/"); nested {
			if !subdirs[name] {
				subdirs[name] = true
				entries = append(entries, object.TreeEntry{Name: name, Mode: filemode.Dir, Hash: putDir(t, store, files, path.Join(dir, name))})
			}
			continue
		}
		entries = append(entries, object.TreeEntry{Name: rest, Mode: filemode.Regular, Hash: putBlob(t, store, data)})
	}
	sort.Sort(object.TreeEntrySorter(entries))
	o := store.NewEncodedObject()
	if err := (&object.Tree{Entries: entries}).Encode(o); err != nil {
		t.Fatalf("encode tree: %v", err)
	}
	h, err := store.SetEncodedObject(o)
	if err != nil {
		t.Fatalf("set tree: %v", err)
	}
	return h
}

func putBlob(t *testing.T, store storage.Storer, data string) plumbing.Hash {
	t.Helper()
	o := store.NewEncodedObject()
	o.SetType(plumbing.BlobObject)
	w, err := o.Writer()
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	if _, err := io.WriteString(w, data); err != nil {
		t.Fatalf("blob write: %v", err)
	}
	_ = w.Close()
	h, err := store.SetEncodedObject(o)
	if err != nil {
		t.Fatalf("set blob: %v", err)
	}
	return h
}
