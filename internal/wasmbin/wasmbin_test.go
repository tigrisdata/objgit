package wasmbin

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Xe/kefka/command"
	"github.com/Xe/kefka/command/registry"
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

// repoBin is the bin directory at the root of the repository, which the
// image copies to /usr/libexec/objgit/bin.
const repoBin = "../../bin"

// trueWASM is the smallest WASI command: one exported _start that returns.
var trueWASM = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // magic, version 1
	0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type section: () -> ()
	0x03, 0x02, 0x01, 0x00, // function section: one function of type 0
	0x07, 0x0a, 0x01, 0x06, '_', 's', 't', 'a', 'r', 't', 0x00, 0x00, // export _start
	0x0a, 0x04, 0x01, 0x02, 0x00, 0x0b, // code section: an empty body
}

// lfsPointer is what a checkout without Git LFS hydration has in place of a
// .wasm file.
var lfsPointer = []byte("version https://git-lfs.github.com/spec/v1\noid sha256:1724a4e9\nsize 24380813\n")

// binDir writes files into a new directory and returns its path. A name that
// ends in a slash makes an empty directory.
func binDir(t *testing.T, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if strings.HasSuffix(name, "/") {
			if err := os.MkdirAll(p, 0o755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func load(t *testing.T, cacheDir string, dirs ...string) *Set {
	t.Helper()
	s, err := Load(context.Background(), dirs, cacheDir)
	if err != nil {
		t.Fatalf("Load(%q): %v", dirs, err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func quietExec() *command.ExecContext {
	return &command.ExecContext{Stdout: io.Discard, Stderr: io.Discard, FS: memfs.New()}
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name string
		dirs []map[string][]byte // a nil map is a directory that does not exist
		want map[string]int      // command name -> index of the directory it comes from
	}{
		{
			name: "one command for each .wasm file",
			dirs: []map[string][]byte{{"kustomize.wasm": trueWASM, "jq.wasm": trueWASM}},
			want: map[string]int{"jq": 0, "kustomize": 0},
		},
		{
			name: "other files and directories are skipped",
			dirs: []map[string][]byte{{
				"README.md":       []byte("# bin\n"),
				"kustomize":       trueWASM,
				".wasm":           trueWASM,
				"dir.wasm/":       nil,
				"nested/jq.wasm":  trueWASM,
				"yq.wasm":         trueWASM,
				"notes.wasm.orig": trueWASM,
			}},
			want: map[string]int{"yq": 0},
		},
		{
			name: "an empty directory has no commands",
			dirs: []map[string][]byte{{}},
			want: map[string]int{},
		},
		{
			name: "Load does not read the files",
			dirs: []map[string][]byte{{"kustomize.wasm": lfsPointer}},
			want: map[string]int{"kustomize": 0},
		},
		{
			name: "an earlier directory wins, as in PATH",
			dirs: []map[string][]byte{
				{"kustomize.wasm": trueWASM},
				{"kustomize.wasm": trueWASM, "jq.wasm": trueWASM},
			},
			want: map[string]int{"kustomize": 0, "jq": 1},
		},
		{
			name: "a directory that does not exist is skipped",
			dirs: []map[string][]byte{nil, {"jq.wasm": trueWASM}},
			want: map[string]int{"jq": 1},
		},
		{
			name: "no directory exists",
			dirs: []map[string][]byte{nil, nil},
			want: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dirs []string
			for _, files := range tt.dirs {
				if files == nil {
					dirs = append(dirs, filepath.Join(t.TempDir(), "nope"))
					continue
				}
				dirs = append(dirs, binDir(t, files))
			}
			s := load(t, "", dirs...)

			want := slices.Sorted(maps.Keys(tt.want))
			if got := s.Names(); !slices.Equal(got, want) {
				t.Fatalf("Names() = %q, want %q", got, want)
			}
			for name, i := range tt.want {
				if got, want := s.modules[name].path, filepath.Join(dirs[i], name+".wasm"); got != want {
					t.Errorf("%s comes from %s, want %s", name, got, want)
				}
			}
		})
	}
}

func TestLoadSkipsEmptyEntries(t *testing.T) {
	s := load(t, "", "", binDir(t, map[string][]byte{"jq.wasm": trueWASM}), " ")
	if got := s.Names(); !slices.Equal(got, []string{"jq"}) {
		t.Fatalf("Names() = %q, want [jq]", got)
	}
}

func TestLoadNotADirectory(t *testing.T) {
	file := filepath.Join(binDir(t, map[string][]byte{"jq.wasm": trueWASM}), "jq.wasm")
	if _, err := Load(context.Background(), []string{file}, ""); err == nil {
		t.Fatal("Load of a file returned nil")
	}
}

// stub is a built-in command that a bin directory can replace.
type stub struct{}

func (stub) Exec(context.Context, *command.ExecContext, []string) error {
	return errors.New("stub ran")
}

func TestRegister(t *testing.T) {
	reg := registry.New()
	reg.Register("jq", stub{})
	reg.Register("rg", stub{})
	load(t, "", binDir(t, map[string][]byte{"jq.wasm": trueWASM, "true.wasm": trueWASM})).Register(reg)

	for _, name := range []string{"jq", "true"} {
		cmd, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if err := cmd.Exec(context.Background(), quietExec(), nil); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if cmd, _ := reg.Get("rg"); cmd != (stub{}) {
		t.Errorf("rg = %T, want the built-in it did not replace", cmd)
	}
}

func TestNilSet(t *testing.T) {
	var s *Set
	reg := registry.New()
	s.Register(reg)
	if names := reg.Names(); len(names) != 0 {
		t.Errorf("a nil Set registered %q", names)
	}
	if names := s.Names(); names != nil {
		t.Errorf("Names() = %q, want nil", names)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestExecReadsOnFirstUse(t *testing.T) {
	dir := binDir(t, map[string][]byte{"true.wasm": lfsPointer})
	s := load(t, "", dir)
	if err := os.WriteFile(filepath.Join(dir, "true.wasm"), trueWASM, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.modules["true"].Exec(context.Background(), quietExec(), nil); err != nil {
		t.Fatalf("Exec after the file changed: %v", err)
	}
}

func TestExecRejectsLFSPointer(t *testing.T) {
	s := load(t, "", binDir(t, map[string][]byte{"kustomize.wasm": lfsPointer}))
	err := s.modules["kustomize"].Exec(context.Background(), quietExec(), []string{"version"})
	if err == nil || !strings.Contains(err.Error(), "Git LFS pointer") {
		t.Fatalf("err = %v, want an error that names the Git LFS pointer", err)
	}
}

func TestExecMissingFile(t *testing.T) {
	dir := binDir(t, map[string][]byte{"true.wasm": trueWASM})
	s := load(t, "", dir)
	if err := os.Remove(filepath.Join(dir, "true.wasm")); err != nil {
		t.Fatal(err)
	}
	err := s.modules["true"].Exec(context.Background(), quietExec(), nil)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v, want fs.ErrNotExist", err)
	}
}

func TestExecCancelled(t *testing.T) {
	s := load(t, "", binDir(t, map[string][]byte{"true.wasm": trueWASM}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.modules["true"].Exec(ctx, quietExec(), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestCompilationCache(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "wasm")
	s := load(t, cacheDir, binDir(t, map[string][]byte{"true.wasm": trueWASM}))
	if err := s.modules["true"].Exec(context.Background(), quietExec(), nil); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("read cache directory: %v", err)
	}
	if len(entries) == 0 {
		t.Error("the compilation cache is empty after a run")
	}
}

// TestRepoBin checks the programs that the image ships. A checkout without
// Git LFS hydration has pointer files here.
func TestRepoBin(t *testing.T) {
	s := load(t, "", repoBin)
	if !slices.Contains(s.Names(), "kustomize") {
		t.Fatalf("Names() = %q, want kustomize", s.Names())
	}
	for _, name := range s.Names() {
		data, err := os.ReadFile(filepath.Join(repoBin, name+".wasm"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(data, wasmMagic) {
			t.Errorf("%s.wasm starts with %q, not the WASM magic; run git lfs pull", name, data[:min(len(data), 16)])
		}
	}
}

// repoSet loads repoBin once, so the kustomize tests compile it once.
var repoSet = sync.OnceValues(func() (*Set, error) {
	return Load(context.Background(), []string{repoBin}, "")
})

func TestKustomize(t *testing.T) {
	s, err := repoSet()
	if err != nil {
		t.Fatal(err)
	}
	kustomize := s.modules["kustomize"]

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
			err := kustomize.Exec(context.Background(), &command.ExecContext{
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
