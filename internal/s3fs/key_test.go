package s3fs

import (
	"strings"
	"testing"
)

func chrootedFS(t *testing.T, root string) *S3FS {
	t.Helper()
	sub, err := (&S3FS{separator: DefaultSeparator}).Chroot(root)
	if err != nil {
		t.Fatalf("Chroot(%q): %v", root, err)
	}
	return sub.(*S3FS)
}

// TestKeyRootedAndSlashFree pins the invariant the chroot fix depends on: an
// S3 key is the root-joined path with no leading slash, so writes (OpenFile)
// and lookups (Stat/Rename) resolve to the same object.
func TestKeyRootedAndSlashFree(t *testing.T) {
	fs := chrootedFS(t, "/xeiaso.net/objgit.git")

	for _, tt := range []struct {
		name, in, want string
	}{
		{name: "plain", in: "config", want: "xeiaso.net/objgit.git/config"},
		{name: "nested", in: "objects/ab/cd", want: "xeiaso.net/objgit.git/objects/ab/cd"},
		{name: "leading slash input", in: "/HEAD", want: "xeiaso.net/objgit.git/HEAD"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := fs.key(tt.in)
			if got != tt.want {
				t.Logf("want: %q", tt.want)
				t.Logf("got:  %q", got)
				t.Error("wrong S3 key")
			}
		})
	}
}

// TestTempFileNameIsRootRelative guards the contract go-git's dotgit relies on:
// File.Name() is root-relative, so fs.Rename(file.Name(), dst) works. The S3
// key derived from that name must still carry the chroot prefix.
func TestTempFileNameIsRootRelative(t *testing.T) {
	fs := chrootedFS(t, "/repo.git")

	f, err := fs.TempFile("objects/pack", "tmp_obj_")
	if err != nil {
		t.Fatalf("TempFile: %v", err)
	}

	name := f.Name()
	if !strings.HasPrefix(name, "objects/pack/tmp_obj_") {
		t.Fatalf("Name() = %q, want a root-relative path under objects/pack/tmp_obj_", name)
	}

	const wantPrefix = "repo.git/objects/pack/tmp_obj_"
	if got := fs.key(name); !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("key(Name()) = %q, want prefix %q", got, wantPrefix)
	}
}
