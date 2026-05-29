package main

import (
	"bytes"
	"io"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing/transport"
)

func TestGitServiceFor(t *testing.T) {
	tt := []struct {
		name    string
		command string
		service string
		ok      bool
	}{
		{
			name:    "upload-pack",
			command: "git-upload-pack",
			service: transport.UploadPackService,
			ok:      true,
		},
		{
			name:    "upload-archive",
			command: "git-upload-archive",
			service: transport.UploadArchiveService,
			ok:      true,
		},
		{
			name:    "receive-pack",
			command: "git-receive-pack",
			service: transport.ReceivePackService,
			ok:      true,
		},
		{
			name:    "git-shell is unsupported",
			command: "git-shell",
			service: "",
			ok:      false,
		},
		{
			name:    "empty string is unsupported",
			command: "",
			service: "",
			ok:      false,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := gitServiceFor(tc.command)
			if ok != tc.ok {
				t.Errorf("gitServiceFor(%q) ok=%v, want %v", tc.command, ok, tc.ok)
			}
			if got != tc.service {
				t.Errorf("gitServiceFor(%q) service=%q, want %q", tc.command, got, tc.service)
			}
		})
	}
}

func TestLoadOrCreateHostKey(t *testing.T) {
	fs := memfs.New()

	s1, err := loadOrCreateHostKey(fs)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	// The key must have been persisted.
	f, err := fs.Open(hostKeyPath)
	if err != nil {
		t.Fatalf("host key not persisted at %s: %v", hostKeyPath, err)
	}
	first, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}

	// A second call must reuse the same key, not regenerate.
	s2, err := loadOrCreateHostKey(fs)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !bytes.Equal(s1.PublicKey().Marshal(), s2.PublicKey().Marshal()) {
		t.Error("second call returned a different key; expected the persisted one to be reused")
	}

	f2, err := fs.Open(hostKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	second, err := io.ReadAll(f2)
	f2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Error("host key file changed on the second call; it must not be rewritten")
	}
}
