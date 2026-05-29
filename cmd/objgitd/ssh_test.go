package main

import (
	"bytes"
	"io"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
)

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
