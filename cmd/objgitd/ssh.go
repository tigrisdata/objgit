package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/go-git/go-billy/v6"
	gossh "golang.org/x/crypto/ssh"
)

const hostKeyPath = ".objgit/ssh_host_ed25519_key"

// loadOrCreateHostKey loads the server's ed25519 host key from fs, generating
// and persisting one on first use so the key survives restarts.
func loadOrCreateHostKey(fs billy.Filesystem) (gossh.Signer, error) {
	f, err := fs.Open(hostKeyPath)
	if err == nil {
		defer f.Close()
		pemBytes, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("reading ssh host key: %w", err)
		}
		signer, err := gossh.ParsePrivateKey(pemBytes)
		if err != nil {
			return nil, fmt.Errorf("parsing ssh host key: %w", err)
		}
		return signer, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("opening ssh host key: %w", err)
	}

	// Generate a new ed25519 key.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating ssh host key: %w", err)
	}

	block, err := gossh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, fmt.Errorf("marshaling ssh host key: %w", err)
	}
	pemBytes := pem.EncodeToMemory(block)

	// Persist the key. MkdirAll first in case the parent dir doesn't exist.
	if err := fs.MkdirAll(filepath.Dir(hostKeyPath), 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("creating ssh host key directory: %w", err)
	}

	wf, err := fs.Create(hostKeyPath)
	if err != nil {
		return nil, fmt.Errorf("creating ssh host key file: %w", err)
	}
	if _, err := wf.Write(pemBytes); err != nil {
		wf.Close()
		return nil, fmt.Errorf("writing ssh host key: %w", err)
	}
	if err := wf.Close(); err != nil {
		return nil, fmt.Errorf("closing ssh host key file: %w", err)
	}

	signer, err := gossh.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing generated ssh host key: %w", err)
	}

	slog.Info("created ssh host key", "path", hostKeyPath)
	return signer, nil
}
