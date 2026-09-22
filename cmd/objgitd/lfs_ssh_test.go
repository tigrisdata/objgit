package main

import (
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runSSHCommand runs one command over ssh against addr, returning stdout,
// stderr, and the exit status. It does not fail the test on a non-zero exit,
// because the exit code is what several of these cases assert on.
func runSSHCommand(t *testing.T, addr string, args ...string) (stdout, stderr string, code int) {
	t.Helper()

	keyDir := t.TempDir()
	key := filepath.Join(keyDir, "id_ed25519")
	if out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-f", key).CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v\n%s", err, out)
	}

	host, port, ok := strings.Cut(addr, ":")
	if !ok {
		t.Fatalf("bad address %q", addr)
	}

	sshArgs := []string{
		"-i", key,
		"-o", "IdentitiesOnly=yes",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-p", port,
		"git@" + host,
	}
	cmd := exec.Command("ssh", append(sshArgs, args...)...)

	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()

	code = 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("running ssh: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return outBuf.String(), errBuf.String(), code
}

func withLFS(external string, allowLocks bool) func(*daemon) {
	return func(d *daemon) {
		d.lfs = &lfsService{
			ttl:         15 * time.Minute,
			maxSize:     1 << 30,
			maxBatch:    100,
			externalURL: external,
			allowLocks:  allowLocks,
		}
	}
}

// TestSSHLFSAuthenticate covers the handshake an ssh:// remote makes before it
// can use LFS at all: git-lfs asks the SSH server where the HTTP API lives.
func TestSSHLFSAuthenticate(t *testing.T) {
	checkSSHBinaries(t)

	addr, _ := startSSHServer(t, true, false, withLFS("https://git.example.com", true))

	stdout, stderr, code := runSSHCommand(t, addr,
		"git-lfs-authenticate", "acme/test.git", "download")
	if code != 0 {
		t.Fatalf("exit %d, want 0\nstderr: %s", code, stderr)
	}

	var got struct {
		Href      string            `json:"href"`
		Header    map[string]string `json:"header"`
		ExpiresIn int               `json:"expires_in"`
	}
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("decoding %q: %v", stdout, err)
	}

	const want = "https://git.example.com/acme/test.git/info/lfs"
	if got.Href != want {
		t.Errorf("href = %q, want %q", got.Href, want)
	}
	if got.ExpiresIn <= 0 {
		t.Errorf("expires_in = %d, want a positive value", got.ExpiresIn)
	}
}

// TestSSHLFSAuthenticateExits127WhenUnavailable is the fallback contract.
// git-lfs only falls back to guessing the HTTP endpoint when the command exits
// 127 or says "command not found"; any other non-zero exit is reported to the
// user as a hard failure. Without this, turning LFS off would break every
// `git lfs` operation over ssh:// rather than leaving it alone.
func TestSSHLFSAuthenticateExits127WhenUnavailable(t *testing.T) {
	checkSSHBinaries(t)

	for _, tt := range []struct {
		name string
		opts []func(*daemon)
	}{
		{
			name: "lfs is disabled",
			opts: nil,
		},
		{
			name: "lfs is on but no external url is configured",
			opts: []func(*daemon){withLFS("", true)},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			addr, _ := startSSHServer(t, true, false, tt.opts...)

			_, stderr, code := runSSHCommand(t, addr,
				"git-lfs-authenticate", "acme/test.git", "download")

			if code != 127 {
				t.Fatalf("exit %d, want 127\nstderr: %s", code, stderr)
			}
			if !strings.Contains(stderr, "command not found") {
				t.Errorf("stderr = %q, want it to mention \"command not found\"", stderr)
			}
		})
	}
}

func TestSSHLFSAuthenticateHonorsAuthorization(t *testing.T) {
	checkSSHBinaries(t)

	addr, _ := startSSHServer(t, false, false, withLFS("https://git.example.com", true))

	// Reads stay allowed with pushes disabled.
	if _, stderr, code := runSSHCommand(t, addr,
		"git-lfs-authenticate", "acme/test.git", "download"); code != 0 {
		t.Fatalf("download exit %d, want 0\nstderr: %s", code, stderr)
	}

	// An upload is a write, so the -allow-push gate applies. This must not be
	// a 127: the command exists and deliberately refused.
	_, stderr, code := runSSHCommand(t, addr,
		"git-lfs-authenticate", "acme/test.git", "upload")
	if code == 0 {
		t.Fatal("upload was authorized with pushes disabled")
	}
	if code == 127 {
		t.Fatal("a denied request must not look like a missing command")
	}
	if !strings.Contains(stderr, "denied") {
		t.Errorf("stderr = %q, want it to mention the denial", stderr)
	}
}

func TestSSHLFSAuthenticateRejectsABadPath(t *testing.T) {
	checkSSHBinaries(t)

	addr, _ := startSSHServer(t, true, false, withLFS("https://git.example.com", true))

	_, _, code := runSSHCommand(t, addr,
		"git-lfs-authenticate", "not-a-two-segment-path-because-it-has-one", "download")
	if code == 0 {
		t.Fatal("a malformed repository path was accepted")
	}
	if code == 127 {
		t.Fatal("a bad path must not look like a missing command")
	}
}
