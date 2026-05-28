package unixmeta

import (
	"os"
	"testing"
	"time"
)

func TestModeRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		mode os.FileMode
	}{
		{name: "regular file rw-r--r--", mode: 0o644},
		{name: "regular file rwxr-xr-x", mode: 0o755},
		{name: "directory", mode: os.ModeDir | 0o755},
		{name: "symlink", mode: os.ModeSymlink | 0o777},
		{name: "block device", mode: os.ModeDevice | 0o660},
		{name: "char device", mode: os.ModeDevice | os.ModeCharDevice | 0o620},
		{name: "fifo", mode: os.ModeNamedPipe | 0o644},
		{name: "socket", mode: os.ModeSocket | 0o755},
		{name: "setuid", mode: os.ModeSetuid | 0o755},
		{name: "setgid", mode: os.ModeSetgid | 0o755},
		{name: "sticky dir", mode: os.ModeDir | os.ModeSticky | 0o777},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := GoFileMode(PosixMode(tt.mode))
			if got != tt.mode {
				t.Logf("want: %v (%#o)", tt.mode, uint32(tt.mode))
				t.Logf("got:  %v (%#o)", got, uint32(got))
				t.Error("mode did not survive round trip")
			}
		})
	}
}

func TestPosixModeKnownValue(t *testing.T) {
	t.Parallel()

	// 0o100644 == 33188: regular file with rw-r--r--, per the reference doc.
	if got := PosixMode(0o644); got != 0o100644 {
		t.Errorf("PosixMode(0o644) = %#o, want %#o", got, 0o100644)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	mtime := time.Unix(1716700000, 0)

	for _, tt := range []struct {
		name string
		in   Attrs
	}{
		{
			name: "regular file",
			in:   Attrs{UID: 1000, GID: 1000, Mode: 0o644, Mtime: mtime},
		},
		{
			name: "directory",
			in:   Attrs{UID: 0, GID: 0, Mode: os.ModeDir | 0o755, Mtime: mtime},
		},
		{
			name: "char device with rdev",
			in:   Attrs{UID: 0, GID: 5, Mode: os.ModeDevice | os.ModeCharDevice | 0o620, Rdev: 1280, Mtime: mtime},
		},
		{
			name: "symlink with awkward target",
			in:   Attrs{UID: 1000, GID: 1000, Mode: os.ModeSymlink | 0o777, Mtime: mtime, SymlinkTarget: "/etc/has spaces/and%percent"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Decode(Encode(tt.in), Attrs{})
			if got.UID != tt.in.UID || got.GID != tt.in.GID {
				t.Errorf("uid/gid: want %d/%d, got %d/%d", tt.in.UID, tt.in.GID, got.UID, got.GID)
			}
			if got.Mode != tt.in.Mode {
				t.Errorf("mode: want %v, got %v", tt.in.Mode, got.Mode)
			}
			if !got.Mtime.Equal(tt.in.Mtime) {
				t.Errorf("mtime: want %v, got %v", tt.in.Mtime, got.Mtime)
			}
			if got.SymlinkTarget != tt.in.SymlinkTarget {
				t.Errorf("symlink target: want %q, got %q", tt.in.SymlinkTarget, got.SymlinkTarget)
			}
			if tt.in.Rdev != 0 && got.Rdev != tt.in.Rdev {
				t.Errorf("rdev: want %d, got %d", tt.in.Rdev, got.Rdev)
			}
		})
	}
}

func TestDecodeMissingKeysKeepDefaults(t *testing.T) {
	t.Parallel()

	defaults := Attrs{UID: 501, GID: 20, Mode: 0o644, Mtime: time.Unix(42, 0)}
	got := Decode(map[string]string{}, defaults)
	if got != defaults {
		t.Logf("want: %+v", defaults)
		t.Logf("got:  %+v", got)
		t.Error("empty metadata should leave defaults untouched")
	}
}

func TestDecodeMalformedTreatedAsMissing(t *testing.T) {
	t.Parallel()

	defaults := Attrs{UID: 501, GID: 20, Mode: 0o644, Mtime: time.Unix(42, 0)}
	meta := map[string]string{
		"uid":   "not-a-number",
		"gid":   "99",
		"mode":  "garbage",
		"mtime": "also-bad",
	}
	got := Decode(meta, defaults)
	if got.UID != defaults.UID {
		t.Errorf("malformed uid should fall back to default: want %d, got %d", defaults.UID, got.UID)
	}
	if got.GID != 99 {
		t.Errorf("valid gid should be parsed: want 99, got %d", got.GID)
	}
	if got.Mode != defaults.Mode {
		t.Errorf("malformed mode should fall back to default: want %v, got %v", defaults.Mode, got.Mode)
	}
	if !got.Mtime.Equal(defaults.Mtime) {
		t.Errorf("malformed mtime should fall back to default: want %v, got %v", defaults.Mtime, got.Mtime)
	}
}
