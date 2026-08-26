package tigris

import (
	"errors"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/index"
)

func TestIndexRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	t.Run("fresh bucket yields pristine v2 index", func(t *testing.T) {
		got, err := s.Index()
		if err != nil {
			t.Fatalf("first read: %v", err)
		}
		if got.Version != 2 || len(got.Entries) != 0 {
			t.Errorf("want pristine v2, got version=%d entries=%d", got.Version, len(got.Entries))
		}
	})

	in := &index.Index{Version: 2}
	entry := &index.Entry{
		Name:       "docs/a.txt",
		CreatedAt:  time.Unix(1700000000, 0).UTC(),
		ModifiedAt: time.Unix(1700000001, 0).UTC(),
		Dev:        1,
		Inode:      42,
		Mode:       filemode.Regular,
		Size:       11,
	}
	entry.Hash, _ = plumbing.FromHex(headAB)
	in.Entries = append(in.Entries, entry)

	if err := s.SetIndex(in); err != nil {
		t.Fatalf("set: %v", err)
	}
	out, err := s.Index()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(out.Entries) != 1 {
		t.Fatalf("entry count lost: %d", len(out.Entries))
	}
	e := out.Entries[0]
	if e.Name != "docs/a.txt" || e.Mode != filemode.Regular || e.Size != 11 ||
		e.Dev != 1 || e.Inode != 42 || e.Hash.String() != headAB {
		t.Errorf("entry fields mangled: %+v", e)
	}
}

func TestConfigRoundTrip(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	cfg, err := s.Config()
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected lazily created default config, got nil")
	}

	cfg.Core.Worktree = "/tmp/demo-worktree"
	cfg.User.Name = "Xe Iaso"
	if err := s.SetConfig(cfg); err != nil {
		t.Fatalf("set: %v", err)
	}

	reloaded, err := s.Config()
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.Core.Worktree != "/tmp/demo-worktree" || reloaded.User.Name != "Xe Iaso" {
		t.Errorf("values lost across save: core=%+v user=%+v", reloaded.Core, reloaded.User)
	}
}

func TestModuleExplicitlyUnsupported(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))

	_, err := s.Module("vendor/dep")
	if !errors.Is(err, ErrModulesNotSupported) {
		t.Errorf("want ErrModulesNotSupported, got %v", err)
	}
}
