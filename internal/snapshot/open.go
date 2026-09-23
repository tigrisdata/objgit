package snapshot

import (
	"context"
	"fmt"

	"github.com/Xe/erofs"
	"github.com/go-git/go-git/v6/plumbing"
)

// Snapshot is an open image. Every read is local. Close releases the file.
type Snapshot struct {
	*erofs.FS
	f File
}

// Close releases the local file behind the image.
func (s *Snapshot) Close() error { return s.f.Close() }

// Open returns the image of tree. It does not build a missing image: it
// returns an error that matches fs.ErrNotExist instead. Call Ensure first when
// the image must exist.
func Open(ctx context.Context, store Store, tree plumbing.Hash) (*Snapshot, error) {
	key := Key(tree)
	f, err := store.OpenSnapshot(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open %s: %w", key, err)
	}
	fsys, err := erofs.Open(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("snapshot: read %s: %w", key, err)
	}
	return &Snapshot{FS: fsys, f: f}, nil
}
