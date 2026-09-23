package snapshot

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

func TestOpenMissing(t *testing.T) {
	_, err := Open(context.Background(), NewMemStore(), plumbing.NewHash("2222222222222222222222222222222222222222"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Open error = %v, want fs.ErrNotExist", err)
	}
}
