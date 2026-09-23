package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/memory"
)

// synthStorer serves trees from a memory.Storage and makes each blob on
// demand, so the test's own heap does not hold the tree's content. Every
// blob load samples the live heap, the same way the upstream erofs memory
// test does.
type synthStorer struct {
	*memory.Storage
	blobs map[plumbing.Hash]int // hash -> index, for the content generator
	size  int
	peak  uint64
	loads int
}

func (s *synthStorer) content(i int) []byte {
	line := fmt.Appendf(nil, "file %d: all work and no play makes jack a dull boy\n", i)
	return bytes.Repeat(line, s.size/len(line)+1)[:s.size]
}

func (s *synthStorer) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	i, ok := s.blobs[h]
	if !ok {
		return s.Storage.EncodedObject(t, h)
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.peak = max(s.peak, ms.HeapAlloc)
	s.loads++

	obj := &plumbing.MemoryObject{}
	obj.SetType(plumbing.BlobObject)
	if _, err := obj.Write(s.content(i)); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *synthStorer) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	if _, ok := s.blobs[h]; ok {
		return int64(s.size), nil
	}
	return s.Storage.EncodedObjectSize(h)
}

// TestEnsureMemory pins that Ensure streams blobs into the builder: a
// 256 MiB tree must build with a small live heap. Reading every blob during
// the walk, as Ensure did before erofs v0.8.0, holds all of it.
func TestEnsureMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("builds 256 MiB of content")
	}
	const files, size = 256, 1 << 20
	st := &synthStorer{Storage: memory.NewStorage(), blobs: map[plumbing.Hash]int{}, size: size}

	var entries []object.TreeEntry
	for i := range files {
		h := plumbing.NewHash(fmt.Sprintf("%040x", i+1))
		st.blobs[h] = i
		entries = append(entries, object.TreeEntry{Name: fmt.Sprintf("f%04d.txt", i), Mode: filemode.Regular, Hash: h})
	}
	obj := st.NewEncodedObject()
	if err := (&object.Tree{Entries: entries}).Encode(obj); err != nil {
		t.Fatal(err)
	}
	tree, err := st.SetEncodedObject(obj)
	if err != nil {
		t.Fatal(err)
	}

	store := NewMemStore()
	res, err := Ensure(context.Background(), st, store, tree, t.TempDir())
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if st.loads != files {
		t.Errorf("blob loads = %d, want %d (one for each file)", st.loads, files)
	}
	t.Logf("peak live heap: %.1f MiB for %d MiB of content; image %d bytes",
		float64(st.peak)/(1<<20), files*size>>20, res.Bytes)
	if st.peak >= 64<<20 {
		t.Errorf("peak live heap %d bytes, want less than 64 MiB", st.peak)
	}
}
