package tigris

import (
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

func seedManyBlobs(t *testing.T, f *fakeS3, of formatcfg.ObjectFormat, contents ...string) []plumbing.Hash {
	t.Helper()
	hs := make([]plumbing.Hash, 0, len(contents))
	for _, c := range contents {
		hs = append(hs, seed(t, f, of, plumbing.BlobObject, c))
	}
	return hs
}

func TestListKeysPaginatesAndSorts(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.listMax = 2
	seeds := seedManyBlobs(t, f, formatcfg.DefaultObjectFormat, "one", "two", "three", "four", "five")
	f.put("objects/not-a-hash", "decoy", nil)

	s := newTestStorer(t, f)

	keys, err := s.listKeys(objectPrefix)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	want := make([]string, 0, len(seeds)+1)
	for _, h := range seeds {
		want = append(want, keyOf(h))
	}
	want = append(want, "objects/not-a-hash")
	slices.Sort(want)

	if !slices.Equal(keys, want) {
		t.Errorf("keys mismatch:\nwant %v\ngot  %v", want, keys)
	}
	if !slices.IsSorted(keys) {
		t.Errorf("S3 order guarantee broken: %v", keys)
	}
}

func TestIterEncodedObjectsFilterAndOrder(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.listMax = 1 // force many tiny pages
	of := formatcfg.DefaultObjectFormat
	blobs := seedManyBlobs(t, f, of, "blob-a", "blob-b", "blob-c")
	commits := seedManyCommits(t, f, of, "commit-a", "commit-b")
	f.put("objects/beefcafe", "foreign junk", nil) // undecodable key skipped

	s := newTestStorer(t, f)

	it, err := s.IterEncodedObjects(plumbing.BlobObject)
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	var got []plumbing.Hash
	for {
		obj, nerr := it.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			t.Fatalf("next: %v", nerr)
		}
		got = append(got, obj.Hash())
	}

	want := append(slices.Clone(blobs), commits[:0]...) // blobs only
	slices.SortFunc(want, func(a, b plumbing.Hash) int {
		if a.String() < b.String() {
			return -1
		}
		if a.String() > b.String() {
			return 1
		}
		return 0
	})
	if !slices.EqualFunc(got, want, func(a, b plumbing.Hash) bool { return a.String() == b.String() }) {
		t.Errorf("hash order mismatch:\nwant %v\ngot  %v",
			formatHashes(want), formatHashes(got))
	}
}

func seedManyCommits(t *testing.T, f *fakeS3, of formatcfg.ObjectFormat, contents ...string) []plumbing.Hash {
	t.Helper()
	hs := make([]plumbing.Hash, 0, len(contents))
	for _, c := range contents {
		hs = append(hs, seed(t, f, of, plumbing.CommitObject, c))
	}
	return hs
}

func formatHashes(in []plumbing.Hash) []string {
	out := make([]string, 0, len(in))
	for _, h := range in {
		out = append(out, h.String())
	}
	return out
}

func TestIterForEachStopAndErrors(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	seedManyBlobs(t, f, formatcfg.DefaultObjectFormat, "eins", "zwei", "drei")
	s := newTestStorer(t, f)

	it, err := s.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	visited := 0
	err = it.ForEach(func(plumbing.EncodedObject) error {
		visited++
		if visited == 2 {
			return storer.ErrStop
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach surfaced stop as error: %v", err)
	}
	if visited != 2 {
		t.Errorf("want stop after 2 visits, saw %d", visited)
	}

	boom := errors.New("callback boom")
	it2, _ := s.IterEncodedObjects(plumbing.AnyObject)
	defer it2.Close()
	if err := it2.ForEach(func(plumbing.EncodedObject) error { return boom }); !errors.Is(err, boom) {
		t.Errorf("custom callback error not propagated: %v", err)
	}

	it3, _ := s.IterEncodedObjects(plumbing.AnyObject)
	defer it3.Close()
	full := 0
	if err := it3.ForEach(func(plumbing.EncodedObject) error { full++; return nil }); err != nil || full != 3 {
		t.Errorf("natural end misbehaved: %d visits, err=%v", full, err)
	}
}

func TestIterSurvivesVanishedKeys(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.listMax = 1
	seedManyBlobs(t, f, formatcfg.DefaultObjectFormat, "stable-one", "stable-two", "stable-three")

	s := newTestStorer(t, f)
	it, err := s.IterEncodedObjects(plumbing.AnyObject) // snapshot taken
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	// Wipe everything AFTER the iterator holds its LIST snapshot: per-key
	// HEADs now report misses exactly like a racing deleter would.
	for _, k := range objectsKeySnapshot(f) {
		f.del(k)
	}

	count := 0
	for {
		obj, nerr := it.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			t.Fatalf("vanished object became fatal: %v", nerr)
		}
		_ = obj
		count++
	}
	if count != 0 {
		t.Errorf("expected zero survivors, got %d", count)
	}
}

func objectsKeySnapshot(f *fakeS3) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.objs {
		keys = append(keys, k)
	}
	return keys
}
