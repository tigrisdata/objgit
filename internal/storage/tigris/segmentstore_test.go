package tigris

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

// putRaw writes one object through the segment store the way packfile.Parser
// does, and returns the hash it landed under.
func putRaw(t *testing.T, ss *segmentStore, typ plumbing.ObjectType, body []byte) plumbing.Hash {
	t.Helper()

	w, err := ss.RawObjectWriter(typ, int64(len(body)))
	if err != nil {
		t.Fatalf("RawObjectWriter: %v", err)
	}
	if _, err := w.Write(body); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	h := plumbing.NewHasher(ss.s.of, typ, int64(len(body)))
	if _, err := h.Write(body); err != nil {
		t.Fatalf("hash: %v", err)
	}
	return h.Sum()
}

func readBack(t *testing.T, ss *segmentStore, h plumbing.Hash) (plumbing.ObjectType, []byte) {
	t.Helper()

	obj, err := ss.EncodedObject(plumbing.AnyObject, h)
	if err != nil {
		t.Fatalf("EncodedObject(%s): %v", h, err)
	}
	rd, err := obj.Reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()

	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return obj.Type(), got
}

func TestSegmentStoreRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		typ  plumbing.ObjectType
		body []byte
	}{
		{name: "tiny blob below the compression floor", typ: plumbing.BlobObject, body: []byte("hello")},
		{name: "empty blob", typ: plumbing.BlobObject, body: []byte{}},
		{name: "commit", typ: plumbing.CommitObject, body: []byte("tree deadbeef\n\nmessage\n")},
		{name: "compressible blob above the floor", typ: plumbing.BlobObject, body: bytes.Repeat([]byte("abcd"), 4096)},
		{name: "incompressible blob above the floor", typ: plumbing.BlobObject, body: pseudoRandom(8192)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ss := newSegmentStore(newTestStorer(t, newFakeS3(t)))
			defer ss.discard()

			h := putRaw(t, ss, tt.typ, tt.body)

			gotTyp, gotBody := readBack(t, ss, h)
			if gotTyp != tt.typ {
				t.Logf("want: %v", tt.typ)
				t.Logf("got:  %v", gotTyp)
				t.Error("wrong type")
			}
			if !bytes.Equal(gotBody, tt.body) {
				t.Logf("want %d bytes", len(tt.body))
				t.Logf("got  %d bytes", len(gotBody))
				t.Error("payload did not round-trip")
			}

			size, err := ss.EncodedObjectSize(h)
			if err != nil {
				t.Fatalf("EncodedObjectSize: %v", err)
			}
			if size != int64(len(tt.body)) {
				t.Logf("want: %d", len(tt.body))
				t.Logf("got:  %d", size)
				t.Error("wrong recorded size")
			}
			if err := ss.HasEncodedObject(h); err != nil {
				t.Errorf("HasEncodedObject: %v", err)
			}
		})
	}
}

func TestSegmentStoreRejections(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		typ  plumbing.ObjectType
		size int64
		err  error
	}{
		{name: "ref delta", typ: plumbing.REFDeltaObject, size: 4, err: plumbing.ErrInvalidType},
		{name: "ofs delta", typ: plumbing.OFSDeltaObject, size: 4, err: plumbing.ErrInvalidType},
		{name: "negative size", typ: plumbing.BlobObject, size: -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ss := newSegmentStore(newTestStorer(t, newFakeS3(t)))
			defer ss.discard()

			_, err := ss.RawObjectWriter(tt.typ, tt.size)
			if err == nil {
				t.Fatal("wanted an error, got none")
			}
			if tt.err != nil && !errors.Is(err, tt.err) {
				t.Logf("want: %v", tt.err)
				t.Logf("got:  %v", err)
				t.Error("wrong error")
			}
		})
	}
}

func TestSegmentStoreShortAndOverlongWrites(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		declare int64
		body    []byte
		failsOn string
	}{
		{name: "fewer bytes than declared", declare: 10, body: []byte("abc"), failsOn: "close"},
		{name: "more bytes than declared", declare: 3, body: []byte("abcdefgh"), failsOn: "write"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ss := newSegmentStore(newTestStorer(t, newFakeS3(t)))
			defer ss.discard()

			w, err := ss.RawObjectWriter(plumbing.BlobObject, tt.declare)
			if err != nil {
				t.Fatalf("RawObjectWriter: %v", err)
			}

			_, writeErr := w.Write(tt.body)
			closeErr := w.Close()

			switch tt.failsOn {
			case "write":
				if writeErr == nil {
					t.Error("wanted the write to fail, got no error")
				}
			case "close":
				if writeErr != nil {
					t.Fatalf("unexpected write error: %v", writeErr)
				}
				if closeErr == nil {
					t.Error("wanted the close to fail, got no error")
				}
			}

			if got := len(ss.segments()); got != 0 {
				t.Logf("want: 0 segments")
				t.Logf("got:  %d", got)
				t.Error("a rejected object still opened a container")
			}
		})
	}
}

func TestSegmentStoreSplitsAtByteLimit(t *testing.T) {
	t.Parallel()

	// Four objects of 1 KiB against a 2 KiB cap: the third has to open a second
	// container, and the cap is checked before the add, so no container ever
	// exceeds it.
	ss := newSegmentStore(newTestStorer(t, newFakeS3(t), withMaxPackBytes(2<<10)))
	defer ss.discard()

	var hashes []plumbing.Hash
	for i := range 4 {
		hashes = append(hashes, putRaw(t, ss, plumbing.BlobObject, pseudoRandomSeeded(1<<10, i)))
	}

	segs := ss.segments()
	if len(segs) < 2 {
		t.Logf("want: at least 2 segments")
		t.Logf("got:  %d", len(segs))
		t.Fatal("byte cap did not split the container")
	}

	total := 0
	for _, seg := range segs {
		total += len(seg.recs)
		if seg.offset > 2<<10 && len(seg.recs) > 1 {
			t.Errorf("segment holds %d bytes over a %d cap with %d records", seg.offset, 2<<10, len(seg.recs))
		}
	}
	if total != 4 {
		t.Logf("want: 4 records across all segments")
		t.Logf("got:  %d", total)
		t.Error("objects went missing across the split")
	}

	// Every object stays readable after its container filled, which is what
	// lets the parser resolve a base written long before its delta arrives.
	for i, h := range hashes {
		if _, body := readBack(t, ss, h); len(body) != 1<<10 {
			t.Errorf("object %d read back %d bytes, want %d", i, len(body), 1<<10)
		}
	}
}

func TestSegmentStoreDeduplicates(t *testing.T) {
	t.Parallel()

	ss := newSegmentStore(newTestStorer(t, newFakeS3(t)))
	defer ss.discard()

	body := []byte("the same object twice")
	first := putRaw(t, ss, plumbing.BlobObject, body)
	second := putRaw(t, ss, plumbing.BlobObject, body)

	if first != second {
		t.Fatal("the same bytes hashed differently")
	}

	segs := ss.segments()
	if len(segs) != 1 {
		t.Fatalf("want 1 segment, got %d", len(segs))
	}
	if got := len(segs[0].recs); got != 1 {
		t.Logf("want: 1 record")
		t.Logf("got:  %d", got)
		t.Error("a duplicate object was stored twice")
	}
}

func TestSegmentStoreMissFallsThroughToStorer(t *testing.T) {
	t.Parallel()

	ss := newSegmentStore(newTestStorer(t, newFakeS3(t)))
	defer ss.discard()

	// Nothing staged, so this can only be answered by the backing Storer. The
	// object is not there either, so the fall-through must surface as a
	// not-found rather than as a staging error.
	absent := plumbing.NewHasher(ss.s.of, plumbing.BlobObject, 3).Sum()

	_, err := ss.EncodedObject(plumbing.AnyObject, absent)
	if err == nil {
		t.Fatal("wanted an error for an object in neither place, got none")
	}
	if !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Logf("want: %v", plumbing.ErrObjectNotFound)
		t.Logf("got:  %v", err)
		t.Error("wrong error from the fall-through path")
	}
}

func TestSegmentStoreTypeFilter(t *testing.T) {
	t.Parallel()

	ss := newSegmentStore(newTestStorer(t, newFakeS3(t)))
	defer ss.discard()

	h := putRaw(t, ss, plumbing.BlobObject, []byte("a blob"))

	if _, err := ss.EncodedObject(plumbing.BlobObject, h); err != nil {
		t.Errorf("matching type should resolve: %v", err)
	}
	_, err := ss.EncodedObject(plumbing.CommitObject, h)
	if !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Logf("want: %v", plumbing.ErrObjectNotFound)
		t.Logf("got:  %v", err)
		t.Error("a type mismatch should read as not found")
	}
}

func TestSegmentStoreIter(t *testing.T) {
	t.Parallel()

	ss := newSegmentStore(newTestStorer(t, newFakeS3(t)))
	defer ss.discard()

	want := map[plumbing.Hash]bool{}
	for i := range 3 {
		want[putRaw(t, ss, plumbing.BlobObject, []byte(fmt.Sprintf("blob %d", i)))] = true
	}
	commit := putRaw(t, ss, plumbing.CommitObject, []byte("a commit"))

	iter, err := ss.IterEncodedObjects(plumbing.BlobObject)
	if err != nil {
		t.Fatalf("IterEncodedObjects: %v", err)
	}
	got := map[plumbing.Hash]bool{}
	if err := iter.ForEach(func(o plumbing.EncodedObject) error {
		got[o.Hash()] = true
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}

	if len(got) != len(want) {
		t.Logf("want: %d blobs", len(want))
		t.Logf("got:  %d", len(got))
		t.Error("wrong number of blobs")
	}
	for h := range want {
		if !got[h] {
			t.Errorf("blob %s missing from the iteration", h)
		}
	}
	if got[commit] {
		t.Error("the commit leaked into a blob-only iteration")
	}
}

func TestSegmentStoreDiscardLeavesNothing(t *testing.T) {
	t.Parallel()

	ss := newSegmentStore(newTestStorer(t, newFakeS3(t), withMaxPackBytes(2<<10)))
	for i := range 4 {
		putRaw(t, ss, plumbing.BlobObject, pseudoRandomSeeded(1<<10, i))
	}

	paths := []string{}
	for _, seg := range ss.segments() {
		paths = append(paths, seg.path)
	}
	if len(paths) < 2 {
		t.Fatalf("want at least 2 staging files, got %d", len(paths))
	}

	ss.discard()

	for _, p := range paths {
		if fileExists(p) {
			t.Errorf("staging file %s survived discard", p)
		}
	}
	if got := len(ss.segments()); got != 0 {
		t.Errorf("want 0 segments after discard, got %d", got)
	}
}

// pseudoRandom returns n bytes that zstd cannot shrink, so a test can reach the
// raw-codec branch deliberately.
func pseudoRandom(n int) []byte { return pseudoRandomSeeded(n, 0) }

// fileExists reports whether a staging path is still on disk.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func pseudoRandomSeeded(n, seed int) []byte {
	out := make([]byte, n)
	x := uint64(seed)*2862933555777941757 + 3037000493
	for i := range out {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x)
	}
	return out
}
