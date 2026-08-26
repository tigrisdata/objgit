package tigris

import (
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
)

// pushThrough streams chunks through a fresh RawObjectWriter and hands back
// the concrete stage writer for introspection (Close stays the caller's job
// unless stated otherwise below).
func pushThrough(t *testing.T, s *Storer, typ plumbing.ObjectType, size int64, chunks []string) *stageWriter {
	t.Helper()
	w, err := s.RawObjectWriter(typ, size)
	if err != nil {
		t.Fatalf("open raw writer: %v", err)
	}
	for _, c := range chunks {
		if _, err := io.WriteString(w, c); err != nil {
			t.Fatalf("write %q: %v", c, err)
		}
	}
	return w.(*stageWriter)
}

func TestRawObjectWriterHappyPath(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	const body = "hello world"
	w := pushThrough(t, s, plumbing.BlobObject, int64(len(body)), []string{"hello ", "world"})
	if f.nputs() != 0 {
		t.Fatalf("uploaded before Close (%d puts)", f.nputs())
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := w.Close(); err != nil { // idempotent
		t.Fatalf("second close: %v", err)
	}
	if n := f.nputs(); n != 1 {
		t.Fatalf("want exactly one upload, saw %d", n)
	}

	want := hashForBody(formatcfg.DefaultObjectFormat, plumbing.BlobObject, body)
	obj := f.get(t, keyOf(want))
	if string(obj.body) != body {
		t.Errorf("stored body mismatch: want %q got %q", body, obj.body)
	}
	if obj.meta[metaType] != plumbing.BlobObject.String() {
		t.Errorf("want metadata %s=%s", metaType, plumbing.BlobObject.String())
	}
	if obj.meta[metaSize] != fmt.Sprintf("%d", len(body)) {
		t.Errorf("want metadata %s=%d", metaSize, len(body))
	}
	if _, err := os.Stat(w.f.Name()); !os.IsNotExist(err) {
		t.Errorf("staging file survived Close: %v", err)
	}
}

func TestRawObjectWriterShortWrite(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	w := pushThrough(t, s, plumbing.BlobObject, 10, []string{"abc"}) // 3 < 10
	before := f.nputs()
	if err := w.Close(); err == nil {
		t.Fatal("short write accepted")
	}
	if f.nputs() != before {
		t.Error("short write uploaded anyway")
	}
	if _, err := os.Stat(w.f.Name()); !os.IsNotExist(err) {
		t.Error("staging file survived failed close")
	}
}

func TestRawObjectWriterOverflow(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	w, err := s.RawObjectWriter(plumbing.BlobObject, 2)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := io.WriteString(w, "longer than two"); err == nil {
		t.Fatal("overflow accepted")
	}
	if !w.(*stageWriter).done {
		t.Error("overflow did not discard the writer")
	}
	if f.nputs() != 0 {
		t.Error("overflow uploaded anyway")
	}
}

func TestRawObjectWriterRejections(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))

	tests := []struct {
		name    string
		typ     plumbing.ObjectType
		size    int64
		wantErr error
	}{
		{name: "OFSDelta refused", typ: plumbing.OFSDeltaObject, size: 5, wantErr: plumbing.ErrInvalidType},
		{name: "REFDelta refused", typ: plumbing.REFDeltaObject, size: 5, wantErr: plumbing.ErrInvalidType},
		{name: "negative size refused", typ: plumbing.BlobObject, size: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.RawObjectWriter(tt.typ, tt.size)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Logf("want: %v\ngot:  %v", tt.wantErr, err)
					t.Error("wrong error")
				}
				return
			}
			if err == nil {
				t.Error("negative size slipped through")
			}
		})
	}
}

func TestRawObjectWriterWriteAfterDiscardPoisonsStream(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	w, err := s.RawObjectWriter(plumbing.BlobObject, 100)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sw := w.(*stageWriter)
	sw.Discard()
	sw.Discard() // idempotent

	if _, err := io.WriteString(w, "late"); err == nil {
		t.Error("post-discard write accepted")
	}
	if err := w.Close(); err != nil {
		t.Errorf("discard-state close should stay quiet, got %v", err)
	}
	if f.nputs() != 0 {
		t.Error("discarded bytes leaked to the bucket")
	}
	if _, err := os.Stat(sw.f.Name()); !os.IsNotExist(err) {
		t.Errorf("staging file leaked after Discard: %v", err)
	}
}

func TestStageWriterUploadFailureLeavesNoTrash(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.putErr = errors.New("network cut")
	s := newTestStorer(t, f)

	w := pushThrough(t, s, plumbing.BlobObject, 1, []string{"z"})
	name := w.f.Name()
	if err := w.Close(); err == nil {
		t.Fatal("upload failure swallowed")
	}
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Errorf("staging file leaked after failed upload: %v", err)
	}
}

func TestNewEncodedObjectBindsFormat(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t), WithObjectFormat(formatcfg.SHA256))

	obj := s.NewEncodedObject()
	if _, ok := obj.(*plumbing.MemoryObject); !ok {
		t.Fatalf("want *plumbing.MemoryObject, got %T", obj)
	}
	obj.SetType(plumbing.CommitObject)
	obj.SetSize(0)
	if _, werr := obj.(*plumbing.MemoryObject).Write(nil); werr != nil {
		t.Fatalf("empty write rejected: %v", werr)
	}
	h := obj.Hash()
	if len(h.String()) != 64 {
		t.Errorf("sha256 object produced %d-length hash", len(h.String()))
	}
}
