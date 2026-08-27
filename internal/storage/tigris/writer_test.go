package tigris

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/tigrisdata/objgit/internal/bundler"
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
	if f.nputs() != 0 {
		t.Fatalf("uploaded synchronously from Close (%d puts)", f.nputs())
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
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

// TestStageWriterUploadFailureLeavesNoTrash confirms an asynchronous upload
// failure surfaces from the next flush (Close itself only enqueues) and still
// cleans up the staging file.
func TestStageWriterUploadFailureLeavesNoTrash(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.putErr = errors.New("network cut")
	s := newTestStorer(t, f)

	w := pushThrough(t, s, plumbing.BlobObject, 1, []string{"z"})
	name := w.f.Name()
	if err := w.Close(); err != nil {
		t.Fatalf("close should only enqueue, got: %v", err)
	}
	if err := s.up.flush(); err == nil {
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

// lyingObject wraps a real encoded object but claims someone else's hash —
// exactly the forgery SetEncodedObject must refuse.
type lyingObject struct {
	plumbing.EncodedObject
	hash plumbing.Hash
}

func (l lyingObject) Hash() plumbing.Hash { return l.hash }

func TestSetEncodedObjectStoresUnderRecomputedHash(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	const body = "committed through set"
	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len(body)))
	if _, err := obj.Write([]byte(body)); err != nil {
		t.Fatalf("buffer: %v", err)
	}
	want := obj.Hash()

	got, err := s.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if got != want {
		t.Fatalf("returned hash %s differs from recomputed %s", got.String(), want.String())
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	stored := f.get(t, keyOf(want))
	if string(stored.body) != body {
		t.Errorf("body mismatch: %q", stored.body)
	}
	if f.nputs() != 1 {
		t.Errorf("want one upload, got %d", f.nputs())
	}
}

func TestSetEncodedObjectForgedHashRejectedWithoutUpload(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	victim := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "innocent bystander")
	putsBefore := f.nputs()

	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len("actually different")))
	if _, err := obj.Write([]byte("actually different")); err != nil {
		t.Fatalf("buffer: %v", err)
	}

	got, err := s.SetEncodedObject(lyingObject{EncodedObject: obj, hash: victim})
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("want ErrHashMismatch, got %v", err)
	}
	if got != plumbing.ZeroHash {
		t.Errorf("want ZeroHash on refusal, got %s", got.String())
	}
	if f.nputs() != putsBefore {
		t.Error("forged object uploaded anyway")
	}
}

func TestSetEncodedObjectRejectsDeltas(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))

	delta := plumbing.NewMemoryObject(s.oh)
	delta.SetType(plumbing.OFSDeltaObject)
	if _, err := s.SetEncodedObject(delta); !errors.Is(err, plumbing.ErrInvalidType) {
		t.Errorf("delta-set went through: %v", err)
	}
}

// TestPendingOverlayServesReadsBeforeUploadLands reproduces the delta-base
// resolution bug: packfile.UpdateObjectStorage (writePack's fallback for a
// storer without PackfileWriter) reads back an object it just wrote, within
// the same push, well before any SetReference flush. With a slow PutObject,
// EncodedObject/HasEncodedObject must still see it — from the local pending
// overlay, not S3 — or a real push resolving deltas against its own bases
// fails with "object not found".
func TestPendingOverlayServesReadsBeforeUploadLands(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.putDelay = 50 * time.Millisecond
	s := newTestStorer(t, f)

	const body = "delta base content"
	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len(body)))
	if _, err := obj.Write([]byte(body)); err != nil {
		t.Fatalf("buffer: %v", err)
	}
	want := obj.Hash()

	got, err := s.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if got != want {
		t.Fatalf("hash mismatch: got %s want %s", got, want)
	}

	// The upload is still in flight (putDelay hasn't elapsed): the real
	// bucket must not have it yet, but reads must still succeed.
	if f.nputs() != 0 {
		t.Fatalf("fake bucket already saw the PUT — test no longer exercises the race")
	}
	if err := s.HasEncodedObject(want); err != nil {
		t.Fatalf("HasEncodedObject before upload lands: %v", err)
	}
	read, err := s.EncodedObject(plumbing.AnyObject, want)
	if err != nil {
		t.Fatalf("EncodedObject before upload lands: %v", err)
	}
	rd, err := read.Reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	gotBody, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(gotBody) != body {
		t.Errorf("pending body mismatch: got %q want %q", gotBody, body)
	}

	pending, ok := s.up.lookupPending(want)
	if !ok {
		t.Fatal("expected the object to still be pending before flush")
	}
	stagedPath := pending.path

	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Once flushed (and thus uploaded), the same reads must now come from S3.
	if err := s.HasEncodedObject(want); err != nil {
		t.Fatalf("HasEncodedObject after flush: %v", err)
	}
	if _, err := os.Stat(stagedPath); !os.IsNotExist(err) {
		t.Errorf("staged file survived flush: %v", err)
	}
}

// sizedJob is an uploadJob that claims a size but uploads nothing: enqueue's
// backpressure accounting is the only thing under test here.
type sizedJob struct{ size int64 }

func (j sizedJob) bytes() int64                           { return j.size }
func (j sizedJob) run(_ context.Context, _ *Storer) error { return nil }
func (j sizedJob) done(_ *Storer, _ error)                {}

// TestEnqueueClampsOversizedSizeHint pins the one case the 128 MiB pack cap
// cannot rule out. AddWait's size is a semaphore weight against the bundler's
// BufferedByteLimit, and x/sync/semaphore parks until ctx is done when a
// weight exceeds capacity outright — so an object larger than that limit,
// which legally spills into a container of its own (see packwriter.go), would
// hang its whole push. enqueue must clamp instead.
func TestEnqueueClampsOversizedSizeHint(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))
	const limit = 1 << 10
	s.up.b.BufferedByteLimit = limit

	// A generous deadline: a regression parks on the semaphore forever, so this
	// fails the test rather than stalling the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.up.enqueue(ctx, sizedJob{size: limit * 4}); err != nil {
		t.Fatalf("enqueue of a job larger than BufferedByteLimit: %v", err)
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The clamp must not leak the weight: a following normal-sized job still
	// has to fit, which it cannot if the oversized one held the whole budget.
	if err := s.up.enqueue(ctx, sizedJob{size: limit / 2}); err != nil {
		t.Fatalf("enqueue after the clamped job: %v", err)
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("second flush: %v", err)
	}
}

// TestUploadsSpanMoreThanOneBundle covers the uploader's concurrency, which
// costs real wall clock on a large push.
//
// internal/bundler cuts a bundle every BundleCountThreshold items and, with
// its default HandlerLimit of 1, runs one bundle's handler at a time. The
// handler itself uploads a bundle's jobs concurrently, so the default gives
// batches of ten uploads separated by a full barrier: nothing in batch two
// starts until every upload in batch one lands. newUploader raises
// HandlerLimit so those batches overlap.
//
// The bytes under the barrier sit in staging files on disk, not in memory, and
// run() streams them — so the budget this trades against is local disk.
func TestUploadsSpanMoreThanOneBundle(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	f.putDelay = 50 * time.Millisecond
	s := newTestStorer(t, f)

	// Four bundles' worth. Staging every object takes microseconds against a
	// 50ms upload, so all of them are queued long before the first one lands.
	const objects = 4 * bundler.DefaultBundleCountThreshold
	for i := range objects {
		obj := plumbing.NewMemoryObject(s.oh)
		obj.SetType(plumbing.BlobObject)
		body := fmt.Sprintf("object number %d", i)
		obj.SetSize(int64(len(body)))
		if _, err := obj.Write([]byte(body)); err != nil {
			t.Fatalf("buffer %d: %v", i, err)
		}
		if _, err := s.SetEncodedObject(obj); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if n := f.nputs(); n != objects {
		t.Fatalf("uploaded %d objects, want %d", n, objects)
	}
	// One bundle's worth in flight means the barrier is still there. More means
	// bundles overlap, which is the whole point of the raised limit.
	if peak := f.peakLivePuts(); peak <= bundler.DefaultBundleCountThreshold {
		t.Errorf("peak concurrent uploads was %d, want more than one bundle's %d: bundles are still serialized",
			peak, bundler.DefaultBundleCountThreshold)
	}
}

func TestAddAlternateUnsupported(t *testing.T) {
	t.Parallel()

	s := newTestStorer(t, newFakeS3(t))

	err := s.AddAlternate("../elsewhere")
	if !errors.Is(err, ErrAlternatesNotSupported) {
		t.Errorf("want ErrAlternatesNotSupported, got %v", err)
	}
}
