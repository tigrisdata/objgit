package tigris

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// TestLiveBucketRoundTrip runs only when OBJGIT_TIGRIS_LIVE_BUCKET points at
// a writable bucket. Without it (CI, laptop runs) the test skips. It pins the
// spec's build-order item 2: confirm which error REAL Tigris emits for a
// missing key, and exercise the full write→read→iterate cycle over the wire.
func TestLiveBucketRoundTrip(t *testing.T) {
	bucket := os.Getenv("OBJGIT_TIGRIS_LIVE_BUCKET")
	if bucket == "" {
		t.Skip("OBJGIT_TIGRIS_LIVE_BUCKET not set; skipping live-bucket verification")
	}

	ctx := context.Background()
	s, err := New(ctx, bucket, WithObserver(func(op string, dur time.Duration, oerr error) {
		t.Logf("s3 %-14s dur=%-12s err=%v", op, dur, oerr)
	}))
	if err != nil {
		t.Fatalf("live construct: %v", err)
	}

	// Valid-format digest no sane bucket contains; probes Head-miss mapping.
	ghostHex := strings.Repeat("f", 40)
	ghost, ok := plumbing.FromHex(ghostHex)
	if !ok || ghost.String() != ghostHex {
		t.Fatalf("fixture broken: %#v", ghost)
	}
	err = s.HasEncodedObject(ghost)
	if !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Fatalf("real Tigris absence surfaced as %v — wire actual codes into isNotFound()", err)
	}
	t.Logf("live Head-miss mapped correctly (observed above)")

	// One genuine object lifecycle.
	const body = "live bucket body"
	obj := plumbing.NewMemoryObject(plumbing.FromObjectFormat(formatcfg.DefaultObjectFormat))
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len(body)))
	if _, werr := obj.Write([]byte(body)); werr != nil {
		t.Fatalf("buffer: %v", werr)
	}

	stored, serr := s.SetEncodedObject(obj)
	if serr != nil {
		t.Fatalf("set: %v", serr)
	}
	if ferr := s.up.flush(); ferr != nil {
		t.Fatalf("flush: %v", ferr)
	}
	t.Cleanup(func() {
		_, _ = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: sp(keyOf(stored))})
	})

	if herr := s.HasEncodedObject(stored); herr != nil {
		t.Fatalf("has: %v", herr)
	}

	got, gerr := s.EncodedObject(plumbing.BlobObject, stored)
	if gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	rd, rdrErr := got.Reader()
	if rdrErr != nil {
		t.Fatalf("reader: %v", rdrErr)
	}
	back, rerr := io.ReadAll(rd)
	rd.Close()
	if rerr != nil {
		t.Fatalf("body read: %v", rerr)
	}
	if string(back) != body {
		t.Fatalf("bytes survived the round trip changed: %q", back)
	}

	sz, szerr := s.EncodedObjectSize(stored)
	if szerr != nil || sz != int64(len(body)) {
		t.Fatalf("size: %d err=%v", sz, szerr)
	}

	it, ierr := s.IterEncodedObjects(plumbing.BlobObject)
	if ierr != nil {
		t.Fatalf("iter: %v", ierr)
	}
	defer it.Close()
	found := false
	if ferr := it.ForEach(func(o plumbing.EncodedObject) error {
		if o.Hash().String() == stored.String() {
			found = true
			return storer.ErrStop
		}
		return nil
	}); ferr != nil {
		t.Fatalf("iterate: %v", ferr)
	}
	if !found {
		t.Error("just-written object missing from live iteration")
	}
}

// TestLiveBucketPackRoundTrip pins the two real-Tigris behaviors the fake S3
// client can only approximate for the bin/cue pack path: that a single-range
// GetObject returns exactly the requested slice (the sub-threshold read
// tier), and that a whole-object GetObject of the same .bin returns bytes
// whose sha256 matches the pack id (the bulk-download tier's integrity
// check). Skips unless OBJGIT_TIGRIS_LIVE_BUCKET points at a writable bucket.
func TestLiveBucketPackRoundTrip(t *testing.T) {
	bucket := os.Getenv("OBJGIT_TIGRIS_LIVE_BUCKET")
	if bucket == "" {
		t.Skip("OBJGIT_TIGRIS_LIVE_BUCKET not set; skipping live-bucket verification")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	ctx := context.Background()
	// A distinct prefix per run keeps this test's packs out of the way of the
	// bucket's real contents, and makes cleanup a single listing.
	prefix := "objgit-live-pack-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	base, err := New(ctx, bucket, WithObserver(func(op string, dur time.Duration, oerr error) {
		t.Logf("s3 %-14s dur=%-12s err=%v", op, dur, oerr)
	}))
	if err != nil {
		t.Fatalf("live construct: %v", err)
	}
	s := base.Scoped(prefix)

	// Enough objects to exercise both read tiers on one pack.
	fx := buildPackFixture(t, packBulkFetchThreshold+8, false)
	writePack(t, s, fx)
	if ferr := s.up.flush(); ferr != nil {
		t.Fatalf("flush: %v", ferr)
	}

	t.Cleanup(func() {
		keys, lerr := s.listKeys(s.prefix)
		if lerr != nil {
			t.Logf("cleanup listing failed, leaving %s behind: %v", prefix, lerr)
			return
		}
		for _, k := range keys {
			if derr := s.removeSimple(k); derr != nil {
				t.Logf("cleanup of %s failed: %v", k, derr)
			}
		}
	})

	// Fresh Storer: cold cue-index build, then reads straight off the wire.
	reader := base.Scoped(prefix)

	blobs := blobHashes(fx)
	if len(blobs) <= packBulkFetchThreshold {
		t.Fatalf("fixture produced only %d blobs, need > %d", len(blobs), packBulkFetchThreshold)
	}

	// Sub-threshold: every read here is a single ranged GET against the .bin.
	for i := 0; i < packBulkFetchThreshold; i++ {
		h := blobs[i]
		obj, gerr := reader.EncodedObject(plumbing.AnyObject, h)
		if gerr != nil {
			t.Fatalf("ranged read of %s: %v", h, gerr)
		}
		assertLiveBlob(t, obj, fx.byHash[h])
	}

	// Crossing the threshold downloads the whole .bin once and verifies its
	// sha256 against the pack id — a real end-to-end integrity check.
	crossing := blobs[packBulkFetchThreshold]
	obj, gerr := reader.EncodedObject(plumbing.AnyObject, crossing)
	if gerr != nil {
		t.Fatalf("bulk-tier read of %s: %v", crossing, gerr)
	}
	assertLiveBlob(t, obj, fx.byHash[crossing])

	// Post-bulk reads come off the local copy; correctness must be unchanged.
	for i := 0; i < 3; i++ {
		h := blobs[i]
		obj, gerr := reader.EncodedObject(plumbing.AnyObject, h)
		if gerr != nil {
			t.Fatalf("post-bulk read of %s: %v", h, gerr)
		}
		assertLiveBlob(t, obj, fx.byHash[h])
	}
}

func assertLiveBlob(t *testing.T, obj plumbing.EncodedObject, want fixtureObj) {
	t.Helper()
	if obj.Type() != want.typ {
		t.Errorf("type = %v, want %v", obj.Type(), want.typ)
	}
	if obj.Size() != want.size {
		t.Errorf("size = %d, want %d", obj.Size(), want.size)
	}
	if want.blob == nil {
		return
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
	if !bytes.Equal(got, want.blob) {
		t.Errorf("body mismatch:\ngot:  %q\nwant: %q", got, want.blob)
	}
}
