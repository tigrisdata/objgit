package tigris

import (
	"context"
	"errors"
	"io"
	"os"
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
