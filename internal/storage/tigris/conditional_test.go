package tigris

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestFakeS3HonorsConditionalWrites pins the fake against the behavior
// TestLiveBucketConditionalWrites observed on real Tigris. Every
// compare-and-swap test in this package trusts the fake, so the fake gets its
// own test.
func TestFakeS3HonorsConditionalWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFakeS3(t)
	const key = "packed-refs"

	body := func(s string) *s3.PutObjectInput {
		return &s3.PutObjectInput{Bucket: sp("test-bucket"), Key: sp(key), Body: newReader(s)}
	}

	t.Run("create-if-absent succeeds then refuses", func(t *testing.T) {
		in := body("one")
		in.IfNoneMatch = sp("*")
		out, err := f.PutObject(ctx, in)
		if err != nil {
			t.Fatalf("first create-if-absent: %v", err)
		}
		if sv(out.ETag) == "" {
			t.Fatal("PutObject returned no ETag")
		}

		again := body("two")
		again.IfNoneMatch = sp("*")
		if _, err := f.PutObject(ctx, again); !isPreconditionFailed(err) {
			t.Fatalf("second create-if-absent must fail the precondition, got %v", err)
		}
		if got := string(f.get(t, key).body); got != "one" {
			t.Errorf("refused write still landed: body = %q", got)
		}
	})

	t.Run("GetObject reports the ETag", func(t *testing.T) {
		out, err := f.GetObject(ctx, &s3.GetObjectInput{Bucket: sp("test-bucket"), Key: sp(key)})
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if sv(out.ETag) != f.etagOf(key) {
			t.Errorf("GetObject ETag %q disagrees with the fake's %q", sv(out.ETag), f.etagOf(key))
		}
	})

	t.Run("compare-and-swap succeeds on a fresh ETag and refuses a stale one", func(t *testing.T) {
		stale := f.etagOf(key)

		in := body("three")
		in.IfMatch = sp(stale)
		out, err := f.PutObject(ctx, in)
		if err != nil {
			t.Fatalf("swap on a fresh ETag: %v", err)
		}
		if sv(out.ETag) == stale {
			t.Error("ETag did not change across a write")
		}

		again := body("four")
		again.IfMatch = sp(stale)
		if _, err := f.PutObject(ctx, again); !isPreconditionFailed(err) {
			t.Fatalf("swap on a stale ETag must fail the precondition, got %v", err)
		}
		if got := string(f.get(t, key).body); got != "three" {
			t.Errorf("refused swap still landed: body = %q", got)
		}
	})

	t.Run("compare-and-swap on an absent key refuses", func(t *testing.T) {
		in := &s3.PutObjectInput{Bucket: sp("test-bucket"), Key: sp("nothing-here"), Body: newReader("x"), IfMatch: sp("whatever")}
		if _, err := f.PutObject(ctx, in); !isPreconditionFailed(err) {
			t.Fatalf("If-Match on an absent key must fail the precondition, got %v", err)
		}
	})
}
