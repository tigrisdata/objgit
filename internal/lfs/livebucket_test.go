package lfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	tstorage "github.com/tigrisdata/storage-go"
)

// The facts in this file cannot be learned from a fake bucket, and they are the
// ones the whole design rests on:
//
//  1. Tigris enforces x-amz-checksum-sha256 on a presigned PUT, so a client
//     cannot write bytes that do not hash to the key.
//  2. The checksum is inside the signature, so a client cannot drop the header
//     to escape that check.
//  3. HeadObject with ChecksumModeEnabled returns the stored digest, which is
//     what Verify compares against.
//
// CI never runs these: it has no Tigris credentials. Run them by hand against a
// throw-away bucket before trusting a change to presign.go or store.go:
//
//	AWS_PROFILE=tigris OBJGIT_TIGRIS_LIVE_BUCKET=my-test-bucket \
//	  go test ./internal/lfs/ -run TestLive -v
//
// Use a Single-region or Multi-region bucket. A Global bucket reads eventually,
// which breaks the conditional writes the lock document depends on.

// liveStore builds a Store against a real bucket, or skips the test.
func liveStore(t *testing.T) (*Store, string) {
	t.Helper()

	bucket := os.Getenv("OBJGIT_TIGRIS_LIVE_BUCKET")
	if bucket == "" {
		t.Skip("OBJGIT_TIGRIS_LIVE_BUCKET not set")
	}

	client, err := tstorage.New(context.Background())
	if err != nil {
		t.Fatalf("dialing Tigris: %v", err)
	}
	return NewStore(client, NewPresigner(client.Client, bucket), bucket), bucket
}

// liveOID mints an object unique to this run, so a failed test never leaves
// another run reading its bytes.
func liveOID(t *testing.T) (oid string, body []byte) {
	t.Helper()
	body = fmt.Appendf(nil, "objgit live test %d", time.Now().UnixNano())
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), body
}

// putPresigned performs the transfer a git-lfs client would, sending exactly
// the headers the batch response named.
func putPresigned(t *testing.T, href string, header map[string]string, body []byte) int {
	t.Helper()

	req, err := http.NewRequest(http.MethodPut, href, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building upload request: %v", err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("uploading: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(resp.Body)
		t.Logf("upload answered %d: %s", resp.StatusCode, detail)
	}
	return resp.StatusCode
}

// TestLiveUploadIntegrity is the acceptance gate for the presigned upload path.
func TestLiveUploadIntegrity(t *testing.T) {
	s, bucket := liveStore(t)
	ctx := context.Background()

	oid, body := liveOID(t)
	t.Cleanup(func() {
		_, _ = s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(BlobKey(oid)),
		})
		_, _ = s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(MarkerKey(testRepo, oid)),
		})
		_, _ = s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(StagingKey(testRepo, oid)),
		})
	})

	// Uploads go to this repository's staging key, exactly as the batch
	// endpoint directs a real client.
	req, err := s.Presigner().PresignPut(ctx, StagingKey(testRepo, oid), oid, 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}
	header := TransferHeader(req)

	t.Run("bytes that do not match the oid are rejected", func(t *testing.T) {
		if code := putPresigned(t, req.URL, header, []byte("not the right bytes")); code < 400 {
			t.Fatalf("upload of wrong bytes answered %d; the bucket is not enforcing the checksum", code)
		}
	})

	t.Run("dropping the checksum header is rejected", func(t *testing.T) {
		// The checksum is a signed header, so removing it invalidates the
		// signature. Without this the checksum would be advisory.
		if code := putPresigned(t, req.URL, nil, body); code < 400 {
			t.Fatalf("upload without the checksum header answered %d; the header is not signed", code)
		}
	})

	t.Run("matching bytes are accepted", func(t *testing.T) {
		if code := putPresigned(t, req.URL, header, body); code >= 300 {
			t.Fatalf("upload of correct bytes answered %d, want 2xx", code)
		}
	})

	t.Run("verify accepts the upload and records membership", func(t *testing.T) {
		status, err := s.Verify(ctx, testRepo, oid, int64(len(body)))
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if status != VerifyOK {
			t.Fatalf("Verify = %v, want ok", status)
		}

		size, held, err := s.Marker(ctx, testRepo, oid)
		if err != nil {
			t.Fatalf("Marker: %v", err)
		}
		if !held {
			t.Fatal("Verify did not record membership")
		}
		if size != int64(len(body)) {
			t.Errorf("marker size = %d, want %d", size, len(body))
		}
	})

	t.Run("a second verify is idempotent", func(t *testing.T) {
		// The staged object is gone after promotion, but the repository still
		// holds the object, so a retried verify must not report a failure.
		status, err := s.Verify(ctx, testRepo, oid, int64(len(body)))
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if status != VerifyOK {
			t.Fatalf("Verify = %v, want ok", status)
		}
	})
}

// TestLiveDownloadRoundTrip proves a presigned GET returns the stored bytes.
func TestLiveDownloadRoundTrip(t *testing.T) {
	s, bucket := liveStore(t)
	ctx := context.Background()

	oid, body := liveOID(t)
	t.Cleanup(func() {
		for _, key := range []string{BlobKey(oid), MarkerKey(testRepo, oid), StagingKey(testRepo, oid)} {
			_, _ = s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
		}
	})

	up, err := s.Presigner().PresignPut(ctx, StagingKey(testRepo, oid), oid, 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}
	if code := putPresigned(t, up.URL, TransferHeader(up), body); code >= 300 {
		t.Fatalf("seeding upload answered %d", code)
	}
	// Verify promotes the staged object to the shared key, which is where a
	// download reads from.
	if status, err := s.Verify(ctx, testRepo, oid, int64(len(body))); err != nil {
		t.Fatalf("Verify: %v", err)
	} else if status != VerifyOK {
		t.Fatalf("Verify = %v, want ok", status)
	}

	down, err := s.Presigner().PresignGet(ctx, BlobKey(oid), 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}

	resp, err := http.Get(down.URL)
	if err != nil {
		t.Fatalf("downloading: %v", err)
	}
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading download: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("downloaded %q, want %q", got, body)
	}
}

// TestLiveLockCAS drives the lock document's conditional writes against a real
// bucket, where the ETag semantics are the bucket's and not a fake's.
func TestLiveLockCAS(t *testing.T) {
	s, bucket := liveStore(t)
	ctx := context.Background()

	repo := fmt.Sprintf("livetest/locks-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(LocksKey(repo)),
		})
	})

	first, created, err := s.CreateLock(ctx, repo, "big.bin", "alice")
	if err != nil {
		t.Fatalf("CreateLock: %v", err)
	}
	if !created {
		t.Fatal("the first lock on a fresh repository must be created")
	}

	// A second create on the same path must lose, not overwrite.
	if _, created, err := s.CreateLock(ctx, repo, "big.bin", "bob"); err != nil {
		t.Fatalf("CreateLock: %v", err)
	} else if created {
		t.Fatal("a held path was locked twice")
	}

	// A second path must succeed against the now-existing document, which
	// exercises the If-Match path rather than If-None-Match.
	if _, created, err := s.CreateLock(ctx, repo, "other.bin", "alice"); err != nil {
		t.Fatalf("CreateLock: %v", err)
	} else if !created {
		t.Fatal("a free path was not locked")
	}

	locks, _, err := s.ListLocks(ctx, repo, LockFilter{})
	if err != nil {
		t.Fatalf("ListLocks: %v", err)
	}
	if len(locks) != 2 {
		t.Fatalf("got %d locks, want 2", len(locks))
	}

	if _, err := s.DeleteLock(ctx, repo, first.ID, "alice", false); err != nil {
		t.Fatalf("DeleteLock: %v", err)
	}
}
