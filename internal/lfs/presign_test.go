package lfs

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const testOID = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

// newTestPresigner builds a Presigner over static credentials and a fixed
// endpoint. Presigning performs no network I/O — the signing middleware
// short-circuits the request stack — so this exercises the real signer, not a
// fake. That is deliberate: a fake presigner cannot catch a change in which
// headers the SDK hoists into the query string, which is the one thing these
// tests exist to pin.
func newTestPresigner(t *testing.T) *Presigner {
	t.Helper()
	c := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String("https://t3.storage.dev"),
		Credentials: credentials.NewStaticCredentialsProvider(
			"AKIAEXAMPLEEXAMPLE", "secretsecretsecret", ""),
	})
	return NewPresigner(c, "test-bucket")
}

// TestPresignPutSignsTheChecksumHeader is the regression test for SigV4 header
// hoisting. The AWS SDK moves any unrecognized "X-Amz-*" header into the query
// string when it presigns, which would leave the checksum unsigned and absent
// from SignedHeader. Git LFS only sends headers the batch response names, so a
// hoisted checksum is an unenforced checksum.
func TestPresignPutSignsTheChecksumHeader(t *testing.T) {
	p := newTestPresigner(t)

	req, err := p.PresignPut(context.Background(), BlobKey(testOID), testOID, 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}

	u, err := url.Parse(req.URL)
	if err != nil {
		t.Fatalf("parsing presigned URL: %v", err)
	}
	q := u.Query()

	signed := q.Get("X-Amz-SignedHeaders")
	if !strings.Contains(signed, "x-amz-checksum-sha256") {
		t.Errorf("X-Amz-SignedHeaders = %q, want it to contain x-amz-checksum-sha256", signed)
	}

	if got := q.Get("x-amz-checksum-sha256"); got != "" {
		t.Errorf("checksum was hoisted into the query string as %q; it must stay a signed header", got)
	}

	if got := req.SignedHeader.Get("x-amz-checksum-sha256"); got == "" {
		t.Error("SignedHeader is missing x-amz-checksum-sha256")
	}
}

// TestPresignPutChecksumMatchesTheOID pins the encoding: S3 wants the raw
// digest in base64, not the hex text of the oid.
func TestPresignPutChecksumMatchesTheOID(t *testing.T) {
	p := newTestPresigner(t)

	req, err := p.PresignPut(context.Background(), BlobKey(testOID), testOID, 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}

	// base64(hex-decode(testOID)), computed independently of the implementation:
	//   printf 'test' | shasum -a 256 -b | cut -d' ' -f1 | xxd -r -p | base64
	const want = "n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg="
	if got := req.SignedHeader.Get("x-amz-checksum-sha256"); got != want {
		t.Errorf("checksum header = %q, want %q", got, want)
	}
}

func TestPresignGet(t *testing.T) {
	p := newTestPresigner(t)

	req, err := p.PresignGet(context.Background(), BlobKey(testOID), 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}

	if req.Method != "GET" {
		t.Errorf("Method = %q, want GET", req.Method)
	}
	if !strings.Contains(req.URL, BlobKey(testOID)) {
		t.Errorf("URL %q does not contain the blob key", req.URL)
	}
	// A download needs no checksum: the client verifies the bytes it receives
	// against the oid it asked for.
	if got := req.SignedHeader.Get("x-amz-checksum-sha256"); got != "" {
		t.Errorf("download carries an unexpected checksum header %q", got)
	}
}

// TestPresignGetNeedsNoRequestHeaders is a regression test for a download URL
// that only works when the caller replays extra headers.
//
// Turning off header hoisting to keep an upload's checksum signed also signs
// "x-amz-checksum-mode" on a GetObject. A URL signed that way is rejected with
// SignatureDoesNotMatch unless the client sends that header back, so a plain
// fetch of the href fails. Only Host may be signed on a download.
func TestPresignGetNeedsNoRequestHeaders(t *testing.T) {
	p := newTestPresigner(t)

	req, err := p.PresignGet(context.Background(), BlobKey(testOID), 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}

	for name := range req.SignedHeader {
		if http.CanonicalHeaderKey(name) != "Host" {
			t.Errorf("download signs %q; a download URL must stand on its own", name)
		}
	}
	if got := TransferHeader(req); got != nil {
		t.Errorf("download action would demand headers %v, want none", got)
	}
}

// TestPresignExpiryReachesTheURL keeps -lfs-url-ttl honest: the value has to
// land in the signature, not just in the batch response's expires_in.
func TestPresignExpiryReachesTheURL(t *testing.T) {
	p := newTestPresigner(t)

	req, err := p.PresignGet(context.Background(), BlobKey(testOID), 42*time.Second)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}

	u, err := url.Parse(req.URL)
	if err != nil {
		t.Fatalf("parsing presigned URL: %v", err)
	}
	if got := u.Query().Get("X-Amz-Expires"); got != "42" {
		t.Errorf("X-Amz-Expires = %q, want %q", got, "42")
	}
}

func TestPresignPutRejectsABadOID(t *testing.T) {
	p := newTestPresigner(t)

	if _, err := p.PresignPut(context.Background(), "lfs/blobs/nope", "nope", time.Minute); err == nil {
		t.Fatal("PresignPut accepted an invalid oid")
	}
}
