package lfs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// headerValue looks a header up without caring how it is capitalized.
func headerValue(h map[string]string, name string) string {
	for k, v := range h {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

func testBatchOptions() BatchOptions {
	return BatchOptions{
		Repo:      testRepo,
		VerifyURL: "https://git.example.com/acme/widgets.git/info/lfs/objects/verify",
		TTL:       15 * time.Minute,
		MaxSize:   1 << 30,
		MaxBatch:  100,
	}
}

func runBatch(t *testing.T, f *fakeS3, req *BatchRequest) *BatchResponse {
	t.Helper()
	resp, err := newTestStore(t, f).Batch(context.Background(), req, testBatchOptions())
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	return resp
}

func TestBatchDownload(t *testing.T) {
	for _, tt := range []struct {
		name        string
		seed        func(*fakeS3)
		wantAction  bool
		wantErrCode int
	}{
		{
			name: "repository holds the object",
			seed: func(f *fakeS3) {
				f.putMarker(testRepo, testOID, 4)
				f.putBlob(testOID, 4, "")
			},
			wantAction: true,
		},
		{
			name:        "object is unknown",
			seed:        func(*fakeS3) {},
			wantErrCode: 404,
		},
		{
			// The read-oracle test. Another repository's upload put the bytes
			// in the bucket, but this repository never proved it holds them,
			// so it gets a 404 and never a URL.
			name: "bytes exist but this repository has no marker",
			seed: func(f *fakeS3) {
				f.putBlob(testOID, 4, "")
				f.putMarker("someone/else", testOID, 4)
			},
			wantErrCode: 404,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3()
			tt.seed(f)

			resp := runBatch(t, f, &BatchRequest{
				Operation: OperationDownload,
				Objects:   []Pointer{{OID: testOID, Size: 4}},
			})

			if len(resp.Objects) != 1 {
				t.Fatalf("got %d objects, want 1", len(resp.Objects))
			}
			obj := resp.Objects[0]

			if tt.wantErrCode != 0 {
				if obj.Error == nil {
					t.Fatalf("object has no error, want code %d", tt.wantErrCode)
				}
				if obj.Error.Code != tt.wantErrCode {
					t.Errorf("error code = %d, want %d", obj.Error.Code, tt.wantErrCode)
				}
				if obj.Actions != nil {
					t.Error("a failed object must carry no actions")
				}
				return
			}

			if obj.Error != nil {
				t.Fatalf("unexpected object error: %+v", obj.Error)
			}
			if _, ok := obj.Actions[ActionDownload]; ok != tt.wantAction {
				t.Fatalf("download action present = %v, want %v", ok, tt.wantAction)
			}
		})
	}
}

func TestBatchUpload(t *testing.T) {
	for _, tt := range []struct {
		name        string
		seed        func(*fakeS3)
		size        int64
		wantUpload  bool
		wantErrCode int
	}{
		{
			name:       "new object gets an upload action",
			seed:       func(*fakeS3) {},
			size:       4,
			wantUpload: true,
		},
		{
			// Deduplication: the repository already holds it, so git-lfs skips
			// the transfer entirely.
			name: "object already held is skipped",
			seed: func(f *fakeS3) {
				f.putMarker(testRepo, testOID, 4)
			},
			size:       4,
			wantUpload: false,
		},
		{
			name: "held at a different size is a conflict",
			seed: func(f *fakeS3) {
				f.putMarker(testRepo, testOID, 9)
			},
			size:        4,
			wantErrCode: 422,
		},
		{
			// A marker whose size cannot be read must leave the client a way
			// out. Counting it as held at size 0 answers every later batch 422
			// and never offers an upload action, so nothing can repair it.
			name: "marker with an unreadable size is uploaded again",
			seed: func(f *fakeS3) {
				f.set(MarkerKey(testRepo, testOID), fakeObject{})
			},
			size:       4,
			wantUpload: true,
		},
		{
			// The bytes are in the bucket but this repository has no marker, so
			// it must upload. Trusting the global blob here is the read oracle.
			name: "bytes exist elsewhere so the client still uploads",
			seed: func(f *fakeS3) {
				f.putBlob(testOID, 4, "")
			},
			size:       4,
			wantUpload: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3()
			tt.seed(f)

			resp := runBatch(t, f, &BatchRequest{
				Operation: OperationUpload,
				Objects:   []Pointer{{OID: testOID, Size: tt.size}},
			})
			obj := resp.Objects[0]

			if tt.wantErrCode != 0 {
				if obj.Error == nil || obj.Error.Code != tt.wantErrCode {
					t.Fatalf("error = %+v, want code %d", obj.Error, tt.wantErrCode)
				}
				return
			}
			if obj.Error != nil {
				t.Fatalf("unexpected object error: %+v", obj.Error)
			}

			_, hasUpload := obj.Actions[ActionUpload]
			if hasUpload != tt.wantUpload {
				t.Fatalf("upload action present = %v, want %v", hasUpload, tt.wantUpload)
			}
			if !tt.wantUpload {
				if obj.Actions != nil {
					t.Error("a skipped upload must carry no actions at all")
				}
				return
			}
			// An upload the daemon never sees needs a verify step, or nothing
			// ever checks the bytes or records membership.
			if _, ok := obj.Actions[ActionVerify]; !ok {
				t.Error("an upload action must be paired with a verify action")
			}
		})
	}
}

// TestBatchUploadCarriesTheChecksumHeader pins the end of the integrity chain:
// the signed checksum has to reach the client, or it never gets sent.
func TestBatchUploadCarriesTheChecksumHeader(t *testing.T) {
	resp := runBatch(t, newFakeS3(), &BatchRequest{
		Operation: OperationUpload,
		Objects:   []Pointer{{OID: testOID, Size: 4}},
	})

	up := resp.Objects[0].Actions[ActionUpload]
	if up == nil {
		t.Fatal("no upload action")
	}
	// Header names are case-insensitive over the wire, and SigV4 lowercases
	// them before it checks a signature, so assert presence and not spelling.
	if got := headerValue(up.Header, "x-amz-checksum-sha256"); got == "" {
		t.Errorf("upload header = %v, want an x-amz-checksum-sha256 entry", up.Header)
	}
	if got := headerValue(up.Header, "host"); got != "" {
		t.Error("upload header must not carry Host; the client sets it from the URL")
	}
}

// TestBatchMarksObjectsAuthenticated stops git-lfs from running credential
// discovery against the storage host, which would attach an Authorization
// header that a presigned request rejects.
func TestBatchMarksObjectsAuthenticated(t *testing.T) {
	f := newFakeS3()
	f.putMarker(testRepo, testOID, 4)

	resp := runBatch(t, f, &BatchRequest{
		Operation: OperationDownload,
		Objects:   []Pointer{{OID: testOID, Size: 4}},
	})

	if !resp.Objects[0].Authenticated {
		t.Error("objects must be marked authenticated")
	}
}

func TestBatchActionsCarryExpiry(t *testing.T) {
	f := newFakeS3()
	f.putMarker(testRepo, testOID, 4)

	resp := runBatch(t, f, &BatchRequest{
		Operation: OperationDownload,
		Objects:   []Pointer{{OID: testOID, Size: 4}},
	})

	dl := resp.Objects[0].Actions[ActionDownload]
	if dl.ExpiresIn != int(15*time.Minute/time.Second) {
		t.Errorf("expires_in = %d, want %d", dl.ExpiresIn, int(15*time.Minute/time.Second))
	}
}

func TestBatchReportsTheBasicTransfer(t *testing.T) {
	resp := runBatch(t, newFakeS3(), &BatchRequest{
		Operation: OperationUpload,
		Transfers: []string{"basic"},
		Objects:   []Pointer{{OID: testOID, Size: 4}},
	})

	if resp.Transfer != TransferBasic {
		t.Errorf("transfer = %q, want %q", resp.Transfer, TransferBasic)
	}
}

// TestBatchRejects covers the whole-request failures, which are distinct from a
// per-object error: these produce an HTTP status, not a 200 with an error
// inside.
func TestBatchRejects(t *testing.T) {
	for _, tt := range []struct {
		name    string
		req     *BatchRequest
		wantErr error
	}{
		{
			name: "unknown operation",
			req: &BatchRequest{
				Operation: "sideways",
				Objects:   []Pointer{{OID: testOID, Size: 4}},
			},
			wantErr: ErrInvalidRequest,
		},
		{
			name: "missing operation",
			req: &BatchRequest{
				Objects: []Pointer{{OID: testOID, Size: 4}},
			},
			wantErr: ErrInvalidRequest,
		},
		{
			name: "malformed oid",
			req: &BatchRequest{
				Operation: OperationDownload,
				Objects:   []Pointer{{OID: "../../etc/passwd", Size: 4}},
			},
			wantErr: ErrInvalidOID,
		},
		{
			name: "uppercase oid",
			req: &BatchRequest{
				Operation: OperationDownload,
				Objects:   []Pointer{{OID: strings.ToUpper(testOID), Size: 4}},
			},
			wantErr: ErrInvalidOID,
		},
		{
			name: "negative size",
			req: &BatchRequest{
				Operation: OperationUpload,
				Objects:   []Pointer{{OID: testOID, Size: -1}},
			},
			wantErr: ErrInvalidRequest,
		},
		{
			name: "size over the cap",
			req: &BatchRequest{
				Operation: OperationUpload,
				Objects:   []Pointer{{OID: testOID, Size: 1<<30 + 1}},
			},
			wantErr: ErrObjectTooLarge,
		},
		{
			name: "unsupported hash algorithm",
			req: &BatchRequest{
				Operation: OperationDownload,
				HashAlgo:  "sha512",
				Objects:   []Pointer{{OID: testOID, Size: 4}},
			},
			wantErr: ErrInvalidRequest,
		},
		{
			name: "no supported transfer offered",
			req: &BatchRequest{
				Operation: OperationDownload,
				Transfers: []string{"tus", "multipart"},
				Objects:   []Pointer{{OID: testOID, Size: 4}},
			},
			wantErr: ErrNoSupportedTransfer,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestStore(t, newFakeS3()).Batch(context.Background(), tt.req, testBatchOptions())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Batch error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestBatchRejectsAnOversizedBatch(t *testing.T) {
	objects := make([]Pointer, 101)
	for i := range objects {
		objects[i] = Pointer{OID: testOID, Size: 4}
	}

	_, err := newTestStore(t, newFakeS3()).Batch(context.Background(), &BatchRequest{
		Operation: OperationDownload,
		Objects:   objects,
	}, testBatchOptions())

	if !errors.Is(err, ErrTooManyObjects) {
		t.Fatalf("Batch error = %v, want ErrTooManyObjects", err)
	}
}

// TestBatchPreservesObjectOrder keeps the response aligned with the request,
// which the concurrent marker reads could otherwise scramble.
func TestBatchPreservesObjectOrder(t *testing.T) {
	const otherOID = "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
	f := newFakeS3()
	f.putMarker(testRepo, otherOID, 1)

	resp := runBatch(t, f, &BatchRequest{
		Operation: OperationDownload,
		Objects: []Pointer{
			{OID: testOID, Size: 4},
			{OID: otherOID, Size: 1},
		},
	})

	if len(resp.Objects) != 2 {
		t.Fatalf("got %d objects, want 2", len(resp.Objects))
	}
	if resp.Objects[0].OID != testOID || resp.Objects[1].OID != otherOID {
		t.Errorf("object order = [%s %s], want [%s %s]",
			resp.Objects[0].OID, resp.Objects[1].OID, testOID, otherOID)
	}
}
