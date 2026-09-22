package lfs

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"testing"
)

const testRepo = "acme/widgets"

func testChecksum(t *testing.T, oid string) string {
	t.Helper()
	raw, err := hex.DecodeString(oid)
	if err != nil {
		t.Fatalf("decoding oid: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// TestMarkerReportsMembership covers the read the batch endpoint makes for
// every object.
func TestMarkerReportsMembership(t *testing.T) {
	for _, tt := range []struct {
		name     string
		seed     func(*fakeS3)
		wantOK   bool
		wantSize int64
	}{
		{
			name:   "no marker and no blob",
			seed:   func(*fakeS3) {},
			wantOK: false,
		},
		{
			name: "marker present",
			seed: func(f *fakeS3) {
				f.putMarker(testRepo, testOID, 1234)
			},
			wantOK:   true,
			wantSize: 1234,
		},
		{
			// The access model rests on this case: the bytes exist, but this
			// repository never proved it holds them, so the answer is no.
			name: "blob present but marker absent",
			seed: func(f *fakeS3) {
				f.putBlob(testOID, 1234, "")
			},
			wantOK: false,
		},
		{
			name: "marker belongs to another repository",
			seed: func(f *fakeS3) {
				f.putMarker("other/repo", testOID, 1234)
				f.putBlob(testOID, 1234, "")
			},
			wantOK: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3()
			tt.seed(f)
			s := newTestStore(t, f)

			size, ok, err := s.Marker(context.Background(), testRepo, testOID)
			if err != nil {
				t.Fatalf("Marker: %v", err)
			}
			if ok != tt.wantOK {
				t.Fatalf("Marker ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && size != tt.wantSize {
				t.Errorf("Marker size = %d, want %d", size, tt.wantSize)
			}
		})
	}
}

func TestMarkerRejectsABadOID(t *testing.T) {
	s := newTestStore(t, newFakeS3())

	if _, _, err := s.Marker(context.Background(), testRepo, "../../etc/passwd"); !errors.Is(err, ErrInvalidOID) {
		t.Fatalf("Marker error = %v, want ErrInvalidOID", err)
	}
}

// TestVerify covers the only integrity check that runs on the daemon's own
// credentials. A presigned PUT is not size-checked by the signature, so this is
// where a wrong upload is caught.
func TestVerify(t *testing.T) {
	for _, tt := range []struct {
		name       string
		seed       func(*testing.T, *fakeS3)
		size       int64
		want       VerifyStatus
		wantMarker bool
		wantBlob   bool
	}{
		{
			name: "size and checksum match",
			seed: func(t *testing.T, f *fakeS3) {
				f.putStaged(testRepo, testOID, 4, testChecksum(t, testOID))
			},
			size:       4,
			want:       VerifyOK,
			wantMarker: true,
			wantBlob:   true,
		},
		{
			name:     "nothing was staged",
			seed:     func(*testing.T, *fakeS3) {},
			size:     4,
			want:     VerifyMissing,
			wantBlob: false,
		},
		{
			name: "size differs",
			seed: func(t *testing.T, f *fakeS3) {
				f.putStaged(testRepo, testOID, 9, testChecksum(t, testOID))
			},
			size: 4,
			want: VerifyMismatch,
			// Wrong bytes are dropped from the caller's own staging area and
			// never reach the shared key.
			wantBlob: false,
		},
		{
			name: "checksum differs",
			seed: func(t *testing.T, f *fakeS3) {
				f.putStaged(testRepo, testOID, 4, base64.StdEncoding.EncodeToString(make([]byte, 32)))
			},
			size:     4,
			want:     VerifyMismatch,
			wantBlob: false,
		},
		{
			// Tigris may answer a HEAD without a stored checksum. The PUT-time
			// checksum already covered the bytes, so size alone is accepted
			// rather than failing every upload.
			name: "no checksum reported falls back to the size check",
			seed: func(t *testing.T, f *fakeS3) {
				f.putStaged(testRepo, testOID, 4, "")
			},
			size:       4,
			want:       VerifyOK,
			wantMarker: true,
			wantBlob:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3()
			tt.seed(t, f)
			s := newTestStore(t, f)

			got, err := s.Verify(context.Background(), testRepo, testOID, tt.size)
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Verify = %v, want %v", got, tt.want)
			}
			if f.has(MarkerKey(testRepo, testOID)) != tt.wantMarker {
				t.Errorf("marker present = %v, want %v", !tt.wantMarker, tt.wantMarker)
			}
			if f.has(BlobKey(testOID)) != tt.wantBlob {
				t.Errorf("blob present = %v, want %v", !tt.wantBlob, tt.wantBlob)
			}
			// A promoted upload leaves no staged copy behind, and a rejected one
			// is removed too, so staging is empty either way.
			if f.has(StagingKey(testRepo, testOID)) {
				t.Error("the staged object was left behind")
			}
		})
	}
}

// TestVerifyRecordsTheSizeOnTheMarker keeps a later batch able to answer an
// upload of the same oid without reading the blob.
func TestVerifyRecordsTheSizeOnTheMarker(t *testing.T) {
	f := newFakeS3()
	f.putStaged(testRepo, testOID, 4, testChecksum(t, testOID))
	s := newTestStore(t, f)

	if _, err := s.Verify(context.Background(), testRepo, testOID, 4); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	size, ok, err := s.Marker(context.Background(), testRepo, testOID)
	if err != nil {
		t.Fatalf("Marker: %v", err)
	}
	if !ok {
		t.Fatal("Verify did not write the marker")
	}
	if size != 4 {
		t.Errorf("marker size = %d, want 4", size)
	}
}

// TestVerifyNeedsProofOfPossession is the security property the whole marker
// split exists for. Knowing an oid must not be enough to join a repository to
// an object: an oid and its size sit side by side in every pointer file, and
// pointer files travel through forks and CI logs.
//
// A caller who never uploaded anything must not be able to name someone else's
// object and come away holding it.
func TestVerifyNeedsProofOfPossession(t *testing.T) {
	f := newFakeS3()
	// Another repository already uploaded this object, so the bytes are in the
	// bucket under the shared key.
	f.putBlob(testOID, 4, testChecksum(t, testOID))
	f.putMarker("victim/private", testOID, 4)

	s := newTestStore(t, f)

	// The attacker uploaded nothing. It only knows the oid and the size.
	status, err := s.Verify(context.Background(), "attacker/scratch", testOID, 4)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if status == VerifyOK {
		t.Fatal("verify granted membership to a caller that never uploaded the object")
	}

	if _, held, err := s.Marker(context.Background(), "attacker/scratch", testOID); err != nil {
		t.Fatalf("Marker: %v", err)
	} else if held {
		t.Fatal("the attacker now holds an object it never uploaded")
	}
}

// TestVerifyDoesNotDeleteTheSharedBlob covers the other half: a bad verify must
// not be able to destroy an object other repositories depend on.
func TestVerifyDoesNotDeleteTheSharedBlob(t *testing.T) {
	f := newFakeS3()
	f.putBlob(testOID, 4, testChecksum(t, testOID))
	f.putMarker("victim/private", testOID, 4)

	s := newTestStore(t, f)

	// A wrong declared size is the reject path.
	if _, err := s.Verify(context.Background(), "attacker/scratch", testOID, 999); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if !f.has(BlobKey(testOID)) {
		t.Fatal("a failed verify deleted the blob another repository holds")
	}
}
