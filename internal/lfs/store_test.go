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
		{
			// An empty file is a real object, and its size parses, so it is
			// held like any other.
			name: "marker records a zero size",
			seed: func(f *fakeS3) {
				f.putMarker(testRepo, testOID, 0)
			},
			wantOK:   true,
			wantSize: 0,
		},
		{
			// A marker with no readable size cannot answer an upload batch. If
			// it read as size 0 the oid would be held at a size nothing
			// matches, batch would answer 422 forever, and it would never offer
			// an upload action to fix it. Reading it as absent costs one
			// re-upload, which rewrites the marker.
			name: "marker has no recorded size",
			seed: func(f *fakeS3) {
				f.set(MarkerKey(testRepo, testOID), fakeObject{})
			},
			wantOK: false,
		},
		{
			name: "marker has a malformed size",
			seed: func(f *fakeS3) {
				f.set(MarkerKey(testRepo, testOID), fakeObject{
					meta: map[string]string{metaSize: "four"},
				})
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

// TestMarkerSeparatesAFaultFromAbsence keeps a bucket that is down from
// reading as "this repository does not hold the object". Every batch decision
// rests on this answer, so absence has to mean absence.
func TestMarkerSeparatesAFaultFromAbsence(t *testing.T) {
	f := newFakeS3()
	f.headErr = errBucketUnavailable
	s := newTestStore(t, f)

	_, held, err := s.Marker(context.Background(), testRepo, testOID)
	if !errors.Is(err, errBucketUnavailable) {
		t.Fatalf("Marker error = %v, want errBucketUnavailable", err)
	}
	if held {
		t.Error("Marker reported a held object from a failed read")
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

// TestVerifyToleratesADuplicate covers two verifies racing for the same
// object. This happens in normal use: git-lfs retries a failed verify, and a
// client may re-POST. The first call consumes the staging key, so the second
// finds nothing to promote. The marker answers it. None of these is a server
// fault, so none of them may return an error.
func TestVerifyToleratesADuplicate(t *testing.T) {
	// promote is what the competing verify does: it moves the staged bytes to
	// the shared key and records that the repository holds them.
	promote := func(t *testing.T, f *fakeS3, staged string) {
		f.putBlob(testOID, 4, testChecksum(t, testOID))
		f.drop(staged)
		f.putMarker(testRepo, testOID, 4)
	}

	for _, tt := range []struct {
		name string
		seed func(*testing.T, *fakeS3)
		want VerifyStatus
	}{
		{
			// The whole race already finished before this call started.
			name: "the earlier verify finished first",
			seed: func(t *testing.T, f *fakeS3) {
				promote(t, f, StagingKey(testRepo, testOID))
			},
			want: VerifyOK,
		},
		{
			// The staging key survives this call's head and is gone by its
			// rename. Before the fix this was the 500.
			name: "the rename loses the race after the marker is written",
			seed: func(t *testing.T, f *fakeS3) {
				f.putStaged(testRepo, testOID, 4, testChecksum(t, testOID))
				f.renameHook = func(src string) { promote(t, f, src) }
			},
			want: VerifyOK,
		},
		{
			// The competing verify has promoted the bytes but has not written
			// the marker yet. Nothing here can tell that apart from an upload
			// that never happened, so the client is told to retry, which is a
			// 404 and not a 500.
			name: "the rename loses the race before the marker is written",
			seed: func(t *testing.T, f *fakeS3) {
				f.putStaged(testRepo, testOID, 4, testChecksum(t, testOID))
				f.renameHook = func(src string) {
					f.putBlob(testOID, 4, testChecksum(t, testOID))
					f.drop(src)
				}
			},
			want: VerifyMissing,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3()
			tt.seed(t, f)
			s := newTestStore(t, f)

			got, err := s.Verify(context.Background(), testRepo, testOID, 4)
			if err != nil {
				t.Fatalf("Verify: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Verify = %v, want %v", got, tt.want)
			}
			// Whoever won the race, the bytes are under the shared key.
			if !f.has(BlobKey(testOID)) {
				t.Error("the promoted blob is gone")
			}
		})
	}
}

// TestVerifyReportsAFailedMarkerRead keeps a bucket fault from being answered
// as a missing object. A 404 sends git-lfs to re-upload an object that is
// already there, and the verify metric records "missing" rather than "error",
// which is the signal an operator alerts on.
func TestVerifyReportsAFailedMarkerRead(t *testing.T) {
	for _, tt := range []struct {
		name string
		seed func(*testing.T, *fakeS3)
	}{
		{
			name: "nothing is staged",
			seed: func(*testing.T, *fakeS3) {},
		},
		{
			name: "the rename found nothing to promote",
			seed: func(t *testing.T, f *fakeS3) {
				f.putStaged(testRepo, testOID, 4, testChecksum(t, testOID))
				f.renameHook = func(src string) { f.drop(src) }
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3()
			tt.seed(t, f)
			// The staging read answers normally; only the marker read fails.
			f.headHook = func(key string) error {
				if key == MarkerKey(testRepo, testOID) {
					return errBucketUnavailable
				}
				return nil
			}
			s := newTestStore(t, f)

			status, err := s.Verify(context.Background(), testRepo, testOID, 4)
			if !errors.Is(err, errBucketUnavailable) {
				t.Fatalf("Verify = (%v, %v), want errBucketUnavailable", status, err)
			}
		})
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
