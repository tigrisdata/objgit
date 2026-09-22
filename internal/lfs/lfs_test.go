package lfs

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateOID(t *testing.T) {
	const valid = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

	for _, tt := range []struct {
		name    string
		oid     string
		wantErr error
	}{
		{
			name: "64 lowercase hex characters",
			oid:  valid,
		},
		{
			name:    "empty",
			oid:     "",
			wantErr: ErrInvalidOID,
		},
		{
			name:    "too short",
			oid:     valid[:63],
			wantErr: ErrInvalidOID,
		},
		{
			name:    "too long",
			oid:     valid + "a",
			wantErr: ErrInvalidOID,
		},
		{
			// Uppercase is valid hex but must be rejected, not folded: git-lfs
			// emits lowercase, so an uppercase oid would key the same bytes
			// twice and break deduplication.
			name:    "uppercase hex",
			oid:     strings.ToUpper(valid),
			wantErr: ErrInvalidOID,
		},
		{
			name:    "non-hex character",
			oid:     "z" + valid[1:],
			wantErr: ErrInvalidOID,
		},
		{
			name:    "path traversal",
			oid:     "../../../etc/passwd",
			wantErr: ErrInvalidOID,
		},
		{
			// 64 characters, so a length-only check would let this through.
			name:    "path traversal padded to the right length",
			oid:     "../.." + strings.Repeat("a", 59),
			wantErr: ErrInvalidOID,
		},
		{
			name:    "contains a slash",
			oid:     valid[:32] + "/" + valid[33:],
			wantErr: ErrInvalidOID,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOID(tt.oid)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateOID(%q) error = %v, want %v", tt.oid, err, tt.wantErr)
			}
		})
	}
}

func TestBlobKey(t *testing.T) {
	const oid = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

	got := BlobKey(oid)
	if want := "lfs/blobs/" + oid; got != want {
		t.Errorf("BlobKey() = %q, want %q", got, want)
	}
}

func TestMarkerKey(t *testing.T) {
	const oid = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

	got := MarkerKey("acme/widgets", oid)
	if want := "acme/widgets/lfs/objects/" + oid; got != want {
		t.Errorf("MarkerKey() = %q, want %q", got, want)
	}
}

// TestMarkerKeyIsNotBlobKey pins the property the whole access model rests on:
// the per-repository marker and the shared blob are different keys, so reading
// one never proves the other.
func TestMarkerKeyIsNotBlobKey(t *testing.T) {
	const oid = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

	if MarkerKey("acme/widgets", oid) == BlobKey(oid) {
		t.Error("marker key and blob key must differ")
	}
}
