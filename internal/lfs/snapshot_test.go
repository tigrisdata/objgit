package lfs

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestOpenVerified(t *testing.T) {
	const repo = "acme/owned"
	const other = "acme/other"
	const oid = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	tests := []struct {
		name       string
		repo       string
		markerSize int64
		marker     bool
		blob       string
		wantErr    string
	}{
		{name: "held object", repo: repo, marker: true, markerSize: 4, blob: "test"},
		{name: "another repository cannot read", repo: other, marker: true, markerSize: 4, blob: "test", wantErr: "does not hold"},
		{name: "missing marker", repo: repo, blob: "test", wantErr: "does not hold"},
		{name: "wrong marker size", repo: repo, marker: true, markerSize: 5, blob: "test", wantErr: "marker size"},
		{name: "missing blob", repo: repo, marker: true, markerSize: 4, wantErr: "get object"},
		{name: "wrong blob size", repo: repo, marker: true, markerSize: 4, blob: "short", wantErr: "pointer says"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := newFakeS3()
			if tt.marker {
				bucket.putMarker(repo, oid, tt.markerSize)
			}
			if tt.blob != "" {
				bucket.set(BlobKey(oid), fakeObject{body: []byte(tt.blob)})
			}
			store := NewStore(bucket, nil, "test-bucket")
			r, err := store.OpenVerified(context.Background(), tt.repo, oid, 4)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("OpenVerified error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := io.ReadAll(r)
			if err != nil || string(got) != "test" {
				t.Errorf("body = %q, %v; want test", got, err)
			}
		})
	}
}
