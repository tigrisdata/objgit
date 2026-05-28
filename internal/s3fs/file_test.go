package s3fs

import (
	"bytes"
	"errors"
	"testing"
)

// TestS3WriteFileWrite locks in two invariants on the write path that a prior
// stub silently broke: every Write must append its bytes to the buffer, and
// writes after Close must return ErrFileClosed. The stub returned (0, nil)
// for all writes regardless of state, which made every gzip/cp/etc. against
// s3fs produce empty objects without surfacing an error.
func TestS3WriteFileWrite(t *testing.T) {
	tests := []struct {
		name      string
		closedPre bool
		chunks    [][]byte
		wantBuf   []byte
		wantErr   error // expected error from the first Write (nil = all chunks succeed)
	}{
		{
			name:    "single chunk",
			chunks:  [][]byte{[]byte("hello")},
			wantBuf: []byte("hello"),
		},
		{
			name:    "multiple chunks accumulate",
			chunks:  [][]byte{[]byte("hello "), []byte("world"), []byte("!")},
			wantBuf: []byte("hello world!"),
		},
		{
			name:    "empty chunks are no-ops",
			chunks:  [][]byte{[]byte("a"), {}, []byte("b")},
			wantBuf: []byte("ab"),
		},
		{
			name:      "write after close returns ErrFileClosed",
			closedPre: true,
			chunks:    [][]byte{[]byte("nope")},
			wantBuf:   nil,
			wantErr:   ErrFileClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &s3WriteFile{
				bucket: "test",
				key:    "k",
				buf:    bytes.NewBuffer(nil),
				closed: tt.closedPre,
			}

			for i, c := range tt.chunks {
				n, err := f.Write(c)
				if i == 0 && tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) {
						t.Errorf("Write: err = %v, want %v", err, tt.wantErr)
					}
					if n != 0 {
						t.Errorf("Write: n = %d, want 0", n)
					}
					break
				}
				if err != nil {
					t.Fatalf("Write(%q): unexpected error: %v", c, err)
				}
				if n != len(c) {
					t.Fatalf("Write(%q): n=%d, want %d", c, n, len(c))
				}
			}

			if got := f.buf.Bytes(); !bytes.Equal(got, tt.wantBuf) {
				t.Errorf("buffer = %q, want %q", got, tt.wantBuf)
			}
		})
	}
}
