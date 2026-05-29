package s3fs

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

// TestS3ReadFileClosed locks in the billy contract go-git's
// packfile.FSObject.Reader depends on: once a read file is closed, Read/ReadAt/Seek
// must return an os.ErrClosed-matching error rather than dereferencing the nil
// reader and panicking. FSObject probes a packed object's handle with a 1-byte
// ReadAt and reopens the pack when it sees os.ErrClosed; the previous nil-deref
// panic crashed the server when a cache-resident object outlived its pack handle
// (e.g. a post-receive hook reading the just-pushed commit).
func TestS3ReadFileClosed(t *testing.T) {
	newClosed := func() *s3ReadFile {
		f := &s3ReadFile{name: "k", reader: bytes.NewReader([]byte("hello"))}
		if err := f.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return f
	}

	t.Run("ReadAt", func(t *testing.T) {
		if _, err := newClosed().ReadAt(make([]byte, 1), 0); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("ReadAt after close: err = %v, want os.ErrClosed", err)
		}
	})
	t.Run("Read", func(t *testing.T) {
		if _, err := newClosed().Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Read after close: err = %v, want os.ErrClosed", err)
		}
	})
	t.Run("Seek", func(t *testing.T) {
		if _, err := newClosed().Seek(0, 0); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Seek after close: err = %v, want os.ErrClosed", err)
		}
	})
}

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
