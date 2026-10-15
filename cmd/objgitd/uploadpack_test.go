package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/utils/ioutil"
)

func TestShallowRoundWriter(t *testing.T) {
	const response = "000ahello\n00000008NAK\n"
	for _, tt := range []struct {
		name  string
		chunk int
	}{{"single write", len(response)}, {"byte writes", 1}, {"split headers", 3}, {"split payloads", 7}} {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			w := &shallowRoundWriter{WriteCloser: ioutil.WriteNopCloser(&output)}
			var err error
			for off := 0; off < len(response) && err == nil; {
				end := min(off+tt.chunk, len(response))
				n, werr := w.Write([]byte(response[off:end]))
				off += n
				err = werr
			}
			if !errors.Is(err, errShallowRoundDone) {
				t.Fatalf("error=%v", err)
			}
			if got := output.String(); got != "000ahello\n0000" {
				t.Fatalf("response=%q", got)
			}
			if n, err := w.Write([]byte("0008NAK\n")); n != 0 || !errors.Is(err, errShallowRoundDone) {
				t.Fatalf("write after flush=%d, %v", n, err)
			}
		})
	}
}

type failedShallowWriter struct{ err error }

func (w failedShallowWriter) Write([]byte) (int, error) { return 0, w.err }
func (w failedShallowWriter) Close() error              { return nil }

func TestShallowRoundWriterPreservesWriteError(t *testing.T) {
	want := io.ErrClosedPipe
	w := &shallowRoundWriter{WriteCloser: failedShallowWriter{want}}
	if _, err := w.Write([]byte("0000")); !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
}

type shortShallowWriter struct{}

func (shortShallowWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func (shortShallowWriter) Close() error                { return nil }
func TestShallowRoundWriterRejectsShortWrite(t *testing.T) {
	w := &shallowRoundWriter{WriteCloser: shortShallowWriter{}}
	if _, err := w.Write([]byte("0000")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("error=%v", err)
	}
}

func TestUploadRequestReader(t *testing.T) {
	const want = "0032want 1111111111111111111111111111111111111111\n"
	for _, tt := range []struct {
		name, input string
		shallow     bool
		wantErr     bool
	}{
		{"ordinary", want + "00000009done\n", false, false},
		{"many wants", strings.Repeat(want, 10000) + "00000009done\n", false, false},
		{"shallow discovery", want + "000ddeepen 1\n0000", true, false},
		{"shallow with done", want + "000ddeepen 1\n00000009done\n", false, false},
		{"shallow since", want + "001cdeepen-since 1234567890\n0000", true, false},
		{"shallow not", want + "0014deepen-not main\n0000", true, false},
		{"flush only", "0000", false, false},
		{"empty", "", false, false},
		{"maximum packet", "fff0" + strings.Repeat("x", pktline.MaxSize-4) + "0000", false, false},
		{"oversized packet", "fff1", false, true},
		{"short header", "00", false, true},
		{"short payload", "0009ab", false, true},
		{"invalid length", "zzzz", false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, chunk := range []int{1, 3, 8192} {
				rd := &uploadRequestReader{rd: bufio.NewReader(iotest.OneByteReader(strings.NewReader(tt.input)))}
				defer rd.Close()
				var got bytes.Buffer
				buf := make([]byte, chunk)
				_, err := io.CopyBuffer(&got, readerOnly{rd}, buf)
				if (err != nil) != tt.wantErr {
					t.Fatalf("chunk %d: error=%v", chunk, err)
				}
				if !tt.wantErr && got.String() != tt.input {
					t.Fatalf("chunk %d: request changed: %q", chunk, got.String())
				}
				if rd.shallowOnly != tt.shallow {
					t.Fatalf("chunk %d: shallow=%v want=%v", chunk, rd.shallowOnly, tt.shallow)
				}
			}
		})
	}
}

type readerOnly struct{ io.Reader }

func BenchmarkUploadRequestReader(b *testing.B) {
	for _, wants := range []int{1, 10000} {
		b.Run(strconv.Itoa(wants), func(b *testing.B) {
			input := strings.Repeat("0032want 1111111111111111111111111111111111111111\n", wants) + "00000009done\n"
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			b.ResetTimer()
			for b.Loop() {
				rd := &uploadRequestReader{rd: bufio.NewReader(strings.NewReader(input))}
				if _, err := io.Copy(io.Discard, rd); err != nil {
					b.Fatal(err)
				}
				rd.Close()
			}
		})
	}
}

// The old adapter decoded, sorted, and encoded the complete request before
// upstream decoded it again. Keep that baseline to measure adapter overhead.
func BenchmarkUploadRequestDecodeReplay(b *testing.B) {
	for _, wants := range []int{1, 10000} {
		b.Run(strconv.Itoa(wants), func(b *testing.B) {
			input := strings.Repeat("0032want 1111111111111111111111111111111111111111\n", wants) + "00000009done\n"
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			b.ResetTimer()
			for b.Loop() {
				rd := bufio.NewReader(strings.NewReader(input))
				var req packp.UploadRequest
				if err := req.Decode(rd); err != nil {
					b.Fatal(err)
				}
				var prefix bytes.Buffer
				if err := req.Encode(&prefix); err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, io.MultiReader(&prefix, rd)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestUploadRequestReaderPreservesReadError(t *testing.T) {
	for _, tt := range []struct{ name, input string }{
		{"header", ""},
		{"payload", "0009ab"},
		{"after shallow flush", "000ddeepen 1\n0000"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rd := &uploadRequestReader{rd: bufio.NewReader(io.MultiReader(strings.NewReader(tt.input), iotest.ErrReader(io.ErrClosedPipe)))}
			defer rd.Close()
			if _, err := io.Copy(io.Discard, rd); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
