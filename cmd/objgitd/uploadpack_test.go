package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

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
