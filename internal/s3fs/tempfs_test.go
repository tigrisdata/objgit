package s3fs

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// newTempFS returns an S3FS with only the fields the temp-file code touches.
// No S3 client is needed because TempFile, Open(temp), Remove(temp), and the
// read/write handles never reach S3 until Rename uploads.
func newTempFS() *S3FS {
	return &S3FS{
		bucket:    "test",
		separator: DefaultSeparator,
		temps:     make(map[string]*tempBuffer),
	}
}

// TestTempFileReadWhileWrite locks in the read-while-write semantics go-git's
// streaming PackWriter relies on: TempFile + Open of the same path must share
// a buffer, reads at the current end must return io.EOF (not "not found"),
// and Seek must let the reader rewind to re-parse from the start.
func TestTempFileReadWhileWrite(t *testing.T) {
	for _, tt := range []struct {
		name string
		run  func(t *testing.T, fs *S3FS, fw, fr io.ReadWriteSeeker)
	}{
		{
			name: "read sees writes",
			run: func(t *testing.T, _ *S3FS, fw, fr io.ReadWriteSeeker) {
				if _, err := fw.Write([]byte("hello world")); err != nil {
					t.Fatalf("Write: %v", err)
				}
				got, err := io.ReadAll(fr)
				if err != nil {
					t.Fatalf("ReadAll: %v", err)
				}
				if string(got) != "hello world" {
					t.Logf("want: %q", "hello world")
					t.Logf("got:  %q", string(got))
					t.Error("read did not see written bytes")
				}
			},
		},
		{
			name: "EOF at current end, then resume after more writes",
			run: func(t *testing.T, _ *S3FS, fw, fr io.ReadWriteSeeker) {
				if _, err := fw.Write([]byte("part1")); err != nil {
					t.Fatalf("Write: %v", err)
				}
				buf := make([]byte, 5)
				if n, err := fr.Read(buf); err != nil || n != 5 {
					t.Fatalf("first Read: n=%d err=%v", n, err)
				}
				if n, err := fr.Read(buf); !errors.Is(err, io.EOF) || n != 0 {
					t.Fatalf("Read at end: n=%d err=%v, want (0, io.EOF)", n, err)
				}
				if _, err := fw.Write([]byte("part2")); err != nil {
					t.Fatalf("Write 2: %v", err)
				}
				if n, err := fr.Read(buf); err != nil || n != 5 {
					t.Fatalf("Read after second write: n=%d err=%v", n, err)
				}
				if string(buf) != "part2" {
					t.Errorf("got %q, want %q", string(buf), "part2")
				}
			},
		},
		{
			name: "seek to start re-reads the whole buffer",
			run: func(t *testing.T, _ *S3FS, fw, fr io.ReadWriteSeeker) {
				if _, err := fw.Write([]byte("abcdef")); err != nil {
					t.Fatalf("Write: %v", err)
				}
				if _, err := io.ReadAll(fr); err != nil {
					t.Fatalf("drain: %v", err)
				}
				if pos, err := fr.Seek(0, io.SeekStart); err != nil || pos != 0 {
					t.Fatalf("Seek(0): pos=%d err=%v", pos, err)
				}
				got, err := io.ReadAll(fr)
				if err != nil {
					t.Fatalf("ReadAll after seek: %v", err)
				}
				if string(got) != "abcdef" {
					t.Errorf("got %q, want %q", string(got), "abcdef")
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fs := newTempFS()
			fw, err := fs.TempFile("objects/pack", "tmp_pack_")
			if err != nil {
				t.Fatalf("TempFile: %v", err)
			}
			if !strings.HasPrefix(fw.Name(), "objects/pack/tmp_pack_") {
				t.Fatalf("unexpected temp name: %q", fw.Name())
			}
			fr, err := fs.Open(fw.Name())
			if err != nil {
				t.Fatalf("Open(%q): %v", fw.Name(), err)
			}
			tt.run(t, fs, fw, fr)
		})
	}
}

// TestTempFileReadAt covers the io.ReaderAt path that idxfile parsing uses
// to seek around in the pack while it is being indexed.
func TestTempFileReadAt(t *testing.T) {
	fs := newTempFS()
	fw, err := fs.TempFile("objects/pack", "tmp_pack_")
	if err != nil {
		t.Fatalf("TempFile: %v", err)
	}
	if _, err := fw.Write([]byte("0123456789")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	fr, err := fs.Open(fw.Name())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, 4)
	n, err := fr.ReadAt(buf, 3)
	if err != nil || n != 4 {
		t.Fatalf("ReadAt(_, 3): n=%d err=%v", n, err)
	}
	if string(buf) != "3456" {
		t.Errorf("got %q, want %q", string(buf), "3456")
	}
	// Reading past end returns io.EOF.
	if n, err := fr.ReadAt(make([]byte, 1), 100); !errors.Is(err, io.EOF) || n != 0 {
		t.Errorf("ReadAt past end: n=%d err=%v, want (0, io.EOF)", n, err)
	}
}

// TestTempFileRemove drops the registry entry without hitting S3 — important
// because a nil S3 client would otherwise crash the test. After Remove, Open
// of the same path must no longer return the temp buffer.
func TestTempFileRemove(t *testing.T) {
	fs := newTempFS()
	fw, err := fs.TempFile("objects/pack", "tmp_pack_")
	if err != nil {
		t.Fatalf("TempFile: %v", err)
	}
	if err := fs.Remove(fw.Name()); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok := fs.lookupTemp(fw.Name()); ok {
		t.Fatal("temp registry still has entry after Remove")
	}
	if len(fs.temps) != 0 {
		t.Errorf("temps len = %d, want 0", len(fs.temps))
	}
}

// TestTempFileConcurrentWriteRead exercises the actual go-git pattern: one
// goroutine writes, another reads, and the reader retries on (0, io.EOF) like
// syncedReader does. The full payload must round-trip.
func TestTempFileConcurrentWriteRead(t *testing.T) {
	fs := newTempFS()
	fw, err := fs.TempFile("objects/pack", "tmp_pack_")
	if err != nil {
		t.Fatalf("TempFile: %v", err)
	}
	fr, err := fs.Open(fw.Name())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	payload := bytes.Repeat([]byte("xyzpdq"), 4096) // ~24 KiB
	var got bytes.Buffer
	var wg sync.WaitGroup

	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		for {
			n, err := fr.Read(buf)
			if n > 0 {
				got.Write(buf[:n])
			}
			if errors.Is(err, io.EOF) {
				if got.Len() == len(payload) {
					return
				}
				select {
				case <-done:
					return
				default:
					continue
				}
			}
			if err != nil {
				t.Errorf("Read: %v", err)
				return
			}
		}
	}()

	for off := 0; off < len(payload); off += 1000 {
		end := off + 1000
		if end > len(payload) {
			end = len(payload)
		}
		if _, err := fw.Write(payload[off:end]); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	close(done)
	wg.Wait()

	if !bytes.Equal(got.Bytes(), payload) {
		t.Errorf("payload mismatch: got %d bytes, want %d", got.Len(), len(payload))
	}
}
