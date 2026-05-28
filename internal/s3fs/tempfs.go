// tempfs.go backs billy.TempFile with an in-memory buffer that supports
// read-while-write on the same path. go-git/v6's streaming PackWriter creates
// a temp pack file, immediately opens the same path for reading, and reads it
// back concurrently while writing to build the index. S3 cannot offer that on
// a single object, so until the final Rename uploads the bytes the buffer is
// the file.

package s3fs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/go-git/go-billy/v6"
)

// tempBuffer is a growable byte buffer that one writer and one reader can
// access concurrently. It is the backing store for a single TempFile entry in
// the S3FS temp registry.
type tempBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *tempBuffer) write(p []byte) (int, error) {
	b.mu.Lock()
	b.data = append(b.data, p...)
	b.mu.Unlock()
	return len(p), nil
}

// readAt copies bytes starting at off. It returns (0, io.EOF) when off is at
// or past the current end so callers (most notably go-git's syncedReader) can
// distinguish "no data right now" from a hard error and retry.
func (b *tempBuffer) readAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("s3fs: negative offset")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	return copy(p, b.data[off:]), nil
}

func (b *tempBuffer) size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int64(len(b.data))
}

// snapshot returns a copy of the current bytes. Used by Rename to upload the
// final pack to S3 without holding the mutex during the network call.
func (b *tempBuffer) snapshot() []byte {
	b.mu.Lock()
	out := make([]byte, len(b.data))
	copy(out, b.data)
	b.mu.Unlock()
	return out
}

// tempWriteFile is the billy.File returned by TempFile. Close marks the handle
// closed but does not upload; the final Rename uploads to S3 and Remove
// discards.
type tempWriteFile struct {
	buf    *tempBuffer
	name   string
	closed bool
}

func (f *tempWriteFile) Name() string { return f.name }

func (f *tempWriteFile) Write(p []byte) (int, error) {
	if f.closed {
		return 0, ErrFileClosed
	}
	return f.buf.write(p)
}

func (f *tempWriteFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, &os.PathError{Op: "write", Path: f.name, Err: ErrNotImplemented}
}

func (f *tempWriteFile) Read(p []byte) (int, error)              { return 0, ErrCantReadFromWriteOnly }
func (f *tempWriteFile) ReadAt(p []byte, off int64) (int, error) { return 0, ErrCantReadFromWriteOnly }

func (f *tempWriteFile) Seek(offset int64, whence int) (int64, error) {
	return 0, &os.PathError{Op: "seek", Path: f.name, Err: ErrNotImplemented}
}

func (f *tempWriteFile) Truncate(size int64) error { return ErrTruncateNotSupported }
func (f *tempWriteFile) Lock() error               { return ErrLockNotSupported }
func (f *tempWriteFile) Unlock() error             { return ErrLockNotSupported }

func (f *tempWriteFile) Close() error {
	if f.closed {
		return ErrFileClosed
	}
	f.closed = true
	return nil
}

func (f *tempWriteFile) Stat() (fs.FileInfo, error) {
	return newFileInfo(f.name, f.buf.size(), time.Now()), nil
}

// tempReadFile is what Open returns for a path that is still in the temp
// registry. It carries its own cursor; Read returns (0, io.EOF) at the current
// end of the buffer so go-git's syncedReader can sleep and retry.
type tempReadFile struct {
	buf    *tempBuffer
	name   string
	pos    int64
	closed bool
}

func (f *tempReadFile) Name() string { return f.name }

func (f *tempReadFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, ErrFileClosed
	}
	n, err := f.buf.readAt(p, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *tempReadFile) ReadAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, ErrFileClosed
	}
	return f.buf.readAt(p, off)
}

func (f *tempReadFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, ErrFileClosed
	}
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = f.buf.size() + offset
	default:
		return 0, fmt.Errorf("s3fs: invalid whence %d", whence)
	}
	return f.pos, nil
}

func (f *tempReadFile) Write(p []byte) (int, error)              { return 0, ErrCantWriteToReadOnly }
func (f *tempReadFile) WriteAt(p []byte, off int64) (int, error) { return 0, ErrCantWriteToReadOnly }
func (f *tempReadFile) Truncate(size int64) error                { return ErrTruncateNotSupported }
func (f *tempReadFile) Lock() error                              { return ErrLockNotSupported }
func (f *tempReadFile) Unlock() error                            { return ErrLockNotSupported }

func (f *tempReadFile) Close() error {
	if f.closed {
		return ErrFileClosed
	}
	f.closed = true
	return nil
}

func (f *tempReadFile) Stat() (fs.FileInfo, error) {
	return newFileInfo(f.name, f.buf.size(), time.Now()), nil
}

// lookupTemp returns the tempBuffer for a path if it is currently registered,
// keyed by the canonical S3 key so the lookup matches the key used when
// inserting.
func (fs3 *S3FS) lookupTemp(name string) (*tempBuffer, bool) {
	fs3.tempMu.Lock()
	defer fs3.tempMu.Unlock()
	buf, ok := fs3.temps[fs3.key(name)]
	return buf, ok
}

// registerTemp installs buf at the canonical key for name. Used by TempFile.
func (fs3 *S3FS) registerTemp(name string, buf *tempBuffer) {
	fs3.tempMu.Lock()
	if fs3.temps == nil {
		fs3.temps = make(map[string]*tempBuffer)
	}
	fs3.temps[fs3.key(name)] = buf
	fs3.tempMu.Unlock()
}

// detachTemp removes a path from the registry and returns its buffer, if any.
// Used by Rename (after which the bytes are uploaded to S3) and Remove (which
// discards them).
func (fs3 *S3FS) detachTemp(name string) (*tempBuffer, bool) {
	fs3.tempMu.Lock()
	defer fs3.tempMu.Unlock()
	k := fs3.key(name)
	buf, ok := fs3.temps[k]
	if ok {
		delete(fs3.temps, k)
	}
	return buf, ok
}

// Compile-time assertions: the temp handles satisfy billy.File.
var (
	_ billy.File = (*tempWriteFile)(nil)
	_ billy.File = (*tempReadFile)(nil)
)
