package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"go.uber.org/atomic"
	"tangled.org/xeiaso.net/objgit/internal/s3fs/unixmeta"
)

// newFileMetadata returns the x-amz-meta-* map to attach to a newly written
// object, or nil when the Unix-metadata feature is disabled. New files take the
// session's default owner and a mode of 0666 masked by the session umask.
func newFileMetadata(cfg *unixMetaConfig) map[string]string {
	if cfg == nil {
		return nil
	}
	return unixmeta.Encode(unixmeta.Attrs{
		UID:   cfg.uid,
		GID:   cfg.gid,
		Mode:  0o666 &^ cfg.umask,
		Mtime: time.Now(),
	})
}

const (
	ModeMultipartUpload os.FileMode = fs.ModePerm + 1 // Custom os.FileMode for S3 multipart upload
)

var (
	ErrNotImplemented        = errors.New("not implemented")
	ErrLockNotSupported      = errors.New("lock not supported by s3")
	ErrTruncateNotSupported  = errors.New("truncate not supported by s3")
	ErrFileClosed            = errors.New("file is closed")
	ErrCantWriteToReadOnly   = errors.New("can't write to read-only file")
	ErrCantReadFromWriteOnly = errors.New("can't read from write-only file")
)

// readChunkSize is the amount ReadAt fetches per Range request on a window
// miss. The fetched chunk is cached so the many small, sequential ReadAt calls
// go-git issues (zlib reads ~512B at a time; idx fanout/hash lookups) coalesce
// into a handful of GETs instead of one request per call. RAM per open handle
// is bounded to one chunk (plus at most one in-flight streaming body), not the
// whole object.
const readChunkSize = 1 << 20 // 1 MiB

// s3ReadFile implements billy.File for S3, representing a file opened in read
// mode. It fetches lazily: no object bytes are buffered at Open. Sequential
// Read streams a GetObject body read on demand; ReadAt issues Range requests
// backed by a small read-ahead window. See docs/plans and CLAUDE.md.
//
// All mutable state is guarded by mu. The io.ReaderAt contract (ReadAt is
// independent of the read cursor and safe for concurrent use) is honoured:
// ReadAt never touches pos/body, and a ReadAt drops any open sequential body so
// a later Read reopens it at pos.
type s3ReadFile struct {
	client s3Client // S3 SDK client
	bucket string   // S3 bucket name
	key    string   // File object's key in S3
	name   string   // Root-relative path as presented to Open

	mu     sync.Mutex
	closed bool                 // Is the file closed?
	head   *s3.HeadObjectOutput // File metadata; nil until first fetch (or cache-supplied)
	size   int64                // Full object size; -1 until known

	// Sequential streaming path (Read/Seek).
	pos     int64         // Logical read cursor.
	body    io.ReadCloser // Open GetObject body, nil until first Read (or set at open on the uncached path).
	bodyPos int64         // Object offset the body currently sits at.

	// Random-access read-ahead window (ReadAt).
	win      []byte // Cached chunk, nil until the first ReadAt miss.
	winStart int64  // Object offset of win[0].
}

// newS3ReadFile creates a new s3ReadFile. key is the full S3 object key; name
// is the root-relative path the caller passed to Open (returned by Name).
//
// When head is non-nil the caller already resolved the object's existence and
// metadata (e.g. from the listing cache), so no I/O happens here — the body is
// fetched lazily on the first Read. When head is nil a GetObject is issued now:
// it both confirms existence (callers rely on a NoSuchKey error to fall back to
// a directory probe) and supplies the metadata via its response headers, so the
// redundant HeadObject round-trip is gone. The body is kept for streaming, not
// read into memory.
func newS3ReadFile(client s3Client, bucket, key, name string, head *s3.HeadObjectOutput) (*s3ReadFile, error) {
	f := &s3ReadFile{
		client: client,
		bucket: bucket,
		key:    key,
		name:   name,
		size:   -1,
	}

	if head != nil {
		f.head = head
		f.size = aws.ToInt64(head.ContentLength)
		return f, nil
	}

	// No cached metadata: open the object now. This GetObject is the existence
	// check (its error drives the caller's directory probe) and the metadata
	// source; the body streams lazily.
	start := time.Now()
	res, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	observeS3("GetObject", start, err)
	if err != nil {
		return nil, fmt.Errorf("unable to perform GetObject operation: %w", err)
	}
	f.adoptMeta(res, false)
	f.body = res.Body
	f.bodyPos = 0
	return f, nil
}

// adoptMeta records the object's full size and a synthetic HeadObjectOutput from
// a GetObject response the first time one is seen. ranged reports whether the
// response answered a Range request: a ranged response's ContentLength is the
// range length, so the full size comes from its Content-Range header instead. If
// the full size cannot be determined (an unparseable ranged response), head is
// left nil so Stat falls back to a HeadObject.
func (f *s3ReadFile) adoptMeta(out *s3.GetObjectOutput, ranged bool) {
	if f.head != nil {
		return
	}
	size := aws.ToInt64(out.ContentLength)
	if ranged {
		size = parseContentRange(out.ContentRange)
	}
	if size < 0 {
		return
	}
	f.size = size
	lastMod := out.LastModified
	if lastMod == nil {
		lastMod = aws.Time(time.Time{})
	}
	f.head = &s3.HeadObjectOutput{
		ContentLength: aws.Int64(size),
		LastModified:  lastMod,
		Metadata:      out.Metadata,
		ETag:          out.ETag,
		ContentType:   out.ContentType,
	}
}

// parseContentRange extracts the total object size from a Content-Range header
// of the form "bytes <start>-<end>/<total>". It returns -1 when the header is
// absent, uses an unknown total ("*"), or cannot be parsed.
func parseContentRange(s *string) int64 {
	if s == nil {
		return -1
	}
	i := strings.LastIndex(*s, "/")
	if i < 0 {
		return -1
	}
	total, err := strconv.ParseInt((*s)[i+1:], 10, 64)
	if err != nil {
		return -1
	}
	return total
}

// rangeHeader formats an HTTP Range header value. A negative end means
// open-ended ("bytes=start-").
func rangeHeader(start, end int64) string {
	if end < 0 {
		return fmt.Sprintf("bytes=%d-", start)
	}
	return fmt.Sprintf("bytes=%d-%d", start, end)
}

// Name returns the name of the file as presented to Open.
func (f *s3ReadFile) Name() string {
	return f.name
}

// Write implements io.Writer for billy.File
func (f *s3ReadFile) Write(p []byte) (n int, err error) {
	return 0, ErrCantWriteToReadOnly
}

// WriteAt implements io.WriterAt for billy.File
func (f *s3ReadFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, ErrCantWriteToReadOnly
}

// Read implements io.Reader for billy.File. The first Read (or the first after a
// Seek/ReadAt dropped the body) opens a GetObject body at the current position;
// subsequent reads stream from it.
func (f *s3ReadFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.size >= 0 && f.pos >= f.size {
		return 0, io.EOF
	}
	if f.body == nil {
		if err := f.openBodyLocked(f.pos); err != nil {
			return 0, err
		}
	}
	n, err = f.body.Read(p)
	f.pos += int64(n)
	f.bodyPos += int64(n)
	return n, err
}

// openBodyLocked starts a GetObject whose body begins at offset at. Callers hold
// f.mu.
func (f *s3ReadFile) openBodyLocked(at int64) error {
	in := &s3.GetObjectInput{Bucket: &f.bucket, Key: &f.key}
	if at > 0 {
		in.Range = aws.String(rangeHeader(at, -1))
	}
	start := time.Now()
	out, err := f.client.GetObject(context.TODO(), in)
	observeS3("GetObject", start, err)
	if err != nil {
		return fmt.Errorf("unable to perform GetObject operation: %w", err)
	}
	f.adoptMeta(out, at > 0)
	f.body = out.Body
	f.bodyPos = at
	return nil
}

// ReadAt implements io.ReaderAt for billy.File. It serves from a read-ahead
// window when possible and otherwise fetches a chunk via a Range request. It
// returns os.ErrClosed once the file is closed (rather than dereferencing a nil
// reader and panicking): go-git's packfile.FSObject.Reader probes with a 1-byte
// ReadAt and reopens the pack when it sees an os.ErrClosed-matching error, so a
// cache-resident object over a closed pack handle recovers instead of crashing.
func (f *s3ReadFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if off < 0 {
		return 0, fmt.Errorf("s3fs: ReadAt negative offset %d", off)
	}

	// Random access abandons any sequential stream; Read reopens it lazily.
	if f.body != nil {
		f.body.Close()
		f.body = nil
	}

	if f.size >= 0 && off >= f.size {
		return 0, io.EOF
	}

	// Serve fully-covered requests straight from the window.
	end := off + int64(len(p))
	if f.win != nil && off >= f.winStart && end <= f.winStart+int64(len(f.win)) {
		copy(p, f.win[off-f.winStart:end-f.winStart])
		return len(p), nil
	}

	// Window miss: fetch a chunk (at least len(p)) starting at off.
	chunk := max(int64(len(p)), readChunkSize)
	last := off + chunk - 1
	if f.size >= 0 && last > f.size-1 {
		last = f.size - 1
	}

	in := &s3.GetObjectInput{
		Bucket: &f.bucket,
		Key:    &f.key,
		Range:  aws.String(rangeHeader(off, last)),
	}
	start := time.Now()
	out, gerr := f.client.GetObject(context.TODO(), in)
	observeS3("GetObject", start, gerr)
	if gerr != nil {
		// A range starting at or past EOF is InvalidRange; treat as EOF.
		if isInvalidRange(gerr) {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("unable to perform GetObject operation: %w", gerr)
	}
	f.adoptMeta(out, true)
	buf, rerr := io.ReadAll(out.Body)
	out.Body.Close()
	if rerr != nil {
		return 0, fmt.Errorf("unable to read range body: %w", rerr)
	}

	f.win = buf
	f.winStart = off
	n = copy(p, buf)
	if n < len(p) {
		// The range hit EOF before filling p.
		return n, io.EOF
	}
	return n, nil
}

// isInvalidRange reports whether err is S3's 416 InvalidRange response, returned
// for a Range whose start is at or beyond the object's end.
func isInvalidRange(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "InvalidRange"
	}
	return false
}

// Seek implements io.Seeker for billy.File. Seeking away from the body's current
// position drops it; the next Read reopens at the new offset.
func (f *s3ReadFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}

	var np int64
	switch whence {
	case io.SeekStart:
		np = offset
	case io.SeekCurrent:
		np = f.pos + offset
	case io.SeekEnd:
		if f.size < 0 {
			if err := f.headLocked(); err != nil {
				return 0, err
			}
		}
		np = f.size + offset
	default:
		return 0, fmt.Errorf("s3fs: Seek invalid whence %d", whence)
	}
	if np < 0 {
		return 0, fmt.Errorf("s3fs: Seek negative position %d", np)
	}

	if f.body != nil && np != f.pos {
		f.body.Close()
		f.body = nil
	}
	f.pos = np
	return np, nil
}

// Close implements io.Closer for billy.File
func (f *s3ReadFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrFileClosed
	}
	if f.body != nil {
		f.body.Close()
		f.body = nil
	}
	f.win = nil
	f.closed = true
	return nil
}

// Lock locks the file like e.g. flock. It protects against access from
// other processes.
func (f *s3ReadFile) Lock() error {
	return ErrLockNotSupported
}

// Unlock unlocks the file.
func (f *s3ReadFile) Unlock() error {
	return ErrLockNotSupported
}

// Truncate the file.
func (f *s3ReadFile) Truncate(size int64) error {
	return ErrTruncateNotSupported
}

func (f *s3ReadFile) Stat() (fs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.head == nil {
		if err := f.headLocked(); err != nil {
			return nil, err
		}
	}
	return enrichedFileInfo{
		HeadObjectOutput: *f.head,
		key:              f.key,
		mode:             fs.ModePerm,
	}, nil
}

// headLocked fetches object metadata via HeadObject and records it. It is the
// sole remaining HeadObject path, reached only when no fetch has populated head
// yet (e.g. Stat or SeekEnd before any read on a cache-supplied-less handle).
// Callers hold f.mu.
func (f *s3ReadFile) headLocked() error {
	start := time.Now()
	ho, err := f.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &f.bucket,
		Key:    &f.key,
	})
	observeS3("HeadObject", start, err)
	if err != nil {
		return &os.PathError{Op: "stat", Path: f.key, Err: err}
	}
	f.head = ho
	f.size = aws.ToInt64(ho.ContentLength)
	return nil
}

// s3WriteFile stores a file opened in write mode and implements billy.File
//
// Upon creation, a buffer is created to store the file contents. Upon close,
// the file is uploaded to S3.
type s3WriteFile struct {
	client   s3Client        // s3 skd client
	bucket   string          // S3 bucket name
	key      string          // File object's key in S3
	name     string          // Root-relative path as presented to Open
	closed   bool            // Is the file closed?
	buf      *bytes.Buffer   // Buffer for storing the file before it's uploaded
	unixMeta *unixMetaConfig // optional POSIX attribute defaults (nil = disabled)
	cache    *ListingCache   // optional listing cache to invalidate on upload
}

// newS3WriteFile creates a new s3WriteFile. key is the full S3 object key; name
// is the root-relative path the caller passed to Open (returned by Name).
func newS3WriteFile(client s3Client, bucket, key, name string, cfg *unixMetaConfig, cache *ListingCache) (*s3WriteFile, error) {
	return &s3WriteFile{
		client:   client,
		bucket:   bucket,
		key:      key,
		name:     name,
		buf:      bytes.NewBuffer(nil),
		unixMeta: cfg,
		cache:    cache,
	}, nil
}

// Name returns the name of the file as presented to Open.
func (f *s3WriteFile) Name() string {
	return f.name
}

// Write implements os.Writer for billy.File
func (f *s3WriteFile) Write(p []byte) (n int, err error) {
	if f.closed {
		return 0, ErrFileClosed
	}
	return f.buf.Write(p)
}

func (f *s3WriteFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, &os.PathError{Op: "write", Path: f.key, Err: ErrNotImplemented}
}

// Read implements os.Reader for billy.File
func (f *s3WriteFile) Read(p []byte) (n int, err error) {
	return 0, ErrCantReadFromWriteOnly
}

// ReadAt implements io.ReaderAt for billy.File
func (f *s3WriteFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, ErrCantReadFromWriteOnly
}

// Seek implements io.Seeker for billy.File
func (f *s3WriteFile) Seek(offset int64, whence int) (int64, error) {
	return 0, &os.PathError{Op: "seek", Path: f.key, Err: ErrNotImplemented}
}

// Close implements io.Closer for billy.File
func (f *s3WriteFile) Close() error {
	if f.closed {
		return ErrFileClosed
	}

	// Set to closed
	f.closed = true

	// Extract the body from the buffer
	body := bytes.NewReader(f.buf.Bytes())

	// Create the context
	ctx := context.TODO() // TODO: How can user-supplied contexts be supported?

	// Run the GetObject operation
	// TODO: Currently `res` is not used. Should it be?
	start := time.Now()
	_, err := f.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   &f.bucket,
		Key:      &f.key,
		Body:     body,
		Metadata: newFileMetadata(f.unixMeta),
	})
	observeS3("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("unable to perform GetObject operation: %w", err)
	}

	// The new object changes its parent folder's listing; drop the cached
	// listing so the next read re-lists and sees it.
	if f.cache != nil {
		prefix, _ := splitKey(f.key)
		f.cache.invalidate(prefix)
	}

	return nil
}

func (f *s3WriteFile) Stat() (fs.FileInfo, error) {
	return newFileInfo(f.key, -1, time.Now()), nil
}

// Lock locks the file like e.g. flock. It protects against access from
// other processes.
func (f *s3WriteFile) Lock() error {
	return ErrLockNotSupported
}

// Unlock unlocks the file.
func (f *s3WriteFile) Unlock() error {
	return ErrLockNotSupported
}

// Truncate the file.
func (f *s3WriteFile) Truncate(size int64) error {
	return ErrTruncateNotSupported
}

// s3MultipartUploadFile implements billy.File
type s3MultipartUploadFile struct {
	client   s3Client      // s3 skd client
	bucket   string        // S3 bucket name
	key      string        // File object's key in S3
	name     string        // Root-relative path as presented to Open
	closed   bool          // Is the file closed?
	uploadID string        // S3 multipart upload ID
	uploadN  *atomic.Int32 // Counter tracking the number of uploads
	cache    *ListingCache // optional listing cache to invalidate on upload
}

// newS3MultipartUploadFile creates a new s3MultipartUploadFile. key is the full
// S3 object key; name is the root-relative path passed to Open.
func newS3MultipartUploadFile(client s3Client, bucket, key, name string, cfg *unixMetaConfig, cache *ListingCache) (*s3MultipartUploadFile, error) {
	// TODO: Check if the file exists
	// ...

	// Create the context
	ctx := context.TODO() // TODO: How can user-supplied contexts be supported?

	// Run the GetObject operation. POSIX attributes (if enabled) must be set
	// now: CompleteMultipartUpload cannot attach user metadata.
	start := time.Now()
	res, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		Metadata: newFileMetadata(cfg),
	})
	observeS3("CreateMultipartUpload", start, err)
	if err != nil {
		return nil, fmt.Errorf("unable to create multipart upload: %w", err)
	}

	// Return the file
	return &s3MultipartUploadFile{
		client:   client,
		bucket:   bucket,
		key:      key,
		name:     name,
		uploadID: *res.UploadId,
		uploadN:  atomic.NewInt32(1),
		cache:    cache,
	}, nil
}

// Name returns the name of the file as presented to Open.
func (f *s3MultipartUploadFile) Name() string { return f.name }

// Write implements os.Writer for billy.File
func (f *s3MultipartUploadFile) Write(p []byte) (n int, err error) {
	// Get the size of the data being written
	n = len(p)

	// Create a context for the operation
	ctx := context.TODO() // TODO: How can user-supplied contexts be supported?

	// Create a reader for the data
	r := bytes.NewReader(p)

	// Get the part number
	pn := f.uploadN.Load()

	// Run the UploadPart operation
	start := time.Now()
	_, err = f.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     &f.bucket,
		Key:        &f.key,
		UploadId:   &f.uploadID,
		PartNumber: new(pn),
		Body:       r,
	})
	observeS3("UploadPart", start, err)
	if err != nil {
		return 0, fmt.Errorf("unable to upload part %d: %w", pn, err)
	}

	// Increment the part number
	f.uploadN.Add(1)

	// Return the number of bytes written
	return n, nil
}

func (f *s3MultipartUploadFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, &os.PathError{Op: "write", Path: f.key, Err: ErrNotImplemented}
}

// Read implements os.Reader for billy.File
func (f *s3MultipartUploadFile) Read(p []byte) (n int, err error) {
	return 0, ErrCantReadFromWriteOnly
}

// ReadAt implements io.ReaderAt for billy.File
func (f *s3MultipartUploadFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, ErrCantReadFromWriteOnly
}

// Seek implements io.Seeker for billy.File
func (f *s3MultipartUploadFile) Seek(offset int64, whence int) (int64, error) {
	return 0, &os.PathError{Op: "seek", Path: f.key, Err: ErrNotImplemented}
}

// Close implements io.Closer for billy.File
func (f *s3MultipartUploadFile) Close() error {
	// Check if the file has been closed
	if f.closed {
		return ErrFileClosed
	}

	// Set to closed
	f.closed = true

	// Create the context
	ctx := context.TODO() // TODO: How can user-supplied contexts be supported?

	// Complete the multipart upload
	// TODO: Currently `res` is not used. Should it be?
	start := time.Now()
	_, err := f.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &f.bucket,
		Key:      &f.key,
		UploadId: &f.uploadID,
	})
	observeS3("CompleteMultipartUpload", start, err)
	if err != nil {
		return fmt.Errorf("unable to complete multipart upload: %w", err)
	}

	if f.cache != nil {
		prefix, _ := splitKey(f.key)
		f.cache.invalidate(prefix)
	}

	return nil
}

func (f *s3MultipartUploadFile) Lock() error               { return ErrLockNotSupported }
func (f *s3MultipartUploadFile) Unlock() error             { return ErrLockNotSupported }
func (f *s3MultipartUploadFile) Truncate(size int64) error { return ErrTruncateNotSupported }

func (f *s3MultipartUploadFile) Stat() (fs.FileInfo, error) {
	return newFileInfo(f.key, -1, time.Now()), nil
}

// s3DirFile is a billy.File handle for a directory in S3. S3 has no real
// directories, but WASI guests (via wazero) open the preopen root by calling
// OpenFile(".", O_RDONLY) and then asking IsDir() — so we return a pseudo-file
// that reports as a directory and rejects byte I/O with EISDIR.
type s3DirFile struct {
	name, bucket string
	closed       bool
	cli          s3Client
}

func newS3DirFile(name, bucket string, cli s3Client) *s3DirFile {
	return &s3DirFile{
		name:   name,
		bucket: bucket,
		cli:    cli,
	}
}

// Name returns the name of the file as presented to Open.
func (f *s3DirFile) Name() string {
	return f.name
}

func (f *s3DirFile) eisdir(op string) error {
	return &os.PathError{Op: op, Path: f.name, Err: syscall.EISDIR}
}

func (f *s3DirFile) Read(p []byte) (int, error)                   { return 0, f.eisdir("read") }
func (f *s3DirFile) ReadAt(p []byte, off int64) (int, error)      { return 0, f.eisdir("read") }
func (f *s3DirFile) Write(p []byte) (int, error)                  { return 0, f.eisdir("write") }
func (f *s3DirFile) WriteAt(p []byte, off int64) (int, error)     { return 0, f.eisdir("write") }
func (f *s3DirFile) Seek(offset int64, whence int) (int64, error) { return 0, f.eisdir("seek") }
func (f *s3DirFile) Truncate(size int64) error                    { return f.eisdir("truncate") }

func (f *s3DirFile) Stat() (fs.FileInfo, error) {
	start := time.Now()
	ho, err := f.cli.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: new(f.bucket),
		Key:    new(f.name),
	})
	observeS3("HeadObject", start, err)
	if err != nil {
		return nil, err
	}

	var mode fs.FileMode

	if strings.HasSuffix(f.name, "/") || *ho.ContentLength == 0 {
		mode = fs.ModeDir
	}

	return enrichedFileInfo{
		HeadObjectOutput: *ho,
		key:              f.name,
		mode:             mode,
	}, nil
}

func (f *s3DirFile) Close() error {
	if f.closed {
		return ErrFileClosed
	}
	f.closed = true
	return nil
}

func (f *s3DirFile) Lock() error   { return ErrLockNotSupported }
func (f *s3DirFile) Unlock() error { return ErrLockNotSupported }
