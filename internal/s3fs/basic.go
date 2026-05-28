// basic.go implements the interface billy.Basic

package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/go-git/go-billy/v6"
)

const (
	O_RDONLY      int = os.O_RDONLY // open the file read-only.
	O_WRONLY      int = os.O_WRONLY // open the file write-only.
	O_WRMULTIPART int = 0x4         // open the file for write-only using multipart upload.

	SupportedOFlags = O_RDONLY | O_WRONLY | O_WRMULTIPART // supported open flags for s3fs
)

var (
	ErrOpenFlagNotSupported = errors.New("open flag not supported")
)

// Create implements billy.Basic
// Create creates the named file with mode 0666 (before umask), truncating
// it if it already exists. If successful, methods on the returned File can
// be used for I/O; the associated file descriptor has mode O_RDWR.
func (fs3 *S3FS) Create(filename string) (billy.File, error) {
	return fs3.OpenFile(filename, O_WRONLY, 0666)
}

// Open opens the named file for reading. If successful, methods on the
// returned file can be used for reading; the associated file descriptor has
// mode O_RDONLY.
func (fs3 *S3FS) Open(filename string) (billy.File, error) {
	return fs3.OpenFile(filename, O_RDONLY, 0666)
}

// OpenFile is the generalized open call; most users will use Open or Create
// instead. It opens the named file with specified flag (O_RDONLY etc.) and
// perm, (0666 etc.) if applicable. If successful, methods on the returned
// File can be used for I/O.
func (fs3 *S3FS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	// Is the supplied flag supported?
	if flag&SupportedOFlags != flag {
		return nil, errors.New("unsupported open flag")
	}

	// Canonical S3 key for this path. Every branch uses it so reads and
	// writes resolve to the same object regardless of chroot depth.
	key := fs3.key(filename)

	switch flag & SupportedOFlags {
	case O_RDONLY:
		// The bucket root is always a directory; short-circuit so WASI
		// preopens (which OpenFile(".", O_RDONLY)) don't issue an S3 call.
		if key == "" || key == "." {
			return newS3DirFile(key, fs3.bucket, fs3.client), nil
		}

		// A TempFile that has not yet been renamed lives only in memory; serve
		// reads from that buffer so go-git's PackWriter can read the pack back
		// while it is still being written.
		if buf, ok := fs3.lookupTemp(filename); ok {
			return &tempReadFile{buf: buf, name: filename}, nil
		}

		f, err := newS3ReadFile(fs3.client, fs3.bucket, key, filename)
		if err == nil {
			return f, nil
		}

		// If the object simply doesn't exist, the path may still be a
		// directory prefix in S3. Probe for that before giving up.
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) {
			return nil, err
		}
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
		default:
			return nil, err
		}

		ctx := context.TODO()
		prefix := key + "/"
		maxKeys := int32(1)
		list, lerr := fs3.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    &fs3.bucket,
			Prefix:    &prefix,
			Delimiter: &fs3.separator,
			MaxKeys:   &maxKeys,
		})
		if lerr != nil {
			return nil, lerr
		}
		if len(list.Contents) > 0 || len(list.CommonPrefixes) > 0 {
			return newS3DirFile(key, fs3.bucket, fs3.client), nil
		}
		return nil, &os.PathError{Op: "open", Path: filename, Err: fs.ErrNotExist}

	case O_WRONLY:
		return newS3WriteFile(fs3.client, fs3.bucket, key, filename, fs3.unixMeta)

	case O_WRMULTIPART:
		return newS3MultipartUploadFile(fs3.client, fs3.bucket, key, filename, fs3.unixMeta)

	default:
		return nil, errors.New("unsupported open flag")
	}
}

// Stat returns a FileInfo describing the named file.
func (fs3 *S3FS) Stat(filename string) (os.FileInfo, error) {
	key := strings.TrimPrefix(fs3.cleanPath(filename), "/")
	if key == "" || key == "." {
		return newDirInfo("/"), nil
	}

	// A still-open TempFile lives only in memory; report its current size so
	// callers that Stat the temp path before Rename see a consistent view.
	if buf, ok := fs3.lookupTemp(filename); ok {
		return newFileInfo(path.Base(filename), buf.size(), time.Now()), nil
	}

	ctx := context.TODO()

	head, err := fs3.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &fs3.bucket,
		Key:    &key,
	})
	if err == nil {
		return newFileInfoFromHead(path.Base(key), head, fs3.unixMeta), nil
	}

	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return nil, err
	}
	switch apiErr.ErrorCode() {
	case "NotFound", "NoSuchKey":
		// fall through to directory probe below
	default:
		return nil, err
	}

	prefix := key + "/"
	maxKeys := int32(1)
	list, lerr := fs3.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    &fs3.bucket,
		Prefix:    &prefix,
		Delimiter: &fs3.separator,
		MaxKeys:   &maxKeys,
	})
	if lerr != nil {
		return nil, lerr
	}
	if len(list.Contents) > 0 || len(list.CommonPrefixes) > 0 {
		return newDirInfo(path.Base(key)), nil
	}
	return nil, &os.PathError{Op: "stat", Path: filename, Err: fs.ErrNotExist}
}

// Rename renames (moves) oldpath to newpath. If oldpath refers to an
// in-memory TempFile, its buffer is uploaded to S3 under newpath and the
// registry entry is dropped — this is how PackWriter's "tmp_pack_… →
// pack-<sha>.pack" promotion lands the final pack in the bucket. Otherwise
// Rename uses Tigris's in-place RenameObject extension.
func (fs3 *S3FS) Rename(oldpath, newpath string) error {
	ctx := context.TODO() // TODO: Get user-supplied context?

	src := fs3.key(oldpath)
	dst := fs3.key(newpath)

	if buf, ok := fs3.detachTemp(oldpath); ok {
		data := buf.snapshot()
		_, err := fs3.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &fs3.bucket,
			Key:    &dst,
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			return fmt.Errorf("failed to upload temp %q to %q: %w", oldpath, newpath, err)
		}
		return nil
	}

	// RenameObject is a Tigris extension that renames in place (no data copy),
	// so we don't need a separate CopyObject + DeleteObject. CopySource is
	// bucket-qualified; Key is the destination key.
	copySource := fs3.bucket + "/" + src
	_, err := fs3.client.RenameObject(ctx, &s3.CopyObjectInput{
		Bucket:     &fs3.bucket,
		CopySource: &copySource,
		Key:        &dst,
	})
	if err != nil {
		return fmt.Errorf("failed to rename %q to %q: %w", oldpath, newpath, err)
	}

	return nil
}

// Remove removes the named file or directory. In-memory TempFile entries are
// dropped from the registry without an S3 call.
func (fs3 *S3FS) Remove(filename string) error {
	if _, ok := fs3.detachTemp(filename); ok {
		return nil
	}

	ctx := context.TODO() // TODO: Get user-supplied context?

	key := fs3.key(filename)

	// Send the request
	// TODO: Parse the response?
	_, err := fs3.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &fs3.bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}
	return nil
}

// Join joins any number of path elements into a single path
func (fs3 *S3FS) Join(elem ...string) string {
	j := path.Join(elem...)
	c := path.Clean(j)
	return c
}
