package lfs

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// metaSize is the user-metadata key holding an object's declared length. A
// membership marker has no body, so its size has to live in metadata.
const metaSize = "lfs-size"

// s3API is the bucket surface this package needs. It is an interface so tests
// can supply an in-memory bucket: CI runs with no Tigris credentials.
type s3API interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	// RenameObject is a Tigris extension, not standard S3. It moves an object
	// in place, with no data copy, which is how a staged upload is promoted to
	// the shared key. objgit already depends on it elsewhere (internal/s3fs).
	RenameObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// Store reads and writes the LFS key space in one bucket.
type Store struct {
	api    s3API
	pre    *Presigner
	bucket string
}

// NewStore builds a Store over a bucket and the presigner that mints its
// transfer URLs.
func NewStore(api s3API, pre *Presigner, bucket string) *Store {
	return &Store{api: api, pre: pre, bucket: bucket}
}

// Presigner returns the presigner this store mints transfer URLs with.
func (s *Store) Presigner() *Presigner { return s.pre }

// VerifyStatus is the outcome of checking an uploaded object.
type VerifyStatus int

const (
	// VerifyMissing means no blob is stored under that oid.
	VerifyMissing VerifyStatus = iota
	// VerifyMismatch means the stored bytes are not the object claimed.
	VerifyMismatch
	// VerifyOK means the object is stored and the repository now holds it.
	VerifyOK
)

// String renders the status for logs and metrics.
func (v VerifyStatus) String() string {
	switch v {
	case VerifyMissing:
		return "missing"
	case VerifyMismatch:
		return "mismatch"
	case VerifyOK:
		return "ok"
	default:
		return "unknown"
	}
}

// Marker reports whether the repository at repo holds oid, and the size it was
// recorded with.
//
// This reads the per-repository marker, never the shared blob. A repository
// that never uploaded an object does not hold it, however many other
// repositories do. Reading the blob here instead would let any caller turn a
// known oid into a presigned URL for someone else's data.
func (s *Store) Marker(ctx context.Context, repo, oid string) (int64, bool, error) {
	if err := ValidateOID(oid); err != nil {
		return 0, false, err
	}
	out, err := s.api.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(MarkerKey(repo, oid)),
	})
	if err != nil {
		if isNotFound(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("heading lfs marker for %s: %w", oid, err)
	}
	size, _ := strconv.ParseInt(out.Metadata[metaSize], 10, 64)
	return size, true, nil
}

// Verify promotes a staged upload and records that repo holds it.
//
// It reads the caller's own staging key, never the shared blob. That is what
// makes a marker mean something: to reach this point the caller had to write
// the bytes, and Tigris checked them against the oid as it did. Reading the
// shared blob instead would grant membership to anyone who can name an oid and
// its size, which is exactly what a pointer file contains.
//
// This is also the only size check anywhere. A presigned PUT does not sign
// Content-Length, so the declared size cannot be enforced at upload time.
//
// A mismatch deletes the staged object, which belongs to this caller. Nothing
// here can touch the shared blob or another repository's data.
func (s *Store) Verify(ctx context.Context, repo, oid string, size int64) (VerifyStatus, error) {
	if err := ValidateOID(oid); err != nil {
		return VerifyMissing, err
	}
	staged := StagingKey(repo, oid)

	out, err := s.api.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(staged),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		if isNotFound(err) {
			// Nothing staged. Either the upload never happened, or it was
			// already promoted by an earlier verify.
			if _, held, mErr := s.Marker(ctx, repo, oid); mErr == nil && held {
				return VerifyOK, nil
			}
			return VerifyMissing, nil
		}
		return VerifyMissing, fmt.Errorf("heading staged lfs object %s: %w", oid, err)
	}

	if aws.ToInt64(out.ContentLength) != size {
		return s.rejectStaged(ctx, staged, oid)
	}
	// Tigris does not have to report a stored checksum. The PUT-time checksum
	// already covered these bytes, so an absent one falls back to the size
	// check rather than failing every upload.
	if got := aws.ToString(out.ChecksumSHA256); got != "" {
		want, err := checksumOf(oid)
		if err != nil {
			return VerifyMissing, err
		}
		if got != want {
			return s.rejectStaged(ctx, staged, oid)
		}
	}

	// Promote the staged bytes to the shared key.
	//
	// RenameObject is a Tigris extension that moves an object in place. It
	// copies no data and leaves nothing behind, so promoting a 5 GiB upload
	// costs the same as promoting an empty one, and there is no staged copy to
	// clean up afterwards.
	//
	// Overwriting an existing blob is safe and expected: the key is the hash of
	// the content, so a blob another repository already uploaded holds exactly
	// these bytes. That is why two repositories uploading the same object need
	// no coordination.
	if _, err := s.api.RenameObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(BlobKey(oid)),
		CopySource: aws.String(s.bucket + "/" + staged),
	}); err != nil {
		return VerifyMissing, fmt.Errorf("promoting staged lfs object %s: %w", oid, err)
	}

	if err := s.putMarker(ctx, repo, oid, size); err != nil {
		return VerifyMissing, err
	}
	return VerifyOK, nil
}

// rejectStaged deletes a staged object whose bytes are not what they claim.
// The key is the caller's own, so this can never affect another repository.
func (s *Store) rejectStaged(ctx context.Context, staged, oid string) (VerifyStatus, error) {
	_, err := s.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(staged),
	})
	if err != nil {
		return VerifyMismatch, fmt.Errorf("deleting mismatched staged lfs object %s: %w", oid, err)
	}
	return VerifyMismatch, nil
}

// putMarker records that repo holds oid. The marker has no body; its size lives
// in user metadata so a later batch can answer an upload without reading the
// blob.
func (s *Store) putMarker(ctx context.Context, repo, oid string, size int64) error {
	_, err := s.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(MarkerKey(repo, oid)),
		Body:     strings.NewReader(""),
		Metadata: map[string]string{metaSize: strconv.FormatInt(size, 10)},
	})
	if err != nil {
		return fmt.Errorf("writing lfs marker for %s: %w", oid, err)
	}
	return nil
}

// checksumOf renders an oid as the base64 raw digest S3 compares against.
func checksumOf(oid string) (string, error) {
	raw, err := hex.DecodeString(oid)
	if err != nil {
		return "", fmt.Errorf("decoding oid %q: %w", oid, err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// isNotFound reports whether err is a bucket miss. It matches the codes
// internal/storage/tigris matches: Tigris reports Head misses as NotFound and
// Get misses as NoSuchKey.
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NotFound", "NoSuchKey":
		return true
	default:
		return false
	}
}

// isPreconditionFailed reports whether err is a rejected conditional write.
// Two writers racing on the lock document is the normal way to see this, so
// the lock writer retries rather than failing. Never map it to absence.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "PreconditionFailed"
}
