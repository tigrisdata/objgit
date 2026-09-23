package tigris

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tigrisdata/objgit/internal/snapshot"
)

// Storer holds the snapshot images of its repository, next to the git data.
var _ snapshot.Store = (*Storer)(nil)

// StatSnapshot sends one HeadObject for key under this Storer's prefix.
func (s *Storer) StatSnapshot(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + key),
	})
	s.observe("HeadObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return 0, fmt.Errorf("tigris: snapshot %s: %w", key, fs.ErrNotExist)
	default:
		return 0, fmt.Errorf("tigris: head snapshot %s: %w", key, err)
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

// PutSnapshot sends one PutObject. It does not use the upload queue: no ref
// waits for an image, so there is nothing for a flush to order.
func (s *Storer) PutSnapshot(ctx context.Context, key string, body io.ReadSeeker, size int64, meta map[string]string) error {
	start := time.Now()
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        sp(s.bucket),
		Key:           sp(s.prefix + key),
		Body:          body,
		ContentLength: &size,
		ContentType:   sp("application/octet-stream"),
		Metadata:      meta,
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: put snapshot %s: %w", key, err)
	}
	return nil
}

// OpenSnapshot returns a local copy of the image at key. With a snapshot
// cache, the copy lives in the cache, and later calls from any repository
// that holds the same tree open it with no request. Without one, the copy is
// a private unlinked temp file, as openWholePack does for packs.
//
// The body must match its erofs-sha256 metadata, so a bad download is never
// served and never cached.
func (s *Storer) OpenSnapshot(ctx context.Context, key string) (snapshot.File, error) {
	fetch := s.snapshotFetch(ctx, key)
	if s.snapCache != nil {
		f, err := s.snapCache.GetChecked(snapshot.CacheID(key), fetch)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	slog.Debug("no snapshot cache installed, downloading to a private temp file", "key", key)
	f, err := os.CreateTemp("", "objgit-snapshot-*")
	if err != nil {
		return nil, fmt.Errorf("tigris: create snapshot temp file: %w", err)
	}
	os.Remove(f.Name()) // the descriptor keeps the data until Close

	h := sha256.New()
	want, err := fetch(io.MultiWriter(f, h))
	if err == nil {
		if got := hex.EncodeToString(h.Sum(nil)); got != want {
			err = fmt.Errorf("tigris: snapshot %s failed checksum (got %s, want %s)", key, got, want)
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// snapshotFetch returns the GetChecked fetch for key: it copies the body and
// returns the digest from the object's erofs-sha256 metadata.
func (s *Storer) snapshotFetch(ctx context.Context, key string) func(io.Writer) (string, error) {
	return func(w io.Writer) (string, error) {
		start := time.Now()
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: sp(s.bucket),
			Key:    sp(s.prefix + key),
		})
		s.observe("GetObject", start, err)
		switch {
		case err == nil:
		case isNotFound(err):
			return "", fmt.Errorf("tigris: snapshot %s: %w", key, fs.ErrNotExist)
		default:
			return "", fmt.Errorf("tigris: get snapshot %s: %w", key, err)
		}
		defer out.Body.Close()
		want := out.Metadata[snapshot.MetaSHA256]
		if want == "" {
			return "", fmt.Errorf("tigris: snapshot %s has no %s metadata", key, snapshot.MetaSHA256)
		}
		if _, err := io.Copy(w, out.Body); err != nil {
			return "", fmt.Errorf("tigris: download snapshot %s: %w", key, err)
		}
		return want, nil
	}
}
