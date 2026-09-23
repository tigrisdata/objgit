package tigris

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/tigrisdata/objgit/internal/snapshot"
)

func putTestSnapshot(t *testing.T, s *Storer, key string, body []byte, sum string) {
	t.Helper()
	meta := map[string]string{snapshot.MetaFormat: "1"}
	if sum != "" {
		meta[snapshot.MetaSHA256] = sum
	}
	if err := s.PutSnapshot(context.Background(), key, bytes.NewReader(body), int64(len(body)), meta); err != nil {
		t.Fatalf("PutSnapshot: %v", err)
	}
}

func digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

const testSnapshotKey = "snapshots/erofs/v1/abc.erofs"

func TestPutSnapshotKeyAndMetadata(t *testing.T) {
	f := newFakeS3(t)
	s := newTestStorer(t, f).Scoped("acme/widgets")
	body := []byte("image")
	putTestSnapshot(t, s, testSnapshotKey, body, digest(body))

	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs["acme/widgets/"+testSnapshotKey]
	if !ok {
		var keys []string
		for k := range f.objs {
			keys = append(keys, k)
		}
		t.Fatalf("no object under the scoped key; have %v", keys)
	}
	if !bytes.Equal(o.body, body) {
		t.Errorf("stored body %q, want %q", o.body, body)
	}
	if got := o.meta[snapshot.MetaSHA256]; got != digest(body) {
		t.Errorf("stored %s = %q, want %q", snapshot.MetaSHA256, got, digest(body))
	}
}

func TestStatSnapshot(t *testing.T) {
	tests := []struct {
		name    string
		put     bool
		wantErr error
	}{
		{"present", true, nil},
		{"absent", false, fs.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStorer(t, newFakeS3(t))
			if tt.put {
				putTestSnapshot(t, s, testSnapshotKey, []byte("12345"), digest([]byte("12345")))
			}
			size, err := s.StatSnapshot(context.Background(), testSnapshotKey)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil || size != 5 {
				t.Fatalf("StatSnapshot = %d, %v; want 5, nil", size, err)
			}
		})
	}
}

// errAny marks a test case that expects some error, of no particular kind.
var errAny = errors.New("any error")

func TestOpenSnapshot(t *testing.T) {
	body := []byte("an erofs image, pretend")
	tests := []struct {
		name    string
		cache   bool
		sum     string
		put     bool
		wantErr error // nil is success; errAny is any error
	}{
		{"cached", true, digest(body), true, nil},
		{"no cache", false, digest(body), true, nil},
		{"bad digest", true, digest([]byte("other")), true, errAny},
		{"bad digest no cache", false, digest([]byte("other")), true, errAny},
		{"missing digest", true, "", true, errAny},
		{"missing digest no cache", false, "", true, errAny},
		{"absent", true, "", false, fs.ErrNotExist},
		{"absent no cache", false, "", false, fs.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []Option
			var c *PackCache
			if tt.cache {
				var err error
				c, err = NewSnapshotCache(t.TempDir(), 1<<20, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { c.Cleanup() })
				opts = append(opts, WithSnapshotCache(c))
			}
			s := newTestStorer(t, newFakeS3(t), opts...)
			if tt.put {
				putTestSnapshot(t, s, testSnapshotKey, body, tt.sum)
			}
			f, err := s.OpenSnapshot(context.Background(), testSnapshotKey)
			switch {
			case tt.wantErr == errAny:
				if err == nil {
					f.Close()
					t.Fatal("OpenSnapshot succeeded, want an error")
				}
				if c != nil && countCacheFiles(t, c) != 0 {
					t.Error("the cache kept a file after a failed download")
				}
				return
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			case err != nil:
				t.Fatalf("OpenSnapshot: %v", err)
			}
			defer f.Close()
			got := make([]byte, len(body))
			if _, err := f.ReadAt(got, 0); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("ReadAt: %v", err)
			}
			if !bytes.Equal(got, body) {
				t.Errorf("read %q, want %q", got, body)
			}
		})
	}
}

// TestOpenSnapshotCachesDownload pins that the cache is shared: two opens in
// one repository, and one in another repository with the same tree, send one
// GetObject between them.
func TestOpenSnapshotCachesDownload(t *testing.T) {
	c, err := NewSnapshotCache(t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Cleanup() })
	obs, counts := countingObserver()
	base := newTestStorer(t, newFakeS3(t), WithSnapshotCache(c), obs)
	widgets, gadgets := base.Scoped("acme/widgets"), base.Scoped("acme/gadgets")

	body := []byte("shared tree image")
	putTestSnapshot(t, widgets, testSnapshotKey, body, digest(body))
	putTestSnapshot(t, gadgets, testSnapshotKey, body, digest(body))

	for _, s := range []*Storer{widgets, widgets, gadgets} {
		sf, err := s.OpenSnapshot(context.Background(), testSnapshotKey)
		if err != nil {
			t.Fatalf("OpenSnapshot: %v", err)
		}
		sf.Close()
	}
	if got := counts()["GetObject"]; got != 1 {
		t.Errorf("GetObject calls = %d, want 1", got)
	}
}

func TestSnapshotKeysInvisible(t *testing.T) {
	s := newTestStorer(t, newFakeS3(t))
	body := []byte("x")
	putTestSnapshot(t, s, testSnapshotKey, body, digest(body))

	refs, err := s.IterReferences()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	if err := refs.ForEach(func(*plumbing.Reference) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("IterReferences returned %d refs, want 0", n)
	}

	objs, err := s.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		t.Fatal(err)
	}
	n = 0
	if err := objs.ForEach(func(plumbing.EncodedObject) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("IterEncodedObjects returned %d objects, want 0", n)
	}
}
