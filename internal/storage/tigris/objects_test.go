package tigris

import (
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
)

func TestHasEncodedObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T, f *fakeS3) plumbing.Hash
		wantErr error // nil means success
	}{
		{
			name: "present",
			setup: func(t *testing.T, f *fakeS3) plumbing.Hash {
				return seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "hello")
			},
		},
		{
			name: "absent maps to the go-git sentinel",
			setup: func(_ *testing.T, _ *fakeS3) plumbing.Hash {
				h, _ := plumbing.FromHex("1111111111111111111111111111111111111111")
				return h
			},
			wantErr: plumbing.ErrObjectNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			h := tt.setup(t, f)
			s := newTestStorer(t, f)

			err := s.HasEncodedObject(h)
			if !errors.Is(err, tt.wantErr) && !(err == nil && tt.wantErr == nil) {
				t.Logf("want: %v", tt.wantErr)
				t.Logf("got:  %v", err)
				t.Error("got wrong error")
			}
		})
	}
}

func TestEncodedObjectSizePrefersMetadata(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.TreeObject, "hello")
	f.mu.Lock()
	ent := f.objs[keyOf(h)]
	ent.meta[metaSize] = "4096"
	f.objs[keyOf(h)] = ent
	f.mu.Unlock()

	s := newTestStorer(t, f)

	got, err := s.EncodedObjectSize(h)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 4096 {
		t.Errorf("want metadata size 4096, got %d", got)
	}
}

func TestEncodedObjectSizeFallsBackToContentLength(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "12345")
	delete(f.get(t, keyOf(h)).meta, metaSize)

	s := newTestStorer(t, f)

	got, err := s.EncodedObjectSize(h)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 5 {
		t.Errorf("want fallback size 5, got %d", got)
	}
}

func TestHeadInfoErrorMapping(t *testing.T) {
	t.Parallel()

	t.Run("transport failures wrap, never masquerade", func(t *testing.T) {
		f := newFakeS3(t)
		h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "x")
		f.headErr = &smithy.GenericAPIError{Code: "AccessDenied", Message: "nope"}
		s := newTestStorer(t, f)

		_, err := s.headInfo(h)
		if errors.Is(err, plumbing.ErrObjectNotFound) || errors.Is(err, errBadMetadata) || err == nil {
			t.Errorf("access denied lost its identity: %v", err)
		}
	})

	t.Run("unknown type name flags bad metadata", func(t *testing.T) {
		f := newFakeS3(t)
		h, _ := plumbing.FromHex("2222222222222222222222222222222222222222")
		f.put(keyOf(h), "junk", map[string]string{
			metaType: "frog",
			metaSize: strconv.Itoa(len("junk")),
		})
		s := newTestStorer(t, f)

		if _, err := s.headInfo(h); !errors.Is(err, errBadMetadata) {
			t.Errorf("want errBadMetadata wrapping, got %v", err)
		}
	})

	t.Run("missing every size source flags bad metadata", func(t *testing.T) {
		f := newFakeS3(t)
		h, _ := plumbing.FromHex("3333333333333333333333333333333333333333")
		f.objs[keyOf(h)] = fakeObject{
			body: []byte("sized-body"),
			meta: map[string]string{metaType: plumbing.BlobObject.String()},
		}
		// Hand-build the HEAD output with no ContentLength at all.
		f.headOverride = map[string]*headShape{
			keyOf(h): {metaOnly: true},
		}
		s := newTestStorer(t, f)

		if _, err := s.headInfo(h); !errors.Is(err, errBadMetadata) {
			t.Errorf("want errBadMetadata wrapping, got %v", err)
		}
	})
}

func TestEncodedObjectRoundTrip(t *testing.T) {
	t.Parallel()

	const content = "the quick brown fox jumps over the lazy dog"
	f := newFakeS3(t)
	of := formatcfg.DefaultObjectFormat
	h := seed(t, f, of, plumbing.BlobObject, content)
	other := seed(t, f, of, plumbing.CommitObject, "an unrelated commit")

	s := newTestStorer(t, f)

	t.Run("typed hit decodes identical bytes", func(t *testing.T) {
		obj, err := s.EncodedObject(plumbing.BlobObject, h)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		rd, rerr := obj.Reader()
		if rerr != nil {
			t.Fatalf("reader: %v", rerr)
		}
		defer rd.Close()
		got, rerr := io.ReadAll(rd)
		if rerr != nil {
			t.Fatalf("read: %v", rerr)
		}
		if string(got) != content {
			t.Errorf("want %q, got %q", content, got)
		}
		if obj.Type() != plumbing.BlobObject || obj.Size() != int64(len(content)) {
			t.Errorf("bad framing: type=%v size=%d", obj.Type(), obj.Size())
		}
	})

	t.Run("wrong requested type behaves like absence", func(t *testing.T) {
		if _, err := s.EncodedObject(plumbing.TagObject, h); !errors.Is(err, plumbing.ErrObjectNotFound) {
			t.Errorf("want ErrObjectNotFound, got %v", err)
		}
	})

	t.Run("AnyObject ignores the type filter", func(t *testing.T) {
		if _, err := s.EncodedObject(plumbing.AnyObject, other); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("absent object", func(t *testing.T) {
		if _, err := s.EncodedObject(plumbing.AnyObject, plumbing.ZeroHash); !errors.Is(err, plumbing.ErrObjectNotFound) {
			t.Errorf("want ErrObjectNotFound, got %v", err)
		}
	})
}

func TestEncodedObjectRejectsDeltaTypes(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	h := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "real bytes underneath")
	f.mu.Lock()
	ent := f.objs[keyOf(h)]
	ent.meta[metaType] = plumbing.OFSDeltaObject.String()
	f.objs[keyOf(h)] = ent
	f.mu.Unlock()

	s := newTestStorer(t, f)

	if _, err := s.EncodedObject(plumbing.AnyObject, h); !errors.Is(err, plumbing.ErrInvalidType) {
		t.Errorf("want ErrInvalidType, got %v", err)
	}
}
