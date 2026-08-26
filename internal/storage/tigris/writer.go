package tigris

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
)

// Writer design follows docs/reference/tigris-backend.md "Write path": bytes
// stream into a local staging file while a hashing tee runs alongside. Close
// performs the single PUT, named by the freshly computed hash. Upload failures
// or size disagreements leave the bucket untouched.
type stageWriter struct {
	s      *Storer
	f      *os.File
	typ    plumbing.ObjectType
	size   int64
	wrote  int64
	hasher plumbing.Hasher
	done   bool
}

func (s *Storer) NewEncodedObject() plumbing.EncodedObject {
	return plumbing.NewMemoryObject(s.oh)
}

func (s *Storer) RawObjectWriter(typ plumbing.ObjectType, sz int64) (io.WriteCloser, error) {
	if typ == plumbing.OFSDeltaObject || typ == plumbing.REFDeltaObject {
		return nil, plumbing.ErrInvalidType
	}
	if sz < 0 {
		return nil, fmt.Errorf("tigris: negative object size %d", sz)
	}

	f, ferr := os.CreateTemp("", "objgit-tigris-*")
	if ferr != nil {
		return nil, fmt.Errorf("tigris: create staging file: %w", ferr)
	}
	return &stageWriter{
		s:      s,
		f:      f,
		typ:    typ,
		size:   sz,
		hasher: plumbing.NewHasher(s.of, typ, sz),
	}, nil
}

func (w *stageWriter) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("tigris: write on discarded raw writer")
	}
	n, err := io.MultiWriter(w.f, w.hasher).Write(p)
	w.wrote += int64(n)
	if err != nil {
		// File and hasher may disagree from here on; the stream is poison.
		w.Discard()
		return n, fmt.Errorf("tigris: stage write: %w", err)
	}
	if w.wrote > w.size {
		w.Discard()
		return n, fmt.Errorf("tigris: wrote %d bytes beyond declared size %d", w.wrote-w.size, w.size)
	}
	return n, nil
}

// Discard aborts the stream: nothing uploads, staged bytes vanish. Idempotent.
func (w *stageWriter) Discard() {
	if w.done {
		return
	}
	w.done = true
	w.f.Close()
	os.Remove(w.f.Name())
}

func (w *stageWriter) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	defer os.Remove(w.f.Name())
	defer w.f.Close()

	if w.wrote != w.size {
		return fmt.Errorf("tigris: staged %d bytes but declared %d", w.wrote, w.size)
	}

	h := w.hasher.Sum()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("tigris: rewind staging file: %w", err)
	}

	start := time.Now()
	_, err := w.s.client.PutObject(w.s.ctx, &s3.PutObjectInput{
		Bucket: sp(w.s.bucket),
		Key:    sp(keyOf(h)),
		Body:   w.f,
		Metadata: map[string]string{
			metaType: w.typ.String(),
			metaSize: strconv.FormatInt(w.wrote, 10),
		},
	})
	w.s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: upload %s: %w", keyOf(h), err)
	}
	return nil
}

func (s *Storer) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	switch obj.Type() {
	case plumbing.OFSDeltaObject, plumbing.REFDeltaObject:
		return plumbing.ZeroHash, plumbing.ErrInvalidType
	}

	rd, err := obj.Reader()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("tigris: open reader for %s: %w", obj.Hash().String(), err)
	}
	defer rd.Close()

	w, err := s.RawObjectWriter(obj.Type(), obj.Size())
	if err != nil {
		return plumbing.ZeroHash, err
	}
	sw := w.(*stageWriter)

	if _, err := io.Copy(sw, rd); err != nil {
		sw.Discard()
		return plumbing.ZeroHash, fmt.Errorf("tigris: copy data for %s: %w", obj.Hash().String(), err)
	}

	// Claimed hashes are untrusted (spec CAUTION): prove the recomputed
	// stream agrees before storing anything under any address.
	got := sw.hasher.Sum()
	if want := obj.Hash(); got.String() != want.String() {
		sw.Discard()
		return plumbing.ZeroHash, ErrHashMismatch
	}

	if err := sw.Close(); err != nil {
		return plumbing.ZeroHash, err
	}
	return got, nil
}

func (s *Storer) AddAlternate(remote string) error {
	return fmt.Errorf("%w: remote %q", ErrAlternatesNotSupported, remote)
}
