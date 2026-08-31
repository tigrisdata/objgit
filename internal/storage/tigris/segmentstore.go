package tigris

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// segmentStore is a storer.EncodedObjectStorer that lands objects straight into
// pack containers.
//
// It exists to replace the scratch go-git repository that PackfileWriter
// currently decodes a push into. go-git's packfile.Parser writes every object
// it resolves through RawObjectWriter (non-deltas from the scanner, resolved
// deltas from storeOrCache), so handing the parser one of these means each
// object is resolved exactly once and appended to a .bin as it arrives, instead
// of being written to a scratch repository and read back out.
//
// Staging files are deliberately kept open for the whole push rather than
// sealed as they fill: the parser reads bases back out through EncodedObject
// (packfile/parser.go's REF-delta path), and a base can live in a segment that
// filled long before the delta that needs it arrives.
var _ storer.EncodedObjectStorer = (*segmentStore)(nil)

type segmentStore struct {
	s         *Storer
	byteLimit int64

	mu     sync.Mutex
	open   *packSegment
	filled []*packSegment
	index  map[plumbing.Hash]stagedRef
}

// stagedRef locates one object inside one still-open staging file.
type stagedRef struct {
	seg *packSegment
	rec cueRecord
}

func newSegmentStore(s *Storer) *segmentStore {
	limit := s.maxPackBytes
	if limit <= 0 {
		limit = maxPackBytes
	}
	return &segmentStore{
		s:         s,
		byteLimit: limit,
		index:     map[plumbing.Hash]stagedRef{},
	}
}

// LowMemoryMode reports that the parser may drop object contents and re-inflate
// them from the pack on demand. Answering true here is what keeps
// packfile.Parser off the path that retains every object's bytes in memory;
// see the seeker check in packfile/parser.go.
func (ss *segmentStore) LowMemoryMode() bool { return true }

// segments returns every staging file written so far, the open one last. The
// caller seals and uploads them; this type never touches the bucket.
func (ss *segmentStore) segments() []*packSegment {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	out := make([]*packSegment, 0, len(ss.filled)+1)
	out = append(out, ss.filled...)
	if ss.open != nil && len(ss.open.recs) > 0 {
		out = append(out, ss.open)
	}
	return out
}

// discard throws away every staging file. Idempotent, and safe to defer for the
// error path: sealing removes segments from this store's ownership first.
func (ss *segmentStore) discard() {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for _, seg := range ss.filled {
		seg.discard()
	}
	if ss.open != nil {
		ss.open.discard()
	}
	ss.filled, ss.open = nil, nil
	ss.index = map[plumbing.Hash]stagedRef{}
}

// segmentFor returns a segment with room for an object of size sz, rotating the
// open one out when it would overflow the byte cap.
//
// The cap is checked before the add and only when the segment already holds
// something, which is the same rule packWriter.Close applies: an object larger
// than the whole cap gets a container to itself, because it has to live
// somewhere.
func (ss *segmentStore) segmentFor(sz int64) (*packSegment, error) {
	if ss.open != nil && len(ss.open.recs) > 0 && ss.open.offset+sz > ss.byteLimit {
		ss.filled = append(ss.filled, ss.open)
		ss.open = nil
	}
	if ss.open == nil {
		seg, err := newPackSegment(ss.s)
		if err != nil {
			return nil, err
		}
		ss.open = seg
	}
	return ss.open, nil
}

// RawObjectWriter stages one object to its own temp file, hashing as it goes,
// and appends it to a container on Close. The staging hop buys streaming: the
// object never exists in memory in one piece, which is the whole point of
// taking the parser's output this way rather than through SetEncodedObject.
func (ss *segmentStore) RawObjectWriter(typ plumbing.ObjectType, sz int64) (io.WriteCloser, error) {
	if typ == plumbing.OFSDeltaObject || typ == plumbing.REFDeltaObject {
		return nil, plumbing.ErrInvalidType
	}
	if sz < 0 {
		return nil, fmt.Errorf("tigris: negative object size %d", sz)
	}

	f, err := os.CreateTemp("", "objgit-tigris-seg-*")
	if err != nil {
		return nil, fmt.Errorf("tigris: create staging file: %w", err)
	}

	return &segmentWriter{
		ss:     ss,
		f:      f,
		typ:    typ,
		size:   sz,
		hasher: plumbing.NewHasher(ss.s.of, typ, sz),
	}, nil
}

type segmentWriter struct {
	ss     *segmentStore
	f      *os.File
	typ    plumbing.ObjectType
	size   int64
	wrote  int64
	hasher plumbing.Hasher
	done   bool
}

func (w *segmentWriter) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("tigris: write on discarded segment writer")
	}

	n, err := io.MultiWriter(w.f, w.hasher).Write(p)
	w.wrote += int64(n)
	if err != nil {
		// File and hasher may disagree from here on; the stream is poison.
		w.discard()
		return n, fmt.Errorf("tigris: stage write: %w", err)
	}
	if w.wrote > w.size {
		w.discard()
		return n, fmt.Errorf("tigris: wrote %d bytes beyond declared size %d", w.wrote-w.size, w.size)
	}
	return n, nil
}

func (w *segmentWriter) discard() {
	if w.done {
		return
	}
	w.done = true
	w.f.Close()
	os.Remove(w.f.Name())
}

func (w *segmentWriter) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	defer os.Remove(w.f.Name())

	if w.wrote != w.size {
		w.f.Close()
		return fmt.Errorf("tigris: staged %d bytes but declared %d", w.wrote, w.size)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("tigris: close staging file: %w", err)
	}

	h := w.hasher.Sum()
	obj := &stagedObject{path: w.f.Name(), typ: w.typ, size: w.size, hash: h}

	w.ss.mu.Lock()
	defer w.ss.mu.Unlock()

	// An object already staged is not written twice. A pack may legitimately
	// carry the same object more than once, and the second copy would only add
	// bytes to the container under a hash the index already resolves.
	if _, ok := w.ss.index[h]; ok {
		return nil
	}

	seg, err := w.ss.segmentFor(w.size)
	if err != nil {
		return err
	}

	before := len(seg.recs)
	if err := seg.add(storedObject{payload: obj, hash: h, typ: w.typ, raw: w.size}); err != nil {
		return err
	}
	if len(seg.recs) != before+1 {
		return fmt.Errorf("tigris: segment recorded %d records for one object", len(seg.recs)-before)
	}

	w.ss.index[h] = stagedRef{seg: seg, rec: seg.recs[before]}
	return nil
}

// stagedObject is one object held in its own temp file, presented to
// packSegment.add as an EncodedObject. Reader opens the file fresh each call so
// writePayload can stream it, and rewind it for the probe path, without holding
// the bytes.
type stagedObject struct {
	path string
	typ  plumbing.ObjectType
	size int64
	hash plumbing.Hash
}

func (o *stagedObject) Hash() plumbing.Hash            { return o.hash }
func (o *stagedObject) Type() plumbing.ObjectType      { return o.typ }
func (o *stagedObject) SetType(t plumbing.ObjectType)  { o.typ = t }
func (o *stagedObject) Size() int64                    { return o.size }
func (o *stagedObject) SetSize(n int64)                { o.size = n }
func (o *stagedObject) Reader() (io.ReadCloser, error) { return os.Open(o.path) }

func (o *stagedObject) Writer() (io.WriteCloser, error) {
	return nil, errors.New("tigris: staged object is read-only")
}

// EncodedObject serves the parser's base lookups out of the staging files. A
// miss falls through to the backing Storer, which is what lets a thin pack
// resolve a delta against an object the repository already holds.
func (ss *segmentStore) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	ss.mu.Lock()
	ref, ok := ss.index[h]
	ss.mu.Unlock()

	if !ok {
		return ss.s.EncodedObject(t, h)
	}
	if t != plumbing.AnyObject && ref.rec.typ != t {
		return nil, plumbing.ErrObjectNotFound
	}

	stored := make([]byte, ref.rec.stored)
	if _, err := ref.seg.file.ReadAt(stored, ref.rec.offset); err != nil {
		return nil, fmt.Errorf("tigris: read %s from staging: %w", h.String(), err)
	}

	plain, err := ss.s.decodePackedPayload(h, packEntry{
		typ:    ref.rec.typ,
		codec:  ref.rec.codec,
		offset: ref.rec.offset,
		stored: ref.rec.stored,
		raw:    ref.rec.raw,
	}, bytes.NewReader(stored))
	if err != nil {
		return nil, err
	}

	obj := plumbing.NewMemoryObject(ss.s.oh)
	obj.SetType(ref.rec.typ)
	obj.SetSize(ref.rec.raw)
	if _, err := obj.Write(plain); err != nil {
		return nil, fmt.Errorf("tigris: rebuild %s: %w", h.String(), err)
	}
	return obj, nil
}

func (ss *segmentStore) HasEncodedObject(h plumbing.Hash) error {
	ss.mu.Lock()
	_, ok := ss.index[h]
	ss.mu.Unlock()

	if ok {
		return nil
	}
	return ss.s.HasEncodedObject(h)
}

func (ss *segmentStore) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	ss.mu.Lock()
	ref, ok := ss.index[h]
	ss.mu.Unlock()

	if ok {
		return ref.rec.raw, nil
	}
	return ss.s.EncodedObjectSize(h)
}

// NewEncodedObject and SetEncodedObject delegate: the parser reaches for
// RawObjectWriter, so these exist to satisfy the interface and to keep any
// caller that does use them behaving exactly as the Storer does.
func (ss *segmentStore) NewEncodedObject() plumbing.EncodedObject { return ss.s.NewEncodedObject() }

func (ss *segmentStore) SetEncodedObject(obj plumbing.EncodedObject) (plumbing.Hash, error) {
	return ss.s.SetEncodedObject(obj)
}

func (ss *segmentStore) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	ss.mu.Lock()
	hashes := make([]plumbing.Hash, 0, len(ss.index))
	for h, ref := range ss.index {
		if t == plumbing.AnyObject || ref.rec.typ == t {
			hashes = append(hashes, h)
		}
	}
	ss.mu.Unlock()

	return &segmentIter{ss: ss, typ: t, hashes: hashes}, nil
}

func (ss *segmentStore) AddAlternate(remote string) error { return ss.s.AddAlternate(remote) }

type segmentIter struct {
	ss     *segmentStore
	typ    plumbing.ObjectType
	hashes []plumbing.Hash
	i      int
}

func (it *segmentIter) Next() (plumbing.EncodedObject, error) {
	if it.i >= len(it.hashes) {
		return nil, io.EOF
	}
	h := it.hashes[it.i]
	it.i++
	return it.ss.EncodedObject(it.typ, h)
}

func (it *segmentIter) ForEach(fn func(plumbing.EncodedObject) error) error {
	for {
		obj, err := it.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(obj); err != nil {
			if errors.Is(err, storer.ErrStop) {
				return nil
			}
			return err
		}
	}
}

func (it *segmentIter) Close() { it.i = len(it.hashes) }
