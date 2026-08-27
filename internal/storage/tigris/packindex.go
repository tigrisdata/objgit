package tigris

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
)

const (
	packPrefix = "packs/"
	binSuffix  = ".bin"
	cueSuffix  = ".cue"

	// packBulkFetchThreshold is how many distinct objects may be served out of
	// one pack via individual ranged GETs, within one Storer instance, before
	// switching to downloading that pack's whole .bin once and serving every
	// later read from the local copy.
	packBulkFetchThreshold = 32

	// maxPackObjects caps how many objects one *written* pack container holds,
	// so a large initial push lands as several bin/cue pairs instead of one
	// enormous one. Reads have no matching limit: packIndex merges every
	// packs/*.cue it lists, so any number of containers, holding any number of
	// objects, resolves exactly the same way.
	maxPackObjects = 1 << 15

	// maxPackBytes caps a written container's payload. An object count alone is
	// a poor proxy for size — 32768 tiny objects is a few megabytes, 32768
	// large blobs is tens of gigabytes — and container size is what sets the
	// size of one PutObject, of one bulk .bin download on the read side
	// (packBulkFetchThreshold), and of one PackCache eviction.
	//
	// packwriter.go checks this cap *before* adding an object, so a container
	// sitting just under it never grows past it by swallowing a large object
	// that arrives late. The one container allowed past the cap holds a single
	// object larger than the cap all by itself; giving it a container of its
	// own beats bloating whichever container it happened to arrive behind.
	//
	// Like maxPackObjects, this is a write-side cap only.
	maxPackBytes = 128 << 20 // 128 MiB
)

var cueMagic = [4]byte{'O', 'G', 'C', 1} // "OGC" + format version 1

// errBadCue marks a malformed pack index: corruption never masquerades as
// absence, the same posture errBadMetadata takes for loose objects.
var errBadCue = fmt.Errorf("tigris: malformed pack cue index")

// cueRecord is one object's entry in a .cue index: its hash plus where its
// raw bytes live in the sibling .bin.
type cueRecord struct {
	hash   plumbing.Hash
	typ    plumbing.ObjectType
	offset int64
	length int64 // == the git object's size, since .bin payloads are raw
}

// encodeCue serializes recs (assumed already sorted by hash) into the .cue
// binary format: a 16-byte header (magic, hash width, reserved, count)
// followed by one hashLen+17-byte record per entry, all big-endian.
func encodeCue(hashLen int, recs []cueRecord) []byte {
	recWidth := hashLen + 17
	buf := make([]byte, 16+len(recs)*recWidth)
	copy(buf[0:4], cueMagic[:])
	buf[4] = byte(hashLen)
	binary.BigEndian.PutUint64(buf[8:16], uint64(len(recs)))

	off := 16
	for _, r := range recs {
		copy(buf[off:off+hashLen], r.hash.Bytes())
		buf[off+hashLen] = byte(r.typ)
		binary.BigEndian.PutUint64(buf[off+hashLen+1:off+hashLen+9], uint64(r.offset))
		binary.BigEndian.PutUint64(buf[off+hashLen+9:off+hashLen+17], uint64(r.length))
		off += recWidth
	}
	return buf
}

// parseCue is encodeCue's inverse. hashLen is the caller's expected width
// (from this Storer's own object format) — a cue written under a different
// format is rejected rather than silently misparsed.
func parseCue(hashLen int, raw []byte) ([]cueRecord, error) {
	if len(raw) < 16 {
		return nil, fmt.Errorf("%w: header truncated (%d bytes)", errBadCue, len(raw))
	}
	if !bytes.Equal(raw[0:4], cueMagic[:]) {
		return nil, fmt.Errorf("%w: bad magic", errBadCue)
	}
	if got := int(raw[4]); got != hashLen {
		return nil, fmt.Errorf("%w: hash width %d, want %d", errBadCue, got, hashLen)
	}
	if raw[5] != 0 || raw[6] != 0 || raw[7] != 0 {
		return nil, fmt.Errorf("%w: reserved bytes not zero", errBadCue)
	}

	count := binary.BigEndian.Uint64(raw[8:16])
	recWidth := hashLen + 17
	if want := 16 + int(count)*recWidth; want != len(raw) {
		return nil, fmt.Errorf("%w: length %d disagrees with %d records (want %d)", errBadCue, len(raw), count, want)
	}

	recs := make([]cueRecord, 0, count)
	off := 16
	for i := uint64(0); i < count; i++ {
		h, ok := plumbing.FromBytes(raw[off : off+hashLen])
		if !ok {
			return nil, fmt.Errorf("%w: record %d has an unreadable hash", errBadCue, i)
		}
		typ := plumbing.ObjectType(int8(raw[off+hashLen]))
		offset := int64(binary.BigEndian.Uint64(raw[off+hashLen+1 : off+hashLen+9]))
		length := int64(binary.BigEndian.Uint64(raw[off+hashLen+9 : off+hashLen+17]))
		recs = append(recs, cueRecord{hash: h, typ: typ, offset: offset, length: length})
		off += recWidth
	}
	return recs, nil
}

// packEntry is what the in-memory index keeps per object: which pack it's
// in and where.
type packEntry struct {
	id     string
	typ    plumbing.ObjectType
	offset int64
	length int64
}

// packedEntry pairs a hash with its packEntry, for iteration (a map alone
// loses the hash as a first-class value).
type packedEntry struct {
	hash plumbing.Hash
	e    packEntry
}

// packAccess tracks one pack's read history within this Storer instance: how
// many distinct objects have been served via individual ranged GETs, and
// (past the threshold) the bulk-downloaded local copy.
type packAccess struct {
	seen map[plumbing.Hash]struct{}
	once sync.Once
	f    *os.File
	err  error
}

// packIndex is a Storer's view of every pack it can reach: entries already
// durable in S3 (built lazily, once, by listing packs/*.cue) merged with
// entries for packs this same instance has staged locally but not yet
// finished uploading. Every Storer value — New's and every Scoped
// descendant's — gets its own packIndex, for the same isolation reasons as
// its uploader: one repository's pack backlog, failures, or read history can
// never affect another's.
type packIndex struct {
	mu      sync.Mutex
	built   bool
	entries map[plumbing.Hash]packEntry
	sizes   map[string]int64  // packID -> total .bin length
	local   map[string]string // packID -> staged local .bin path while its upload is pending
	access  map[string]*packAccess
}

func newPackIndex() *packIndex {
	return &packIndex{
		entries: make(map[plumbing.Hash]packEntry),
		sizes:   make(map[string]int64),
		local:   make(map[string]string),
		access:  make(map[string]*packAccess),
	}
}

// register makes a not-yet-uploaded pack's objects visible to reads (see
// packLookup) and its .bin visible to the local-cache read tier (see
// localPath). Call it before enqueueing the upload, mirroring writer.go's
// "register pending before enqueue" rule: a read must never race ahead of
// the write it depends on.
func (p *packIndex) register(id string, recs []cueRecord, localBin string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var size int64
	for _, r := range recs {
		p.entries[r.hash] = packEntry{id: id, typ: r.typ, offset: r.offset, length: r.length}
		if end := r.offset + r.length; end > size {
			size = end
		}
	}
	p.sizes[id] = size
	p.local[id] = localBin
}

// markUploaded drops a pack's local-cache visibility once its upload lands:
// its entries stay valid (now served from S3 instead), only the local .bin
// path stops being offered to readers.
func (p *packIndex) markUploaded(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.local, id)
}

// deregister removes a pack that failed to upload entirely: its entries must
// not point at a pack S3 will never have.
func (p *packIndex) deregister(id string, recs []cueRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range recs {
		if e, ok := p.entries[r.hash]; ok && e.id == id {
			delete(p.entries, r.hash)
		}
	}
	delete(p.sizes, id)
	delete(p.local, id)
}

func (p *packIndex) localPath(id string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	path, ok := p.local[id]
	return path, ok
}

func (p *packIndex) binSize(id string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sizes[id]
}

// snapshotEntries lists every indexed object, ordered by pack and then by
// offset within that pack. The order matters because a push above either
// write-side cap lands as several containers (see packwriter.go): map order
// would interleave them, so a full iteration would hold every container's bulk
// download open at once. Draining one pack at a time keeps that to one.
func (p *packIndex) snapshotEntries() []packedEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]packedEntry, 0, len(p.entries))
	for h, e := range p.entries {
		out = append(out, packedEntry{hash: h, e: e})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].e.id != out[j].e.id {
			return out[i].e.id < out[j].e.id
		}
		return out[i].e.offset < out[j].e.offset
	})
	return out
}

// recordAccess registers h as a distinct ranged-GET read of pack id and
// reports whether this call is the one that crosses packBulkFetchThreshold —
// distinct objects, not reads: re-reading the same handful of hashes never
// triggers a bulk download.
func (p *packIndex) recordAccess(id string, h plumbing.Hash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.access[id]
	if !ok {
		a = &packAccess{seen: make(map[plumbing.Hash]struct{})}
		p.access[id] = a
	}
	a.seen[h] = struct{}{}
	return len(a.seen) > packBulkFetchThreshold
}

func (p *packIndex) getAccess(id string) *packAccess {
	p.mu.Lock()
	defer p.mu.Unlock()
	a, ok := p.access[id]
	if !ok {
		a = &packAccess{seen: make(map[plumbing.Hash]struct{})}
		p.access[id] = a
	}
	return a
}

// bulkCopy reports the pack's already-downloaded local copy, if any.
func (p *packIndex) bulkCopy(id string) (*os.File, bool) {
	a := p.getAccess(id)
	if a.f == nil {
		return nil, false
	}
	return a.f, true
}

// ensurePacksBuilt lists packs/*.cue under this Storer's prefix and merges
// their entries into the index, once per instance. Not sticky on error — a
// transient S3 failure is retried on the next call, not remembered forever.
// Entries this instance has itself register-ed (its own in-flight pushes)
// are untouched: they live outside anything a listing could ever race with.
func (s *Storer) ensurePacksBuilt() error {
	s.packs.mu.Lock()
	built := s.packs.built
	s.packs.mu.Unlock()
	if built {
		return nil
	}

	keys, err := s.listKeys(s.prefix + packPrefix)
	if err != nil {
		return fmt.Errorf("tigris: list packs: %w", err)
	}

	hashLen := s.oh.Size()
	byID := make(map[string][]cueRecord)
	for _, k := range keys {
		if !strings.HasSuffix(k, cueSuffix) {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(k, s.prefix+packPrefix), cueSuffix)

		raw, err := s.fetchSmall(k)
		if err != nil {
			return fmt.Errorf("tigris: fetch cue %s: %w", k, err)
		}
		recs, err := parseCue(hashLen, raw)
		if err != nil {
			return fmt.Errorf("tigris: parse cue %s: %w", k, err)
		}
		byID[id] = recs
	}

	s.packs.mu.Lock()
	defer s.packs.mu.Unlock()
	if s.packs.built {
		return nil // lost a race with a concurrent cold build on this instance
	}
	for id, recs := range byID {
		var size int64
		for _, r := range recs {
			s.packs.entries[r.hash] = packEntry{id: id, typ: r.typ, offset: r.offset, length: r.length}
			if end := r.offset + r.length; end > size {
				size = end
			}
		}
		s.packs.sizes[id] = size
	}
	s.packs.built = true
	return nil
}

// packLookup reports whether h lives in a pack this Storer knows about,
// triggering the cold index build on first use.
func (s *Storer) packLookup(h plumbing.Hash) (packEntry, bool, error) {
	if err := s.ensurePacksBuilt(); err != nil {
		return packEntry{}, false, err
	}
	s.packs.mu.Lock()
	defer s.packs.mu.Unlock()
	e, ok := s.packs.entries[h]
	return e, ok, nil
}

// downloadPack fetches packs/<id>.bin whole, once per pack per Storer
// instance (guarded by packAccess.once). Failure is sticky for this instance —
// later reads quietly fall back to ranged GETs rather than retrying or
// erroring.
func (s *Storer) downloadPack(id string) (*os.File, error) {
	a := s.packs.getAccess(id)
	a.once.Do(func() {
		a.f, a.err = s.fetchWholePack(id)
	})
	return a.f, a.err
}

// fetchWholePack produces a local descriptor over the whole .bin. With a
// PackCache installed the copy lives in the shared cache directory and outlives
// this request, so the next clone of the same repository downloads nothing;
// without one it is a private unlinked-but-open temp file, whose disk space the
// kernel reclaims as soon as every descriptor referencing it closes. Both paths
// verify the bytes' sha256 against id — the pack's name is its checksum.
func (s *Storer) fetchWholePack(id string) (*os.File, error) {
	f, err := s.openWholePack(id)
	if err != nil {
		return nil, err
	}

	// A cue whose records run past the end of its own .bin means the two
	// disagree. The checksum above already proves the bytes are the ones this
	// id names, so this catches an index/container mismatch, not corruption.
	if want := s.packs.binSize(id); want != 0 {
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("tigris: stat downloaded pack %s: %w", id, err)
		}
		if fi.Size() != want {
			f.Close()
			return nil, fmt.Errorf("tigris: downloaded pack %s: got %d bytes, want %d", id, fi.Size(), want)
		}
	}

	// Close the fd once this Storer (and thus this packIndex, and thus this
	// packAccess) becomes unreachable — normally promptly after the request
	// that created it finishes. Worst case is a few leaked descriptors until
	// the next GC, never unbounded disk usage.
	runtime.AddCleanup(s.packs, func(file *os.File) { file.Close() }, f)
	return f, nil
}

func (s *Storer) openWholePack(id string) (*os.File, error) {
	if s.cache != nil {
		return s.cache.Get(id, s.streamPack(id))
	}

	f, err := os.CreateTemp("", "objgit-tigris-bulk-*")
	if err != nil {
		return nil, fmt.Errorf("tigris: create bulk-download temp file: %w", err)
	}
	os.Remove(f.Name()) // unlink now; fd keeps the data alive until it's closed

	if _, err := verifiedCopy(f, id, s.streamPack(id)); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("tigris: rewind downloaded pack %s: %w", id, err)
	}
	return f, nil
}

// streamPack returns a fetch function that writes packs/<id>.bin whole into w,
// in the shape PackCache.Get and verifiedCopy both consume.
func (s *Storer) streamPack(id string) func(w io.Writer) error {
	return func(w io.Writer) error {
		start := time.Now()
		out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
			Bucket: sp(s.bucket),
			Key:    sp(s.prefix + packPrefix + id + binSuffix),
		})
		s.observe("GetObject", start, err)
		if err != nil {
			return fmt.Errorf("tigris: download pack %s: %w", id, err)
		}
		defer out.Body.Close()

		if _, err := io.Copy(w, out.Body); err != nil {
			return fmt.Errorf("tigris: download pack %s: %w", id, err)
		}
		return nil
	}
}

// packObject reads one object out of a pack, in increasing order of cost:
// this instance's own in-flight (not-yet-uploaded) local copy, an
// already-bulk-downloaded local copy, triggering a bulk download on the read
// that crosses packBulkFetchThreshold, or — the common case below that
// threshold — a single ranged GetObject straight into the object's byte
// range. Every tier decodes through decodeBody (objects.go).
func (s *Storer) packObject(t plumbing.ObjectType, h plumbing.Hash, e packEntry) (plumbing.EncodedObject, error) {
	if t != plumbing.AnyObject && e.typ != t {
		return nil, plumbing.ErrObjectNotFound
	}
	hs := objHead{typ: e.typ, size: e.length}

	if e.length == 0 {
		obj := plumbing.NewMemoryObject(s.oh)
		obj.SetType(e.typ)
		obj.SetSize(0)
		return obj, nil
	}

	if path, ok := s.packs.localPath(e.id); ok {
		if f, err := os.Open(path); err == nil {
			defer f.Close()
			return s.decodeBody(h, hs, io.NewSectionReader(f, e.offset, e.length))
		}
		// os.ErrNotExist: evicted underneath us (the upload just finished);
		// fall through to the tiers below — S3 now has it.
	}

	if f, ok := s.packs.bulkCopy(e.id); ok {
		return s.decodeBody(h, hs, io.NewSectionReader(f, e.offset, e.length))
	}

	if s.packs.recordAccess(e.id, h) {
		if f, err := s.downloadPack(e.id); err == nil {
			return s.decodeBody(h, hs, io.NewSectionReader(f, e.offset, e.length))
		}
		// Sticky failure: degrade to the ranged GET below rather than fail
		// the read outright.
	}

	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + packPrefix + e.id + binSuffix),
		Range:  sp(fmt.Sprintf("bytes=%d-%d", e.offset, e.offset+e.length-1)),
	})
	s.observe("GetObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, plumbing.ErrObjectNotFound
	default:
		return nil, fmt.Errorf("tigris: get packed %s: %w", h.String(), err)
	}
	defer out.Body.Close()

	return s.decodeBody(h, hs, out.Body)
}
