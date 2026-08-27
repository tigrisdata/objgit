package tigris

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
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
	//
	// Tuned when payloads were uncompressed, so it is conservative now: a
	// ranged GET moves fewer bytes than it used to, and a bulk download moves
	// fewer too. Left alone deliberately — the crossover is a measurement, not
	// a guess.
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

// cueMagicPrefix is shared by every .cue format version; the 4th header byte
// carries the version itself.
var cueMagicPrefix = [3]byte{'O', 'G', 'C'}

const (
	cueVersion1 = 1 // fixed-width records, raw payloads, no codecs
	cueVersion2 = 2 // columnar records, optionally zstd, per-object codecs

	cueHeaderLen = 16

	// cueRecCodecOff is header byte 5: the codec of the *record block*, which
	// is independent of any object's payload codec. v1 held a reserved zero
	// here, so a v1 cue reads as codecRaw and stays correct.
	cueRecCodecOff = 5
)

// errBadCue marks a malformed pack index: corruption never masquerades as
// absence, the same posture errBadMetadata takes for loose objects.
var errBadCue = fmt.Errorf("tigris: malformed pack cue index")

// cueRecord is one object's entry in a .cue index: its hash, its payload
// codec, and where its bytes live in the sibling .bin.
//
// stored and raw are deliberately separate. stored is the byte span to fetch —
// the only length a ranged GetObject may ever use — while raw is the git
// object's own size, which is what every caller outside this file means by
// "length". They are equal exactly when codec is codecRaw.
type cueRecord struct {
	hash   plumbing.Hash
	typ    plumbing.ObjectType
	codec  uint8
	offset int64 // byte offset of the stored bytes within the .bin
	stored int64 // stored byte span; == raw when codec is codecRaw
	raw    int64 // the git object's size, i.e. the decoded length
}

// cueRecWidth is one v2 record's contribution to the record block. The block
// is columnar rather than one struct per record, so this is a total rather
// than a stride.
func cueRecWidth(hashLen int) int { return hashLen + 26 }

// encodeCue serializes recs (assumed already sorted by hash) into the v2 .cue
// format: a 16-byte plaintext header, then a record block holding six columns
// — hashes, types, codecs, offsets, stored lengths, raw sizes.
//
// Columnar, not one record after another, because interleaving puts 20-32
// bytes of incompressible hash entropy every 26+ bytes and starves zstd's
// match finder. Split into columns, the noise is quarantined in the hash
// column while the other five compress nearly to nothing: types take about
// four distinct values, codecs two, and the three big-endian uint64 columns
// have all-zero high bytes.
func encodeCue(hashLen int, recs []cueRecord) []byte {
	n := len(recs)
	block := make([]byte, 0, n*cueRecWidth(hashLen))

	for _, r := range recs {
		block = append(block, r.hash.Bytes()...)
	}
	for _, r := range recs {
		block = append(block, byte(r.typ))
	}
	for _, r := range recs {
		block = append(block, r.codec)
	}
	for _, r := range recs {
		block = binary.BigEndian.AppendUint64(block, uint64(r.offset))
	}
	for _, r := range recs {
		block = binary.BigEndian.AppendUint64(block, uint64(r.stored))
	}
	for _, r := range recs {
		block = binary.BigEndian.AppendUint64(block, uint64(r.raw))
	}

	body, compressed := compressBlock(block)

	buf := make([]byte, cueHeaderLen, cueHeaderLen+len(body))
	copy(buf[0:3], cueMagicPrefix[:])
	buf[3] = cueVersion2
	buf[4] = byte(hashLen)
	if compressed {
		buf[cueRecCodecOff] = codecZstd
	}
	binary.BigEndian.PutUint64(buf[8:16], uint64(n))
	return append(buf, body...)
}

// parseCue is encodeCue's inverse, and also the reader for every v1 cue
// already sitting in a live bucket. hashLen is the caller's expected width
// (from this Storer's own object format) — a cue written under a different
// format is rejected rather than silently misparsed.
//
// Both versions yield the same []cueRecord, so nothing downstream of here
// knows or cares which version it came from.
func parseCue(hashLen int, raw []byte) ([]cueRecord, error) {
	if len(raw) < cueHeaderLen {
		return nil, fmt.Errorf("%w: header truncated (%d bytes)", errBadCue, len(raw))
	}
	if !bytes.Equal(raw[0:3], cueMagicPrefix[:]) {
		return nil, fmt.Errorf("%w: bad magic", errBadCue)
	}
	version := raw[3]
	if version != cueVersion1 && version != cueVersion2 {
		return nil, fmt.Errorf("%w: unsupported format version %d", errBadCue, version)
	}
	if got := int(raw[4]); got != hashLen {
		return nil, fmt.Errorf("%w: hash width %d, want %d", errBadCue, got, hashLen)
	}
	if raw[6] != 0 || raw[7] != 0 {
		return nil, fmt.Errorf("%w: reserved bytes not zero", errBadCue)
	}

	count := binary.BigEndian.Uint64(raw[8:cueHeaderLen])
	if count > uint64(len(raw)) {
		// Cheap guard before any allocation sized by count: even a
		// one-byte-per-record format could not fit this many.
		return nil, fmt.Errorf("%w: %d records cannot fit in %d bytes", errBadCue, count, len(raw))
	}

	if version == cueVersion1 {
		if raw[cueRecCodecOff] != 0 {
			return nil, fmt.Errorf("%w: reserved bytes not zero", errBadCue)
		}
		return parseCueV1(hashLen, raw, int(count))
	}
	return parseCueV2(hashLen, raw, int(count))
}

// parseCueV1 reads the original fixed-width layout: one hashLen+17-byte record
// per entry, holding hash, type, offset, and length. A v1 payload is raw by
// definition, so stored == raw and the codec is codecRaw.
func parseCueV1(hashLen int, raw []byte, count int) ([]cueRecord, error) {
	recWidth := hashLen + 17
	if want := cueHeaderLen + count*recWidth; want != len(raw) {
		return nil, fmt.Errorf("%w: length %d disagrees with %d records (want %d)", errBadCue, len(raw), count, want)
	}

	recs := make([]cueRecord, 0, count)
	off := cueHeaderLen
	for i := 0; i < count; i++ {
		h, ok := plumbing.FromBytes(raw[off : off+hashLen])
		if !ok {
			return nil, fmt.Errorf("%w: record %d has an unreadable hash", errBadCue, i)
		}
		size := int64(binary.BigEndian.Uint64(raw[off+hashLen+9 : off+hashLen+17]))
		recs = append(recs, cueRecord{
			hash:   h,
			typ:    plumbing.ObjectType(int8(raw[off+hashLen])),
			codec:  codecRaw,
			offset: int64(binary.BigEndian.Uint64(raw[off+hashLen+1 : off+hashLen+9])),
			stored: size,
			raw:    size,
		})
		off += recWidth
	}
	return recs, nil
}

// parseCueV2 reads the columnar block, decompressing it first when the header
// says it is zstd. The decompressed length taking the place of v1's
// length-versus-count check is what catches a truncated or lying index.
func parseCueV2(hashLen int, rawCue []byte, count int) ([]cueRecord, error) {
	block := rawCue[cueHeaderLen:]

	switch codec := rawCue[cueRecCodecOff]; codec {
	case codecRaw:
	case codecZstd:
		out, err := cueDecoder().DecodeAll(block, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: record block does not decompress: %w", errBadCue, err)
		}
		block = out
	default:
		return nil, fmt.Errorf("%w: unknown record block codec %d", errBadCue, codec)
	}

	if want := count * cueRecWidth(hashLen); want != len(block) {
		return nil, fmt.Errorf("%w: record block is %d bytes, but %d records need %d", errBadCue, len(block), count, want)
	}

	// Column bases, in the order encodeCue wrote them.
	hashes := block
	types := hashes[count*hashLen:]
	codecs := types[count:]
	offsets := codecs[count:]
	storeds := offsets[count*8:]
	raws := storeds[count*8:]

	recs := make([]cueRecord, 0, count)
	for i := 0; i < count; i++ {
		h, ok := plumbing.FromBytes(hashes[i*hashLen : (i+1)*hashLen])
		if !ok {
			return nil, fmt.Errorf("%w: record %d has an unreadable hash", errBadCue, i)
		}
		codec := codecs[i]
		if codec != codecRaw && codec != codecZstd {
			return nil, fmt.Errorf("%w: record %d has unknown payload codec %d", errBadCue, i, codec)
		}
		recs = append(recs, cueRecord{
			hash:   h,
			typ:    plumbing.ObjectType(int8(types[i])),
			codec:  codec,
			offset: int64(binary.BigEndian.Uint64(offsets[i*8:])),
			stored: int64(binary.BigEndian.Uint64(storeds[i*8:])),
			raw:    int64(binary.BigEndian.Uint64(raws[i*8:])),
		})
	}
	return recs, nil
}

// packEntry is what the in-memory index keeps per object: which pack it's
// in, where, and how its bytes are encoded there.
//
// raw keeps the meaning the old single length field had — the git object's
// size — so every caller outside this file is unaffected. stored is the byte
// span to fetch, and is the only length a ranged GetObject may use.
type packEntry struct {
	id     string
	typ    plumbing.ObjectType
	codec  uint8
	offset int64
	stored int64
	raw    int64
}

// indexRecords folds one pack's cue records into the entry map, returning the
// .bin's total length. Shared by register (a pack this instance just staged)
// and ensurePacksBuilt (a pack listed out of the bucket) so the two can never
// disagree about what an entry means. Callers hold p.mu.
func (p *packIndex) indexRecords(id string, recs []cueRecord) int64 {
	var size int64
	for _, r := range recs {
		p.entries[r.hash] = packEntry{
			id:     id,
			typ:    r.typ,
			codec:  r.codec,
			offset: r.offset,
			stored: r.stored,
			raw:    r.raw,
		}
		if end := r.offset + r.stored; end > size {
			size = end
		}
	}
	return size
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
	p.sizes[id] = p.indexRecords(id, recs)
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
		s.packs.sizes[id] = s.packs.indexRecords(id, recs)
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
		slog.Debug("pack read threshold crossed, bulk-fetching pack",
			"pack", id, "threshold", packBulkFetchThreshold, "prefix", s.prefix)
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

	slog.Debug("no pack cache installed, downloading pack to a private temp file", "pack", id)

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
		key := s.prefix + packPrefix + id + binSuffix
		slog.Debug("fetching whole pack", "pack", id, "bucket", s.bucket, "key", key)

		start := time.Now()
		out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
			Bucket: sp(s.bucket),
			Key:    sp(key),
		})
		s.observe("GetObject", start, err)
		if err != nil {
			slog.Debug("fetching whole pack failed", "pack", id, "key", key, "dur", time.Since(start), "err", err)
			return fmt.Errorf("tigris: download pack %s: %w", id, err)
		}
		defer out.Body.Close()

		n, err := io.Copy(w, out.Body)
		if err != nil {
			slog.Debug("fetching whole pack failed", "pack", id, "key", key, "bytes", n, "dur", time.Since(start), "err", err)
			return fmt.Errorf("tigris: download pack %s: %w", id, err)
		}
		slog.Debug("fetched whole pack", "pack", id, "key", key, "bytes", n, "dur", time.Since(start))
		return nil
	}
}

// packObject reads one object out of a pack, in increasing order of cost:
// this instance's own in-flight (not-yet-uploaded) local copy, an
// already-bulk-downloaded local copy, triggering a bulk download on the read
// that crosses packBulkFetchThreshold, or — the common case below that
// threshold — a single ranged GetObject straight into the object's byte
// range. Every tier fetches e.stored bytes and decodes through decodePacked,
// which is what makes a compressed payload invisible to callers.
func (s *Storer) packObject(t plumbing.ObjectType, h plumbing.Hash, e packEntry) (plumbing.EncodedObject, error) {
	if t != plumbing.AnyObject && e.typ != t {
		return nil, plumbing.ErrObjectNotFound
	}
	hs := objHead{typ: e.typ, size: e.raw}

	if e.stored == 0 {
		obj := plumbing.NewMemoryObject(s.oh)
		obj.SetType(e.typ)
		obj.SetSize(0)
		return obj, nil
	}

	if path, ok := s.packs.localPath(e.id); ok {
		f, err := os.Open(path)
		if err == nil {
			defer f.Close()
			return s.decodePacked(h, hs, e.codec, io.NewSectionReader(f, e.offset, e.stored))
		}
		// os.ErrNotExist: evicted underneath us (the upload just finished);
		// fall through to the tiers below — S3 now has it.
		slog.Debug("staged pack no longer on disk, reading from the bucket",
			"pack", e.id, "path", path, "err", err)
	}

	if f, ok := s.packs.bulkCopy(e.id); ok {
		return s.decodePacked(h, hs, e.codec, io.NewSectionReader(f, e.offset, e.stored))
	}

	if s.packs.recordAccess(e.id, h) {
		f, err := s.downloadPack(e.id)
		if err == nil {
			return s.decodePacked(h, hs, e.codec, io.NewSectionReader(f, e.offset, e.stored))
		}
		// Sticky failure: degrade to the ranged GET below rather than fail
		// the read outright.
		slog.Debug("bulk pack fetch unavailable, falling back to ranged reads", "pack", e.id, "err", err)
	}

	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + packPrefix + e.id + binSuffix),
		Range:  sp(fmt.Sprintf("bytes=%d-%d", e.offset, e.offset+e.stored-1)),
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

	return s.decodePacked(h, hs, e.codec, out.Body)
}
