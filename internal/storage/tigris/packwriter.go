package tigris

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/klauspost/compress/zstd"
)

// PackfileWriter accepts an incoming git packfile — already exactly
// delimited by the caller (see cmd/objgitd/receivepack.go's writePack,
// which needs no changes: its own st.(storer.PackfileWriter) type assertion
// just starts succeeding once this method exists). Rather than storing git's
// own delta-compressed pack format, it decodes the pack (by handing it to a
// scratch go-git filesystem.Storage, which does all the actual pack/delta
// work — see docs/reference/tigris-backend.md) and re-encodes every object,
// fully resolved, into this package's own flat bin/cue container.
func (s *Storer) PackfileWriter() (io.WriteCloser, error) {
	dir, err := os.MkdirTemp("", "objgit-tigris-scratch-*")
	if err != nil {
		return nil, fmt.Errorf("tigris: create scratch dir: %w", err)
	}

	// A real temp dir, not memfs: incoming packs can be large.
	scratch := filesystem.NewStorageWithOptions(
		osfs.New(dir),
		cache.NewObjectLRUDefault(),
		filesystem.Options{ObjectFormat: s.of},
	)
	inner, err := scratch.PackfileWriter()
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("tigris: open scratch packfile writer: %w", err)
	}
	return &packWriter{s: s, dir: dir, scratch: scratch, inner: inner}, nil
}

type packWriter struct {
	s       *Storer
	dir     string
	scratch *filesystem.Storage
	inner   io.WriteCloser
	done    bool
}

func (w *packWriter) Write(p []byte) (int, error) { return w.inner.Write(p) }

func (w *packWriter) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	defer os.RemoveAll(w.dir)

	if err := w.inner.Close(); err != nil {
		return fmt.Errorf("tigris: decode incoming pack: %w", err)
	}

	objs, order, err := w.planObjects()
	if err != nil {
		return err
	}

	// One push becomes as many containers as its total size needs: the walk
	// seals a segment the moment it is full and opens the next one lazily, so a
	// push that divides evenly by the cap never leaves an empty trailing
	// container. Reads impose no matching limit — packIndex merges every
	// packs/*.cue it can see (see packindex.go).
	//
	// New sets the cap and Scoped copies it, so the fallback is unreachable
	// today; it stays because an unset cap would seal after every single
	// object, which is precisely the per-object PUT storm this writer exists to
	// prevent.
	byteLimit := w.s.maxPackBytes
	if byteLimit <= 0 {
		byteLimit = maxPackBytes
	}

	var seg *packSegment
	defer func() {
		if seg != nil { // only reachable on an error before this segment sealed
			seg.discard()
		}
	}()

	// inSeg is the containment rule made concrete: a delta may only be stored
	// as a delta when its base is in the container being built. It resets on
	// every seal, so a delta stranded on the far side of a split is demoted to
	// its whole form rather than pointing at a sibling container.
	//
	// No size hint: only the byte cap bounds a container now, so nothing here
	// knows how many objects one holds. clear keeps the buckets, so the map
	// grows once to the largest container's object count and stays there.
	inSeg := map[plumbing.Hash]struct{}{}

	walkErr := func() error {
		for _, idx := range order {
			p := objs[idx]
			so, err := w.payloadFor(p, inSeg)
			if err != nil {
				return err
			}

			// The cap seals *before* the add: a container sitting at 127 MiB
			// must not swallow a 500 MiB blob and land at 627 MiB. The
			// len(seg.recs) > 0 guard is what permits the one legal spill — an
			// object larger than the whole cap gets a container to itself,
			// since it has to live somewhere. That container then seals on the
			// next object's check here, or at the end of the walk below.
			//
			// p.size is trustworthy: add fails the push if the bytes it copies
			// disagree with the payload's own declared size.
			//
			// seg.offset counts *stored* bytes while p.size is the raw size, so
			// the comparison mixes the two. It stays correct because the codec
			// policy guarantees stored <= raw (compress.go, and the rewind in
			// writeProbed), which makes p.size a valid upper bound on what this
			// object can add — more so for a delta, whose payload is smaller
			// still. The only cost is sealing a little early.
			if seg != nil && len(seg.recs) > 0 && seg.offset+p.size > byteLimit {
				full := seg
				seg = nil // ownership moves to seal, which owns its staging files
				if err := w.seal(full); err != nil {
					return err
				}
				clear(inSeg)

				// The seal moved the goalposts: this object's base is no longer
				// in the container being built, so its delta form is no longer
				// legal. Ask again, now that inSeg is empty.
				if so, err = w.payloadFor(p, inSeg); err != nil {
					return err
				}
			}
			if seg == nil {
				var err error
				if seg, err = newPackSegment(w.s); err != nil {
					return err
				}
			}
			if err := seg.add(so); err != nil {
				return err
			}
			inSeg[p.hash] = struct{}{}
		}
		return nil
	}()
	if walkErr != nil {
		// Any segment this push already sealed is enqueued and cannot be
		// recalled. Each one is a complete, self-consistent container holding
		// real objects, so nothing dangles; the error fails the push, so no ref
		// ever points into the incomplete set.
		return fmt.Errorf("tigris: build pack container: %w", walkErr)
	}
	if seg == nil {
		return nil // defensive: writePack only calls us when a packfile is expected
	}
	last := seg
	seg = nil
	return w.seal(last)
}

// plannedObject is one object's metadata, gathered before any bytes are
// written. Type and size come from the *resolved* object, never from the delta
// header: git records a delta entry's size as the length of its instruction
// stream, not of the object it rebuilds, and every record's raw field means the
// latter.
type plannedObject struct {
	hash plumbing.Hash
	typ  plumbing.ObjectType
	size int64         // the reconstructed object's size
	base plumbing.Hash // zero when the client did not send this as a delta
}

// storedObject is one object as a segment will record it: the payload bytes to
// write, and the identity they rebuild. The two come apart for a delta, where
// the payload's hash, type, and size all describe the instruction stream rather
// than the object — which is exactly the mistake this type exists to prevent.
type storedObject struct {
	payload plumbing.EncodedObject
	hash    plumbing.Hash
	typ     plumbing.ObjectType
	raw     int64
	base    plumbing.Hash // zero when payload is the whole object
}

// planObjects walks the scratch storage once and returns every object
// (flat) alongside an order — indices into flat — where a delta always
// follows its base.
//
// The ordering is the reason this pass exists. IterEncodedObjects yields in
// index order, which is hash order, so a delta can arrive long before the
// object it is built from — and the containment rule cannot be checked against
// a container that has not been filled yet.
//
// order and placed hold indices/flags rather than copies of plannedObject or
// a second map keyed by plumbing.Hash: a 32-byte SHA256 hash makes a
// map[plumbing.Hash]* expensive per entry, and a push the size of a large
// repository's full history has enough objects that the difference is the
// gap between this fitting in memory and not. flat is already hash-sorted
// (the property above), so base lookups use binary search instead of a
// second map.
func (w *packWriter) planObjects() ([]plannedObject, []int32, error) {
	iter, err := w.scratch.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		return nil, nil, fmt.Errorf("tigris: walk scratch objects: %w", err)
	}
	defer iter.Close()

	var flat []plannedObject
	if err := iter.ForEach(func(obj plumbing.EncodedObject) error {
		p := plannedObject{hash: obj.Hash(), typ: obj.Type(), size: obj.Size()}
		if do, ok := w.deltaForm(p.hash); ok {
			p.base = do.BaseHash()
		}
		flat = append(flat, p)
		return nil
	}); err != nil {
		return nil, nil, fmt.Errorf("tigris: plan pack container: %w", err)
	}

	// Emit bases first. Marking before recursing is what makes a cycle
	// terminate: a chain that loops back on itself simply places the object it
	// looped to, and the delta behind it is demoted when its base turns out not
	// to be in the container yet.
	order := make([]int32, 0, len(flat))
	placed := make([]bool, len(flat))
	var emit func(i int32)
	emit = func(i int32) {
		if placed[i] {
			return
		}
		placed[i] = true
		if base := flat[i].base; base != plumbing.ZeroHash {
			if j, ok := findByHash(flat, base); ok {
				emit(j)
			}
		}
		order = append(order, i)
	}
	for i := range flat {
		emit(int32(i))
	}
	return flat, order, nil
}

// findByHash locates h in flat by binary search. Callers hold flat in the
// hash order IterEncodedObjects produced, which is the order this depends on.
func findByHash(flat []plannedObject, h plumbing.Hash) (int32, bool) {
	i := sort.Search(len(flat), func(i int) bool {
		return bytes.Compare(flat[i].hash.Bytes(), h.Bytes()) >= 0
	})
	if i < len(flat) && flat[i].hash == h {
		return int32(i), true
	}
	return -1, false
}

// deltaForm asks the scratch storage for h's delta, if the client sent one.
// The scratch storage is a real go-git filesystem.Storage over the pushed
// packfile, so this is the client's own delta chain, not one we computed.
func (w *packWriter) deltaForm(h plumbing.Hash) (plumbing.DeltaObject, bool) {
	obj, err := w.scratch.DeltaObject(plumbing.AnyObject, h)
	if err != nil {
		// Not fatal: a base we cannot read as a delta is simply stored whole.
		return nil, false
	}
	do, ok := obj.(plumbing.DeltaObject)
	if !ok || do.BaseHash() == plumbing.ZeroHash {
		return nil, false
	}
	return do, true
}

// payloadFor fetches the bytes to store for p: the client's delta when its base
// is already in the container being built, and the whole object otherwise.
func (w *packWriter) payloadFor(p plannedObject, inSeg map[plumbing.Hash]struct{}) (storedObject, error) {
	whole := func() (storedObject, error) {
		obj, err := w.scratch.EncodedObject(plumbing.AnyObject, p.hash)
		if err != nil {
			return storedObject{}, fmt.Errorf("tigris: read %s from scratch: %w", p.hash, err)
		}
		return storedObject{payload: obj, hash: p.hash, typ: p.typ, raw: p.size}, nil
	}

	if p.base == plumbing.ZeroHash {
		return whole()
	}
	if _, ok := inSeg[p.base]; !ok {
		return whole()
	}
	do, ok := w.deltaForm(p.hash)
	if !ok || do.BaseHash() != p.base {
		return whole()
	}
	// A delta that does not actually save anything is worse than the object:
	// it costs a base read on every future fetch for no bytes back.
	if do.Size() >= p.size {
		return whole()
	}
	return storedObject{payload: do, hash: p.hash, typ: p.typ, raw: p.size, base: p.base}, nil
}

// packSegment is one container under construction: the staging .bin and the
// cue records for the objects written into it so far.
//
// The pack's sha256 id is computed in seal, by one pass over the finished
// file, rather than as a running hash while objects are added. A running hash
// cannot survive add's rewind (see writePayload), and the extra pass reads a
// page-cached file off the network path.
type packSegment struct {
	s    *Storer
	file *os.File
	// path is an os.CreateTemp name, deliberately not derived from content:
	// two unrelated pushes can easily produce a byte-identical pack (a lone
	// empty-tree commit, say), and a content-addressed path would let one
	// push's cleanup race a sibling push's still-in-flight upload — see
	// looseJob's doc comment in upload.go for the same reasoning applied to
	// individual objects.
	path   string
	recs   []cueRecord
	offset int64
}

func newPackSegment(s *Storer) (*packSegment, error) {
	f, err := os.CreateTemp("", "objgit-tigris-bin-*")
	if err != nil {
		return nil, fmt.Errorf("create pack staging file: %w", err)
	}
	return &packSegment{s: s, file: f, path: f.Name()}, nil
}

// add appends one object to the segment's .bin — compressed or not, per
// writePayload — and records where it landed and how it is encoded.
func (g *packSegment) add(so storedObject) error {
	codec, rawN, storedN, err := g.writePayload(so.payload)
	if err != nil {
		return err
	}

	// The integrity check stays on the *raw* count, never the stored one, and
	// on the payload's own size rather than so.raw. For a whole object those
	// are the same number; for a delta they are not, and checking against the
	// rebuilt size would reject every delta ever written.
	if rawN != so.payload.Size() {
		return fmt.Errorf("%s: copied %d bytes, payload reports size %d", so.hash, rawN, so.payload.Size())
	}

	// hash and typ come from so, never from so.payload. A delta payload hashes
	// as a REF delta over its instruction stream and reports a delta type, so
	// deriving either from it would index the container under a hash no client
	// will ever ask for.
	g.recs = append(g.recs, cueRecord{
		hash:   so.hash,
		typ:    so.typ,
		codec:  codec,
		offset: g.offset,
		stored: storedN,
		raw:    so.raw,
		base:   so.base,
	})
	g.offset += storedN

	if g.s.payloadObserver != nil {
		g.s.payloadObserver(codecName(codec), rawN, storedN)
	}
	return nil
}

// writePayload writes one object's bytes into the .bin and reports how they
// were encoded, how many raw bytes the object held, and how many bytes landed.
//
// Three bands, by object size — see compress.go for the measurements behind
// the thresholds:
//
//   - below compressionFloor: raw, with no probe, no buffering, and no
//     allocation. Most objects in a repository take this path.
//   - up to inMemoryCap: decided exactly, by compressing and keeping whichever
//     form is smaller. Never mispredicts.
//   - above inMemoryCap: decided by compressing probeWindow bytes, because
//     compressing 500 MiB of video to learn it is incompressible is a waste.
//     This is the only band that can guess wrong, and the only one that can
//     reach the rewind below.
func (g *packSegment) writePayload(obj plumbing.EncodedObject) (codec uint8, rawN, storedN int64, err error) {
	rd, err := obj.Reader()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open reader for %s: %w", obj.Hash(), err)
	}
	defer rd.Close()

	size := obj.Size()
	if !g.s.packCompression || size < compressionFloor {
		n, err := g.copyRaw(obj, rd)
		return codecRaw, n, n, err
	}

	if size <= g.inMemoryCap() {
		plain, err := io.ReadAll(rd)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("read %s: %w", obj.Hash(), err)
		}
		body, compressed := compressBlock(plain)
		n, err := g.file.Write(body)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("write %s: %w", obj.Hash(), err)
		}
		codec := codecRaw
		if compressed {
			codec = codecZstd
		}
		return codec, int64(len(plain)), int64(n), nil
	}

	return g.writeProbed(obj, rd)
}

// writeProbed handles the largest band: compress a head window to decide, then
// verify the decision against the whole object once its real stored size is
// known, rewinding to raw if compression did not earn its keep.
func (g *packSegment) writeProbed(obj plumbing.EncodedObject, rd io.Reader) (uint8, int64, int64, error) {
	head := make([]byte, g.probeWindow())
	headN, err := io.ReadFull(rd, head)
	switch {
	case err == nil, errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
	default:
		return 0, 0, 0, fmt.Errorf("probe %s: %w", obj.Hash(), err)
	}
	head = head[:headN]

	if !probeWins(len(encoder().EncodeAll(head, nil)), len(head)) {
		n, err := g.copyRawWithHead(obj, head, rd)
		return codecRaw, n, n, err
	}

	rawN, storedN, err := g.copyCompressed(obj, head, rd)
	if err != nil {
		return 0, 0, 0, err
	}
	if worthStoring(int(storedN), int(rawN)) {
		return codecZstd, rawN, storedN, nil
	}

	// The head lied: this object's tail gave the saving back. Truncate what
	// was just written and store it raw instead, so the stored form is never
	// larger than the object and the container byte cap stays sound.
	slog.Debug("pack payload probe mispredicted, rewinding to raw",
		"object", obj.Hash().String(), "raw", rawN, "compressed", storedN)
	if err := g.rewind(); err != nil {
		return 0, 0, 0, err
	}
	n, err := g.copyRaw(obj, nil)
	return codecRaw, n, n, err
}

// copyRaw streams the object's bytes in verbatim. A nil rd reopens the object,
// which is what the rewind path needs after having consumed the first reader.
func (g *packSegment) copyRaw(obj plumbing.EncodedObject, rd io.Reader) (int64, error) {
	if rd == nil {
		fresh, err := obj.Reader()
		if err != nil {
			return 0, fmt.Errorf("reopen reader for %s: %w", obj.Hash(), err)
		}
		defer fresh.Close()
		rd = fresh
	}
	n, err := io.Copy(g.file, rd)
	if err != nil {
		return n, fmt.Errorf("copy %s: %w", obj.Hash(), err)
	}
	return n, nil
}

// copyRawWithHead is copyRaw for the probe path, where the head has already
// been read out of rd and must be written back ahead of the remainder.
func (g *packSegment) copyRawWithHead(obj plumbing.EncodedObject, head []byte, rd io.Reader) (int64, error) {
	return g.copyRaw(obj, io.MultiReader(bytes.NewReader(head), rd))
}

// copyCompressed streams head followed by the rest of rd through a zstd
// encoder into the .bin, returning the raw and stored byte counts. The
// encoder comes from streamEncPool (compress.go) rather than the shared
// encoder() singleton: the singleton serves concurrent EncodeAll calls and
// must never be Reset underneath them, while a pooled streaming encoder is
// owned for the duration of one object and returned after. That reuse is
// what keeps this path from allocating a fresh match-history buffer per
// object — objects reaching this path are larger than inMemoryCap, but a
// repository's history can still hold enough of them that the allocation
// adds up.
func (g *packSegment) copyCompressed(obj plumbing.EncodedObject, head []byte, rd io.Reader) (int64, int64, error) {
	before, err := g.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("locate %s in pack staging file: %w", obj.Hash(), err)
	}

	zw := streamEncPool.Get().(*zstd.Encoder)
	zw.Reset(g.file)
	rawN, copyErr := io.Copy(zw, io.MultiReader(bytes.NewReader(head), rd))
	closeErr := zw.Close()
	streamEncPool.Put(zw)
	switch {
	case copyErr != nil:
		return 0, 0, fmt.Errorf("compress %s: %w", obj.Hash(), copyErr)
	case closeErr != nil:
		return 0, 0, fmt.Errorf("finish compressing %s: %w", obj.Hash(), closeErr)
	}

	after, err := g.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("measure %s in pack staging file: %w", obj.Hash(), err)
	}
	return rawN, after - before, nil
}

// rewind discards whatever the current object wrote, returning the staging
// file to the end of the last successfully added object.
func (g *packSegment) rewind() error {
	if err := g.file.Truncate(g.offset); err != nil {
		return fmt.Errorf("truncate pack staging file: %w", err)
	}
	if _, err := g.file.Seek(g.offset, io.SeekStart); err != nil {
		return fmt.Errorf("rewind pack staging file: %w", err)
	}
	return nil
}

// checksum names the finished container after its own contents. Called once
// per segment, from seal, after the file is closed.
func (g *packSegment) checksum() (string, error) {
	f, err := os.Open(g.path)
	if err != nil {
		return "", fmt.Errorf("reopen pack staging file: %w", err)
	}
	defer f.Close()

	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		return "", fmt.Errorf("checksum pack staging file: %w", err)
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

// The two policy knobs New sets and tests lower; see withInMemoryCap and
// withProbeWindow for why they are overridable at all.
func (g *packSegment) inMemoryCap() int64 {
	if g.s.inMemoryCap > 0 {
		return g.s.inMemoryCap
	}
	return inMemoryCap
}

func (g *packSegment) probeWindow() int {
	if g.s.probeWindow > 0 {
		return g.s.probeWindow
	}
	return probeWindow
}

// discard throws away a segment that never reached seal.
func (g *packSegment) discard() {
	g.file.Close()
	os.Remove(g.path)
}

// seal finishes one segment: it names the pack after its own checksum, stages
// the sibling .cue, and queues both for upload. It takes ownership of the
// segment's staging files, so every error path here removes them itself.
func (w *packWriter) seal(seg *packSegment) error {
	if err := seg.file.Close(); err != nil {
		os.Remove(seg.path)
		return fmt.Errorf("tigris: close pack staging file: %w", err)
	}

	sort.Slice(seg.recs, func(i, j int) bool {
		return bytes.Compare(seg.recs[i].hash.Bytes(), seg.recs[j].hash.Bytes()) < 0
	})
	cueBytes := encodeCue(w.s.oh.Size(), seg.recs)

	id, err := seg.checksum()
	if err != nil {
		os.Remove(seg.path)
		return fmt.Errorf("tigris: %w", err)
	}

	cueFile, err := os.CreateTemp("", "objgit-tigris-cue-*")
	if err != nil {
		os.Remove(seg.path)
		return fmt.Errorf("tigris: create pack cue staging file: %w", err)
	}
	cuePath := cueFile.Name()
	if _, err := cueFile.Write(cueBytes); err != nil {
		cueFile.Close()
		os.Remove(seg.path)
		os.Remove(cuePath)
		return fmt.Errorf("tigris: stage pack cue: %w", err)
	}
	if err := cueFile.Close(); err != nil {
		os.Remove(seg.path)
		os.Remove(cuePath)
		return fmt.Errorf("tigris: close pack cue staging file: %w", err)
	}

	// Register before enqueueing (upload.go's ordering rule): a read must
	// never race ahead of the write it depends on.
	w.s.packs.register(id, seg.recs, seg.path)
	job := &packJob{
		id:      id,
		recs:    seg.recs,
		binPath: seg.path,
		cuePath: cuePath,
		binSize: seg.offset,
		cueSize: int64(len(cueBytes)),
	}
	if err := w.s.up.enqueue(w.s.ctx, job); err != nil {
		w.s.packs.deregister(id, seg.recs)
		os.Remove(seg.path)
		os.Remove(cuePath)
		return err
	}
	return nil
}

// packJob uploads one push's pack container (.bin then .cue — a .cue must
// never be visible in S3 without its .bin, since cold index builders trust
// every .cue they list) through the same uploader/bundler the loose-object
// path uses (see upload.go), so one SetReference-triggered flush() covers
// both kinds of upload and surfaces either kind of failure.
type packJob struct {
	id      string
	recs    []cueRecord
	binPath string
	cuePath string
	binSize int64
	cueSize int64
}

func (j *packJob) bytes() int64 { return j.binSize + j.cueSize }

func (j *packJob) run(ctx context.Context, s *Storer) error {
	if err := j.putFile(ctx, s, j.binPath, s.prefix+packPrefix+j.id+binSuffix); err != nil {
		return fmt.Errorf("tigris: upload pack %s bin: %w", j.id, err)
	}
	if err := j.putFile(ctx, s, j.cuePath, s.prefix+packPrefix+j.id+cueSuffix); err != nil {
		return fmt.Errorf("tigris: upload pack %s cue: %w", j.id, err)
	}
	return nil
}

func (j *packJob) putFile(ctx context.Context, s *Storer, localPath, key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	start := time.Now()
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
		Body:   f,
	})
	s.observe("PutObject", start, err)
	return err
}

func (j *packJob) done(s *Storer, err error) {
	if err != nil {
		s.packs.deregister(j.id, j.recs)
	} else {
		s.packs.markUploaded(j.id)
	}
	os.Remove(j.binPath)
	os.Remove(j.cuePath)
}
