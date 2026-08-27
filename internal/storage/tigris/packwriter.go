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

	iter, err := w.scratch.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		return fmt.Errorf("tigris: walk scratch objects: %w", err)
	}
	defer iter.Close()

	// One push becomes as many containers as its object count and its total
	// size need: the walk seals a segment the moment it is full and opens the
	// next one lazily, so a push that divides evenly by a cap never leaves an
	// empty trailing container. Reads impose no matching limit — packIndex
	// merges every packs/*.cue it can see (see packindex.go).
	//
	// New sets both caps and Scoped copies them, so the fallbacks are
	// unreachable today; they stay because an unset cap would seal after every
	// single object, which is precisely the per-object PUT storm this writer
	// exists to prevent.
	limit := w.s.maxPack
	if limit <= 0 {
		limit = maxPackObjects
	}
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

	walkErr := iter.ForEach(func(obj plumbing.EncodedObject) error {
		// The byte cap seals *before* the add, unlike the object cap below: a
		// container sitting at 127 MiB must not swallow a 500 MiB blob and
		// land at 627 MiB. The len(seg.recs) > 0 guard is what permits the one
		// legal spill — an object larger than the whole cap gets a container to
		// itself, since it has to live somewhere. That container then seals on
		// the next object's check here, or at the end of the walk below.
		//
		// obj.Size() is trustworthy: add fails the push if the bytes it copies
		// disagree with it.
		//
		// seg.offset counts *stored* bytes while obj.Size() is the raw size, so
		// the comparison mixes the two. It stays correct because the codec
		// policy guarantees stored <= raw (compress.go, and the rewind in
		// writeProbed), which makes obj.Size() a valid upper bound on what this
		// object can add. The only cost is sealing a little early.
		if seg != nil && len(seg.recs) > 0 && seg.offset+obj.Size() > byteLimit {
			full := seg
			seg = nil // ownership moves to seal, which owns its staging files
			if err := w.seal(full); err != nil {
				return err
			}
		}
		if seg == nil {
			var err error
			if seg, err = newPackSegment(w.s); err != nil {
				return err
			}
		}
		if err := seg.add(obj); err != nil {
			return err
		}
		if len(seg.recs) < limit {
			return nil
		}
		full := seg
		seg = nil
		return w.seal(full)
	})
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
func (g *packSegment) add(obj plumbing.EncodedObject) error {
	codec, rawN, storedN, err := g.writePayload(obj)
	if err != nil {
		return err
	}

	// The integrity check stays on the *raw* count, never the stored one.
	// obj.Size() being trustworthy is what lets the container byte cap treat
	// it as an upper bound, and comparing a compressed length against a
	// declared object size would quietly destroy that.
	if rawN != obj.Size() {
		return fmt.Errorf("%s: copied %d bytes, object reports size %d", obj.Hash(), rawN, obj.Size())
	}

	g.recs = append(g.recs, cueRecord{
		hash:   obj.Hash(),
		typ:    obj.Type(),
		codec:  codec,
		offset: g.offset,
		stored: storedN,
		raw:    rawN,
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
// encoder into the .bin, returning the raw and stored byte counts. A dedicated
// streaming encoder rather than the shared one: the shared encoder serves
// concurrent EncodeAll calls and must never be Reset underneath them. Objects
// reaching this path are larger than inMemoryCap, so they are rare enough to
// afford the allocation.
func (g *packSegment) copyCompressed(obj plumbing.EncodedObject, head []byte, rd io.Reader) (int64, int64, error) {
	before, err := g.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, fmt.Errorf("locate %s in pack staging file: %w", obj.Hash(), err)
	}

	zw, err := zstd.NewWriter(g.file, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return 0, 0, fmt.Errorf("open encoder for %s: %w", obj.Hash(), err)
	}
	rawN, copyErr := io.Copy(zw, io.MultiReader(bytes.NewReader(head), rd))
	closeErr := zw.Close()
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
