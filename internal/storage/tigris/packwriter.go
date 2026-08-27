package tigris

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
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

	// One push becomes as many containers as its object count needs: the walk
	// seals a segment the moment it is full and opens the next one lazily, so
	// a count that divides evenly by the cap never leaves an empty trailing
	// container. Reads impose no matching limit — packIndex merges every
	// packs/*.cue it can see (see packindex.go).
	//
	// New sets maxPack and Scoped copies it, so the fallback is unreachable
	// today; it stays because an unset cap would seal after every single
	// object, which is precisely the per-object PUT storm this writer exists
	// to prevent.
	limit := w.s.maxPack
	if limit <= 0 {
		limit = maxPackObjects
	}

	var seg *packSegment
	defer func() {
		if seg != nil { // only reachable on an error before this segment sealed
			seg.discard()
		}
	}()

	walkErr := iter.ForEach(func(obj plumbing.EncodedObject) error {
		if seg == nil {
			var err error
			if seg, err = newPackSegment(); err != nil {
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
		seg = nil // ownership moves to seal, which owns its staging files
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

// packSegment is one container under construction: the staging .bin, the
// running sha256 that becomes the finished pack's id, and the cue records for
// the objects written into it so far.
type packSegment struct {
	file *os.File
	// path is an os.CreateTemp name, deliberately not derived from content:
	// two unrelated pushes can easily produce a byte-identical pack (a lone
	// empty-tree commit, say), and a content-addressed path would let one
	// push's cleanup race a sibling push's still-in-flight upload — see
	// looseJob's doc comment in upload.go for the same reasoning applied to
	// individual objects.
	path   string
	sum    hash.Hash
	recs   []cueRecord
	offset int64
}

func newPackSegment() (*packSegment, error) {
	f, err := os.CreateTemp("", "objgit-tigris-bin-*")
	if err != nil {
		return nil, fmt.Errorf("create pack staging file: %w", err)
	}
	return &packSegment{file: f, path: f.Name(), sum: sha256.New()}, nil
}

// add appends one object's raw bytes to the segment's .bin and records where
// they landed.
func (g *packSegment) add(obj plumbing.EncodedObject) error {
	rd, err := obj.Reader()
	if err != nil {
		return fmt.Errorf("open reader for %s: %w", obj.Hash(), err)
	}
	defer rd.Close()

	n, err := io.Copy(io.MultiWriter(g.file, g.sum), rd)
	if err != nil {
		return fmt.Errorf("copy %s: %w", obj.Hash(), err)
	}
	if n != obj.Size() {
		return fmt.Errorf("%s: copied %d bytes, object reports size %d", obj.Hash(), n, obj.Size())
	}
	g.recs = append(g.recs, cueRecord{hash: obj.Hash(), typ: obj.Type(), offset: g.offset, length: n})
	g.offset += n
	return nil
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
	id := hex.EncodeToString(seg.sum.Sum(nil))

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
