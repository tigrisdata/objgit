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

// objHead is everything a HEAD reveals about one git object.
type objHead struct {
	typ  plumbing.ObjectType
	size int64
}

var zeroObjHead objHead

// headInfo fetches HEAD-derived facts for a loose object. A write this Storer
// queued but hasn't finished uploading yet answers from the pending overlay
// (upload.go) instead of S3 — the object may not exist there yet. Absence
// keeps the go-git contract sentinel; damaged metadata surfaces as wrapped
// errBadMetadata so corruption never masquerades as "doesn't exist". Reserved
// for callers that only need existence/size, not the body — EncodedObject
// reads metadata off the GetObject response instead so fetching an object
// costs one round trip, not two.
func (s *Storer) headInfo(h plumbing.Hash) (objHead, error) {
	if p, ok := s.up.lookupPending(h); ok {
		return parsePendingHead(h, p)
	}
	if e, ok, err := s.packLookup(h); err != nil {
		return zeroObjHead, err
	} else if ok {
		return objHead{typ: e.typ, size: e.raw}, nil
	}

	start := time.Now()
	out, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + keyOf(h)),
	})
	s.observe("HeadObject", start, err)

	switch {
	case err == nil:
	case isNotFound(err):
		return zeroObjHead, plumbing.ErrObjectNotFound
	default:
		return zeroObjHead, fmt.Errorf("tigris: head %s: %w", h.String(), err)
	}

	return parseObjHead(h, out.Metadata, out.ContentLength)
}

// parsePendingHead mirrors parseObjHead for a pending-overlay entry, whose
// type/size were captured straight from the writer rather than S3 metadata.
func parsePendingHead(h plumbing.Hash, p pendingMeta) (objHead, error) {
	typ, terr := plumbing.ParseObjectType(p.typ)
	if terr != nil {
		return zeroObjHead, fmt.Errorf("%w: %s has bad pending type %q: %w",
			errBadMetadata, h.String(), p.typ, terr)
	}
	return objHead{typ: typ, size: p.size}, nil
}

// parseObjHead derives an objHead from the git-type/git-size user metadata
// carried by both HeadObjectOutput and GetObjectOutput, falling back to
// ContentLength for legacy writes lacking metadata. Garbage digits fall back
// too rather than fail; absence of both sources is the hard case.
func parseObjHead(h plumbing.Hash, meta map[string]string, contentLength *int64) (objHead, error) {
	typ, terr := plumbing.ParseObjectType(meta[metaType])
	if terr != nil {
		return zeroObjHead, fmt.Errorf("%w: %s has bad %s %q: %w",
			errBadMetadata, h.String(), metaType, meta[metaType], terr)
	}

	if raw, present := meta[metaSize]; present {
		if n, perr := strconv.ParseInt(raw, 10, 64); perr == nil {
			return objHead{typ: typ, size: n}, nil
		}
	}
	if contentLength != nil {
		return objHead{typ: typ, size: *contentLength}, nil
	}
	return zeroObjHead, fmt.Errorf("%w: %s lacks any size source", errBadMetadata, h.String())
}

func (s *Storer) HasEncodedObject(h plumbing.Hash) error {
	switch _, err := s.headInfo(h); {
	case err == nil:
		return nil
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return plumbing.ErrObjectNotFound
	default:
		return fmt.Errorf("tigris: probe %s: %w", h.String(), err)
	}
}

func (s *Storer) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	hs, err := s.headInfo(h)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return 0, plumbing.ErrObjectNotFound
	default:
		return 0, fmt.Errorf("tigris: size of %s: %w", h.String(), err)
	}
	return hs.size, nil
}

// EncodedObject fetches a loose object in one round trip: type and size ride
// along in the GetObject response's user metadata, so there is no separate
// HEAD probe before the read. A write this Storer queued but hasn't finished
// uploading yet is served from the local pending overlay instead — the
// packfile.UpdateObjectStorage fallback (writePack's non-PackfileWriter path)
// resolves ofs-/ref-delta bases by reading back objects the same push just
// wrote, before any flush ever runs.
func (s *Storer) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	if p, ok := s.up.lookupPending(h); ok {
		obj, err := s.decodePending(t, h, p)
		if err == nil || !errors.Is(err, os.ErrNotExist) {
			return obj, err
		}
		// Evicted between lookupPending and opening the cache file: the
		// upload finished (or the object is gone) right underneath us. Either
		// way S3 is now the authority — fall through to the normal read.
	}
	if e, ok, err := s.packLookup(h); err != nil {
		return nil, err
	} else if ok {
		return s.packObject(t, h, e)
	}

	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + keyOf(h)),
	})
	s.observe("GetObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, plumbing.ErrObjectNotFound
	default:
		return nil, fmt.Errorf("tigris: get %s: %w", h.String(), err)
	}
	defer out.Body.Close()

	hs, err := parseObjHead(h, out.Metadata, out.ContentLength)
	if err != nil {
		return nil, err
	}
	if hs.typ == plumbing.OFSDeltaObject || hs.typ == plumbing.REFDeltaObject {
		return nil, plumbing.ErrInvalidType
	}
	if t != plumbing.AnyObject && hs.typ != t {
		return nil, plumbing.ErrObjectNotFound
	}

	return s.decodeBody(h, hs, out.Body)
}

// loadObject fetches a loose object's body given a type/size already known
// from a prior HEAD (the iterator's case: filter by type before paying for a
// body download). EncodedObject does not go through here — it has no reason
// to HEAD before it GETs, since the GetObject response already carries the
// same metadata.
func (s *Storer) loadObject(h plumbing.Hash, hs objHead) (plumbing.EncodedObject, error) {
	if hs.typ == plumbing.OFSDeltaObject || hs.typ == plumbing.REFDeltaObject {
		return nil, plumbing.ErrInvalidType
	}

	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + keyOf(h)),
	})
	s.observe("GetObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, plumbing.ErrObjectNotFound
	default:
		return nil, fmt.Errorf("tigris: get %s: %w", h.String(), err)
	}
	defer out.Body.Close()

	return s.decodeBody(h, hs, out.Body)
}

// decodePending reads a pending-overlay object's cached bytes off disk,
// applying the same type-filter contract as the S3 read path. Returns an
// os.ErrNotExist-wrapping error if the cache file is gone (the caller falls
// back to S3 in that case), distinct from every other error.
func (s *Storer) decodePending(t plumbing.ObjectType, h plumbing.Hash, p pendingMeta) (plumbing.EncodedObject, error) {
	hs, err := parsePendingHead(h, p)
	if err != nil {
		return nil, err
	}
	if hs.typ == plumbing.OFSDeltaObject || hs.typ == plumbing.REFDeltaObject {
		return nil, plumbing.ErrInvalidType
	}
	if t != plumbing.AnyObject && hs.typ != t {
		return nil, plumbing.ErrObjectNotFound
	}

	f, err := os.Open(p.path)
	if err != nil {
		return nil, err // os.ErrNotExist-wrapping *PathError; caller checks
	}
	defer f.Close()

	return s.decodeBody(h, hs, f)
}

// decodePackedPayload reads one record's *stored* bytes and hands back the
// real bytes to decodeBody, decompressing first when the cue says the payload
// is zstd. Every tier of packObject funnels through here, so a compressed
// container is invisible above this line.
//
// DecodeAll rather than a streaming reader on purpose: decodeBody buffers the
// whole body into a MemoryObject regardless, and one shared decoder doing
// DecodeAll beats allocating a zstd.Decoder's window buffers per read.
func (s *Storer) decodePackedPayload(h plumbing.Hash, e packEntry, body io.Reader) ([]byte, error) {
	stored, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("tigris: read %s: %w", h.String(), err)
	}
	if e.codec == codecRaw {
		return stored, nil
	}

	// The capacity hint is clamped: e.raw comes from the .cue, so a corrupt
	// record claiming a huge size must not turn into a huge allocation here.
	// DecodeAll grows the slice when the real payload needs it. For a delta
	// record e.raw overshoots — the instruction stream is smaller than what it
	// rebuilds — which costs a little slack and never correctness.
	plain, err := payloadDecoder().DecodeAll(stored, make([]byte, 0, min(e.raw, decodeHintCap)))
	if err != nil {
		return nil, fmt.Errorf("%w: %s payload does not decompress: %w", errBadMetadata, h.String(), err)
	}
	return plain, nil
}

// decodeBody reads a GetObject body into a MemoryObject framed by an already
// known objHead.
func (s *Storer) decodeBody(h plumbing.Hash, hs objHead, body io.Reader) (plumbing.EncodedObject, error) {
	buf, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("tigris: read %s: %w", h.String(), err)
	}

	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(hs.typ)
	obj.SetSize(hs.size)
	if _, err := obj.Write(buf); err != nil {
		return nil, fmt.Errorf("%w: %s body disagrees with declared size %d: %w",
			errBadMetadata, h.String(), hs.size, err)
	}
	return obj, nil
}
