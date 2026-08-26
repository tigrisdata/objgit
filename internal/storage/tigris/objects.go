package tigris

import (
	"errors"
	"fmt"
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

// headInfo fetches HEAD-derived facts for a loose object. Absence keeps the
// go-git contract sentinel; damaged metadata surfaces as wrapped
// errBadMetadata so corruption never masquerades as "doesn't exist".
func (s *Storer) headInfo(h plumbing.Hash) (objHead, error) {
	start := time.Now()
	out, err := s.client.HeadObject(s.ctx, &s3.HeadObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(keyOf(h)),
	})
	s.observe("HeadObject", start, err)

	switch {
	case err == nil:
	case isNotFound(err):
		return zeroObjHead, plumbing.ErrObjectNotFound
	default:
		return zeroObjHead, fmt.Errorf("tigris: head %s: %w", h.String(), err)
	}

	typ, terr := plumbing.ParseObjectType(out.Metadata[metaType])
	if terr != nil {
		return zeroObjHead, fmt.Errorf("%w: %s has bad %s %q: %w",
			errBadMetadata, h.String(), metaType, out.Metadata[metaType], terr)
	}

	size, ok := declaredSize(out)
	if !ok {
		return zeroObjHead, fmt.Errorf("%w: %s lacks any size source", errBadMetadata, h.String())
	}
	return objHead{typ: typ, size: size}, nil
}

// declaredSize prefers the written-at git-size declaration and falls back to
// ContentLength for legacy writes lacking metadata. Garbage digits fall back
// too rather than fail; absence of both sources is the hard case.
func declaredSize(out *s3.HeadObjectOutput) (int64, bool) {
	if raw, present := out.Metadata[metaSize]; present {
		if n, perr := strconv.ParseInt(raw, 10, 64); perr == nil {
			return n, true
		}
	}
	if out.ContentLength != nil {
		return *out.ContentLength, true
	}
	return 0, false
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
