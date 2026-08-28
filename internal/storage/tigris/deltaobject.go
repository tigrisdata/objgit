package tigris

import (
	"fmt"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// Storer answers delta reads, which is what lets go-git reuse the delta chain a
// client already computed instead of deriving a new one on every clone.
//
// Without this interface, packfile.deltaSelector.encodedDeltaObject falls
// through to the fully-inflated path, every object enters the packer as a
// non-delta, and the whole rolling-hash search in diffDelta runs again for each
// fetch. With it, objectsToPack marks these objects IsDelta, fixAndBreakChains
// wires their bases, and deltaSelector.walk skips them outright.
var _ storer.DeltaObjectStorer = (*Storer)(nil)

// deltaObject presents a stored delta the way go-git's packer expects one: the
// embedded object holds the delta instruction stream, while the object the
// delta *rebuilds* is described by the three extra methods.
//
// The embedded type is deliberately REFDeltaObject rather than the real one.
// fixAndBreakChainsOne bails on anything whose Object.Type() is not a delta
// type, so reporting "blob" here would quietly disable the reuse this type
// exists to enable. Our base reference is a hash, which is what REF means.
//
// One footgun worth naming: the embedded Hash() hashes the *payload* framed as
// a REF delta, so it is not this object's hash. Callers inside go-git never
// reach it — ObjectToPack.Hash falls through to ActualHash once Original has
// been cleaned — but nothing in the type system stops a new caller from
// getting it wrong, so use ActualHash.
type deltaObject struct {
	plumbing.EncodedObject
	hash plumbing.Hash // the object this delta rebuilds
	base plumbing.Hash // the object it rebuilds *from*
	size int64         // the rebuilt object's size, not the payload's
}

func (o *deltaObject) BaseHash() plumbing.Hash   { return o.base }
func (o *deltaObject) ActualHash() plumbing.Hash { return o.hash }
func (o *deltaObject) ActualSize() int64         { return o.size }

// DeltaObject is EncodedObject without resolving deltas: when the record for h
// names a base, the delta instruction stream is returned as-is, wrapped so the
// caller can find the base and the rebuilt object's identity.
//
// Everything else — an object stored whole, a loose object, one still in the
// pending overlay — delegates to EncodedObject, which is exactly what the
// interface asks for. go-git treats a non-DeltaObject result as "no delta
// available here" and falls back to deltifying it itself.
func (s *Storer) DeltaObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	e, ok, err := s.packLookup(h)
	if err != nil {
		return nil, err
	}
	if !ok || e.base == plumbing.ZeroHash {
		return s.EncodedObject(t, h)
	}
	if t != plumbing.AnyObject && e.typ != t {
		return nil, plumbing.ErrObjectNotFound
	}

	payload, err := s.packPayload(h, e)
	if err != nil {
		return nil, err
	}

	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(plumbing.REFDeltaObject)
	obj.SetSize(int64(len(payload)))
	if _, werr := obj.Write(payload); werr != nil {
		return nil, fmt.Errorf("%w: %s delta payload is unreadable: %w", errBadCue, h.String(), werr)
	}

	return &deltaObject{EncodedObject: obj, hash: h, base: e.base, size: e.raw}, nil
}
