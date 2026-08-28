package tigris

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-git/go-git/v6/plumbing"
)

// refCache is one Storer's memoized view of every ref in its repository. It
// mirrors packIndex: built once per instance, and not sticky on error.
//
// repofs calls Scoped once per request (internal/repofs/repofs.go:94), so one
// cache serves one request. That is the same lifetime ensurePacksBuilt relies
// on, and it is what turns a push's N existence checks into N map lookups.
//
// packed and loose stay separate rather than pre-merged. A write needs them
// apart: it rewrites packed, and it must know which loose keys it has to fold
// and then delete.
type refCache struct {
	mu    sync.Mutex
	built bool
	// etag is the compare-and-swap token for packedRefsKey. An empty string
	// means the object does not exist, and a writer must use If-None-Match
	// instead of If-Match.
	etag   string
	packed map[plumbing.ReferenceName]*plumbing.Reference
	loose  map[plumbing.ReferenceName]*plumbing.Reference
}

func newRefCache() *refCache { return &refCache{} }

// ensureRefsBuilt fills the cache once. Two calls: one GetObject for
// packed-refs, and one ListObjectsV2 for the legacy loose layer.
//
// The list runs every time. No flag records that a repository has been folded
// and that the list can be skipped, and that is deliberate: one cheap round
// trip is the price of a safety net that catches anything writing a loose ref
// outside this code, including an older binary during a rolling deploy.
func (s *Storer) ensureRefsBuilt() error {
	s.refs.mu.Lock()
	defer s.refs.mu.Unlock()
	if s.refs.built {
		return nil
	}

	body, etag, err := s.fetchSmallETag(s.prefix + packedRefsKey)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		body, etag = nil, ""
	default:
		return fmt.Errorf("tigris: load packed refs: %w", err)
	}

	packed := map[plumbing.ReferenceName]*plumbing.Reference{}
	if len(body) > 0 {
		// A corrupt object is an error and never an empty ref set. An empty set
		// makes a repository look brand new, and git would then accept a
		// force-push straight over the top of it.
		packed, err = decodePackedRefs(body)
		if err != nil {
			return err
		}
	}

	looseRefs, err := s.listLooseRefs()
	if err != nil {
		return err
	}
	loose := make(map[plumbing.ReferenceName]*plumbing.Reference, len(looseRefs))
	for _, r := range looseRefs {
		loose[r.Name()] = r
	}

	s.refs.etag = etag
	s.refs.packed = packed
	s.refs.loose = loose
	s.refs.built = true
	return nil
}

// refView returns the merged read view: packed, with the legacy loose layer on
// top. The caller must not mutate the result.
//
// A loose ref wins. That reads backwards, and it is only sound because of the
// invariant commitRefs holds: a write through the packed path deletes the loose
// keys for every name it touched before it reports success. So a loose key can
// exist only if something wrote it after the last packed write, which makes it
// newer.
func (s *Storer) refView() (map[plumbing.ReferenceName]*plumbing.Reference, error) {
	if err := s.ensureRefsBuilt(); err != nil {
		return nil, err
	}

	s.refs.mu.Lock()
	defer s.refs.mu.Unlock()

	out := make(map[plumbing.ReferenceName]*plumbing.Reference, len(s.refs.packed)+len(s.refs.loose))
	for n, r := range s.refs.packed {
		out[n] = r
	}
	for n, r := range s.refs.loose {
		out[n] = r
	}
	return out, nil
}

// invalidateRefs drops the cache so the next read rebuilds it. Called after a
// refused compare-and-swap, and after any write that took the loose path.
func (s *Storer) invalidateRefs() {
	s.refs.mu.Lock()
	defer s.refs.mu.Unlock()
	s.refs.built = false
	s.refs.etag = ""
	s.refs.packed = nil
	s.refs.loose = nil
}
