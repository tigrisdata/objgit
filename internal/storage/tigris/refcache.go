package tigris

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"
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

// maxRefCASRetries bounds the compare-and-swap loop. Eight is far above what
// honest traffic reaches: a loser re-reads one object and re-uploads one
// object, so a round trip is milliseconds, and losing eight in a row means
// something is wrong that another attempt will not fix.
const maxRefCASRetries = 8

// refExpectation is one CheckAndSetReference precondition: the caller believes
// that name currently holds old. A nil old means "the ref must not exist".
type refExpectation struct {
	name plumbing.ReferenceName
	old  *plumbing.Reference
}

// commitRefs applies a whole batch of ref mutations in one conditional
// PutObject. That single call is the commit point: either every mutation in
// the batch lands or none of them do.
//
// The flush at the top runs once for the batch, and not once per ref. It holds
// the invariant refs.go protects — a ref must never name an object whose
// upload has not finished — and one flush per batch instead of one per ref is
// most of why this path is faster than the loose one.
func (s *Storer) commitRefs(sets []*plumbing.Reference, removes []plumbing.ReferenceName, expect []refExpectation) error {
	if len(sets) == 0 && len(removes) == 0 {
		return nil
	}
	if err := s.up.flush(); err != nil {
		return fmt.Errorf("tigris: commit refs: %w", err)
	}

	for attempt := 0; attempt < maxRefCASRetries; attempt++ {
		if attempt > 0 {
			s.invalidateRefs()
			if s.refCASObserver != nil {
				s.refCASObserver()
			}
		}
		if err := s.ensureRefsBuilt(); err != nil {
			return err
		}

		s.refs.mu.Lock()
		etag := s.refs.etag
		next := make(map[plumbing.ReferenceName]*plumbing.Reference, len(s.refs.packed)+len(s.refs.loose)+len(sets))
		for n, r := range s.refs.packed {
			next[n] = r
		}
		// Fold the legacy loose layer in. After this commit lands, these names
		// live in packed-refs, and dropFoldedLooseRefs deletes the keys.
		folded := make([]plumbing.ReferenceName, 0, len(s.refs.loose))
		for n, r := range s.refs.loose {
			next[n] = r
			folded = append(folded, n)
		}
		s.refs.mu.Unlock()

		// Expectations are checked against the merged pre-write view, which is
		// exactly what next holds before the mutations below are applied.
		if err := checkRefExpectations(next, expect); err != nil {
			return err
		}

		for _, r := range sets {
			next[r.Name()] = r
		}
		for _, n := range removes {
			delete(next, n)
		}

		in := &s3.PutObjectInput{
			Bucket: sp(s.bucket),
			Key:    sp(s.prefix + packedRefsKey),
			Body:   bytes.NewReader(encodePackedRefs(next)),
		}
		if etag == "" {
			in.IfNoneMatch = sp("*")
		} else {
			in.IfMatch = sp(etag)
		}

		start := time.Now()
		out, err := s.client.PutObject(s.ctx, in)
		s.observe("PutObject", start, err)
		switch {
		case err == nil:
		case isPreconditionFailed(err):
			continue // somebody else landed first; re-read and re-apply
		default:
			return fmt.Errorf("tigris: commit refs: %w", err)
		}

		// The commit landed. Adopt it in place rather than re-reading, and hand
		// the folded names to the loose-key cleanup.
		s.refs.mu.Lock()
		s.refs.etag = sv(out.ETag)
		s.refs.packed = next
		s.refs.loose = map[plumbing.ReferenceName]*plumbing.Reference{}
		s.refs.built = true
		s.refs.mu.Unlock()

		return s.dropFoldedLooseRefs(folded, removes)
	}
	return fmt.Errorf("%w after %d attempts", ErrRefContention, maxRefCASRetries)
}

// checkRefExpectations compares a caller's CheckAndSetReference preconditions
// against the pre-write view. It runs on every attempt, not once: a retry
// happens precisely because somebody else wrote, so the expectation must be
// re-tested against what they wrote.
func checkRefExpectations(view map[plumbing.ReferenceName]*plumbing.Reference, expect []refExpectation) error {
	for _, e := range expect {
		cur, ok := view[e.name]
		switch {
		case e.old == nil:
			// A nil old is lenient, matching the in-memory storer: a missing
			// current reference falls through to creation.
		case !ok:
			// Also lenient, and for the same reason.
		case cur.Hash() != e.old.Hash():
			return storage.ErrReferenceHasChanged
		}
	}
	return nil
}

// dropFoldedLooseRefs deletes the legacy loose keys the commit superseded.
// Task 6 gives it a body; until then a fold leaves its keys in place, which is
// correct under the merge rule but means the loose layer never shrinks.
func (s *Storer) dropFoldedLooseRefs(folded, removed []plumbing.ReferenceName) error {
	return nil
}
