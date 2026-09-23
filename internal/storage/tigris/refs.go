package tigris

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
)

// Refs store dotgit-loose-ref-style text: raw hex for hashes, "ref: target"
// for symbolics, newline-terminated. Keys carry the FULL ref name after the
// refs/ prefix, giving every addressable name (including HEAD) one namespace
// in the flat bucket.
//
// Two layers hold refs. packed-refs holds them all in one object under a
// compare-and-swap (see refcache.go and packedrefs.go); refs/<name> is the
// legacy loose layer, which is read-only and folded away on the first packed
// write. A loose ref wins over a packed ref with the same name — refView
// explains why.

func encodeRefValue(ref *plumbing.Reference) string {
	if ref.Type() == plumbing.SymbolicReference {
		return symRefPrefix + ref.Target().String() + "\n"
	}
	return ref.Hash().String() + "\n"
}

func decodeRefValue(name plumbing.ReferenceName, v string) (*plumbing.Reference, error) {
	value := strings.TrimSpace(v)
	if target, ok := strings.CutPrefix(value, symRefPrefix); ok {
		return plumbing.NewSymbolicReference(name, plumbing.ReferenceName(strings.TrimSpace(target))), nil
	}
	h, ok := plumbing.FromHex(value)
	if !ok {
		return nil, fmt.Errorf("%w: ref %s body %q", errMalformedRef, name.String(), value)
	}
	return plumbing.NewHashReference(name, h), nil
}

// SetReference writes one ref. With WithPackedRefs set it goes through
// commitRefs, which is one conditional PutObject; otherwise it writes one
// loose object, which is what every release before packed refs did.
func (s *Storer) SetReference(ref *plumbing.Reference) error {
	if ref == nil {
		return nil // tolerated identically by the in-memory storer
	}
	if s.packedRefs {
		return s.commitRefs([]*plumbing.Reference{ref}, nil, nil)
	}
	return s.setLooseReference(ref)
}

// UpdateReferences applies a whole batch of ref mutations at once. With
// WithPackedRefs set that is one conditional PutObject for the entire batch,
// which is the difference between one round trip and one per ref on a push of
// many tags.
//
// The batch is all-or-nothing: the single commit PUT either lands or it does
// not. cmd/objgitd/receivepack.go relies on that, and reports one shared error
// for every command in a failed push.
//
// Without WithPackedRefs it walks the loose path, so the method is always safe
// to call and the transport never needs to know which mode it is in.
func (s *Storer) UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error {
	if s.packedRefs {
		return s.commitRefs(sets, removes, nil)
	}
	for _, r := range sets {
		if err := s.setLooseReference(r); err != nil {
			return err
		}
	}
	for _, n := range removes {
		if err := s.RemoveReference(n); err != nil {
			return err
		}
	}
	return nil
}

// SupportsCheckedRefUpdates reports whether this storer can compare every
// advertised old hash at the atomic packed-refs commit point. Loose refs have
// no batch compare-and-swap, so receive-pack uses its per-command path there.
func (s *Storer) SupportsCheckedRefUpdates() bool { return s.packedRefs }

// UpdateReferencesChecked applies a batch only if every ref still has the
// value advertised by the pushing client. A zero expected hash requires the
// ref to be absent. The check is repeated after each packed-refs CAS retry.
// It returns the packed write's acceptance time, even when subsequent
// loose-ref cleanup fails.
func (s *Storer) UpdateReferencesChecked(sets []*plumbing.Reference, removes []plumbing.ReferenceName, expected map[plumbing.ReferenceName]plumbing.Hash) (time.Time, error) {
	if !s.packedRefs {
		return time.Time{}, errors.New("checked ref updates require packed refs")
	}
	expectations := make([]refExpectation, 0, len(expected))
	for name, old := range expected {
		var ref *plumbing.Reference
		if !old.IsZero() {
			ref = plumbing.NewHashReference(name, old)
		}
		expectations = append(expectations, refExpectation{name: name, old: ref, strict: true})
	}
	var acceptedAt time.Time
	err := s.commitRefsNotifying(sets, removes, expectations, func(at time.Time) { acceptedAt = at })
	return acceptedAt, err
}

// setLooseReference writes one refs/<name> object. It flushes every upload
// queued through this Storer first, so a ref can never point at an object that
// failed — or hasn't yet finished — its asynchronous upload (see upload.go).
func (s *Storer) setLooseReference(ref *plumbing.Reference) error {
	if err := s.up.flush(); err != nil {
		return fmt.Errorf("tigris: set ref %s: %w", ref.Name().String(), err)
	}
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + refKey(ref.Name())),
		Body:   strings.NewReader(encodeRefValue(ref)),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: set ref %s: %w", ref.Name().String(), err)
	}
	// The cache no longer matches the bucket. Without this a caller that reads
	// its own write — CheckAndSetReference does, to compare before writing —
	// sees the pre-write value.
	s.invalidateRefs()
	return nil
}

// CheckAndSetReference writes newRef only if the ref currently holds old.
//
// With WithPackedRefs set, the compare and the write are one conditional
// request, so the check cannot go stale between them. Without it, the two
// steps race exactly as every release before packed refs did.
func (s *Storer) CheckAndSetReference(newRef, old *plumbing.Reference) error {
	if newRef == nil {
		return nil
	}
	if s.packedRefs {
		expect := []refExpectation{{name: newRef.Name(), old: old}}
		return s.commitRefs([]*plumbing.Reference{newRef}, nil, expect)
	}

	if old != nil {
		current, err := s.Reference(newRef.Name())
		if err == nil && current.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
		// Missing current reference falls through to creation, mirroring the
		// in-memory storer's lenient behavior.
	}
	return s.setLooseReference(newRef)
}

func (s *Storer) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	view, err := s.refView()
	if err != nil {
		return nil, err
	}
	ref, ok := view[n]
	if !ok {
		return nil, plumbing.ErrReferenceNotFound
	}
	return ref, nil
}

// looseReference loads one refs/<name> object directly, with no cache in the
// path. ensureRefsBuilt is what fills the cache, and it calls listLooseRefs
// below to do so, so nothing on this path may consult the cache: Reference
// does, and routing through it would have ensureRefsBuilt wait on the lock it
// already holds.
func (s *Storer) looseReference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	body, err := s.fetchSmall(s.prefix + refKey(n))
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return nil, plumbing.ErrReferenceNotFound
	default:
		return nil, fmt.Errorf("tigris: load ref %s: %w", n.String(), err)
	}
	return decodeRefValue(n, string(body))
}

// listLooseRefs walks the legacy loose layer, which ensureRefsBuilt merges
// under the packed object. Malformed entries log-and-skip: each loose key is an
// independent object, so one bad one says nothing about its neighbors. That is
// deliberately gentler than decodePackedRefs, where every ref shares one object
// and a single bad line makes all of them untrustworthy.
//
// Vanished-mid-list keys behave like the object iterator's race rule.
func (s *Storer) listLooseRefs() ([]*plumbing.Reference, error) {
	keys, err := s.listKeys(s.prefix + refPrefix)
	if err != nil {
		return nil, err
	}

	var refs []*plumbing.Reference
	for _, k := range keys {
		name := plumbing.ReferenceName(strings.TrimPrefix(k, s.prefix+refPrefix))

		ref, rerr := s.looseReference(name)
		switch {
		case rerr == nil:
			refs = append(refs, ref)
		case errors.Is(rerr, plumbing.ErrReferenceNotFound):
			continue
		case errors.Is(rerr, errMalformedRef):
			slog.Warn("skipping malformed loose ref", "err", rerr, "key", k)
			continue
		default:
			return nil, rerr
		}
	}
	return refs, nil
}

// IterReferences walks every ref, sorted by name. The order used to come free
// from S3's lexicographic listing; the merged view is a map, so it is sorted
// here instead. Callers depend on it: a ref advertisement is more compressible
// in name order, and a stable order keeps test assertions exact.
func (s *Storer) IterReferences() (storer.ReferenceIter, error) {
	view, err := s.refView()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(view))
	for n := range view {
		names = append(names, n.String())
	}
	sort.Strings(names)

	refs := make([]*plumbing.Reference, 0, len(names))
	for _, n := range names {
		refs = append(refs, view[plumbing.ReferenceName(n)])
	}
	return storer.NewReferenceSliceIter(refs), nil
}

func (s *Storer) RemoveReference(n plumbing.ReferenceName) error {
	if s.packedRefs {
		return s.commitRefs(nil, []plumbing.ReferenceName{n}, nil)
	}
	if err := s.removeSimple(s.prefix + refKey(n)); err != nil {
		return fmt.Errorf("tigris: remove ref %s: %w", n.String(), err)
	}
	s.invalidateRefs()
	return nil
}

// CountLooseRefs reports how many legacy loose refs are left. After a fold it
// returns 0, which correctly tells go-git there is nothing to pack. It
// deliberately does not count packed refs: go-git reads this number to decide
// whether compaction is worth doing, and packed refs are already compacted.
func (s *Storer) CountLooseRefs() (int, error) {
	if err := s.ensureRefsBuilt(); err != nil {
		return 0, err
	}
	s.refs.mu.Lock()
	defer s.refs.mu.Unlock()
	return len(s.refs.loose), nil
}

// PackRefs folds the legacy loose refs into packed-refs and deletes them.
//
// It used to be a no-op, on the reasoning that every ref stayed individually
// addressable so compaction bought nothing. Packed refs change that: the fold
// is what turns an advertisement from one GetObject per ref into one for the
// whole repository. Exposing it here gives an operator a way to pay that cost
// on demand instead of on whichever push happens to be first.
//
// It is a no-op while WithPackedRefs is off, so a release that can only read
// the new format never creates it.
func (s *Storer) PackRefs() error {
	if !s.packedRefs {
		return nil
	}
	if err := s.ensureRefsBuilt(); err != nil {
		return err
	}

	s.refs.mu.Lock()
	loose := make([]*plumbing.Reference, 0, len(s.refs.loose))
	for _, r := range s.refs.loose {
		loose = append(loose, r)
	}
	s.refs.mu.Unlock()

	if len(loose) == 0 {
		return nil
	}
	// commitRefs folds the whole loose layer on any write, so handing it the
	// loose refs as sets is enough — and it keeps one commit path rather than
	// two.
	return s.commitRefs(loose, nil, nil)
}

// --- shallow marks ---

// shallowCache is one Storer's memoized answer for the shallow key. It mirrors
// refCache: built once per instance, and not sticky on error.
//
// It exists because go-git asks far more than once. revlist.ObjectsWithRef
// walks one want at a time, and every walk builds a shallow set, so a clone
// costs refs + 1 lookups. Almost every repository is not shallow, so almost
// every one of those lookups is a 404 on a key that is not there. Measured on
// a 1,077-ref repository, those 404s took 338 of the 412 seconds the clone
// spent. The listing cache in internal/s3fs used to answer them for free, and
// ebfe4e3 removed it when repository storage stopped going through s3fs.
//
// Absent is therefore the answer worth remembering, so built and hashes are
// separate fields: a nil hashes with built set means "confirmed not shallow".
type shallowCache struct {
	mu     sync.Mutex
	built  bool
	hashes []plumbing.Hash
}

func newShallowCache() *shallowCache { return &shallowCache{} }

// setShallowCache records a definitive answer. SetShallow calls it after a
// write lands, so a read of its own write inside the same request never sees
// the pre-write value.
func (s *Storer) setShallowCache(hashes []plumbing.Hash) {
	s.shallow.mu.Lock()
	defer s.shallow.mu.Unlock()
	s.shallow.hashes = slices.Clone(hashes)
	s.shallow.built = true
}

// invalidateShallow drops the cache so the next read goes to the bucket. A
// failed write is the reason: the request either landed or it did not, and
// nothing here knows which.
func (s *Storer) invalidateShallow() {
	s.shallow.mu.Lock()
	defer s.shallow.mu.Unlock()
	s.shallow.built = false
	s.shallow.hashes = nil
}

func (s *Storer) SetShallow(commits []plumbing.Hash) error {
	if len(commits) == 0 {
		if err := s.removeSimple(s.prefix + shallowKey); err != nil {
			s.invalidateShallow()
			return err
		}
		s.setShallowCache(nil)
		return nil
	}
	var b strings.Builder
	for _, c := range commits {
		b.WriteString(c.String())
		b.WriteString("\n")
	}
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + shallowKey),
		Body:   strings.NewReader(b.String()),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		s.invalidateShallow()
		return fmt.Errorf("tigris: store shallow marks: %w", err)
	}
	s.setShallowCache(commits)
	return nil
}

// Shallow reports the repository's shallow marks, reading the key at most once
// per Storer. See shallowCache for why the memo matters.
//
// The lock spans the GET, as ensureRefsBuilt's does. Two first callers then
// cost one round trip rather than two, which is the whole point here. An error
// leaves the cache empty, so the next caller retries.
func (s *Storer) Shallow() ([]plumbing.Hash, error) {
	s.shallow.mu.Lock()
	defer s.shallow.mu.Unlock()
	if s.shallow.built {
		// A copy. The caller must not be able to edit the cache.
		return slices.Clone(s.shallow.hashes), nil
	}

	body, err := s.fetchSmall(s.prefix + shallowKey)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		// Absent marker == not shallow, like dotgit. This is the common case
		// and the one the memo is for.
		s.shallow.hashes = nil
		s.shallow.built = true
		return nil, nil
	default:
		return nil, fmt.Errorf("tigris: load shallow marks: %w", err)
	}

	var out []plumbing.Hash
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		h, ok := plumbing.FromHex(line)
		if !ok {
			return nil, fmt.Errorf("%w: shallow mark %q unreadable", errMalformedRef, line)
		}
		out = append(out, h)
	}

	s.shallow.hashes = out
	s.shallow.built = true
	return slices.Clone(out), nil
}

// --- small-payload primitives shared by refs, shallow, index, config ---

// fetchSmall GETs one whole ancillary object and returns its body. Misses
// normalize to plumbing.ErrObjectNotFound so every caller maps absence the
// same way object reads do.
func (s *Storer) fetchSmall(key string) ([]byte, error) {
	body, _, err := s.fetchSmallETag(key)
	return body, err
}

// fetchSmallETag is fetchSmall plus the object's ETag. Only packed-refs wants
// the ETag, which it uses as its compare-and-swap token, so the plain wrapper
// above keeps the other callers (shallow, index, config) untouched.
func (s *Storer) fetchSmallETag(key string) ([]byte, string, error) {
	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
	})
	s.observe("GetObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, "", plumbing.ErrObjectNotFound
	default:
		return nil, "", err
	}
	defer out.Body.Close()
	body, rerr := io.ReadAll(out.Body)
	return body, sv(out.ETag), rerr
}

// removeSimple deletes one root-level key, tolerating its absence.
func (s *Storer) removeSimple(key string) error {
	start := time.Now()
	_, err := s.client.DeleteObject(s.ctx, &s3.DeleteObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
	})
	s.observe("DeleteObject", start, err)
	switch {
	case err == nil, isNotFound(err):
		return nil
	default:
		return fmt.Errorf("tigris: delete %s: %w", key, err)
	}
}
