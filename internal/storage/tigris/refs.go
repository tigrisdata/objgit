package tigris

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
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
// Concurrency note: CheckAndSetReference compares then writes non-atomically.
// Real CAS via conditional PutObject (If-Match ETag) is listed as follow-up
// work; today the window races exactly like the in-memory storer does.

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

func (s *Storer) SetReference(ref *plumbing.Reference) error {
	if ref == nil {
		return nil // tolerated identically by the in-memory storer
	}
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(refKey(ref.Name())),
		Body:   strings.NewReader(encodeRefValue(ref)),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: set ref %s: %w", ref.Name().String(), err)
	}
	return nil
}

func (s *Storer) CheckAndSetReference(newRef, old *plumbing.Reference) error {
	if newRef == nil {
		return nil
	}
	if old != nil {
		current, err := s.Reference(newRef.Name())
		if err == nil && current.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
		// Missing current reference falls through to creation, mirroring the
		// in-memory storer's lenient behavior.
	}
	return s.SetReference(newRef)
}

func (s *Storer) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	body, err := s.fetchSmall(refKey(n))
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return nil, plumbing.ErrReferenceNotFound
	default:
		return nil, fmt.Errorf("tigris: load ref %s: %w", n.String(), err)
	}

	ref, derr := decodeRefValue(n, string(body))
	if derr != nil {
		return nil, derr
	}
	return ref, nil
}

// listLooseRefs is the single source of truth behind IterReferences and
// CountLooseRefs, so the two can never disagree. Malformed entries log-and-
// skip; vanished-mid-list keys behave like the object iterator's race rule.
func (s *Storer) listLooseRefs() ([]*plumbing.Reference, error) {
	keys, err := s.listKeys(refPrefix)
	if err != nil {
		return nil, err
	}

	var refs []*plumbing.Reference
	for _, k := range keys {
		name := plumbing.ReferenceName(strings.TrimPrefix(k, refPrefix))

		ref, rerr := s.Reference(name)
		switch {
		case rerr == nil:
			refs = append(refs, ref)
		case errors.Is(rerr, plumbing.ErrReferenceNotFound):
			continue
		case errors.Is(rerr, errMalformedRef):
			slog.Warn("skipping malformed loose ref", "key", k, "err", rerr)
			continue
		default:
			return nil, rerr
		}
	}
	return refs, nil
}

func (s *Storer) IterReferences() (storer.ReferenceIter, error) {
	refs, lerr := s.listLooseRefs()
	if lerr != nil {
		return nil, lerr
	}
	return storer.NewReferenceSliceIter(refs), nil
}

func (s *Storer) RemoveReference(n plumbing.ReferenceName) error {
	if err := s.removeSimple(refKey(n)); err != nil {
		return fmt.Errorf("tigris: remove ref %s: %w", n.String(), err)
	}
	return nil
}

func (s *Storer) CountLooseRefs() (int, error) {
	refs, err := s.listLooseRefs()
	if err != nil {
		return 0, err
	}
	return len(refs), nil
}

// PackRefs is deliberately a no-op: every ref stays individually addressable,
// so packed-refs compaction offers nothing in a flat bucket.
func (s *Storer) PackRefs() error {
	return nil
}

// --- shallow marks ---

func (s *Storer) SetShallow(commits []plumbing.Hash) error {
	if len(commits) == 0 {
		return s.removeSimple(shallowKey)
	}
	var b strings.Builder
	for _, c := range commits {
		b.WriteString(c.String())
		b.WriteString("\n")
	}
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(shallowKey),
		Body:   strings.NewReader(b.String()),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: store shallow marks: %w", err)
	}
	return nil
}

func (s *Storer) Shallow() ([]plumbing.Hash, error) {
	body, err := s.fetchSmall(shallowKey)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return nil, nil // absent marker == not shallow, like dotgit
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
	return out, nil
}

// --- small-payload primitives shared by refs, shallow, index, config ---

// fetchSmall GETs one whole ancillary object and returns its body. Misses
// normalize to plumbing.ErrObjectNotFound so every caller maps absence the
// same way object reads do.
func (s *Storer) fetchSmall(key string) ([]byte, error) {
	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
	})
	s.observe("GetObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, plumbing.ErrObjectNotFound
	default:
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
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
