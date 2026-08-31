package tigris

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// listKeys walks one prefix fully. S3 returns Contents lexicographically with
// monotone continuation tokens, so results come back sorted.
func (s *Storer) listKeys(prefix string) ([]string, error) {
	var keys []string
	token := ""

	for {
		in := &s3.ListObjectsV2Input{
			Bucket: sp(s.bucket),
			Prefix: sp(prefix),
		}
		if token != "" {
			in.ContinuationToken = sp(token)
		}

		start := time.Now()
		page, err := s.client.ListObjectsV2(s.ctx, in)
		s.observe("ListObjectsV2", start, err)
		if err != nil {
			return nil, fmt.Errorf("tigris: list %q: %w", prefix, err)
		}

		for _, entry := range page.Contents {
			if k := sv(entry.Key); k != "" {
				keys = append(keys, k)
			}
		}
		if !bv(page.IsTruncated) || sv(page.NextContinuationToken) == "" {
			break
		}
		token = sv(page.NextContinuationToken)
	}
	return keys, nil
}

// objectIter walks packed objects first — one whole pack at a time, in offset
// order, as snapshotEntries hands them over — then resolves loose keys one HEAD
// at a time. Laziness buys the cost profile the spec asks for: type mismatches
// cost a HEAD, never a body download.
//
// Offset order is also what makes this cheap for a whole pack. The first packed
// read starts a background download of the container (see packObject), that
// download fills the file in offset order too, and the iteration is walking the
// same direction — so it catches up with the watermark early and reads the rest
// locally, without ever having waited for the download.
type objectIter struct {
	s      *Storer
	want   plumbing.ObjectType
	packed []packedEntry
	ppos   int
	keys   []string
	pos    int
	seen   map[plumbing.Hash]struct{} // packed hashes already yielded, so the loose walk can skip duplicates
}

func (s *Storer) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	if err := s.ensurePacksBuilt(); err != nil {
		return nil, err
	}
	packed := s.packs.snapshotEntries()

	keys, err := s.listKeys(s.prefix + objectPrefix)
	if err != nil {
		return nil, err
	}
	return &objectIter{s: s, want: t, packed: packed, keys: keys, seen: make(map[plumbing.Hash]struct{}, len(packed))}, nil
}

func (it *objectIter) Next() (plumbing.EncodedObject, error) {
	for it.ppos < len(it.packed) {
		pe := it.packed[it.ppos]
		it.ppos++

		if _, dup := it.seen[pe.hash]; dup {
			continue
		}
		it.seen[pe.hash] = struct{}{}

		if it.want != plumbing.AnyObject && pe.e.typ != it.want {
			continue
		}
		obj, err := it.s.packObject(it.want, pe.hash, pe.e)
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			continue // pack vanished between the index build and this read: tolerate, like the loose race below
		}
		if err != nil {
			return nil, err
		}
		return obj, nil
	}

	for it.pos < len(it.keys) {
		raw := strings.TrimPrefix(it.keys[it.pos], it.s.prefix+objectPrefix)
		it.pos++

		h, ok := plumbing.FromHex(raw)
		if !ok {
			continue // junk under objects/: skip, never poison the walk
		}
		if _, dup := it.seen[h]; dup {
			continue // already yielded from a pack
		}

		hs, herr := it.s.headInfo(h)
		switch {
		case errors.Is(herr, plumbing.ErrObjectNotFound):
			continue // vanished between LIST and HEAD: tolerate the race
		case errors.Is(herr, errBadMetadata):
			continue // undecodable entry behaves like junk
		case herr != nil:
			return nil, herr
		}

		if it.want != plumbing.AnyObject && hs.typ != it.want {
			continue
		}
		return it.s.loadObject(h, hs)
	}
	return nil, io.EOF
}

func (it *objectIter) ForEach(cb func(plumbing.EncodedObject) error) error {
	for {
		obj, err := it.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		cbErr := cb(obj)
		if cbErr == nil {
			continue
		}
		if errors.Is(cbErr, storer.ErrStop) {
			return nil
		}
		return cbErr
	}
}

func (it *objectIter) Close() {
	it.ppos = len(it.packed)
	it.pos = len(it.keys)
}
