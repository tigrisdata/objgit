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
// order, as packSnapshot.order hands them over — then resolves loose keys one
// HEAD at a time. Laziness buys the cost profile the spec asks for: type mismatches
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
	// snap and packed replace a copy of every packEntry: packed costs 8
	// bytes for each object. A hash in more than one pack is yielded once,
	// from the record that snap.lookup returns, and the loose walk skips any
	// hash that snap holds, so no set of seen hashes is needed.
	snap   packSnapshot
	packed []packRef
	ppos   int
	keys   []string
	pos    int
}

func (s *Storer) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	if err := s.ensurePacksBuilt(); err != nil {
		return nil, err
	}
	snap := s.packs.snapshot()

	keys, err := s.listKeys(s.prefix + objectPrefix)
	if err != nil {
		return nil, err
	}
	return &objectIter{s: s, want: t, snap: snap, packed: snap.order(), keys: keys}, nil
}

func (it *objectIter) Next() (plumbing.EncodedObject, error) {
	for it.ppos < len(it.packed) {
		ref := it.packed[it.ppos]
		it.ppos++

		t := it.snap.tables[ref.t]
		hb := t.hashAt(int(ref.i))
		if ti, i, _ := it.snap.lookup(hb); ti != int(ref.t) || i != int(ref.i) {
			continue // another pack holds the record that wins for this hash
		}
		if it.want != plumbing.AnyObject && t.recs[ref.i].typ != it.want {
			continue
		}
		h, _ := plumbing.FromBytes(hb)
		obj, err := it.s.packObject(it.want, h, t.entry(int(ref.i), it.snap.ids))
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
		if _, _, packed := it.snap.lookup(h.Bytes()); packed {
			continue // a pack holds it, so the packed walk dealt with it
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
