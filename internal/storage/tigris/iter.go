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

// objectIter resolves keys one HEAD at a time. Laziness buys the cost profile
// the spec asks for: type mismatches cost a HEAD, never a body download.
type objectIter struct {
	s    *Storer
	want plumbing.ObjectType
	keys []string
	pos  int
}

func (s *Storer) IterEncodedObjects(t plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	keys, err := s.listKeys(objectPrefix)
	if err != nil {
		return nil, err
	}
	return &objectIter{s: s, want: t, keys: keys}, nil
}

func (it *objectIter) Next() (plumbing.EncodedObject, error) {
	for it.pos < len(it.keys) {
		raw := strings.TrimPrefix(it.keys[it.pos], objectPrefix)
		it.pos++

		h, ok := plumbing.FromHex(raw)
		if !ok {
			continue // junk under objects/: skip, never poison the walk
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
	it.pos = len(it.keys)
}
