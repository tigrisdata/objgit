package tigris

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/index"
)

// idxChecksum picks the trailer checksum the index format expects for this
// repository's object format — the same choice dotgit makes by format.
func (s *Storer) idxChecksum() hash.Hash {
	if s.of == formatcfg.SHA256 {
		return sha256.New()
	}
	return sha1.New()
}

func (s *Storer) SetIndex(idx *index.Index) error {
	var buf bytes.Buffer
	if err := index.NewEncoder(&buf, s.idxChecksum()).Encode(idx); err != nil {
		return fmt.Errorf("tigris: encode index: %w", err)
	}
	if err := s.putBytes(indexKey, buf.Bytes()); err != nil {
		return fmt.Errorf("tigris: store index: %w", err)
	}
	return nil
}

func (s *Storer) Index() (*index.Index, error) {
	raw, err := s.fetchSmall(indexKey)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return &index.Index{Version: 2}, nil // memory-storer parity
	default:
		return nil, fmt.Errorf("tigris: load index: %w", err)
	}

	idx := &index.Index{}
	if derr := index.NewDecoder(bytes.NewReader(raw), s.idxChecksum()).Decode(idx); derr != nil {
		return nil, fmt.Errorf("tigris: decode index: %w", derr)
	}
	return idx, nil
}

// putBytes is the trivial whole-payload PUT used by ancillary keys (index,
// config). Objects bypass this for the streaming staging writer.
func (s *Storer) putBytes(key string, body []byte) error {
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
		Body:   bytes.NewReader(body),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: put %s: %w", key, err)
	}
	return nil
}
