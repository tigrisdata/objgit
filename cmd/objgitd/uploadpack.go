package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
)

// uploadPackHTTP handles Git's shallow-only discovery round. In stateless
// HTTP, Git first sends wants + deepen + flush, without haves or done. The
// response must end at the shallow-update flush. go-git otherwise appends a
// NAK, which Git mistakes for the next round's shallow list.
func uploadPackHTTP(ctx context.Context, st storage.Storer, in io.ReadCloser, out io.WriteCloser, opts *transport.UploadPackRequest) error {
	rd := bufio.NewReader(in)
	n, _, err := pktline.PeekLine(rd)
	if err != nil {
		return err
	}
	if n == pktline.Flush {
		return nil
	}
	var req packp.UploadRequest
	if err := req.Decode(rd); err != nil {
		return fmt.Errorf("decode upload request: %w", err)
	}
	var prefix bytes.Buffer
	if err := req.Encode(&prefix); err != nil {
		return err
	}
	var shallowOnly bool
	if !req.Depth.IsZero() {
		_, err := rd.Peek(1)
		shallowOnly = errors.Is(err, io.EOF)
		if err != nil && !shallowOnly {
			return err
		}
	}
	if shallowOnly {
		out = &shallowRoundWriter{WriteCloser: out}
	}
	err = transport.UploadPack(ctx, st, io.NopCloser(io.MultiReader(&prefix, rd)), out, opts)
	if shallowOnly && errors.Is(err, errShallowRoundDone) {
		return nil
	}
	return err
}

var errShallowRoundDone = errors.New("shallow discovery round complete")

// shallowRoundWriter forwards complete pkt-lines through the first flush,
// then stops the upstream command. It holds only the four-byte packet header
// and supports arbitrary Write boundaries, including several packets at once.
type shallowRoundWriter struct {
	io.WriteCloser
	header    [4]byte
	headerN   int
	remaining int
	done      bool
}

func (w *shallowRoundWriter) Write(p []byte) (int, error) {
	consumed := 0
	for len(p) > 0 {
		if w.done {
			return consumed, errShallowRoundDone
		}
		if w.remaining > 0 {
			take := min(len(p), w.remaining)
			n, err := w.WriteCloser.Write(p[:take])
			consumed += n
			p = p[n:]
			w.remaining -= n
			if err != nil {
				return consumed, err
			}
			if n != take {
				return consumed, io.ErrShortWrite
			}
			continue
		}
		n := copy(w.header[w.headerN:], p)
		w.headerN += n
		consumed += n
		p = p[n:]
		if w.headerN < 4 {
			continue
		}
		size, err := strconv.ParseUint(string(w.header[:]), 16, 16)
		if err != nil || (size != 0 && size < 4) {
			return consumed, fmt.Errorf("invalid shallow response packet %q", w.header)
		}
		n, err = w.WriteCloser.Write(w.header[:])
		if err != nil {
			return consumed, err
		}
		if n != 4 {
			return consumed, io.ErrShortWrite
		}
		w.headerN = 0
		if size == 0 {
			w.done = true
			return consumed, errShallowRoundDone
		}
		w.remaining = int(size) - 4
	}
	return consumed, nil
}
