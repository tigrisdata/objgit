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
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
)

// uploadPackHTTP handles Git's shallow-only discovery round. In stateless
// HTTP, Git first sends wants + deepen + flush, without haves or done. The
// response must end at the shallow-update flush. go-git otherwise appends a
// NAK, which Git mistakes for the next round's shallow list.
func uploadPackHTTP(ctx context.Context, st storage.Storer, in io.ReadCloser, out io.WriteCloser, opts *transport.UploadPackRequest) error {
	rd := &uploadRequestReader{rd: bufio.NewReader(in)}
	defer rd.Close()
	response := &shallowRoundWriter{WriteCloser: out, request: rd}
	err := transport.UploadPack(ctx, st, io.NopCloser(rd), response, opts)
	if rd.shallowOnly && errors.Is(err, errShallowRoundDone) {
		return nil
	}
	return err
}

// uploadRequestReader forwards the original bytes, one packet at a time, until
// the first flush. It only recognizes deepen and request EOF; upstream owns
// validation. Memory stays bounded regardless of the number of wants/shallows.
type uploadRequestReader struct {
	rd          *bufio.Reader
	packet      *[pktline.MaxSize]byte
	pending     []byte
	deepen      bool
	finished    bool
	shallowOnly bool
}

func (r *uploadRequestReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) == 0 {
		if r.finished {
			return r.rd.Read(p)
		}
		if r.packet == nil {
			r.packet = pktline.GetBuffer()
		}
		n, err := pktline.Read(r.rd, r.packet[:])
		if err != nil {
			return 0, err
		}
		if n == pktline.Flush {
			r.finished = true
			if r.deepen {
				_, err := r.rd.Peek(1)
				r.shallowOnly = errors.Is(err, io.EOF)
				if err != nil && !r.shallowOnly {
					return 0, err
				}
			}
		}
		r.pending = r.packet[:max(n, pktline.LenSize)]
		if bytes.HasPrefix(r.pending[4:], []byte("deepen")) {
			r.deepen = true
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	if r.finished && len(r.pending) == 0 {
		// Pack generation can outlive negotiation by minutes. Return the
		// packet buffer as soon as the flush has reached upstream.
		_ = r.Close()
	}
	return n, nil
}

func (r *uploadRequestReader) Close() error {
	if r.packet != nil {
		pktline.PutBuffer(r.packet)
		r.packet = nil
		r.pending = nil
	}
	return nil
}

var errShallowRoundDone = errors.New("shallow discovery round complete")

// shallowRoundWriter forwards complete pkt-lines through the first flush,
// then stops the upstream command. It holds only the four-byte packet header
// and supports arbitrary Write boundaries, including several packets at once.
type shallowRoundWriter struct {
	io.WriteCloser
	request   *uploadRequestReader
	header    [4]byte
	headerN   int
	remaining int
	done      bool
}

func (w *shallowRoundWriter) Write(p []byte) (int, error) {
	if w.request != nil && !w.request.shallowOnly {
		return w.WriteCloser.Write(p)
	}
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
