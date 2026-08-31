package main

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/utils/ioutil"
)

// receivePackStreaming is a fork of go-git's transport.ReceivePack (v6) that
// adds one seam: onUpdated runs after refs are updated and report-status is
// sent, but *before* the closing sideband flush-pkt. go-git keeps its sideband
// Muxer internal and flushes it before returning, so there is no other way to
// stream "remote:" progress to the client. The seam hands back a band-2
// (sideband.ProgressMessage) writer when the client negotiated sideband, or nil
// otherwise — callers stream hook output through it and fall back to logging
// when it is nil.
//
// Everything except the onUpdated seam mirrors transport.ReceivePack verbatim,
// including the helper functions copied below.
func receivePackStreaming(
	ctx context.Context,
	st storage.Storer,
	r io.ReadCloser,
	w io.WriteCloser,
	opts *transport.ReceivePackRequest,
	onUpdated func(progress io.Writer),
) error {
	if w == nil {
		return fmt.Errorf("nil writer")
	}

	w = ioutil.NewContextWriteCloser(ctx, w)

	if opts == nil {
		opts = &transport.ReceivePackRequest{}
	}

	if opts.AdvertiseRefs || !opts.StatelessRPC {
		switch version := transport.ProtocolVersion(opts.GitProtocol); version {
		case protocol.V1:
			if _, err := pktline.Writef(w, "version %d\n", version); err != nil {
				return err
			}
		case protocol.V0, protocol.V2:
		default:
			return fmt.Errorf("%w: %q", transport.ErrUnsupportedVersion, version)
		}

		if err := transport.AdvertiseRefs(ctx, st, w, transport.ReceivePackService, opts.StatelessRPC); err != nil {
			return err
		}
	}

	if opts.AdvertiseRefs {
		// Done, there's nothing else to do
		return nil
	}

	if r == nil {
		return fmt.Errorf("nil reader")
	}

	r = ioutil.NewContextReadCloser(ctx, r)

	rd := bufio.NewReader(r)
	l, _, err := pktline.PeekLine(rd)
	if err != nil {
		return err
	}

	// At this point, if we get a flush packet, it means the client
	// has nothing to send, so we can return early.
	if l == pktline.Flush {
		return nil
	}

	updreq := &packp.UpdateRequests{}
	if err := updreq.Decode(rd); err != nil {
		return err
	}

	var (
		caps         = updreq.Capabilities
		needPackfile bool
		pushOpts     packp.PushOptions
	)

	if updreq.Capabilities.Supports(capability.PushOptions) {
		if err := pushOpts.Decode(rd); err != nil {
			return fmt.Errorf("decoding push-options: %w", err)
		}
	}

	// Should we expect a packfile?
	for _, cmd := range updreq.Commands {
		if cmd.Action() != packp.Delete {
			needPackfile = true
			break
		}
	}

	// Receive the packfile
	var unpackErr error
	if needPackfile {
		unpackErr = writePack(st, rd)
	}

	// Done with the request, now close the reader
	// to indicate that we are done reading from it.
	if err := r.Close(); err != nil {
		return fmt.Errorf("closing reader: %w", err)
	}

	// Report status if the client supports it
	if !updreq.Capabilities.Supports(capability.ReportStatus) {
		return unpackErr
	}

	var (
		useSideband bool
		mux         *sideband.Muxer
		writer      io.Writer = w
	)
	if !caps.Supports(capability.NoProgress) {
		if caps.Supports(capability.Sideband64k) {
			mux = sideband.NewMuxer(sideband.Sideband64k, w)
			writer = mux
			useSideband = true
		} else if caps.Supports(capability.Sideband) {
			mux = sideband.NewMuxer(sideband.Sideband, w)
			writer = mux
			useSideband = true
		}
	}

	writeCloser := ioutil.NewWriteCloser(writer, w)
	if unpackErr != nil {
		res := sendReportStatus(writeCloser, unpackErr, nil)
		_ = closeWriter(w)
		return res
	}

	var firstErr error
	cmdStatus := make(map[plumbing.ReferenceName]error)
	updateReferences(st, updreq, cmdStatus, &firstErr)

	if err := sendReportStatus(writeCloser, firstErr, cmdStatus); err != nil {
		return err
	}

	// Stream hook output over the sideband progress channel before the closing
	// flush-pkt; once the client sees that flush it stops reading the sideband.
	if onUpdated != nil {
		var progress io.Writer
		if useSideband {
			progress = sidebandProgress{mux: mux}
		}
		onUpdated(progress)
	}

	if useSideband {
		if err := pktline.WriteFlush(w); err != nil {
			return fmt.Errorf("flushing sideband: %w", err)
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return closeWriter(w)
}

// writePack stores the incoming packfile as a single packfile object via the
// storer's PackfileWriter, delimiting the pack with a Scanner rather than waiting
// for the reader to reach io.EOF. go-git's default PackfileWriter path
// (WritePackfileToObjectStorage → io.CopyBufferPool until EOF) deadlocks on a
// persistent git:// / SSH socket, where the client holds the connection open
// awaiting report-status. The Scanner knows the pack's end from its own framing
// (header object count + trailer checksum) and stops there, while a TeeReader
// mirrors exactly those bytes into the PackfileWriter — so the whole pack lands as
// one object on every transport. Falls back to UpdateObjectStorage (loose objects)
// if the storer cannot write packs.
func writePack(st storage.Storer, rd io.Reader) error {
	pw, ok := st.(storer.PackfileWriter)
	if !ok {
		return packfile.UpdateObjectStorage(st, rd)
	}

	var sopts []packfile.ScannerOption
	if c, ok := st.(config.ConfigStorer); ok {
		if cfg, err := c.Config(); err == nil && cfg.Extensions.ObjectFormat == formatcfg.SHA256 {
			sopts = append(sopts, packfile.WithSHA256())
		}
	}

	w, err := pw.PackfileWriter()
	if err != nil {
		return err
	}

	sc := packfile.NewScanner(io.TeeReader(rd, w), sopts...)
	for sc.Scan() {
	}
	if err := sc.Error(); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

// sidebandProgress writes to the sideband progress channel (band 2), which the
// git client renders as "remote: " lines.
type sidebandProgress struct {
	mux *sideband.Muxer
}

func (s sidebandProgress) Write(p []byte) (int, error) {
	return s.mux.WriteChannel(sideband.ProgressMessage, p)
}

// The helpers below are copied verbatim from go-git's transport package
// (plumbing/transport/receive_pack.go), which keeps them unexported.

func closeWriter(w io.WriteCloser) error {
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}
	return nil
}

func sendReportStatus(w io.WriteCloser, unpackErr error, cmdStatus map[plumbing.ReferenceName]error) error {
	rs := &packp.ReportStatus{}
	rs.UnpackStatus = "ok"
	if unpackErr != nil {
		rs.UnpackStatus = unpackErr.Error()
	}

	for ref, err := range cmdStatus {
		msg := "ok"
		if err != nil {
			msg = err.Error()
		}
		status := &packp.CommandStatus{
			ReferenceName: ref,
			Status:        msg,
		}
		rs.CommandStatuses = append(rs.CommandStatuses, status)
	}

	if err := rs.Encode(w); err != nil {
		return err
	}

	return nil
}

func setStatus(cmdStatus map[plumbing.ReferenceName]error, firstErr *error, ref plumbing.ReferenceName, err error) {
	cmdStatus[ref] = err
	if *firstErr == nil && err != nil {
		*firstErr = err
	}
}

func referenceExists(s storer.ReferenceStorer, n plumbing.ReferenceName) (bool, error) {
	_, err := s.Reference(n)
	if err == plumbing.ErrReferenceNotFound {
		return false, nil
	}

	return err == nil, err
}

// refUpdater is the optional bulk ref-update surface. A storer that has one
// turns a push's N ref writes into a single round trip; a storer without one
// falls back to updateReferencesOneByOne.
//
// It is one flattened method rather than a batch object, because Go needs an
// exact signature match: a method returning *tigris.RefBatch would not satisfy
// an interface declaring NewRefBatch() refBatch. Every type here comes from
// plumbing, which both packages already import, so the seam needs no shared
// package.
type refUpdater interface {
	UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error
}

func updateReferences(st storage.Storer, req *packp.UpdateRequests, cmdStatus map[plumbing.ReferenceName]error, firstErr *error) {
	if bu, ok := st.(refUpdater); ok {
		updateReferencesBatched(bu, st, req, cmdStatus, firstErr)
		return
	}
	updateReferencesOneByOne(st, req, cmdStatus, firstErr)
}

// updateReferencesBatched validates every command first, then applies the
// whole push in one call.
//
// Validation is cheap here in a way it is not on the one-by-one path: the
// tigris storer answers every referenceExists out of its per-request ref
// cache, so N existence checks cost N map lookups instead of N GetObject
// calls.
//
// A commit failure fails every staged command, because the batch is
// all-or-nothing. That differs from the one-by-one path, where a failure
// mid-loop leaves earlier commands applied. All-or-nothing is the better
// behavior — it is what git push --atomic means — but report-status now
// carries one shared error where it used to carry a mix.
func updateReferencesBatched(bu refUpdater, st storage.Storer, req *packp.UpdateRequests, cmdStatus map[plumbing.ReferenceName]error, firstErr *error) {
	var (
		sets    []*plumbing.Reference
		removes []plumbing.ReferenceName
		staged  []plumbing.ReferenceName
	)

	for _, cmd := range req.Commands {
		exists, err := referenceExists(st, cmd.Name)
		if err != nil {
			setStatus(cmdStatus, firstErr, cmd.Name, err)
			continue
		}

		switch cmd.Action() {
		case packp.Create:
			if exists {
				setStatus(cmdStatus, firstErr, cmd.Name, transport.ErrUpdateReference)
				continue
			}
			sets = append(sets, plumbing.NewHashReference(cmd.Name, cmd.New))
		case packp.Delete:
			if !exists {
				setStatus(cmdStatus, firstErr, cmd.Name, transport.ErrUpdateReference)
				continue
			}
			removes = append(removes, cmd.Name)
		case packp.Update:
			if !exists {
				setStatus(cmdStatus, firstErr, cmd.Name, transport.ErrUpdateReference)
				continue
			}
			sets = append(sets, plumbing.NewHashReference(cmd.Name, cmd.New))
		default:
			continue
		}
		staged = append(staged, cmd.Name)
	}

	if len(staged) == 0 {
		return
	}

	err := bu.UpdateReferences(sets, removes)
	for _, n := range staged {
		setStatus(cmdStatus, firstErr, n, err)
	}
}

// updateReferencesOneByOne is the pre-batch path, kept for any storer without
// a refUpdater — memory.Storage in the tests, most notably.
func updateReferencesOneByOne(st storage.Storer, req *packp.UpdateRequests, cmdStatus map[plumbing.ReferenceName]error, firstErr *error) {
	for _, cmd := range req.Commands {
		exists, err := referenceExists(st, cmd.Name)
		if err != nil {
			setStatus(cmdStatus, firstErr, cmd.Name, err)
			continue
		}

		switch cmd.Action() {
		case packp.Create:
			if exists {
				setStatus(cmdStatus, firstErr, cmd.Name, transport.ErrUpdateReference)
				continue
			}

			ref := plumbing.NewHashReference(cmd.Name, cmd.New)
			err := st.SetReference(ref)
			setStatus(cmdStatus, firstErr, cmd.Name, err)
		case packp.Delete:
			if !exists {
				setStatus(cmdStatus, firstErr, cmd.Name, transport.ErrUpdateReference)
				continue
			}

			err := st.RemoveReference(cmd.Name)
			setStatus(cmdStatus, firstErr, cmd.Name, err)
		case packp.Update:
			if !exists {
				setStatus(cmdStatus, firstErr, cmd.Name, transport.ErrUpdateReference)
				continue
			}

			ref := plumbing.NewHashReference(cmd.Name, cmd.New)
			err := st.SetReference(ref)
			setStatus(cmdStatus, firstErr, cmd.Name, err)
		}
	}
}
