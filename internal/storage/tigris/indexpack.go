package tigris

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/klauspost/compress/zlib"
)

// This file is a small git index-pack: it turns a pushed packfile into the
// objects this package stores. Its memory does not grow with the size of the
// history or with the size of any one object. Only a small index grows, by
// about 60 bytes for each object.
//
// It replaces a scratch go-git filesystem.Storage. That storage builds its
// index with a packfile.Parser that has no storage attached, so the parser
// runs outside its low-memory mode and keeps the inflated body of every base
// and every resolved delta until the whole pack is parsed. For golang/go that
// is more than 20 GiB of heap for a 465 MiB pack.
//
// The work is two passes:
//
//  1. scanPack reads the pack once, as it arrives. It stages the bytes in a
//     local file, verifies the trailer checksum, and keeps an index entry for
//     each object: where it is, how big it is, and which object it is a delta
//     against. It inflates every entry to find where the entry ends, and it
//     never keeps a body.
//  2. The resolver walks each delta tree depth first, from its base. A body
//     lives only while an object below it still needs it as a base, so a
//     linear chain costs two bodies at a time. Small bodies stay in memory and
//     large ones go to disk; see bodySink. This is how git's own index-pack
//     works.

// incomingObject is one entry of the pushed pack, in pack order.
type incomingObject struct {
	offset  int64 // the entry header
	content int64 // the zlib stream
	size    int64 // inflated length: the object, or the delta's instruction stream
	base    int32 // index of the OFS-delta base, or -1
	typ     plumbing.ObjectType
}

// refDelta is a REF-delta entry, found through its base's hash. A client that
// was offered ofs-delta rarely sends one.
type refDelta struct {
	base plumbing.Hash
	idx  int32
	done bool
}

// incomingPack is the index scanPack builds over the staged pack.
type incomingPack struct {
	f        io.ReaderAt
	end      int64 // where the trailer starts
	objs     []incomingObject
	hashSize int
	// hashes holds hashSize bytes per object. Only whole objects are filled
	// in: a delta's hash is known only once it is resolved, and it is used
	// straight away.
	hashes []byte

	// kidStart and kids list the OFS-delta children of each object, CSR
	// style: the children of i are kids[kidStart[i]:kidStart[i+1]].
	kidStart []int32
	kids     []int32

	refs []refDelta // sorted by base
}

// maxIncomingPrealloc caps the capacity reserved from the pack header's
// object count, which the client controls.
const maxIncomingPrealloc = 1 << 20

// packScanner is the reader under scanPack. It counts and checksums every
// byte it hands out, and it copies each byte to tee, so the staged file holds
// exactly the pack.
//
// It is an io.ByteReader, which is what keeps zlib from reading ahead: flate
// reads a ByteReader one byte at a time and stops at the end of its stream.
// Without that, the scanner would consume bytes past the end of the pack, and
// on a git:// or SSH socket it would block waiting for bytes the client never
// sends, because the client is waiting for report-status.
type packScanner struct {
	br  *bufio.Reader
	n   int64
	sum hash.Hash
	tee io.Writer
	err error // the first tee error

	// pend batches single bytes from ReadByte, which flate calls for nearly
	// every byte, so the checksum and the tee see large writes.
	pend []byte
}

func (s *packScanner) flush() {
	if len(s.pend) == 0 {
		return
	}
	s.sum.Write(s.pend)
	if s.tee != nil && s.err == nil {
		_, s.err = s.tee.Write(s.pend)
	}
	s.pend = s.pend[:0]
}

func (s *packScanner) ReadByte() (byte, error) {
	if s.err != nil {
		return 0, s.err
	}
	b, err := s.br.ReadByte()
	if err != nil {
		return 0, err
	}
	s.pend = append(s.pend, b)
	s.n++
	if len(s.pend) == cap(s.pend) {
		s.flush()
	}
	return b, nil
}

func (s *packScanner) Read(p []byte) (int, error) {
	s.flush()
	if s.err != nil {
		return 0, s.err
	}
	n, err := s.br.Read(p)
	if n > 0 {
		s.n += int64(n)
		s.sum.Write(p[:n])
		if s.tee != nil {
			if _, werr := s.tee.Write(p[:n]); werr != nil {
				s.err = werr
				return n, werr
			}
		}
	}
	return n, err
}

// scanPack reads exactly one packfile from r and indexes it. Every byte it
// reads is copied to tee, when tee is not nil. It stops at the end of the
// trailer and does not read further.
func scanPack(r io.Reader, tee io.Writer, of formatcfg.ObjectFormat, hashSize int) (*incomingPack, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, 64<<10)
	}
	s := &packScanner{br: br, tee: tee, pend: make([]byte, 0, 32<<10)}
	if of == formatcfg.SHA256 {
		s.sum = sha256.New()
	} else {
		s.sum = sha1.New()
	}

	var hdr [12]byte
	if _, err := io.ReadFull(s, hdr[:]); err != nil {
		return nil, fmt.Errorf("read pack header: %w", err)
	}
	if string(hdr[:4]) != "PACK" {
		return nil, fmt.Errorf("%w: bad signature", packfile.ErrMalformedPackfile)
	}
	if v := binary.BigEndian.Uint32(hdr[4:8]); v != 2 && v != 3 {
		return nil, fmt.Errorf("%w: unsupported version %d", packfile.ErrMalformedPackfile, v)
	}
	count := binary.BigEndian.Uint32(hdr[8:12])
	if count > 1<<31-1 {
		return nil, fmt.Errorf("%w: %d objects is too many", packfile.ErrMalformedPackfile, count)
	}

	n := min(int(count), maxIncomingPrealloc)
	p := &incomingPack{
		hashSize: hashSize,
		objs:     make([]incomingObject, 0, n),
		hashes:   make([]byte, 0, n*hashSize),
	}

	hs := plumbing.NewHasher(of, plumbing.BlobObject, 0)
	ref := make([]byte, hashSize)
	for range count {
		o, err := scanEntryHeader(s, p, ref)
		if err != nil {
			return nil, err
		}

		idx := int32(len(p.objs))
		p.hashes = append(p.hashes, make([]byte, hashSize)...)
		if o.typ == plumbing.REFDeltaObject {
			h, _ := plumbing.FromBytes(ref)
			p.refs = append(p.refs, refDelta{base: h, idx: idx})
		}

		var dst io.Writer = io.Discard
		if !o.typ.IsDelta() {
			hs.Reset(o.typ, o.size)
			dst = hs
		}
		if err := inflateTo(s, dst, o); err != nil {
			return nil, err
		}
		if !o.typ.IsDelta() {
			copy(p.hashes[int(idx)*hashSize:], hs.Sum().Bytes())
		}
		p.objs = append(p.objs, o)
	}

	s.flush()
	if s.err != nil {
		return nil, s.err
	}
	p.end = s.n
	want := s.sum.Sum(nil)
	got := make([]byte, len(want))
	if _, err := io.ReadFull(s.br, got); err != nil {
		return nil, fmt.Errorf("read pack trailer: %w", err)
	}
	if tee != nil {
		if _, err := tee.Write(got); err != nil {
			return nil, err
		}
	}
	if !bytes.Equal(got, want) {
		return nil, fmt.Errorf("%w: pack checksum mismatch", packfile.ErrMalformedPackfile)
	}

	p.linkKids()
	sort.Slice(p.refs, func(i, j int) bool {
		return bytes.Compare(p.refs[i].base.Bytes(), p.refs[j].base.Bytes()) < 0
	})
	return p, nil
}

// scanEntryHeader reads one entry's header: its type and size, then the base
// reference of a delta. A REF-delta's base hash lands in ref.
func scanEntryHeader(s *packScanner, p *incomingPack, ref []byte) (incomingObject, error) {
	o := incomingObject{offset: s.n, base: -1}

	c, err := s.ReadByte()
	if err != nil {
		return o, fmt.Errorf("read entry header at %d: %w", o.offset, err)
	}
	o.typ = plumbing.ObjectType((c >> 4) & 7)
	size := uint64(c & 0x0f)
	for shift := uint(4); c&0x80 != 0; shift += 7 {
		if shift > 57 {
			return o, fmt.Errorf("%w: entry size at %d overflows", packfile.ErrMalformedPackfile, o.offset)
		}
		if c, err = s.ReadByte(); err != nil {
			return o, fmt.Errorf("read entry header at %d: %w", o.offset, err)
		}
		size |= uint64(c&0x7f) << shift
	}
	if size > 1<<62 {
		return o, fmt.Errorf("%w: entry size at %d overflows", packfile.ErrMalformedPackfile, o.offset)
	}
	o.size = int64(size)

	switch o.typ {
	case plumbing.CommitObject, plumbing.TreeObject, plumbing.BlobObject, plumbing.TagObject:
	case plumbing.OFSDeltaObject:
		// git's offset encoding adds one before each shift, so every length
		// has exactly one encoding.
		if c, err = s.ReadByte(); err != nil {
			return o, err
		}
		dist := int64(c & 0x7f)
		for c&0x80 != 0 {
			if dist > 1<<55 {
				return o, fmt.Errorf("%w: ofs-delta distance at %d overflows", packfile.ErrMalformedPackfile, o.offset)
			}
			if c, err = s.ReadByte(); err != nil {
				return o, err
			}
			dist = (dist+1)<<7 | int64(c&0x7f)
		}
		if dist <= 0 || dist > o.offset {
			return o, fmt.Errorf("%w: invalid ofs-delta distance at %d", packfile.ErrMalformedPackfile, o.offset)
		}
		// Entries arrive in offset order, so the base is already in objs, and
		// a binary search finds it without a map.
		at := o.offset - dist
		j := sort.Search(len(p.objs), func(k int) bool { return p.objs[k].offset >= at })
		if j == len(p.objs) || p.objs[j].offset != at {
			return o, fmt.Errorf("%w: ofs-delta at %d names no entry at %d", packfile.ErrMalformedPackfile, o.offset, at)
		}
		o.base = int32(j)
	case plumbing.REFDeltaObject:
		if _, err := io.ReadFull(s, ref); err != nil {
			return o, err
		}
	default:
		return o, fmt.Errorf("%w: invalid object type %d at %d", packfile.ErrMalformedPackfile, o.typ, o.offset)
	}

	o.content = s.n
	return o, nil
}

// inflateTo inflates entry o from r into dst, and fails unless the stream
// holds exactly the size the header declared. Reading to the zlib EOF also
// consumes the stream's Adler-32 trailer, so r is left at the next entry.
func inflateTo(r io.Reader, dst io.Writer, o incomingObject) error {
	zr, err := getInflater(r)
	if err != nil {
		return fmt.Errorf("inflate entry at %d: %w", o.offset, err)
	}
	defer zr.Close()

	n, err := io.Copy(dst, io.LimitReader(zr, o.size+1))
	if err != nil {
		return fmt.Errorf("inflate entry at %d: %w", o.offset, err)
	}
	if n != o.size {
		return fmt.Errorf("%w: entry at %d inflates to %d bytes or more, header says %d",
			packfile.ErrMalformedPackfile, o.offset, n, o.size)
	}
	return nil
}

func (p *incomingPack) linkKids() {
	p.kidStart = make([]int32, len(p.objs)+1)
	for _, o := range p.objs {
		if o.base >= 0 {
			p.kidStart[o.base+1]++
		}
	}
	for i := 1; i < len(p.kidStart); i++ {
		p.kidStart[i] += p.kidStart[i-1]
	}
	p.kids = make([]int32, p.kidStart[len(p.objs)])
	fill := make([]int32, len(p.objs))
	for i, o := range p.objs {
		if o.base >= 0 {
			p.kids[p.kidStart[o.base]+fill[o.base]] = int32(i)
			fill[o.base]++
		}
	}
}

func (p *incomingPack) hash(i int32) plumbing.Hash {
	h, _ := plumbing.FromBytes(p.hashes[int(i)*p.hashSize : int(i+1)*p.hashSize])
	return h
}

// children returns every delta built on i, whose hash is h: its OFS-delta
// children, then any REF-delta that names h and is not yet resolved. The REF
// entries are marked done here, so a duplicate base cannot resolve them
// twice. An i of -1 is a base from outside the pack.
func (p *incomingPack) children(i int32, h plumbing.Hash) []int32 {
	var out []int32
	if i >= 0 {
		out = append(out, p.kids[p.kidStart[i]:p.kidStart[i+1]]...)
	}
	if len(p.refs) == 0 {
		return out
	}
	k := sort.Search(len(p.refs), func(k int) bool { return p.refs[k].base.Compare(h.Bytes()) >= 0 })
	for ; k < len(p.refs) && p.refs[k].base == h; k++ {
		if !p.refs[k].done {
			p.refs[k].done = true
			out = append(out, p.refs[k].idx)
		}
	}
	return out
}

// section is the zlib stream of entry i. It ends at the next entry, or at the
// trailer for the last one.
func (p *incomingPack) section(i int32) *io.SectionReader {
	end := p.end
	if int(i)+1 < len(p.objs) {
		end = p.objs[i+1].offset
	}
	o := p.objs[i]
	return io.NewSectionReader(p.f, o.content, end-o.content)
}

// open streams entry i's inflated body, for a whole object that nothing
// deltas against and that therefore never needs to be anywhere but the
// container.
func (p *incomingPack) open(i int32) (io.ReadCloser, error) {
	zr, err := getInflater(p.section(i))
	if err != nil {
		return nil, fmt.Errorf("inflate entry at %d: %w", p.objs[i].offset, err)
	}
	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(zr, p.objs[i].size), zr}, nil
}

// inflater is a zlib reader and the buffer under it, pooled because every
// object in a push is inflated at least twice and a fresh reader allocates a
// 32 KiB window each time.
type inflater struct {
	br *bufio.Reader
	zr io.ReadCloser
}

var inflaterPool sync.Pool

// getInflater reads a zlib stream from r. A reader that is an io.ByteReader
// is used as is, so the stream is not read past its end; any other reader
// gets a buffer, which can read ahead.
func getInflater(r io.Reader) (*inflater, error) {
	src := r
	v, _ := inflaterPool.Get().(*inflater)
	if v == nil {
		v = &inflater{br: bufio.NewReaderSize(nil, 32<<10)}
	}
	if _, ok := r.(io.ByteReader); !ok {
		v.br.Reset(r)
		src = v.br
	}
	if v.zr == nil {
		zr, err := zlib.NewReader(src)
		if err != nil {
			return nil, err
		}
		v.zr = zr
		return v, nil
	}
	if err := v.zr.(zlib.Resetter).Reset(src, nil); err != nil {
		return nil, err
	}
	return v, nil
}

func (in *inflater) Read(p []byte) (int, error) { return in.zr.Read(p) }

// Close returns the inflater to the pool. It must not be used after.
func (in *inflater) Close() error {
	err := in.zr.Close()
	in.br.Reset(nil)
	inflaterPool.Put(in)
	return err
}

// payloadObject is the plumbing.EncodedObject that packSegment.add copies
// from. The segment needs only a size and a reader. hash is there so its
// error messages name the object.
type payloadObject struct {
	hash plumbing.Hash
	typ  plumbing.ObjectType
	size int64
	open func() (io.ReadCloser, error)
}

func (o *payloadObject) Hash() plumbing.Hash            { return o.hash }
func (o *payloadObject) Type() plumbing.ObjectType      { return o.typ }
func (o *payloadObject) SetType(t plumbing.ObjectType)  { o.typ = t }
func (o *payloadObject) Size() int64                    { return o.size }
func (o *payloadObject) SetSize(n int64)                { o.size = n }
func (o *payloadObject) Reader() (io.ReadCloser, error) { return o.open() }
func (o *payloadObject) Writer() (io.WriteCloser, error) {
	return nil, errors.New("tigris: payload objects are read-only")
}

// The resolver's memory bound. A body stays in memory when it is at most
// bodyMemLimit and the bodies already in memory leave room for it under
// bodyMemBudget. Everything else goes to a temp file. The budget covers
// every body alive at once, which is what keeps a branching delta tree, or
// a chain of large objects, from adding up.
//
// Variables so a test can lower them; treat them as constants everywhere
// else.
var (
	bodyMemLimit  int64 = 8 << 20  // 8 MiB
	bodyMemBudget int64 = 64 << 20 // 64 MiB
)

// body is an object, or a delta, while the resolver needs it: in memory, or
// in a temp file.
type body struct {
	mem  []byte
	f    *os.File
	size int64
}

func (b *body) readerAt() io.ReaderAt {
	if b.f != nil {
		return b.f
	}
	return bytes.NewReader(b.mem)
}

func (b *body) open() (io.ReadCloser, error) {
	return io.NopCloser(io.NewSectionReader(b.readerAt(), 0, b.size)), nil
}

// bodySink collects one body. It starts in memory and moves to a temp file
// as soon as the body passes what the memory bound admits. It never trusts a
// size hint, because every hint comes from the client.
type bodySink struct {
	r     *resolver
	limit int64 // the most this body may hold in memory
	buf   []byte
	f     *os.File
	w     *bufio.Writer
	n     int64
}

func (r *resolver) newSink(hint int64) *bodySink {
	limit := min(bodyMemLimit, bodyMemBudget-r.mem)
	s := &bodySink{r: r, limit: limit}
	if hint <= limit {
		s.buf = make([]byte, 0, max(hint, 0))
	}
	return s
}

func (s *bodySink) Write(p []byte) (int, error) {
	if s.f == nil && int64(len(s.buf)+len(p)) > s.limit {
		f, err := os.CreateTemp(s.r.dir, "body-*")
		if err != nil {
			return 0, fmt.Errorf("spill object body: %w", err)
		}
		s.f = f
		s.w = bufio.NewWriterSize(f, 256<<10)
		if _, err := s.w.Write(s.buf); err != nil {
			return 0, err
		}
		s.buf = nil
	}
	s.n += int64(len(p))
	if s.f != nil {
		return s.w.Write(p)
	}
	s.buf = append(s.buf, p...)
	return len(p), nil
}

func (s *bodySink) finish() (*body, error) {
	if s.f != nil {
		if err := s.w.Flush(); err != nil {
			return nil, fmt.Errorf("spill object body: %w", err)
		}
		return &body{f: s.f, size: s.n}, nil
	}
	s.r.mem += int64(len(s.buf))
	return &body{mem: s.buf, size: s.n}, nil
}

// release frees a body. It is safe on nil and on a body already released.
func (r *resolver) release(b *body) {
	if b == nil {
		return
	}
	if b.f != nil {
		b.f.Close()
		os.Remove(b.f.Name())
		b.f = nil
	}
	if b.mem != nil {
		r.mem -= int64(len(b.mem))
		b.mem = nil
	}
}

// external resolves the base of a REF-delta that the pack does not carry. A
// client should not send one, because receive-pack advertises no-thin, but
// the base can still be in the repository already.
type externalBase func(plumbing.Hash) (plumbing.EncodedObject, error)

// resolver writes a scanned pack into containers, base before delta.
type resolver struct {
	w        *packWriter
	p        *incomingPack
	of       formatcfg.ObjectFormat
	external externalBase
	dir      string // the temp directory spilled bodies go to

	mem int64 // bytes of body held in memory right now

	byteLimit int64
	seg       *packSegment
	// serial numbers the container being built. An object's delta form is
	// legal only when its base went into the same serial.
	serial int32
}

// run resolves and writes every object in the pack.
func (r *resolver) run() error {
	p := r.p
	for i := range p.objs {
		if p.objs[i].typ.IsDelta() {
			continue
		}
		if err := r.root(int32(i)); err != nil {
			return err
		}
	}

	// Any REF-delta still pending names a base outside the pack.
	for k := range p.refs {
		if p.refs[k].done {
			continue
		}
		h := p.refs[k].base
		if r.external == nil {
			return fmt.Errorf("%w: ref-delta base %s is not in the pack", plumbing.ErrObjectNotFound, h)
		}
		obj, err := r.external(h)
		if err != nil {
			return fmt.Errorf("resolve ref-delta base %s: %w", h, err)
		}
		b, err := r.readBody(obj)
		if err != nil {
			return fmt.Errorf("resolve ref-delta base %s: %w", h, err)
		}
		// The base is not in any container this push builds, so serial -1
		// keeps every child in its whole form.
		if err := r.walk(p.children(-1, h), h, obj.Type(), b, -1, 0, 1); err != nil {
			return err
		}
	}
	return nil
}

func (r *resolver) readBody(obj plumbing.EncodedObject) (*body, error) {
	rd, err := obj.Reader()
	if err != nil {
		return nil, err
	}
	defer rd.Close()
	sink := r.newSink(obj.Size())
	if _, err := io.Copy(sink, rd); err != nil {
		return nil, err
	}
	return sink.finish()
}

// inflateBody reads entry i's whole inflated body into a body.
func (r *resolver) inflateBody(i int32) (*body, error) {
	o := r.p.objs[i]
	sink := r.newSink(o.size)
	if err := inflateTo(r.p.section(i), sink, o); err != nil {
		return nil, err
	}
	return sink.finish()
}

func (r *resolver) root(i int32) error {
	p := r.p
	o := p.objs[i]
	h := p.hash(i)
	kids := p.children(i, h)

	if len(kids) == 0 {
		// Nothing needs this body as a base, so it streams from the pack file
		// into the container.
		so := storedObject{
			payload: &payloadObject{hash: h, typ: o.typ, size: o.size, open: func() (io.ReadCloser, error) { return p.open(i) }},
			hash:    h, typ: o.typ, raw: o.size,
		}
		_, _, err := r.place(so, nil, plumbing.ZeroHash, -1, 0)
		return err
	}

	b, err := r.inflateBody(i)
	if err != nil {
		return err
	}
	so := storedObject{
		payload: &payloadObject{hash: h, typ: o.typ, size: b.size, open: b.open},
		hash:    h, typ: o.typ, raw: o.size,
	}
	serial, _, err := r.place(so, nil, plumbing.ZeroHash, -1, 0)
	if err != nil {
		r.release(b)
		return err
	}
	return r.walk(kids, h, o.typ, b, serial, 0, 1)
}

// walk resolves kids, which are all deltas against base, and then everything
// built on each of them. It owns base and releases it.
//
// baseChain is how many stored delta links sit under the base: 0 when the
// base is stored whole. depth is the client's chain depth, which can be
// deeper, because place stores some objects whole.
func (r *resolver) walk(kids []int32, baseHash plumbing.Hash, typ plumbing.ObjectType, base *body, baseSerial int32, baseChain, depth int) error {
	defer r.release(base)
	if depth > maxIncomingDeltaDepth {
		return fmt.Errorf("%w: delta chain deeper than %d", packfile.ErrMalformedPackfile, maxIncomingDeltaDepth)
	}

	for n, c := range kids {
		delta, err := r.inflateBody(c)
		if err != nil {
			return err
		}
		out, h, err := r.apply(typ, base, delta)
		if err != nil {
			r.release(delta)
			return fmt.Errorf("apply delta at %d: %w", r.p.objs[c].offset, err)
		}
		if n == len(kids)-1 {
			// The last child is the base's last use. Free it before going
			// deeper, so a linear chain holds two bodies and not the chain.
			r.release(base)
		}

		so := storedObject{
			payload: &payloadObject{hash: h, typ: typ, size: out.size, open: out.open},
			hash:    h, typ: typ, raw: out.size,
		}
		serial, chain, err := r.place(so, delta, baseHash, baseSerial, baseChain)
		r.release(delta)
		if err != nil {
			r.release(out)
			return err
		}

		grand := r.p.children(c, h)
		if len(grand) == 0 {
			r.release(out)
			continue
		}
		if err := r.walk(grand, h, typ, out, serial, chain, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// maxIncomingDeltaDepth matches git's own ceiling on a delta chain,
// (1 << OE_DEPTH_BITS) - 1 in pack-objects.
const maxIncomingDeltaDepth = 4095

// place adds one object to the container being built, sealing it first when
// the object would push it past the byte cap. It returns the serial of the
// container the object went into, and the number of stored delta links under
// it: 0 when it is stored whole.
//
// delta is the client's delta against baseHash, or nil. It is stored instead
// of the whole object only when all three hold:
//
//   - the base went into this same container, so a read never chases a base
//     into another container;
//   - it is smaller than the object, so a read never pays a base fetch for no
//     bytes back;
//   - the stored chain stays within maxDeltaDepth, the longest chain a read
//     walks. A client can send chains up to 4095 links.
func (r *resolver) place(so storedObject, delta *body, baseHash plumbing.Hash, baseSerial int32, baseChain int) (int32, int, error) {
	// The cap seals before the add: a container at 127 MiB must not swallow
	// a 500 MiB blob. The len(recs) guard lets an object bigger than the
	// whole cap have a container to itself.
	//
	// seg.offset counts stored bytes and so.raw is the raw size. The codec
	// policy guarantees stored <= raw, so so.raw is a safe upper bound.
	if r.seg != nil && len(r.seg.recs) > 0 && r.seg.offset+so.raw > r.byteLimit {
		full := r.seg
		r.seg = nil // ownership moves to seal
		if err := r.w.seal(full); err != nil {
			return 0, 0, err
		}
	}
	if r.seg == nil {
		seg, err := newPackSegment(r.w.s)
		if err != nil {
			return 0, 0, err
		}
		r.seg = seg
		r.serial++
	}

	chain := 0
	if delta != nil && baseSerial == r.serial && delta.size < so.raw && baseChain < maxDeltaDepth {
		so.payload = &payloadObject{hash: so.hash, typ: plumbing.REFDeltaObject, size: delta.size, open: delta.open}
		so.base = baseHash
		chain = baseChain + 1
	}
	if err := r.seg.add(so); err != nil {
		return 0, 0, err
	}
	return r.serial, chain, nil
}

// apply rebuilds an object of type typ from base and a git delta, and hashes
// it on the way. It reads base through io.ReaderAt, so a base on disk is never
// loaded whole.
//
// The format is git's: the source and target sizes as little-endian base-128
// numbers, then commands. A command with the high bit set copies a range of
// the base; one without it inserts that many literal bytes from the delta.
func (r *resolver) apply(typ plumbing.ObjectType, base, delta *body) (*body, plumbing.Hash, error) {
	drc, _ := delta.open()
	defer drc.Close()
	d := bufio.NewReaderSize(drc, 64<<10)

	src, err := readDeltaSize(d)
	if err != nil {
		return nil, plumbing.ZeroHash, err
	}
	if int64(src) != base.size {
		return nil, plumbing.ZeroHash, packfile.ErrInvalidDelta
	}
	tgt, err := readDeltaSize(d)
	if err != nil {
		return nil, plumbing.ZeroHash, err
	}
	if tgt > 1<<62 {
		return nil, plumbing.ZeroHash, packfile.ErrInvalidDelta
	}

	sink := r.newSink(int64(tgt))
	hs := plumbing.NewHasher(r.of, typ, int64(tgt))
	out := bufio.NewWriterSize(io.MultiWriter(sink, hs), 64<<10)
	fail := func(err error) (*body, plumbing.Hash, error) {
		if b, ferr := sink.finish(); ferr == nil {
			r.release(b)
		}
		return nil, plumbing.ZeroHash, err
	}

	ra := base.readerAt()
	remaining := tgt
	for remaining > 0 {
		cmd, err := d.ReadByte()
		if err != nil {
			return fail(packfile.ErrInvalidDelta)
		}
		switch {
		case cmd&0x80 != 0:
			var off, sz uint64
			for k := range 4 {
				if cmd&(1<<k) != 0 {
					b, err := d.ReadByte()
					if err != nil {
						return fail(packfile.ErrInvalidDelta)
					}
					off |= uint64(b) << (8 * k)
				}
			}
			for k := range 3 {
				if cmd&(0x10<<k) != 0 {
					b, err := d.ReadByte()
					if err != nil {
						return fail(packfile.ErrInvalidDelta)
					}
					sz |= uint64(b) << (8 * k)
				}
			}
			if sz == 0 {
				sz = 0x10000
			}
			if sz > remaining || off+sz > src {
				return fail(packfile.ErrInvalidDelta)
			}
			if _, err := io.Copy(out, io.NewSectionReader(ra, int64(off), int64(sz))); err != nil {
				return fail(err)
			}
			remaining -= sz
		case cmd != 0:
			sz := uint64(cmd)
			if sz > remaining {
				return fail(packfile.ErrInvalidDelta)
			}
			if _, err := io.CopyN(out, d, int64(sz)); err != nil {
				return fail(packfile.ErrInvalidDelta)
			}
			remaining -= sz
		default:
			return fail(packfile.ErrDeltaCmd)
		}
	}
	// Every byte of the delta must be used, as git requires.
	if _, err := d.ReadByte(); err != io.EOF {
		return fail(packfile.ErrInvalidDelta)
	}
	if err := out.Flush(); err != nil {
		return fail(err)
	}
	b, err := sink.finish()
	if err != nil {
		return nil, plumbing.ZeroHash, err
	}
	return b, hs.Sum(), nil
}

// readDeltaSize reads one of a delta header's two sizes.
func readDeltaSize(d io.ByteReader) (uint64, error) {
	var n uint64
	for shift := uint(0); ; shift += 7 {
		if shift > 63 {
			return 0, packfile.ErrInvalidDelta
		}
		b, err := d.ReadByte()
		if err != nil {
			return 0, packfile.ErrInvalidDelta
		}
		n |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return n, nil
		}
	}
}
