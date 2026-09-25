package tigris

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
)

// entrySpec is one entry of a pack that buildTestPack writes by hand. Real
// git never sends some of these shapes, such as a REF-delta whose base comes
// later in the pack, so the tests cannot get them from the git CLI.
type entrySpec struct {
	typ  plumbing.ObjectType // the resolved type; blob when zero
	body []byte              // the resolved object

	ofs int           // for an OFS-delta: 1 + the index of the base entry
	ref plumbing.Hash // for a REF-delta: the base hash
	// base is the resolved body of the base, for a delta. The delta itself
	// copies all of base and then inserts the rest of body.
	base []byte

	// rawDelta, when set, is written as the delta instead of one built from
	// base and body.
	rawDelta []byte
	// declared, when not zero, replaces the inflated size in the header.
	declared int64
	// ofsDist, when not zero, replaces the OFS-delta distance.
	ofsDist int64
}

func (e entrySpec) objType() plumbing.ObjectType {
	if e.typ == 0 {
		return plumbing.BlobObject
	}
	return e.typ
}

func (e entrySpec) hash() plumbing.Hash {
	hs := plumbing.NewHasher(formatcfg.DefaultObjectFormat, e.objType(), int64(len(e.body)))
	hs.Write(e.body)
	return hs.Sum()
}

// testDelta builds a git delta that copies all of base and then inserts the
// rest of target. target must start with base.
func testDelta(t *testing.T, base, target []byte) []byte {
	t.Helper()
	if !bytes.HasPrefix(target, base) {
		t.Fatalf("testDelta: target does not start with base")
	}
	var d []byte
	putSize := func(n int) {
		for {
			b := byte(n & 0x7f)
			n >>= 7
			if n == 0 {
				d = append(d, b)
				return
			}
			d = append(d, b|0x80)
		}
	}
	putSize(len(base))
	putSize(len(target))

	// Copy commands, at most 0xffffff bytes each.
	for off := 0; off < len(base); {
		n := min(len(base)-off, 0xffffff)
		cmd := byte(0x80)
		var args []byte
		for k := range 4 {
			if b := byte(off >> (8 * k)); b != 0 {
				cmd |= 1 << k
				args = append(args, b)
			}
		}
		for k := range 3 {
			if b := byte(n >> (8 * k)); b != 0 {
				cmd |= 0x10 << k
				args = append(args, b)
			}
		}
		d = append(d, cmd)
		d = append(d, args...)
		off += n
	}
	// Insert commands, at most 127 bytes each.
	rest := target[len(base):]
	for len(rest) > 0 {
		n := min(len(rest), 127)
		d = append(d, byte(n))
		d = append(d, rest[:n]...)
		rest = rest[n:]
	}
	return d
}

func deflate(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// buildTestPack writes a version 2 packfile of entries, with a correct
// trailer.
func buildTestPack(t *testing.T, entries []entrySpec) []byte {
	t.Helper()
	var pack bytes.Buffer
	pack.WriteString("PACK")
	binary.Write(&pack, binary.BigEndian, uint32(2))
	binary.Write(&pack, binary.BigEndian, uint32(len(entries)))

	offsets := make([]int64, len(entries))
	for i, e := range entries {
		offsets[i] = int64(pack.Len())

		payload := e.body
		typ := e.objType()
		switch {
		case e.ofs != 0:
			typ = plumbing.OFSDeltaObject
		case e.ref != plumbing.ZeroHash:
			typ = plumbing.REFDeltaObject
		}
		if typ.IsDelta() {
			payload = e.rawDelta
			if payload == nil {
				payload = testDelta(t, e.base, e.body)
			}
		}

		size := int64(len(payload))
		if e.declared != 0 {
			size = e.declared
		}
		c := byte(typ)<<4 | byte(size&0x0f)
		size >>= 4
		for size != 0 {
			pack.WriteByte(c | 0x80)
			c = byte(size & 0x7f)
			size >>= 7
		}
		pack.WriteByte(c)

		switch typ {
		case plumbing.OFSDeltaObject:
			dist := offsets[i] - offsets[e.ofs-1]
			if e.ofsDist != 0 {
				dist = e.ofsDist
			}
			var enc [10]byte
			pos := len(enc) - 1
			enc[pos] = byte(dist & 0x7f)
			for dist >>= 7; dist != 0; dist >>= 7 {
				dist--
				pos--
				enc[pos] = 0x80 | byte(dist&0x7f)
			}
			pack.Write(enc[pos:])
		case plumbing.REFDeltaObject:
			pack.Write(e.ref.Bytes())
		}
		pack.Write(deflate(t, payload))
	}
	sum := sha1.Sum(pack.Bytes())
	pack.Write(sum[:])
	return pack.Bytes()
}

// pushPack stores pack through s, the way writePack in cmd/objgitd does
// when viaReadPack is true, and through Write when it is false.
func pushPack(t *testing.T, s *Storer, pack []byte, viaReadPack bool) error {
	t.Helper()
	w, err := s.PackfileWriter()
	if err != nil {
		t.Fatalf("PackfileWriter: %v", err)
	}
	if viaReadPack {
		if err := w.(*packWriter).ReadPack(bufio.NewReader(bytes.NewReader(pack))); err != nil {
			_ = w.Close()
			return err
		}
	} else if _, err := w.Write(pack); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		return err
	}
	return s.up.flush()
}

// verifyReadBack opens a new Storer over f and verifies that every entry reads
// back whole, with the right type and body.
func verifyReadBack(t *testing.T, f *fakeS3, prefix string, entries []entrySpec) *Storer {
	t.Helper()
	cold := newTestStorer(t, f).Scoped(prefix)
	for _, e := range entries {
		h := e.hash()
		obj, err := cold.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			t.Fatalf("EncodedObject(%s): %v", h, err)
		}
		if obj.Type() != e.objType() {
			t.Errorf("%s: type %s, want %s", h, obj.Type(), e.objType())
		}
		rd, err := obj.Reader()
		if err != nil {
			t.Fatalf("%s: Reader: %v", h, err)
		}
		got, err := io.ReadAll(rd)
		rd.Close()
		if err != nil {
			t.Fatalf("%s: read: %v", h, err)
		}
		if !bytes.Equal(got, e.body) {
			t.Errorf("%s: body of %d bytes differs from the %d bytes pushed", h, len(got), len(e.body))
		}
	}
	return cold
}

// storedChains reports how many records are stored as deltas, and the
// longest stored delta chain, over every table of s.
func storedChains(t *testing.T, s *Storer) (deltas, longest int) {
	t.Helper()
	if err := s.ensurePacksBuilt(); err != nil {
		t.Fatal(err)
	}
	sn := s.packs.snapshot()
	for _, tb := range sn.tables {
		for i := range tb.recs {
			n := 0
			for h := tb.baseHash(i); h != plumbing.ZeroHash; n++ {
				ti, j, ok := sn.lookup(h.Bytes())
				if !ok {
					t.Fatalf("base %s is not indexed", h)
				}
				h = sn.tables[ti].baseHash(j)
			}
			if n > 0 {
				deltas++
			}
			longest = max(longest, n)
		}
	}
	return deltas, longest
}

// chain returns n+1 entries: a whole blob, then n OFS-deltas, each one on
// the entry before it.
func chain(n int) []entrySpec {
	body := []byte("line 0\n")
	out := []entrySpec{{body: body}}
	for i := 1; i <= n; i++ {
		next := append(bytes.Clone(body), fmt.Sprintf("line %d\n", i)...)
		out = append(out, entrySpec{body: next, base: body, ofs: i})
		body = next
	}
	return out
}

func TestIndexPackRoundTrip(t *testing.T) {
	a := []byte(strings.Repeat("a base blob\n", 20))
	ab := append(bytes.Clone(a), "and a line on top\n"...)
	abc := append(bytes.Clone(ab), "and one more\n"...)
	big := bytes.Repeat([]byte("0123456789abcdef"), 1<<14) // 256 KiB
	bigger := append(bytes.Clone(big), "tail"...)

	blobA := entrySpec{body: a}

	tests := []struct {
		name       string
		existing   []entrySpec // pushed first, in a pack of its own
		entries    []entrySpec
		spill      bool // send every body to disk
		wantDeltas int  // stored delta records; -1 skips the check
		wantMaxLen int  // the longest stored chain may not be longer
	}{
		{
			name: "whole objects of every type",
			entries: []entrySpec{
				{body: []byte("a blob\n")},
				{typ: plumbing.TreeObject, body: []byte("not a real tree")},
				{typ: plumbing.CommitObject, body: []byte("not a real commit")},
				{typ: plumbing.TagObject, body: []byte("not a real tag")},
				{body: []byte{}},
			},
			wantDeltas: 0,
		},
		{
			name:       "ofs-delta chain",
			entries:    chain(5),
			wantDeltas: 5,
			wantMaxLen: 5,
		},
		{
			name: "ref-delta after its base",
			entries: []entrySpec{
				blobA,
				{body: ab, base: a, ref: blobA.hash()},
			},
			wantDeltas: 1,
			wantMaxLen: 1,
		},
		{
			name: "ref-delta before its base",
			entries: []entrySpec{
				{body: ab, base: a, ref: blobA.hash()},
				blobA,
			},
			wantDeltas: 1,
			wantMaxLen: 1,
		},
		{
			name: "ref-delta on a ref-delta",
			entries: []entrySpec{
				{body: abc, base: ab, ref: entrySpec{body: ab}.hash()},
				{body: ab, base: a, ref: blobA.hash()},
				blobA,
			},
			wantDeltas: 2,
			wantMaxLen: 2,
		},
		{
			name:     "ref-delta on a base already in the repository",
			existing: []entrySpec{blobA},
			entries: []entrySpec{
				{body: ab, base: a, ref: blobA.hash()},
			},
			// The base is in another container, so the delta is stored whole.
			wantDeltas: 0,
		},
		{
			name:       "a chain deeper than a read walks",
			entries:    chain(3*maxDeltaDepth + 7),
			wantDeltas: -1,
			wantMaxLen: maxDeltaDepth,
		},
		{
			name:       "a chain of exactly maxDeltaDepth links",
			entries:    chain(maxDeltaDepth),
			wantDeltas: maxDeltaDepth,
			wantMaxLen: maxDeltaDepth,
		},
		{
			name: "every body on disk",
			entries: append(chain(12),
				entrySpec{body: big},
				entrySpec{body: bigger, base: big, ofs: 14},
			),
			spill:      true,
			wantDeltas: 13,
			wantMaxLen: 12,
		},
	}

	for _, tt := range tests {
		for _, viaReadPack := range []bool{true, false} {
			name := tt.name + "/write"
			if viaReadPack {
				name = tt.name + "/readpack"
			}
			t.Run(name, func(t *testing.T) {
				if tt.spill {
					limit, budget := bodyMemLimit, bodyMemBudget
					bodyMemLimit, bodyMemBudget = 0, 0
					t.Cleanup(func() { bodyMemLimit, bodyMemBudget = limit, budget })
				}

				f := newFakeS3(t)
				s := newTestStorer(t, f).Scoped("org/repo")
				if tt.existing != nil {
					if err := pushPack(t, s, buildTestPack(t, tt.existing), viaReadPack); err != nil {
						t.Fatalf("push existing: %v", err)
					}
				}
				if err := pushPack(t, s, buildTestPack(t, tt.entries), viaReadPack); err != nil {
					t.Fatalf("push: %v", err)
				}

				cold := verifyReadBack(t, f, "org/repo", append(tt.existing, tt.entries...))
				deltas, longest := storedChains(t, cold)
				if tt.wantDeltas >= 0 && deltas != tt.wantDeltas {
					t.Errorf("got %d stored deltas, want %d", deltas, tt.wantDeltas)
				}
				if longest > tt.wantMaxLen {
					t.Errorf("longest stored chain is %d links, want at most %d", longest, tt.wantMaxLen)
				}
			})
		}
	}
}

func TestIndexPackRejects(t *testing.T) {
	a := []byte("a base blob\n")
	ab := append(bytes.Clone(a), "on top\n"...)
	good := func(t *testing.T) []byte {
		return buildTestPack(t, []entrySpec{{body: a}, {body: ab, base: a, ofs: 1}})
	}

	tests := []struct {
		name    string
		pack    func(t *testing.T) []byte
		wantErr error // nil only checks that there is an error
	}{
		{
			name: "bad signature",
			pack: func(t *testing.T) []byte {
				p := good(t)
				copy(p, "KCAP")
				return p
			},
			wantErr: packfile.ErrMalformedPackfile,
		},
		{
			name: "bad trailer checksum",
			pack: func(t *testing.T) []byte {
				p := good(t)
				p[len(p)-1] ^= 0xff
				return p
			},
			wantErr: packfile.ErrMalformedPackfile,
		},
		{
			name: "truncated",
			pack: func(t *testing.T) []byte {
				p := good(t)
				return p[:len(p)-30]
			},
		},
		{
			name: "ofs-delta names no entry",
			pack: func(t *testing.T) []byte {
				return buildTestPack(t, []entrySpec{{body: a}, {body: ab, base: a, ofs: 1, ofsDist: 3}})
			},
			wantErr: packfile.ErrMalformedPackfile,
		},
		{
			name: "entry shorter than its header says",
			pack: func(t *testing.T) []byte {
				return buildTestPack(t, []entrySpec{{body: a, declared: int64(len(a)) + 5}})
			},
			wantErr: packfile.ErrMalformedPackfile,
		},
		{
			name: "delta names the wrong base size",
			pack: func(t *testing.T) []byte {
				d := testDelta(t, a, ab)
				d[0]++ // the source size
				return buildTestPack(t, []entrySpec{{body: a}, {body: ab, ofs: 1, rawDelta: d}})
			},
			wantErr: packfile.ErrInvalidDelta,
		},
		{
			name: "ref-delta base missing",
			pack: func(t *testing.T) []byte {
				return buildTestPack(t, []entrySpec{{body: ab, base: a, ref: entrySpec{body: a}.hash()}})
			},
			wantErr: plumbing.ErrObjectNotFound,
		},
	}

	for _, tt := range tests {
		for _, viaReadPack := range []bool{true, false} {
			name := tt.name + "/write"
			if viaReadPack {
				name = tt.name + "/readpack"
			}
			t.Run(name, func(t *testing.T) {
				f := newFakeS3(t)
				s := newTestStorer(t, f).Scoped("org/repo")
				err := pushPack(t, s, tt.pack(t), viaReadPack)
				if err == nil {
					t.Fatal("push succeeded, want an error")
				}
				if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
					t.Errorf("error = %v, want %v", err, tt.wantErr)
				}
				if n := f.nputs(); n != 0 {
					t.Errorf("a rejected push made %d PutObject calls, want 0", n)
				}
			})
		}
	}
}

// TestReadPackStopsAtTrailer pins the property that writePack depends on for
// git:// and SSH: ReadPack consumes the pack and not one byte more.
func TestReadPackStopsAtTrailer(t *testing.T) {
	pack := buildTestPack(t, chain(3))
	after := []byte("0000 bytes the client sends after the pack")

	s := newTestStorer(t, newFakeS3(t)).Scoped("org/repo")
	w, err := s.PackfileWriter()
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(bytes.NewReader(append(bytes.Clone(pack), after...)))
	if err := w.(*packWriter).ReadPack(br); err != nil {
		t.Fatalf("ReadPack: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rest, err := io.ReadAll(br)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rest, after) {
		t.Errorf("left %q unread, want %q", rest, after)
	}
}
