package tigris

import (
	"bytes"
	"slices"
	"sort"

	"github.com/go-git/go-git/v6/plumbing"
)

// A packTable is part of the packIndex: the records of one or more packs,
// sorted by hash. It replaces a map[plumbing.Hash]packEntry, which cost about
// 210 bytes for each object. A table costs hashSize bytes and one 40-byte
// tableRec for each object, which is about 60 bytes with SHA-1.
//
// A table never changes after newPackTable or mergeTables builds it. An
// iterator can therefore hold a table without a lock, and a merge builds a new
// table and does not edit the old ones.
type packTable struct {
	hashSize int
	hashes   []byte // hashSize bytes for each record, sorted
	recs     []tableRec

	// farBase holds the base of a delta whose base is not in this table. The
	// writer keeps a base and its delta in the same container, so this map is
	// almost always empty.
	farBase map[int32]plumbing.Hash

	// pack is the one pack number that every record has, or -1 when the
	// table holds more than one pack. deregister can drop a table with one
	// pack. It must mark the pack dead in a table with more.
	pack int64
}

// tableRec is one object of a packTable. It is a packEntry without the two
// wide fields: the pack id is a number into packIndex.ids, and the base is a
// record number.
type tableRec struct {
	offset int64
	stored int64
	raw    int64
	base   int32 // record number of the delta base, or baseNone, or baseFar
	pack   uint32
	typ    plumbing.ObjectType
	codec  uint8
}

const (
	baseNone int32 = -1 // the payload is the whole object
	baseFar  int32 = -2 // the base is in farBase
)

func (t *packTable) len() int { return len(t.recs) }

func (t *packTable) hashAt(i int) []byte {
	return t.hashes[i*t.hashSize : (i+1)*t.hashSize]
}

func (t *packTable) hash(i int) plumbing.Hash {
	h, _ := plumbing.FromBytes(t.hashAt(i))
	return h
}

// find returns the record number of h, or -1.
func (t *packTable) find(h []byte) int {
	i := sort.Search(len(t.recs), func(i int) bool { return bytes.Compare(t.hashAt(i), h) >= 0 })
	if i < len(t.recs) && bytes.Equal(t.hashAt(i), h) {
		return i
	}
	return -1
}

// baseHash returns the base of record i, or the zero hash for a whole object.
func (t *packTable) baseHash(i int) plumbing.Hash {
	switch b := t.recs[i].base; b {
	case baseNone:
		return plumbing.ZeroHash
	case baseFar:
		return t.farBase[int32(i)]
	default:
		return t.hash(int(b))
	}
}

// entry builds the packEntry of record i. ids maps a pack number to its id.
func (t *packTable) entry(i int, ids []string) packEntry {
	r := t.recs[i]
	return packEntry{
		id:     ids[r.pack],
		typ:    r.typ,
		codec:  r.codec,
		offset: r.offset,
		stored: r.stored,
		raw:    r.raw,
		base:   t.baseHash(i),
	}
}

// tableSource is one record for buildTable: its hash, its record with the
// base not yet numbered, and the hash of its base.
type tableSource struct {
	hash []byte
	rec  tableRec
	base plumbing.Hash
}

// buildTable makes a table from n records. hashOf returns the hash of record
// i, and at returns all of it. The records can come in any order, and a hash
// can repeat. For a repeated hash, the last record wins, as it did when a
// later indexRecords call overwrote a map key.
func buildTable(hashSize, n int, hashOf func(i int) []byte, at func(i int) tableSource) *packTable {
	order := make([]int32, n)
	for i := range order {
		order[i] = int32(i)
	}
	// A stable sort keeps repeated hashes in input order, so the dedupe below
	// can keep the last one.
	slices.SortStableFunc(order, func(a, b int32) int {
		return bytes.Compare(hashOf(int(a)), hashOf(int(b)))
	})

	t := &packTable{
		hashSize: hashSize,
		hashes:   make([]byte, 0, n*hashSize),
		recs:     make([]tableRec, 0, n),
		pack:     -1,
	}
	src := make([]int32, 0, n) // input index of each kept record
	for k, i := range order {
		if k+1 < len(order) && bytes.Equal(hashOf(int(i)), hashOf(int(order[k+1]))) {
			continue // a later record with the same hash wins
		}
		s := at(int(i))
		t.hashes = append(t.hashes, s.hash...)
		t.recs = append(t.recs, s.rec)
		src = append(src, i)
	}

	for k := range t.recs {
		b := at(int(src[k])).base
		if b == plumbing.ZeroHash {
			t.recs[k].base = baseNone
			continue
		}
		if j := t.find(b.Bytes()); j >= 0 {
			t.recs[k].base = int32(j)
			continue
		}
		if t.farBase == nil {
			t.farBase = make(map[int32]plumbing.Hash)
		}
		t.recs[k].base = baseFar
		t.farBase[int32(k)] = b
	}

	for k, r := range t.recs {
		switch {
		case k == 0:
			t.pack = int64(r.pack)
		case t.pack != int64(r.pack):
			t.pack = -1
		}
	}
	return t
}

// newPackTable makes a table from the cue records of one pack.
func newPackTable(hashSize int, pack uint32, recs []cueRecord) *packTable {
	// ObjectID.Bytes copies, so copy every hash once rather than once for
	// each comparison of the sort.
	flat := make([]byte, 0, len(recs)*hashSize)
	for _, r := range recs {
		flat = append(flat, r.hash.Bytes()...)
	}
	hashOf := func(i int) []byte { return flat[i*hashSize : (i+1)*hashSize] }
	return buildTable(hashSize, len(recs), hashOf, func(i int) tableSource {
		r := recs[i]
		return tableSource{
			hash: hashOf(i),
			rec: tableRec{
				offset: r.offset,
				stored: r.stored,
				raw:    r.raw,
				pack:   pack,
				typ:    r.typ,
				codec:  r.codec,
			},
			base: r.base,
		}
	})
}

// mergeTables makes one table from tables, and drops every record whose pack
// is in dead. When a hash is in more than one table, the record from the
// later table wins, as it does in packIndex.lookup.
func mergeTables(hashSize int, tables []*packTable, dead map[uint32]struct{}) *packTable {
	type ref struct {
		t int32
		i int32
	}
	var refs []ref
	for ti, t := range tables {
		for i, r := range t.recs {
			if _, gone := dead[r.pack]; gone {
				continue
			}
			refs = append(refs, ref{int32(ti), int32(i)})
		}
	}
	hashOf := func(k int) []byte { return tables[refs[k].t].hashAt(int(refs[k].i)) }
	return buildTable(hashSize, len(refs), hashOf, func(k int) tableSource {
		t := tables[refs[k].t]
		i := int(refs[k].i)
		return tableSource{hash: t.hashAt(i), rec: t.recs[i], base: t.baseHash(i)}
	})
}
