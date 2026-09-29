package tigris

import (
	"crypto/sha1"
	"fmt"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

// th makes a distinct SHA-1 hash from a label.
func th(label string) plumbing.Hash {
	sum := sha1.Sum([]byte(label))
	h, _ := plumbing.FromBytes(sum[:])
	return h
}

// rec makes a cue record for th(label) at offset, as a delta against base
// when base is not empty.
func rec(label string, offset int64, base string) cueRecord {
	r := cueRecord{hash: th(label), typ: plumbing.BlobObject, offset: offset, stored: 10, raw: 10}
	if base != "" {
		r.base = th(base)
	}
	return r
}

// indexOp is one step against a packIndex.
type indexOp struct {
	register   string // pack id to register with recs
	recs       []cueRecord
	deregister string // pack id to deregister
}

// named registers n packs of one object each, called prefix00, prefix01, and
// so on.
func named(prefix string, n int) []indexOp {
	var ops []indexOp
	for i := range n {
		id := fmt.Sprintf("%s%02d", prefix, i)
		ops = append(ops, indexOp{register: id, recs: []cueRecord{rec(id, 0, "")}})
	}
	return ops
}

func TestPackIndexTables(t *testing.T) {
	// many registers packs p00 onward, enough of them to force merges.
	many := func(n int) []indexOp { return named("p", n) }

	type want struct {
		label  string
		pack   string // "" means the hash must be missing
		offset int64
		base   string
	}
	tests := []struct {
		name       string
		ops        []indexOp
		want       []want
		wantTables int      // 0 skips the check
		wantOrder  []string // labels in iteration order, after the dedupe
	}{
		{
			name: "one pack resolves bases inside the table",
			ops: []indexOp{{register: "a", recs: []cueRecord{
				rec("x", 0, ""), rec("y", 10, "x"), rec("z", 20, "y"),
			}}},
			want: []want{
				{label: "x", pack: "a", offset: 0},
				{label: "y", pack: "a", offset: 10, base: "x"},
				{label: "z", pack: "a", offset: 20, base: "y"},
				{label: "nope"},
			},
			wantTables: 1,
			wantOrder:  []string{"x", "y", "z"},
		},
		{
			name: "a base outside the table goes to farBase",
			ops: []indexOp{
				{register: "a", recs: []cueRecord{rec("x", 0, "")}},
				{register: "b", recs: []cueRecord{rec("y", 0, "x")}},
			},
			want: []want{
				{label: "x", pack: "a"},
				{label: "y", pack: "b", base: "x"},
			},
			wantOrder: []string{"x", "y"},
		},
		{
			name: "the newest pack wins a repeated hash",
			ops: []indexOp{
				{register: "a", recs: []cueRecord{rec("x", 0, ""), rec("only-a", 10, "")}},
				{register: "b", recs: []cueRecord{rec("x", 50, "")}},
			},
			want: []want{
				{label: "x", pack: "b", offset: 50},
				{label: "only-a", pack: "a", offset: 10},
			},
			// x is yielded once, from pack b.
			wantOrder: []string{"only-a", "x"},
		},
		{
			name: "deregister drops a table of one pack",
			ops: []indexOp{
				{register: "a", recs: []cueRecord{rec("x", 0, "")}},
				{register: "b", recs: []cueRecord{rec("x", 50, ""), rec("y", 60, "")}},
				{deregister: "b"},
			},
			want: []want{
				{label: "x", pack: "a", offset: 0},
				{label: "y"},
			},
			wantTables: 1,
			wantOrder:  []string{"x"},
		},
		{
			name: "more than maxPackTables merges into one table",
			ops:  many(maxPackTables + 1),
			want: []want{
				{label: "p00", pack: "p00"},
				{label: fmt.Sprintf("p%02d", maxPackTables), pack: fmt.Sprintf("p%02d", maxPackTables)},
			},
			wantTables: 1,
		},
		{
			name: "deregister after a merge marks the pack dead",
			ops:  append(many(maxPackTables+1), indexOp{deregister: "p03"}),
			want: []want{
				{label: "p03"},
				{label: "p04", pack: "p04"},
			},
			wantTables: 1,
		},
		{
			name: "a merge drops the records of a dead pack",
			ops: append(append(many(maxPackTables+1), indexOp{deregister: "p03"}),
				named("q", maxPackTables)...),
			want: []want{
				{label: "p03"},
				{label: "p05", pack: "p05"},
			},
			wantTables: 1,
		},
		{
			name: "a pack written again after it failed is live",
			ops: append(append(many(maxPackTables+1), indexOp{deregister: "p03"}),
				indexOp{register: "p03", recs: []cueRecord{rec("p03", 7, "")}}),
			want: []want{
				{label: "p03", pack: "p03", offset: 7},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newPackIndex(20)
			for _, op := range tt.ops {
				switch {
				case op.register != "":
					p.register(op.register, op.recs, "")
				case op.deregister != "":
					p.deregister(op.deregister)
				}
			}

			sn := p.snapshot()
			if tt.wantTables != 0 && len(sn.tables) != tt.wantTables {
				t.Errorf("got %d tables, want %d", len(sn.tables), tt.wantTables)
			}
			for _, w := range tt.want {
				ti, i, ok := sn.lookup(th(w.label).Bytes())
				if w.pack == "" {
					if ok {
						t.Errorf("%s: found in %s, want missing", w.label, sn.tables[ti].entry(i, sn.ids).id)
					}
					continue
				}
				if !ok {
					t.Errorf("%s: missing, want pack %s", w.label, w.pack)
					continue
				}
				e := sn.tables[ti].entry(i, sn.ids)
				if e.id != w.pack || e.offset != w.offset {
					t.Errorf("%s: got pack %s offset %d, want pack %s offset %d", w.label, e.id, e.offset, w.pack, w.offset)
				}
				wantBase := plumbing.ZeroHash
				if w.base != "" {
					wantBase = th(w.base)
				}
				if e.base != wantBase {
					t.Errorf("%s: got base %s, want %s", w.label, e.base, wantBase)
				}
			}

			if tt.wantOrder != nil {
				labels := map[plumbing.Hash]string{}
				for _, op := range tt.ops {
					for _, r := range op.recs {
						for _, l := range tt.wantOrder {
							if th(l) == r.hash {
								labels[r.hash] = l
							}
						}
					}
				}
				var got []string
				for _, ref := range sn.order() {
					hb := sn.tables[ref.t].hashAt(int(ref.i))
					if ti, i, _ := sn.lookup(hb); ti != int(ref.t) || i != int(ref.i) {
						continue
					}
					got = append(got, labels[sn.tables[ref.t].hash(int(ref.i))])
				}
				if fmt.Sprint(got) != fmt.Sprint(tt.wantOrder) {
					t.Errorf("got order %v, want %v", got, tt.wantOrder)
				}
			}
		})
	}
}
