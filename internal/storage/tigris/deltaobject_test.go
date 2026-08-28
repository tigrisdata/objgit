package tigris

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
)

// --- helpers -----------------------------------------------------------

// stageContainer puts one .bin/.cue pair straight into the fake bucket. Going
// through PackfileWriter instead would mean hand-rolling a git packfile just to
// exercise a read, and the read path only ever sees these two objects anyway.
func stageContainer(t *testing.T, f *fakeS3, hashLen int, bin []byte, recs []cueRecord) {
	t.Helper()
	sum := sha256.Sum256(bin)
	id := hex.EncodeToString(sum[:])
	f.put(packPrefix+id+binSuffix, string(bin), nil)
	f.put(packPrefix+id+cueSuffix, string(encodeCue(hashLen, sortedRecs(append([]cueRecord(nil), recs...)))), nil)
}

func memObj(oh *plumbing.ObjectHasher, typ plumbing.ObjectType, body string) plumbing.EncodedObject {
	o := plumbing.NewMemoryObject(oh)
	o.SetType(typ)
	o.SetSize(int64(len(body)))
	if _, err := o.Write([]byte(body)); err != nil {
		panic("memObj buffers only ever-valid sizes: " + err.Error())
	}
	return o
}

// deltaBytes returns the git delta instruction stream that rebuilds target from
// base — byte for byte the payload a v3 delta record stores.
func deltaBytes(t *testing.T, base, target plumbing.EncodedObject) []byte {
	t.Helper()
	d, err := packfile.GetDelta(base, target)
	if err != nil {
		t.Fatalf("GetDelta: %v", err)
	}
	r, err := d.Reader()
	if err != nil {
		t.Fatalf("delta reader: %v", err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read delta: %v", err)
	}
	return b
}

func readAllObj(t *testing.T, o plumbing.EncodedObject) string {
	t.Helper()
	r, err := o.Reader()
	if err != nil {
		t.Fatalf("object reader: %v", err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	return string(b)
}

// --- reads through a delta ---------------------------------------------

// TestEncodedObjectReconstructsDelta is the whole point of the v3 format: a
// record whose payload is a delta must still hand ordinary callers the finished
// object, indistinguishable from one stored whole.
func TestEncodedObjectReconstructsDelta(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	baseBody := strings.Repeat("the quick brown fox\n", 300)
	targetBody := baseBody + "and one more line\n"

	baseObj := memObj(s.oh, plumbing.BlobObject, baseBody)
	targetObj := memObj(s.oh, plumbing.BlobObject, targetBody)
	delta := deltaBytes(t, baseObj, targetObj)

	if len(delta) >= len(targetBody) {
		t.Fatalf("delta is %d bytes for a %d-byte object; the fixture proves nothing", len(delta), len(targetBody))
	}

	bin := append([]byte(baseBody), delta...)
	hashLen := len(baseObj.Hash().Bytes())
	stageContainer(t, f, hashLen, bin, []cueRecord{
		{hash: baseObj.Hash(), typ: plumbing.BlobObject, offset: 0, stored: int64(len(baseBody)), raw: int64(len(baseBody))},
		{
			hash: targetObj.Hash(), typ: plumbing.BlobObject,
			offset: int64(len(baseBody)), stored: int64(len(delta)), raw: int64(len(targetBody)),
			base: baseObj.Hash(),
		},
	})

	got, err := s.EncodedObject(plumbing.AnyObject, targetObj.Hash())
	if err != nil {
		t.Fatalf("EncodedObject: %v", err)
	}
	if body := readAllObj(t, got); body != targetBody {
		t.Errorf("reconstructed %d bytes, want the %d-byte target", len(body), len(targetBody))
	}
	if got.Type() != plumbing.BlobObject {
		t.Errorf("type = %s, want blob: a delta record keeps the real object type", got.Type())
	}
	if got.Size() != int64(len(targetBody)) {
		t.Errorf("size = %d, want %d", got.Size(), len(targetBody))
	}
	if got.Hash() != targetObj.Hash() {
		t.Errorf("hash = %s, want %s", got.Hash(), targetObj.Hash())
	}
}

// TestDeltaObjectReturnsDeltaForm covers the seam that makes Storer satisfy
// storer.DeltaObjectStorer. Without it go-git's delta selector re-derives every
// delta on every clone.
func TestDeltaObjectReturnsDeltaForm(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	baseBody := strings.Repeat("alpha beta gamma\n", 200)
	targetBody := baseBody + "delta epsilon\n"

	baseObj := memObj(s.oh, plumbing.BlobObject, baseBody)
	targetObj := memObj(s.oh, plumbing.BlobObject, targetBody)
	delta := deltaBytes(t, baseObj, targetObj)

	bin := append([]byte(baseBody), delta...)
	hashLen := len(baseObj.Hash().Bytes())
	stageContainer(t, f, hashLen, bin, []cueRecord{
		{hash: baseObj.Hash(), typ: plumbing.BlobObject, offset: 0, stored: int64(len(baseBody)), raw: int64(len(baseBody))},
		{
			hash: targetObj.Hash(), typ: plumbing.BlobObject,
			offset: int64(len(baseBody)), stored: int64(len(delta)), raw: int64(len(targetBody)),
			base: baseObj.Hash(),
		},
	})

	t.Run("a delta record yields a DeltaObject", func(t *testing.T) {
		got, err := s.DeltaObject(plumbing.AnyObject, targetObj.Hash())
		if err != nil {
			t.Fatalf("DeltaObject: %v", err)
		}
		do, ok := got.(plumbing.DeltaObject)
		if !ok {
			t.Fatalf("got %T, want a plumbing.DeltaObject", got)
		}
		if do.BaseHash() != baseObj.Hash() {
			t.Errorf("BaseHash = %s, want %s", do.BaseHash(), baseObj.Hash())
		}
		if do.ActualHash() != targetObj.Hash() {
			t.Errorf("ActualHash = %s, want %s", do.ActualHash(), targetObj.Hash())
		}
		if do.ActualSize() != int64(len(targetBody)) {
			t.Errorf("ActualSize = %d, want %d", do.ActualSize(), len(targetBody))
		}
		if body := readAllObj(t, do); body != string(delta) {
			t.Errorf("payload is %d bytes, want the %d-byte delta stream", len(body), len(delta))
		}
	})

	t.Run("a whole record yields the object itself", func(t *testing.T) {
		got, err := s.DeltaObject(plumbing.AnyObject, baseObj.Hash())
		if err != nil {
			t.Fatalf("DeltaObject: %v", err)
		}
		if _, ok := got.(plumbing.DeltaObject); ok {
			t.Fatalf("got a DeltaObject for a record with no base")
		}
		if body := readAllObj(t, got); body != baseBody {
			t.Errorf("payload is %d bytes, want the %d-byte object", len(body), len(baseBody))
		}
	})
}

// TestDeltaChainRejectsAbuse pins the guards on chain walking. A .cue is just
// bytes in a bucket; a corrupt or hostile one must not be able to spin the
// daemon forever or recurse without bound.
func TestDeltaChainRejectsAbuse(t *testing.T) {
	t.Parallel()

	body := strings.Repeat("payload\n", 64)

	tests := []struct {
		name string
		// recs is built per-test from the hashes it needs.
		build func(oh *plumbing.ObjectHasher) (bin []byte, recs []cueRecord, want plumbing.Hash)
	}{
		{
			name: "a record that is its own base",
			build: func(oh *plumbing.ObjectHasher) ([]byte, []cueRecord, plumbing.Hash) {
				h := memObj(oh, plumbing.BlobObject, body).Hash()
				return []byte(body), []cueRecord{
					{hash: h, typ: plumbing.BlobObject, offset: 0, stored: int64(len(body)), raw: int64(len(body)), base: h},
				}, h
			},
		},
		{
			name: "two records that are each other's base",
			build: func(oh *plumbing.ObjectHasher) ([]byte, []cueRecord, plumbing.Hash) {
				a := memObj(oh, plumbing.BlobObject, body).Hash()
				b := memObj(oh, plumbing.BlobObject, body+"other").Hash()
				return []byte(body), []cueRecord{
					{hash: a, typ: plumbing.BlobObject, offset: 0, stored: int64(len(body)), raw: int64(len(body)), base: b},
					{hash: b, typ: plumbing.BlobObject, offset: 0, stored: int64(len(body)), raw: int64(len(body)), base: a},
				}, a
			},
		},
		{
			name: "a base that is not in any container",
			build: func(oh *plumbing.ObjectHasher) ([]byte, []cueRecord, plumbing.Hash) {
				h := memObj(oh, plumbing.BlobObject, body).Hash()
				missing := memObj(oh, plumbing.BlobObject, "nowhere").Hash()
				return []byte(body), []cueRecord{
					{hash: h, typ: plumbing.BlobObject, offset: 0, stored: int64(len(body)), raw: int64(len(body)), base: missing},
				}, h
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			s := newTestStorer(t, f)
			bin, recs, want := tt.build(s.oh)
			stageContainer(t, f, len(want.Bytes()), bin, recs)

			if _, err := s.EncodedObject(plumbing.AnyObject, want); !errors.Is(err, errBadCue) {
				t.Errorf("want errBadCue, got %v", err)
			}
		})
	}
}

// --- writes that keep a delta ------------------------------------------

// TestPackfileWriterKeepsDeltas is the write half of the reuse story: the delta
// chain the client already computed has to survive the trip into a container,
// or the read side has nothing to reuse and every clone re-derives it.
//
// It also pins the containment rule. A delta whose base sits in a *different*
// container would still resolve — reads look bases up by hash across the whole
// index — but it would break the promise that each container is complete on its
// own, and an interrupted push could then leave a delta whose base never landed.
func TestPackfileWriterKeepsDeltas(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 5, true)

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	byID := assertContainers(t, s, f, fx, 0)

	deltas := 0
	for id, recs := range byID {
		here := make(map[plumbing.Hash]struct{}, len(recs))
		for _, r := range recs {
			here[r.hash] = struct{}{}
		}
		for _, r := range recs {
			if r.base == plumbing.ZeroHash {
				continue
			}
			deltas++
			if _, ok := here[r.base]; !ok {
				t.Errorf("pack %s: record %s deltas against %s, which is not in this container", id, r.hash, r.base)
			}
			if r.raw <= r.stored {
				t.Errorf("pack %s: record %s stores %d bytes to rebuild %d; that delta is not worth keeping",
					id, r.hash, r.stored, r.raw)
			}
			if r.typ.IsDelta() {
				t.Errorf("pack %s: record %s has type %s; a record keeps the real object type, never a delta type",
					id, r.hash, r.typ)
			}
		}
	}
	if deltas == 0 {
		t.Error("no record kept a delta: the fixture's third commit edits a 43 KiB file, so git certainly sent one")
	}
}

// TestPackfileWriterDemotesDeltaAcrossSplit forces every object into a
// container of its own, which makes the containment rule bite on every delta
// the client sent. Nothing may be stored as a delta, and every object must
// still read back — the demotion path is what keeps a split from producing a
// container that cannot resolve alone.
func TestPackfileWriterDemotesDeltaAcrossSplit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 5, true)

	f := newFakeS3(t)
	// A one-byte cap seals after every object: the walk checks the cap before
	// the add, and every object in the fixture is larger than one byte.
	s := newTestStorer(t, f, withMaxPackBytes(1))
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	byID := assertContainers(t, s, f, fx, 0)
	if len(byID) != len(fx.hashes) {
		t.Fatalf("got %d containers for %d objects, want one each", len(byID), len(fx.hashes))
	}
	for id, recs := range byID {
		for _, r := range recs {
			if r.base != plumbing.ZeroHash {
				t.Errorf("pack %s: record %s kept a delta against %s, but it is alone in its container",
					id, r.hash, r.base)
			}
		}
	}
}
