package tigris

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
)

// --- .cue binary format ------------------------------------------------

func sortedRecs(recs []cueRecord) []cueRecord {
	sort.Slice(recs, func(i, j int) bool {
		return bytes.Compare(recs[i].hash.Bytes(), recs[j].hash.Bytes()) < 0
	})
	return recs
}

func TestCueRoundTrip(t *testing.T) {
	t.Parallel()

	h1 := hashForBody(formatcfg.DefaultObjectFormat, plumbing.BlobObject, "one")
	h2 := hashForBody(formatcfg.DefaultObjectFormat, plumbing.TreeObject, "two")
	sh1 := hashForBody(formatcfg.SHA256, plumbing.BlobObject, "one")

	tests := []struct {
		name    string
		hashLen int
		recs    []cueRecord
	}{
		{name: "empty, sha1 width", hashLen: 20, recs: nil},
		{name: "one raw record, sha1", hashLen: 20, recs: []cueRecord{
			{hash: h1, typ: plumbing.BlobObject, offset: 0, stored: 3, raw: 3},
		}},
		{name: "one zstd record, sha1", hashLen: 20, recs: []cueRecord{
			{hash: h1, typ: plumbing.BlobObject, codec: codecZstd, offset: 0, stored: 40, raw: 4096},
		}},
		{name: "mixed codecs, sha1, sorted", hashLen: 20, recs: sortedRecs([]cueRecord{
			{hash: h2, typ: plumbing.TreeObject, offset: 40, stored: 7, raw: 7},
			{hash: h1, typ: plumbing.BlobObject, codec: codecZstd, offset: 0, stored: 40, raw: 8192},
		})},
		{name: "sha256 width", hashLen: 32, recs: []cueRecord{
			{hash: sh1, typ: plumbing.BlobObject, codec: codecZstd, offset: 0, stored: 3, raw: 9},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enc := encodeCue(tt.hashLen, tt.recs)
			got, err := parseCue(tt.hashLen, enc)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if len(got) != len(tt.recs) {
				t.Fatalf("got %d records, want %d", len(got), len(tt.recs))
			}
			for i, r := range tt.recs {
				if got[i] != r {
					t.Errorf("record %d = %+v, want %+v", i, got[i], r)
				}
			}
		})
	}
}

// TestCueEncodesVersion2 pins the on-the-wire version byte. Without it the
// round-trip test above would pass just as well against v1's layout.
func TestCueEncodesVersion2(t *testing.T) {
	t.Parallel()

	h1 := hashForBody(formatcfg.DefaultObjectFormat, plumbing.BlobObject, "one")
	enc := encodeCue(20, []cueRecord{{hash: h1, typ: plumbing.BlobObject, stored: 3, raw: 3}})

	if got := enc[3]; got != 2 {
		t.Errorf("format version = %d, want 2", got)
	}
	if got := string(enc[0:3]); got != "OGC" {
		t.Errorf("magic = %q, want %q", got, "OGC")
	}
}

// TestCueParsesV1 is the back-compat guarantee: containers already in live
// buckets carry v1 cues, and they must keep resolving forever. The bytes are
// hand-built on purpose — encodeCue no longer emits v1, so an encoder-based
// fixture could not prove anything.
func TestCueParsesV1(t *testing.T) {
	t.Parallel()

	h1 := hashForBody(formatcfg.DefaultObjectFormat, plumbing.BlobObject, "one")
	h2 := hashForBody(formatcfg.DefaultObjectFormat, plumbing.TreeObject, "two")

	// v1: 16-byte header (magic "OGC"+0x01, hash width, 3 reserved, count),
	// then one hashLen+17 record per entry: hash, type, offset, length.
	v1 := []byte{'O', 'G', 'C', 1, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}
	for _, r := range sortedRecs([]cueRecord{
		{hash: h1, typ: plumbing.BlobObject, offset: 0, raw: 3},
		{hash: h2, typ: plumbing.TreeObject, offset: 3, raw: 7},
	}) {
		v1 = append(v1, r.hash.Bytes()...)
		v1 = append(v1, byte(r.typ))
		v1 = binary.BigEndian.AppendUint64(v1, uint64(r.offset))
		v1 = binary.BigEndian.AppendUint64(v1, uint64(r.raw))
	}

	got, err := parseCue(20, v1)
	if err != nil {
		t.Fatalf("parse v1: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	// A v1 payload is raw, so stored must equal raw and the codec must be raw.
	for i, r := range got {
		if r.codec != codecRaw {
			t.Errorf("record %d codec = %d, want codecRaw", i, r.codec)
		}
		if r.stored != r.raw {
			t.Errorf("record %d stored = %d, raw = %d; v1 payloads are raw so they must match", i, r.stored, r.raw)
		}
	}
	// Keyed by hash, not position: the records are in hash order, which is not
	// the order they were declared in above.
	want := map[plumbing.Hash]cueRecord{
		h1: {hash: h1, typ: plumbing.BlobObject, codec: codecRaw, offset: 0, stored: 3, raw: 3},
		h2: {hash: h2, typ: plumbing.TreeObject, codec: codecRaw, offset: 3, stored: 7, raw: 7},
	}
	for _, r := range got {
		if r != want[r.hash] {
			t.Errorf("record %s = %+v, want %+v", r.hash, r, want[r.hash])
		}
	}
}

func TestCueParseRejectsCorruption(t *testing.T) {
	t.Parallel()

	h1 := hashForBody(formatcfg.DefaultObjectFormat, plumbing.BlobObject, "one")
	valid := encodeCue(20, []cueRecord{{hash: h1, typ: plumbing.BlobObject, offset: 0, stored: 3, raw: 3}})

	tests := []struct {
		name string
		raw  []byte
	}{
		{"truncated header", valid[:10]},
		{"truncated record block", valid[:len(valid)-1]},
		{"bad magic", tamper(valid, 0, 'X')},
		// Byte 5 was a reserved zero in v1; v2 spends it on the record-block
		// codec, so the reserved-bytes check now covers 6 and 7 only.
		{"non-zero reserved", tamper(valid, 6, 1)},
		{"non-zero reserved, second byte", tamper(valid, 7, 1)},
		{"unknown format version", tamper(valid, 3, 9)},
		{"unknown record block codec", tamper(valid, 5, 9)},
		{"record count larger than the file", tamper(valid, 15, 0xff)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseCue(20, tt.raw); !errors.Is(err, errBadCue) {
				t.Errorf("want errBadCue, got %v", err)
			}
		})
	}

	t.Run("wrong hash width for this storer's format", func(t *testing.T) {
		t.Parallel()
		if _, err := parseCue(32, valid); !errors.Is(err, errBadCue) {
			t.Errorf("want errBadCue, got %v", err)
		}
	})
}

// TestCueParseRejectsDecompressionBomb pins the memory bound on the record
// block. The zstd default is 64 GiB, so without a bound a few kilobytes of
// hostile or corrupt .cue could exhaust the daemon's memory before any length
// check ever ran. A legitimate block is under 2 MiB: 32768 records at 58 bytes.
func TestCueParseRejectsDecompressionBomb(t *testing.T) {
	// Not parallel: it lowers a package-level bound.
	restore := cueMaxDecoded
	cueMaxDecoded = 64 << 10
	resetCueDecoder()
	t.Cleanup(func() {
		cueMaxDecoded = restore
		resetCueDecoder()
	})

	// One record's worth of header, then a block that decompresses far past
	// the bound. Zeros compress to almost nothing, which is the whole trick.
	bomb := encoder().EncodeAll(make([]byte, 4<<20), nil)
	if len(bomb) >= 64<<10 {
		t.Fatalf("bomb is %d bytes compressed, too big to prove the point", len(bomb))
	}

	cue := []byte{'O', 'G', 'C', 2, 20, codecZstd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	cue = append(cue, bomb...)

	_, err := parseCue(20, cue)
	if !errors.Is(err, errBadCue) {
		t.Errorf("want errBadCue for a block that exceeds the decode bound, got %v", err)
	}
}

func tamper(b []byte, i int, v byte) []byte {
	cp := append([]byte(nil), b...)
	cp[i] = v
	return cp
}

// --- payload compression -------------------------------------------------

// blobSpec asks buildBlobFixture for one blob of a given size, either
// maximally compressible (a repeated byte) or incompressible (a fixed-seed
// PRNG, so the fixture stays deterministic across runs).
type blobSpec struct {
	size         int
	compressible bool
	// headCompressible makes the first head bytes compressible and the rest
	// random, which is exactly the shape that fools a head probe.
	headCompressible bool
	// head is how much of the blob is compressible when headCompressible is
	// set. Zero means probeWindow.
	head int
}

// buildBlobFixture packs one commit holding one file per spec. Sizes must be
// distinct, since the assertions below locate a blob's cue record by its size.
func buildBlobFixture(t *testing.T, specs ...blobSpec) packFixture {
	t.Helper()
	dir := t.TempDir()
	gitRun(t, dir, "init", "-b", "main")
	gitRun(t, dir, "config", "user.email", "test@example.com")
	gitRun(t, dir, "config", "user.name", "Test")

	rng := rand.New(rand.NewSource(1))
	for i, spec := range specs {
		body := make([]byte, spec.size)
		switch {
		case spec.compressible:
			for j := range body {
				body[j] = byte('a' + i%26)
			}
		case spec.headCompressible:
			head := spec.head
			if head == 0 {
				head = probeWindow
			}
			if head > len(body) {
				head = len(body)
			}
			for j := 0; j < head; j++ {
				body[j] = byte('a' + i%26)
			}
			rng.Read(body[head:])
		default:
			rng.Read(body)
		}
		name := filepath.Join(dir, fmt.Sprintf("file%03d.bin", i))
		if err := os.WriteFile(name, body, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	gitRun(t, dir, "add", ".")
	gitRun(t, dir, "commit", "-m", "blobs")

	return collectPackFixture(t, dir)
}

// recordForSize finds the single cue record whose raw size matches, across
// every container the push produced.
func recordForSize(t *testing.T, byID map[string][]cueRecord, size int) cueRecord {
	t.Helper()
	var found []cueRecord
	for _, recs := range byID {
		for _, r := range recs {
			if r.raw == int64(size) {
				found = append(found, r)
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one record of raw size %d, found %d", size, len(found))
	}
	return found[0]
}

// TestPackPayloadCodec pins the compression policy at every band boundary.
// The floor cases are the ones most likely to regress if someone later
// "improves" the threshold, so each boundary is tested from both sides.
func TestPackPayloadCodec(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	tests := []struct {
		name      string
		spec      blobSpec
		wantCodec uint8
	}{
		{
			name:      "compressible, just under the floor, stays raw",
			spec:      blobSpec{size: compressionFloor - 1, compressible: true},
			wantCodec: codecRaw,
		},
		{
			name:      "compressible, at the floor, compresses",
			spec:      blobSpec{size: compressionFloor, compressible: true},
			wantCodec: codecZstd,
		},
		{
			name:      "incompressible, well over the floor, stays raw",
			spec:      blobSpec{size: 64 << 10, compressible: false},
			wantCodec: codecRaw,
		},
		{
			name:      "compressible, well over the floor, compresses",
			spec:      blobSpec{size: 32 << 10, compressible: true},
			wantCodec: codecZstd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			s := newTestStorer(t, f)
			fx := buildBlobFixture(t, tt.spec)
			writePack(t, s, fx)
			if err := s.up.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			byID := assertContainers(t, s, f, fx, maxPackObjects, maxPackBytes)
			r := recordForSize(t, byID, tt.spec.size)

			if r.codec != tt.wantCodec {
				t.Errorf("codec = %d, want %d (raw=%d stored=%d)", r.codec, tt.wantCodec, r.raw, r.stored)
			}
			switch tt.wantCodec {
			case codecRaw:
				if r.stored != r.raw {
					t.Errorf("raw payload: stored = %d, want %d", r.stored, r.raw)
				}
			case codecZstd:
				if r.stored >= r.raw {
					t.Errorf("compressed payload is not smaller: stored = %d, raw = %d", r.stored, r.raw)
				}
			}
		})
	}
}

// TestPackCompressionRatioOnRealSource is the end-to-end value check: every
// test above proves the machinery works, and this one proves it is worth
// having. The fixture is this package's own Go source, so the content is
// genuinely representative of what a git server stores rather than synthetic.
//
// The bar is deliberately loose. The measurement behind the 2 KiB floor put
// source blobs around 2.5x, so 1.5x on the whole push — trees, commits and
// sub-floor files included — has plenty of headroom while still failing loudly
// if compression silently stops happening.
func TestPackCompressionRatioOnRealSource(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	sources, err := filepath.Glob("*.go")
	if err != nil || len(sources) == 0 {
		t.Skipf("no Go sources beside the test to use as a fixture: %v", err)
	}

	dir := t.TempDir()
	gitRun(t, dir, "init", "-b", "main")
	gitRun(t, dir, "config", "user.email", "test@example.com")
	gitRun(t, dir, "config", "user.name", "Test")
	for _, src := range sources {
		body, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("read %s: %v", src, err)
		}
		if err := os.WriteFile(filepath.Join(dir, src), body, 0o644); err != nil {
			t.Fatalf("write %s: %v", src, err)
		}
	}
	gitRun(t, dir, "add", ".")
	gitRun(t, dir, "commit", "-m", "real source")
	fx := collectPackFixture(t, dir)

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	byID := assertContainers(t, s, f, fx, maxPackObjects, maxPackBytes)

	var raw, stored int64
	var compressed int
	for _, recs := range byID {
		for _, r := range recs {
			raw += r.raw
			stored += r.stored
			if r.codec == codecZstd {
				compressed++
			}
		}
	}
	if stored == 0 {
		t.Fatal("no bytes stored")
	}

	ratio := float64(raw) / float64(stored)
	t.Logf("%d objects (%d compressed): %d raw -> %d stored, %.2fx",
		len(fx.hashes), compressed, raw, stored, ratio)
	if ratio < 1.5 {
		t.Errorf("compression ratio %.2fx on real source is below 1.5x", ratio)
	}
}

// TestPackPayloadObserver proves the metrics seam fires once per object with
// the codec actually used and both byte counts, since a wrong codec label here
// would silently misreport the feature's whole value in production.
func TestPackPayloadObserver(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	const (
		compressible   = 16 << 10
		incompressible = 32 << 10
	)

	type seen struct {
		codec       string
		raw, stored int64
	}
	var mu sync.Mutex
	byRaw := map[int64]seen{}
	var calls int

	f := newFakeS3(t)
	s := newTestStorer(t, f, WithPayloadObserver(func(codec string, raw, stored int64) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		byRaw[raw] = seen{codec: codec, raw: raw, stored: stored}
	}))

	fx := buildBlobFixture(t,
		blobSpec{size: compressible, compressible: true},
		blobSpec{size: incompressible},
	)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if calls != len(fx.hashes) {
		t.Errorf("observer fired %d times, want once per object (%d)", calls, len(fx.hashes))
	}

	if got := byRaw[compressible]; got.codec != "zstd" || got.stored >= got.raw {
		t.Errorf("compressible blob reported %+v, want codec zstd with stored < raw", got)
	}
	if got := byRaw[incompressible]; got.codec != "raw" || got.stored != got.raw {
		t.Errorf("incompressible blob reported %+v, want codec raw with stored == raw", got)
	}
}

// TestPackPayloadNeverInflates is the invariant the container byte cap leans
// on: whatever the policy decides, a stored payload is never larger than the
// object it came from. The head-compressible case is the one that can break
// it — it passes a head probe and then inflates over the incompressible tail —
// so it must come back raw after the writer notices and rewinds.
func TestPackPayloadNeverInflates(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	const (
		// mispredicts: a compressible head over an incompressible tail.
		mispredicts = 8 << 10
		// compresses: wins on the same probe path, as a control.
		compresses = 6 << 10
		probe      = 64
	)

	f := newFakeS3(t)
	// A tiny in-memory cap forces the probe path, and a tiny probe window makes
	// the misprediction reachable — at the production 64 KiB window a
	// compressible head saves more than an incompressible tail can give back,
	// so no fixture of a sane size would ever rewind. Same motivation as
	// withMaxPackObjects.
	s := newTestStorer(t, f, withInMemoryCap(probe*2), withProbeWindow(probe))
	fx := buildBlobFixture(t,
		blobSpec{size: mispredicts, headCompressible: true, head: probe},
		blobSpec{size: compresses, compressible: true},
	)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	byID := assertContainers(t, s, f, fx, maxPackObjects, maxPackBytes)

	for _, recs := range byID {
		for _, r := range recs {
			if r.stored > r.raw {
				t.Errorf("object %s inflated: stored = %d > raw = %d", r.hash, r.stored, r.raw)
			}
		}
	}

	// The head-compressible blob passed the probe, then gave the saving back
	// over its tail, so the writer must have rewound it to raw.
	if r := recordForSize(t, byID, mispredicts); r.codec != codecRaw || r.stored != r.raw {
		t.Errorf("head-compressible blob: codec = %d, stored = %d, raw = %d; want raw with stored == raw",
			r.codec, r.stored, r.raw)
	}
	// The fully compressible one went through the same probe path and won.
	if r := recordForSize(t, byID, compresses); r.codec != codecZstd {
		t.Errorf("compressible blob over the in-memory cap: codec = %d, want zstd", r.codec)
	}
}

// --- PackfileWriter / read-path integration ------------------------------

// fixtureObj is what buildPackFixture records about one object for later
// verification: its type, size, and — for blobs only — exact content (tree
// and commit objects don't round-trip through `git cat-file -p` byte for
// byte, since -p reformats them).
type fixtureObj struct {
	typ  plumbing.ObjectType
	size int64
	blob []byte
}

type packFixture struct {
	bytes  []byte
	hashes []plumbing.Hash
	byHash map[plumbing.Hash]fixtureObj
}

// gitOutput runs git in dir with an isolated config, feeding stdin if
// non-nil, and fails the test on any error.
func gitOutput(t *testing.T, dir string, stdin []byte, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_TERMINAL_PROMPT=0",
	)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, stderr.String())
	}
	return out
}

func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	gitOutput(t, dir, nil, args...)
}

// buildPackFixture creates numFiles small files in one commit, plus — if
// withDeltaEdit — a second commit editing a larger file (to exercise real
// delta chains), then packs the whole history with the real git CLI and
// records ground truth for every resulting object via `git cat-file`.
func buildPackFixture(t *testing.T, numFiles int, withDeltaEdit bool) packFixture {
	t.Helper()
	dir := t.TempDir()
	gitRun(t, dir, "init", "-b", "main")
	gitRun(t, dir, "config", "user.email", "test@example.com")
	gitRun(t, dir, "config", "user.name", "Test")

	for i := 0; i < numFiles; i++ {
		name := filepath.Join(dir, fmt.Sprintf("file%03d.txt", i))
		content := fmt.Sprintf("content of file %d\n", i)
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	gitRun(t, dir, "add", ".")
	gitRun(t, dir, "commit", "-m", "initial")

	if withDeltaEdit {
		big := strings.Repeat("the quick brown fox jumps over the lazy dog\n", 1000)
		bigPath := filepath.Join(dir, "big.txt")
		if err := os.WriteFile(bigPath, []byte(big), 0o644); err != nil {
			t.Fatalf("write big.txt: %v", err)
		}
		gitRun(t, dir, "add", ".")
		gitRun(t, dir, "commit", "-m", "add big")

		edited := big + "trailing edit line\n"
		if err := os.WriteFile(bigPath, []byte(edited), 0o644); err != nil {
			t.Fatalf("edit big.txt: %v", err)
		}
		gitRun(t, dir, "add", ".")
		gitRun(t, dir, "commit", "-m", "edit big")
	}

	return collectPackFixture(t, dir)
}

// collectPackFixture packs the whole history of the repository in dir with the
// real git CLI and records ground truth for every resulting object via
// `git cat-file`.
func collectPackFixture(t *testing.T, dir string) packFixture {
	t.Helper()

	revOut := gitOutput(t, dir, nil, "rev-list", "--objects", "--all")
	packBytes := gitOutput(t, dir, revOut, "pack-objects", "--stdout")

	fx := packFixture{bytes: packBytes, byHash: map[plumbing.Hash]fixtureObj{}}
	for _, line := range strings.Split(strings.TrimSpace(string(revOut)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		hexHash := fields[0]
		h, ok := plumbing.FromHex(hexHash)
		if !ok {
			t.Fatalf("bad hash from rev-list: %q", hexHash)
		}
		if _, dup := fx.byHash[h]; dup {
			continue
		}

		typOut := strings.TrimSpace(string(gitOutput(t, dir, nil, "cat-file", "-t", hexHash)))
		typ, err := plumbing.ParseObjectType(typOut)
		if err != nil {
			t.Fatalf("parse type %q for %s: %v", typOut, hexHash, err)
		}
		sizeOut := strings.TrimSpace(string(gitOutput(t, dir, nil, "cat-file", "-s", hexHash)))
		size, err := strconv.ParseInt(sizeOut, 10, 64)
		if err != nil {
			t.Fatalf("parse size %q for %s: %v", sizeOut, hexHash, err)
		}

		fo := fixtureObj{typ: typ, size: size}
		if typ == plumbing.BlobObject {
			fo.blob = gitOutput(t, dir, nil, "cat-file", "blob", hexHash)
		}
		fx.byHash[h] = fo
		fx.hashes = append(fx.hashes, h)
	}
	return fx
}

// writePack feeds fx's pack bytes through s.PackfileWriter the same way
// production does: cmd/objgitd/receivepack.go's writePack drives a
// packfile.Scanner over an io.TeeReader rather than writing the bytes
// directly, so that the pack's own framing (not io.EOF) delimits the stream
// on persistent git:// and SSH sockets. Mirroring it here keeps this test on
// the real call path.
func writePack(t *testing.T, s *Storer, fx packFixture) {
	t.Helper()

	w, err := s.PackfileWriter()
	if err != nil {
		t.Fatalf("PackfileWriter: %v", err)
	}

	var sopts []packfile.ScannerOption
	if cfg, err := s.Config(); err == nil && cfg.Extensions.ObjectFormat == formatcfg.SHA256 {
		sopts = append(sopts, packfile.WithSHA256())
	}

	sc := packfile.NewScanner(io.TeeReader(bytes.NewReader(fx.bytes), w), sopts...)
	for sc.Scan() {
	}
	if err := sc.Error(); err != nil {
		_ = w.Close()
		t.Fatalf("scan pack: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestPackfileWriterRoundTrip(t *testing.T) {
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

	var bins, cues, loose int
	for k := range f.objs {
		switch {
		case strings.HasPrefix(k, packPrefix) && strings.HasSuffix(k, binSuffix):
			bins++
		case strings.HasPrefix(k, packPrefix) && strings.HasSuffix(k, cueSuffix):
			cues++
		case strings.HasPrefix(k, objectPrefix):
			loose++
		}
	}
	if bins != 1 || cues != 1 {
		t.Fatalf("want exactly one .bin and .cue, got %d bins, %d cues", bins, cues)
	}
	if loose != 0 {
		t.Errorf("expected zero loose objects, got %d", loose)
	}

	// Cold read: a fresh Storer over the same fake bucket must resolve every
	// object — this is the delta-resolution proof, since the scratch storer
	// that produced the .bin already flattened every delta before we ever
	// touched S3.
	obs, snapshot := countingObserver()
	s2 := newTestStorer(t, f, obs)

	for _, h := range fx.hashes {
		want := fx.byHash[h]

		if err := s2.HasEncodedObject(h); err != nil {
			t.Errorf("HasEncodedObject(%s): %v", h, err)
		}
		if sz, err := s2.EncodedObjectSize(h); err != nil || sz != want.size {
			t.Errorf("EncodedObjectSize(%s) = (%d, %v), want (%d, nil)", h, sz, err, want.size)
		}

		obj, err := s2.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			t.Fatalf("EncodedObject(%s): %v", h, err)
		}
		if obj.Type() != want.typ {
			t.Errorf("%s: type = %v, want %v", h, obj.Type(), want.typ)
		}
		if want.blob != nil {
			rd, err := obj.Reader()
			if err != nil {
				t.Fatalf("reader for %s: %v", h, err)
			}
			got, err := io.ReadAll(rd)
			rd.Close()
			if err != nil {
				t.Fatalf("read %s: %v", h, err)
			}
			if !bytes.Equal(got, want.blob) {
				t.Errorf("%s: body mismatch (delta resolution wrong?)\ngot:  %q\nwant: %q", h, got, want.blob)
			}
		}
	}
	if seen := snapshot(); seen["HeadObject"] != 0 {
		t.Errorf("Has/SizeEncodedObject used HeadObject %d times, want 0 (pack lookups cost nothing extra)", seen["HeadObject"])
	}
}

// packContents collects every pack container in the fake bucket, keyed by pack
// id, plus the loose-object count.
func packContents(t *testing.T, f *fakeS3) (bins, cues map[string][]byte, loose int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()

	bins, cues = map[string][]byte{}, map[string][]byte{}
	for k, o := range f.objs {
		switch {
		case strings.HasPrefix(k, packPrefix) && strings.HasSuffix(k, binSuffix):
			bins[strings.TrimSuffix(strings.TrimPrefix(k, packPrefix), binSuffix)] = o.body
		case strings.HasPrefix(k, packPrefix) && strings.HasSuffix(k, cueSuffix):
			cues[strings.TrimSuffix(strings.TrimPrefix(k, packPrefix), cueSuffix)] = o.body
		case strings.HasPrefix(k, objectPrefix):
			loose++
		}
	}
	return bins, cues, loose
}

// assertContainers checks every invariant a sealed set of containers must
// hold, whichever write-side cap produced the split: the id is the .bin's own
// checksum, no .cue is orphaned, records are hash-sorted and inside their
// .bin, every fixture object is indexed exactly once, and a cold Storer
// resolves all of them.
//
// maxRecs and maxBytes are the caps in force; 0 skips either check. A
// container may exceed maxBytes only when it holds exactly one object, which
// is the deliberate spill for an object larger than the whole cap.
//
// It returns the parsed records keyed by pack id, so a caller can assert more
// about how the split actually fell.
func assertContainers(t *testing.T, s *Storer, f *fakeS3, fx packFixture, maxRecs int, maxBytes int64) map[string][]cueRecord {
	t.Helper()

	bins, cues, loose := packContents(t, f)
	if loose != 0 {
		t.Errorf("expected zero loose objects, got %d", loose)
	}

	byID := make(map[string][]cueRecord, len(cues))
	counts := map[plumbing.Hash]int{}
	for id, raw := range cues {
		bin, ok := bins[id]
		if !ok {
			t.Fatalf("cue %s has no sibling .bin", id)
		}
		// The id is the .bin's sha256, so a shared or reused hasher across
		// segments shows up right here.
		if got := fmt.Sprintf("%x", sha256.Sum256(bin)); got != id {
			t.Errorf("pack %s: .bin hashes to %s (id is not its own checksum)", id, got)
		}

		recs, err := parseCue(s.oh.Size(), raw)
		if err != nil {
			t.Fatalf("parse cue %s: %v", id, err)
		}
		byID[id] = recs
		if len(recs) == 0 {
			t.Errorf("pack %s holds no records", id)
		}
		if maxRecs > 0 && len(recs) > maxRecs {
			t.Errorf("pack %s holds %d records, cap is %d", id, len(recs), maxRecs)
		}
		if maxBytes > 0 && int64(len(bin)) > maxBytes && len(recs) != 1 {
			t.Errorf("pack %s holds %d objects in %d bytes, past the %d-byte cap; only a lone oversized object may spill",
				id, len(recs), len(bin), maxBytes)
		}
		if !sort.SliceIsSorted(recs, func(i, j int) bool {
			return bytes.Compare(recs[i].hash.Bytes(), recs[j].hash.Bytes()) < 0
		}) {
			t.Errorf("pack %s: records are not hash-sorted", id)
		}
		for _, r := range recs {
			if end := r.offset + r.stored; end > int64(len(bin)) {
				t.Errorf("pack %s: record %s runs to %d, past the %d-byte .bin", id, r.hash, end, len(bin))
			}
			counts[r.hash]++
		}
	}

	for _, h := range fx.hashes {
		if counts[h] != 1 {
			t.Errorf("object %s indexed %d times across %d containers, want 1", h, counts[h], len(cues))
		}
	}
	if len(counts) != len(fx.hashes) {
		t.Errorf("containers index %d distinct objects, want %d", len(counts), len(fx.hashes))
	}

	// Cold read across every container: reads impose neither cap.
	s2 := newTestStorer(t, f)
	for _, h := range fx.hashes {
		want := fx.byHash[h]
		obj, err := s2.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			t.Fatalf("EncodedObject(%s): %v", h, err)
		}
		if obj.Type() != want.typ || obj.Size() != want.size {
			t.Errorf("%s = (%v, %d), want (%v, %d)", h, obj.Type(), obj.Size(), want.typ, want.size)
		}
		if want.blob == nil {
			continue
		}
		rd, err := obj.Reader()
		if err != nil {
			t.Fatalf("reader for %s: %v", h, err)
		}
		got, err := io.ReadAll(rd)
		rd.Close()
		if err != nil {
			t.Fatalf("read %s: %v", h, err)
		}
		if !bytes.Equal(got, want.blob) {
			t.Errorf("%s: body mismatch\ngot:  %q\nwant: %q", h, got, want.blob)
		}
	}
	return byID
}

// TestPackfileWriterSplitsAtObjectLimit covers the write-side cap: one push
// above the limit lands as several self-consistent bin/cue pairs, and the read
// side — which has no such cap — resolves every object across all of them.
func TestPackfileWriterSplitsAtObjectLimit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	n := len(fx.hashes)
	if n < 8 {
		t.Fatalf("fixture too small to split: %d objects", n)
	}

	tests := []struct {
		name  string
		limit int
	}{
		{name: "one object per container", limit: 1},
		{name: "uneven split", limit: 5},
		{name: "half the object count", limit: n / 2},
		{name: "cap equals object count", limit: n},
		{name: "cap above object count", limit: n + 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			s := newTestStorer(t, f, withMaxPackObjects(tt.limit))
			writePack(t, s, fx)
			if err := s.up.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			bins, cues, _ := packContents(t, f)
			// Ceiling division, never a rounded-up count that would imply an
			// empty trailing container: limits 1 and n divide n exactly.
			want := (n + tt.limit - 1) / tt.limit
			if len(bins) != want || len(cues) != want {
				t.Fatalf("got %d bins and %d cues, want %d of each (%d objects, cap %d)", len(bins), len(cues), want, n, tt.limit)
			}

			// No byte cap in play here: the fixture's files are tiny, so the
			// object cap is the only one that can bind.
			assertContainers(t, s, f, fx, tt.limit, 0)
		})
	}
}

// buildSizedPackFixture commits one file for each entry in sizes, filled to
// exactly that many bytes with content unique to that file (identical content
// would collapse into one blob and one container record), then packs the
// result the same way buildPackFixture does. It exists so the byte cap can be
// exercised with a small cap and known blob sizes, instead of a 128 MiB
// fixture.
//
// The bodies are incompressible, from a fixed-seed PRNG. The byte cap counts
// *stored* bytes, so compressible bodies would make a blob's contribution to
// its container some unpredictable fraction of the size asked for here, and
// every expectation in the caller's table would be guesswork. Incompressible
// bodies keep stored == raw, which lets the caller reason in the sizes it
// passed while still running the real codec path.
func buildSizedPackFixture(t *testing.T, sizes ...int) packFixture {
	t.Helper()
	dir := t.TempDir()
	gitRun(t, dir, "init", "-b", "main")
	gitRun(t, dir, "config", "user.email", "test@example.com")
	gitRun(t, dir, "config", "user.name", "Test")

	rng := rand.New(rand.NewSource(2))
	for i, size := range sizes {
		name := filepath.Join(dir, fmt.Sprintf("file%03d.bin", i))
		body := make([]byte, size)
		rng.Read(body)
		if err := os.WriteFile(name, body, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	gitRun(t, dir, "add", ".")
	gitRun(t, dir, "commit", "-m", "sized files")

	return collectPackFixture(t, dir)
}

// TestPackfileWriterSplitsAtByteLimit covers the write-side byte cap. Which
// objects share a container depends on the scratch storer's iteration order,
// so the assertions here are order-independent: no container holding more than
// one object may exceed the cap, and an object larger than the cap all by
// itself gets a container to itself rather than bloating whichever container
// it happened to arrive behind.
func TestPackfileWriterSplitsAtByteLimit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	tests := []struct {
		name      string
		sizes     []int
		byteCap   int64
		objCap    int // 0 leaves the object cap at its production value
		wantPacks int // 0 skips the exact-count check; -1 wants more than one
		oversized int // a blob of this size must sit alone in its container
	}{
		{
			name:      "cap above the whole push",
			sizes:     []int{1000, 1000, 1000},
			byteCap:   1 << 20,
			wantPacks: 1,
		},
		{
			name:      "even blobs split evenly",
			sizes:     []int{4096, 4096, 4096, 4096},
			byteCap:   8192,
			wantPacks: -1,
		},
		{
			name:      "uneven blobs split unevenly",
			sizes:     []int{5000, 300, 4000, 900, 3000},
			byteCap:   6000,
			wantPacks: -1,
		},
		{
			name:      "cap below every object",
			sizes:     []int{500, 600, 700},
			byteCap:   1,
			wantPacks: 5, // three blobs plus the tree and the commit
		},
		{
			name:      "one object larger than the cap spills alone",
			sizes:     []int{800, 800, 800, 50000},
			byteCap:   4096,
			oversized: 50000,
		},
		{
			name:      "both caps bind within one push",
			sizes:     []int{100, 100, 100, 9000, 100, 100},
			byteCap:   8192,
			objCap:    2,
			wantPacks: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := buildSizedPackFixture(t, tt.sizes...)

			f := newFakeS3(t)
			opts := []Option{withMaxPackBytes(tt.byteCap)}
			if tt.objCap > 0 {
				opts = append(opts, withMaxPackObjects(tt.objCap))
			}
			s := newTestStorer(t, f, opts...)
			writePack(t, s, fx)
			if err := s.up.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			byID := assertContainers(t, s, f, fx, tt.objCap, tt.byteCap)

			switch {
			case tt.wantPacks > 0 && len(byID) != tt.wantPacks:
				t.Errorf("got %d containers, want %d (cap %d bytes)", len(byID), tt.wantPacks, tt.byteCap)
			case tt.wantPacks == -1 && len(byID) < 2:
				t.Errorf("got %d containers, want the push to split (cap %d bytes)", len(byID), tt.byteCap)
			}

			if tt.oversized == 0 {
				return
			}
			var found int
			for id, recs := range byID {
				for _, r := range recs {
					if r.raw != int64(tt.oversized) {
						continue
					}
					found++
					if len(recs) != 1 {
						t.Errorf("container %s holds the %d-byte object alongside %d others; an object past the cap gets a container to itself",
							id, tt.oversized, len(recs)-1)
					}
				}
			}
			if found != 1 {
				t.Fatalf("found %d objects of %d bytes across the containers, want 1", found, tt.oversized)
			}
		})
	}
}

// TestIterDrainsOnePackAtATime guards snapshotEntries' ordering: with a push
// split across several containers, iteration must finish one container before
// starting the next, or a clone holds every container's bulk download open at
// the same time.
func TestIterDrainsOnePackAtATime(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)

	f := newFakeS3(t)
	s := newTestStorer(t, f, withMaxPackObjects(5))
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, cues, _ := packContents(t, f)
	if len(cues) < 3 {
		t.Fatalf("want at least 3 containers to test ordering, got %d", len(cues))
	}
	owner := map[plumbing.Hash]string{}
	for id, raw := range cues {
		recs, err := parseCue(s.oh.Size(), raw)
		if err != nil {
			t.Fatalf("parse cue %s: %v", id, err)
		}
		for _, r := range recs {
			owner[r.hash] = id
		}
	}

	s2 := newTestStorer(t, f)
	iter, err := s2.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		t.Fatalf("IterEncodedObjects: %v", err)
	}
	defer iter.Close()

	var last string
	done := map[string]bool{}
	if err := iter.ForEach(func(o plumbing.EncodedObject) error {
		id, ok := owner[o.Hash()]
		if !ok {
			t.Fatalf("%s came from no known container", o.Hash())
		}
		if id == last {
			return nil
		}
		if done[id] {
			t.Errorf("returned to container %s after leaving it", id)
		}
		if last != "" {
			done[last] = true
		}
		last = id
		return nil
	}); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if len(done)+1 != len(cues) {
		t.Errorf("iteration touched %d containers, want %d", len(done)+1, len(cues))
	}
}

func TestPackPendingVisibleBeforeUpload(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 3, false)

	f := newFakeS3(t)
	f.putDelay = 50 * time.Millisecond
	s := newTestStorer(t, f)
	writePack(t, s, fx)

	if f.nputs() != 0 {
		t.Fatal("uploaded synchronously from Close — test no longer exercises the race")
	}

	h := fx.hashes[0]
	want := fx.byHash[h]
	obj, err := s.EncodedObject(plumbing.AnyObject, h)
	if err != nil {
		t.Fatalf("EncodedObject before upload lands: %v", err)
	}
	if obj.Type() != want.typ || obj.Size() != want.size {
		t.Errorf("pending pack read = (%v, %d), want (%v, %d)", obj.Type(), obj.Size(), want.typ, want.size)
	}

	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := s.EncodedObject(plumbing.AnyObject, h); err != nil {
		t.Fatalf("EncodedObject after flush: %v", err)
	}
}

func TestPackUploadFailureBlocksRef(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 3, false)

	f := newFakeS3(t)
	f.putErr = errors.New("network cut")
	s := newTestStorer(t, f)
	writePack(t, s, fx)

	ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), fx.hashes[0])
	if err := s.SetReference(ref); err == nil {
		t.Fatal("expected SetReference to surface the pack upload failure")
	}

	if err := s.HasEncodedObject(fx.hashes[0]); !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Errorf("want ErrObjectNotFound after deregistration, got %v", err)
	}

	for k := range f.objs {
		if strings.HasSuffix(k, cueSuffix) {
			t.Errorf("unexpected cue key %q present after a failed upload (bin-before-cue ordering broken)", k)
		}
	}
}

func TestIterMergesPackAndLoose(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 3, false)

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Re-write one already-packed blob directly (same content ⇒ same hash):
	// exercises dedup between the pack index and the loose fallback.
	for h, fo := range fx.byHash {
		if fo.typ != plumbing.BlobObject {
			continue
		}
		obj := plumbing.NewMemoryObject(s.oh)
		obj.SetType(plumbing.BlobObject)
		obj.SetSize(int64(len(fo.blob)))
		if _, err := obj.Write(fo.blob); err != nil {
			t.Fatalf("buffer: %v", err)
		}
		if _, err := s.SetEncodedObject(obj); err != nil {
			t.Fatalf("SetEncodedObject: %v", err)
		}
		_ = h
		break
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	looseOnly := seed(t, f, formatcfg.DefaultObjectFormat, plumbing.BlobObject, "purely loose content")

	iter, err := s.IterEncodedObjects(plumbing.AnyObject)
	if err != nil {
		t.Fatalf("IterEncodedObjects: %v", err)
	}
	defer iter.Close()

	counts := map[plumbing.Hash]int{}
	if err := iter.ForEach(func(o plumbing.EncodedObject) error {
		counts[o.Hash()]++
		return nil
	}); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	for h := range fx.byHash {
		if counts[h] != 1 {
			t.Errorf("packed hash %s seen %d times, want 1", h, counts[h])
		}
	}
	if counts[looseOnly] != 1 {
		t.Errorf("loose-only hash seen %d times, want 1", counts[looseOnly])
	}
}

// TestHookReadAfterPushSameStorer regresses the scenario that makes pending
// pack registration mandatory rather than a nice-to-have: on one Storer
// instance, an early read (e.g. ref-advertisement peeling annotated tags)
// can trigger the cold pack-index build before the current push's pack
// exists — a post-receive hook then reading the just-pushed tree/blobs, on
// that same instance, must not consult the now-stale cold index.
func TestHookReadAfterPushSameStorer(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 3, false)

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	if err := s.ensurePacksBuilt(); err != nil {
		t.Fatalf("cold build: %v", err)
	}

	writePack(t, s, fx)

	ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), fx.hashes[0])
	if err := s.SetReference(ref); err != nil {
		t.Fatalf("SetReference: %v", err)
	}

	for _, h := range fx.hashes {
		if _, err := s.EncodedObject(plumbing.AnyObject, h); err != nil {
			t.Errorf("hook-style read of %s failed: %v", h, err)
		}
	}
}

func blobHashes(fx packFixture) []plumbing.Hash {
	var out []plumbing.Hash
	for h, fo := range fx.byHash {
		if fo.typ == plumbing.BlobObject {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// packRecords parses every .cue in the fake bucket, returning each container's
// records sorted by offset — the order a .bin is written in, and therefore the
// order a download fills it in. Zero-length records are dropped, since
// packObject answers those without touching any tier.
func packRecords(t *testing.T, s *Storer, f *fakeS3) map[string][]cueRecord {
	t.Helper()
	_, cues, _ := packContents(t, f)

	out := make(map[string][]cueRecord, len(cues))
	for id, raw := range cues {
		recs, err := parseCue(s.oh.Size(), raw)
		if err != nil {
			t.Fatalf("parse cue %s: %v", id, err)
		}
		kept := recs[:0]
		for _, r := range recs {
			if r.stored > 0 {
				kept = append(kept, r)
			}
		}
		sort.Slice(kept, func(i, j int) bool { return kept[i].offset < kept[j].offset })
		out[id] = kept
	}
	return out
}

// onlyPack is packRecords for a fixture that produced exactly one container.
func onlyPack(t *testing.T, s *Storer, f *fakeS3) (string, []cueRecord) {
	t.Helper()
	byID := packRecords(t, s, f)
	if len(byID) != 1 {
		t.Fatalf("expected exactly one pack container, got %d", len(byID))
	}
	for id, recs := range byID {
		return id, recs
	}
	panic("unreachable: the map holds exactly one entry")
}

// waitPackFetch blocks until s's background download of id has settled, either
// way. Reads never wait on that, so a test that wants to observe the landed
// state has to.
func waitPackFetch(t *testing.T, s *Storer, id string) {
	t.Helper()
	select {
	case <-s.packs.getAccess(id).done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the background download of pack %s never settled", id)
	}
}

// waitWatermark blocks until s can see at least want bytes of id's container
// on local disk.
func waitWatermark(t *testing.T, s *Storer, id string, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if ps := s.partialPack(id); ps != nil && ps.n.Load() >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the download of pack %s never reached %d bytes on disk", id, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// seededReader writes fx into a fresh bucket and returns a fake plus a cold
// Storer over it, which is the opening move of every prefetch test here.
func seededReader(t *testing.T, fx packFixture, wopts ...Option) (*fakeS3, *Storer) {
	t.Helper()
	f := newFakeS3(t)
	w := newTestStorer(t, f, wopts...)
	writePack(t, w, fx)
	if err := w.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return f, newTestStorer(t, f)
}

// TestPackPrefetchStartsOnFirstRead pins the trigger: one packed read is enough
// to start the container's download, and that read serves itself over a ranged
// GET rather than waiting for it.
func TestPackPrefetchStartsOnFirstRead(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	blobs := blobHashes(fx)
	f, reader := seededReader(t, fx)
	id, _ := onlyPack(t, reader, f)

	if _, err := reader.EncodedObject(plumbing.AnyObject, blobs[0]); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if got := f.nrangedGets(); got != 1 {
		t.Errorf("ranged GETs for the first read = %d, want 1", got)
	}

	waitPackFetch(t, reader, id)
	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("whole-pack downloads started by one read = %d, want 1", got)
	}

	// With the container on disk, nothing else goes to the network.
	settled := f.nrangedGets()
	for _, h := range blobs {
		if _, err := reader.EncodedObject(plumbing.AnyObject, h); err != nil {
			t.Fatalf("read of %s after the download landed: %v", h, err)
		}
	}
	if got := f.nrangedGets(); got != settled {
		t.Errorf("ranged GETs grew after the download landed: %d -> %d", settled, got)
	}
	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("whole-pack downloads = %d, want 1", got)
	}
}

// TestPackReadNeverBlocksOnPrefetch is the point of the whole design. With a
// container's download wedged open and yielding nothing, every read of that
// container must still come back — over ranged GETs — instead of queueing
// behind it.
func TestPackReadNeverBlocksOnPrefetch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	blobs := blobHashes(fx)
	f, reader := seededReader(t, fx)

	release, waitStarted := f.holdBinBodies(t, 0) // the download never yields a byte
	defer release()

	if _, err := reader.EncodedObject(plumbing.AnyObject, blobs[0]); err != nil {
		t.Fatalf("first read: %v", err)
	}
	waitStarted()

	done := make(chan error, 1)
	go func() {
		for _, h := range blobs[1:] {
			if _, err := reader.EncodedObject(plumbing.AnyObject, h); err != nil {
				done <- fmt.Errorf("read %s: %w", h, err)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reading while the download was stalled: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a packed read blocked on the stalled whole-pack download")
	}

	if got, want := f.nrangedGets(), len(blobs); got != want {
		t.Errorf("ranged GETs = %d, want %d — every read should have taken the ranged tier", got, want)
	}
	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("whole-pack downloads = %d, want 1", got)
	}
}

// TestPackWatermarkServesPartialDownload covers the tier that keeps the
// non-blocking design from paying for the same bytes twice: an object whose
// span has already reached disk is read locally, and only one past the
// watermark costs a ranged GET.
func TestPackWatermarkServesPartialDownload(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	f, reader := seededReader(t, fx)
	id, recs := onlyPack(t, reader, f)
	if len(recs) < 4 {
		t.Fatalf("fixture produced only %d records, need at least 4", len(recs))
	}

	// Park the download exactly at the end of a record in the middle.
	mid := recs[len(recs)/2]
	stop := mid.offset + mid.stored
	last := recs[len(recs)-1]

	release, waitStarted := f.holdBinBodies(t, int(stop))
	defer release()

	// The read that starts the prefetch sees a watermark of zero, so it is a
	// ranged GET no matter where its object lives.
	if _, err := reader.EncodedObject(plumbing.AnyObject, last.hash); err != nil {
		t.Fatalf("first read: %v", err)
	}
	waitStarted()
	waitWatermark(t, reader, id, stop)

	before := f.nrangedGets()
	below := 0
	for _, r := range recs {
		if r.offset+r.stored > stop {
			continue
		}
		below++
		obj, err := reader.EncodedObject(plumbing.AnyObject, r.hash)
		if err != nil {
			t.Fatalf("read of %s below the watermark: %v", r.hash, err)
		}
		if want := fx.byHash[r.hash].size; obj.Size() != want {
			t.Errorf("read of %s below the watermark: size %d, want %d", r.hash, obj.Size(), want)
		}
	}
	if below == 0 {
		t.Fatal("no record ended at or below the watermark; the fixture cannot exercise this tier")
	}
	if got := f.nrangedGets(); got != before {
		t.Errorf("%d reads below the watermark cost %d ranged GETs, want 0", below, got-before)
	}

	// One past it still goes to the network.
	if _, err := reader.EncodedObject(plumbing.AnyObject, last.hash); err != nil {
		t.Fatalf("read past the watermark: %v", err)
	}
	if got := f.nrangedGets(); got != before+1 {
		t.Errorf("a read past the watermark cost %d ranged GETs, want 1", got-before)
	}
}

// TestPackWatermarkThroughCacheSingleflight covers the reason the in-progress
// stream lives on the cache entry: a Storer whose own download is parked behind
// another Storer's must still read out of the bytes that one has landed.
func TestPackWatermarkThroughCacheSingleflight(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	f := newFakeS3(t)
	cache := newTestPackCache(t, 0)

	w := newTestStorer(t, f, WithPackCache(cache))
	writePack(t, w, fx)
	if err := w.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	owner := newTestStorer(t, f, WithPackCache(cache))
	waiter := newTestStorer(t, f, WithPackCache(cache))
	id, recs := onlyPack(t, owner, f)

	mid := recs[len(recs)/2]
	stop := mid.offset + mid.stored
	release, waitStarted := f.holdBinBodies(t, int(stop))
	// Let the download finish before the test ends, so it cannot be renaming a
	// file into the cache directory while the cache's own cleanup removes it.
	defer func() {
		release()
		waitPackFetch(t, owner, id)
	}()

	if _, err := owner.EncodedObject(plumbing.AnyObject, recs[len(recs)-1].hash); err != nil {
		t.Fatalf("owner's first read: %v", err)
	}
	waitStarted()
	waitWatermark(t, owner, id, stop)

	// The waiter's own prefetch lands in the cache's singleflight and downloads
	// nothing, but its reads below the watermark are still local.
	before := f.nrangedGets()
	obj, err := waiter.EncodedObject(plumbing.AnyObject, recs[0].hash)
	if err != nil {
		t.Fatalf("waiter's read below the watermark: %v", err)
	}
	if want := fx.byHash[recs[0].hash].size; obj.Size() != want {
		t.Errorf("waiter read size %d, want %d", obj.Size(), want)
	}
	if got := f.nrangedGets(); got != before {
		t.Errorf("the waiter's read below the watermark cost %d ranged GETs, want 0", got-before)
	}
	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("whole-pack downloads = %d, want 1 — the cache should have deduplicated them", got)
	}
}

// TestPackPrefetchConcurrencyCap pins maxLivePackFetches. Backgrounding the
// download removed the accidental throttle that blocking gave us, so the cap
// is now the only thing between a repository with many containers and every
// one of them on the wire at once.
func TestPackPrefetchConcurrencyCap(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	f, reader := seededReader(t, fx, withMaxPackObjects(2))
	byID := packRecords(t, reader, f)
	if len(byID) <= maxLivePackFetches {
		t.Fatalf("fixture produced %d containers, need more than %d", len(byID), maxLivePackFetches)
	}

	release, _ := f.holdBinBodies(t, 0)
	defer release()

	// One read per container, so every container wants a download. None of
	// these reads blocks, so a plain loop starts them all.
	for id, recs := range byID {
		if _, err := reader.EncodedObject(plumbing.AnyObject, recs[0].hash); err != nil {
			t.Fatalf("read from pack %s: %v", id, err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for f.peakLiveBinGets() < maxLivePackFetches {
		if time.Now().After(deadline) {
			t.Fatalf("only %d downloads ever ran at once, want %d", f.peakLiveBinGets(), maxLivePackFetches)
		}
		time.Sleep(time.Millisecond)
	}
	// Give any download that the cap should have held back a chance to show up.
	time.Sleep(200 * time.Millisecond)
	if got := f.peakLiveBinGets(); got != maxLivePackFetches {
		t.Errorf("%d whole-pack downloads ran at once, want at most %d", got, maxLivePackFetches)
	}
}

// TestPackPrefetchChecksumFailure covers the failure posture. A container whose
// bytes do not hash to its id is refused, the failure is sticky for this
// Storer, and — the part the watermark tier adds — no read is ever served out
// of the partial file it left behind.
func TestPackPrefetchChecksumFailure(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	blobs := blobHashes(fx)

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var binKey string
	for k := range f.objs {
		if strings.HasPrefix(k, packPrefix) && strings.HasSuffix(k, binSuffix) {
			binKey = k
		}
	}
	if binKey == "" {
		t.Fatal("no .bin found in the fake bucket")
	}
	f.mu.Lock()
	o := f.objs[binKey]
	tampered := append([]byte(nil), o.body...)
	tampered[0] ^= 0xFF
	o.body = tampered
	f.objs[binKey] = o
	f.mu.Unlock()

	reader := newTestStorer(t, f)
	id, _ := onlyPack(t, reader, f)

	if _, err := reader.EncodedObject(plumbing.AnyObject, blobs[0]); err != nil {
		t.Fatalf("first read: %v", err)
	}
	waitPackFetch(t, reader, id)
	if f.nfullBinGets() == 0 {
		t.Error("expected a whole-pack download attempt")
	}
	if ps := reader.partialPack(id); ps != nil {
		t.Error("a failed download left its partial file readable")
	}

	// Every later read still works, over ranged GETs, and never retries.
	before := f.nfullBinGets()
	for _, h := range blobs {
		obj, err := reader.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			t.Fatalf("read of %s after the sticky failure: %v", h, err)
		}
		if want := fx.byHash[h].size; obj.Size() != want {
			t.Errorf("read of %s: size %d, want %d", h, obj.Size(), want)
		}
	}
	if f.nfullBinGets() != before {
		t.Error("the whole-pack download was retried after a sticky checksum failure")
	}
}

// TestIterConvergesOnTheLocalCopy checks the iteration case end to end. The
// walk hands objects out in offset order and the download fills the file in
// offset order, so one download covers the whole walk and a second walk over
// the same Storer costs nothing at all.
func TestIterConvergesOnTheLocalCopy(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	f, reader := seededReader(t, fx)
	id, _ := onlyPack(t, reader, f)

	iterate := func() int {
		t.Helper()
		iter, err := reader.IterEncodedObjects(plumbing.BlobObject)
		if err != nil {
			t.Fatalf("IterEncodedObjects: %v", err)
		}
		defer iter.Close()

		count := 0
		if err := iter.ForEach(func(obj plumbing.EncodedObject) error {
			count++
			if want := fx.byHash[obj.Hash()].size; obj.Size() != want {
				t.Errorf("iterated %s: size %d, want %d", obj.Hash(), obj.Size(), want)
			}
			return nil
		}); err != nil {
			t.Fatalf("iterate: %v", err)
		}
		return count
	}

	if count := iterate(); count == 0 {
		t.Fatal("iterator yielded nothing")
	}

	// Not asserted before the wait: the walk never blocks on the download, so a
	// small fixture can finish iterating before the download goroutine has even
	// issued its GET.
	waitPackFetch(t, reader, id)
	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("whole-pack downloads after a full iteration = %d, want 1", got)
	}

	settled := f.nrangedGets()
	iterate()
	if got := f.nrangedGets(); got != settled {
		t.Errorf("a second iteration cost %d ranged GETs, want 0", got-settled)
	}
	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("whole-pack downloads after two iterations = %d, want 1", got)
	}
}
