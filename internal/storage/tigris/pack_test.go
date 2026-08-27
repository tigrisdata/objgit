package tigris

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
		{name: "one record, sha1", hashLen: 20, recs: []cueRecord{
			{hash: h1, typ: plumbing.BlobObject, offset: 0, length: 3},
		}},
		{name: "many records, sha1, sorted", hashLen: 20, recs: sortedRecs([]cueRecord{
			{hash: h2, typ: plumbing.TreeObject, offset: 3, length: 7},
			{hash: h1, typ: plumbing.BlobObject, offset: 0, length: 3},
		})},
		{name: "sha256 width", hashLen: 32, recs: []cueRecord{
			{hash: sh1, typ: plumbing.BlobObject, offset: 0, length: 3},
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
				if got[i].hash != r.hash || got[i].typ != r.typ || got[i].offset != r.offset || got[i].length != r.length {
					t.Errorf("record %d = %+v, want %+v", i, got[i], r)
				}
			}
		})
	}
}

func TestCueParseRejectsCorruption(t *testing.T) {
	t.Parallel()

	h1 := hashForBody(formatcfg.DefaultObjectFormat, plumbing.BlobObject, "one")
	valid := encodeCue(20, []cueRecord{{hash: h1, typ: plumbing.BlobObject, offset: 0, length: 3}})

	tests := []struct {
		name string
		raw  []byte
	}{
		{"truncated header", valid[:10]},
		{"truncated record", valid[:len(valid)-1]},
		{"bad magic", tamper(valid, 0, 'X')},
		{"non-zero reserved", tamper(valid, 5, 1)},
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

func tamper(b []byte, i int, v byte) []byte {
	cp := append([]byte(nil), b...)
	cp[i] = v
	return cp
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
	seen := map[string]int{}
	s2 := newTestStorer(t, f, WithObserver(func(op string, _ time.Duration, _ error) { seen[op]++ }))

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
	if seen["HeadObject"] != 0 {
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

			bins, cues, loose := packContents(t, f)
			// Ceiling division, never a rounded-up count that would imply an
			// empty trailing container: limits 1 and n divide n exactly.
			want := (n + tt.limit - 1) / tt.limit
			if len(bins) != want || len(cues) != want {
				t.Fatalf("got %d bins and %d cues, want %d of each (%d objects, cap %d)", len(bins), len(cues), want, n, tt.limit)
			}
			if loose != 0 {
				t.Errorf("expected zero loose objects, got %d", loose)
			}

			counts := map[plumbing.Hash]int{}
			for id, raw := range cues {
				bin, ok := bins[id]
				if !ok {
					t.Fatalf("cue %s has no sibling .bin", id)
				}
				// The id is the .bin's sha256, so a shared or reused hasher
				// across segments shows up right here.
				if got := fmt.Sprintf("%x", sha256.Sum256(bin)); got != id {
					t.Errorf("pack %s: .bin hashes to %s (id is not its own checksum)", id, got)
				}

				recs, err := parseCue(s.oh.Size(), raw)
				if err != nil {
					t.Fatalf("parse cue %s: %v", id, err)
				}
				if len(recs) == 0 {
					t.Errorf("pack %s holds no records", id)
				}
				if len(recs) > tt.limit {
					t.Errorf("pack %s holds %d records, cap is %d", id, len(recs), tt.limit)
				}
				if !sort.SliceIsSorted(recs, func(i, j int) bool {
					return bytes.Compare(recs[i].hash.Bytes(), recs[j].hash.Bytes()) < 0
				}) {
					t.Errorf("pack %s: records are not hash-sorted", id)
				}
				for _, r := range recs {
					if end := r.offset + r.length; end > int64(len(bin)) {
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
			if len(counts) != n {
				t.Errorf("containers index %d distinct objects, want %d", len(counts), n)
			}

			// Cold read across every container: reads have no cap.
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

func TestPackBulkDownloadThreshold(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	blobs := blobHashes(fx)
	if len(blobs) < packBulkFetchThreshold+1 {
		t.Fatalf("fixture too small: %d blobs, need > %d", len(blobs), packBulkFetchThreshold)
	}

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	s2 := newTestStorer(t, f)

	for i := 0; i < packBulkFetchThreshold; i++ {
		if _, err := s2.EncodedObject(plumbing.BlobObject, blobs[i]); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if got := f.nrangedGets(); got != packBulkFetchThreshold {
		t.Errorf("ranged GETs after %d distinct reads = %d, want %d", packBulkFetchThreshold, got, packBulkFetchThreshold)
	}
	if f.nfullBinGets() != 0 {
		t.Error("bulk pack download fired before crossing the threshold")
	}

	// Re-reading an already-seen hash below the threshold: still a ranged
	// GET (no bulk copy exists yet), but must not trigger a download.
	if _, err := s2.EncodedObject(plumbing.BlobObject, blobs[0]); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if f.nfullBinGets() != 0 {
		t.Error("re-reading a seen hash below threshold triggered a bulk download")
	}

	// The (threshold+1)th distinct object crosses the line.
	if _, err := s2.EncodedObject(plumbing.BlobObject, blobs[packBulkFetchThreshold]); err != nil {
		t.Fatalf("crossing read: %v", err)
	}
	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("bulk pack downloads after crossing threshold = %d, want 1", got)
	}
	rangedAtCrossing := f.nrangedGets()

	// Every further read — new or repeated — costs nothing more.
	for i := 0; i <= packBulkFetchThreshold; i++ {
		if _, err := s2.EncodedObject(plumbing.BlobObject, blobs[i]); err != nil {
			t.Fatalf("post-bulk read %d: %v", i, err)
		}
	}
	if f.nfullBinGets() != 1 {
		t.Errorf("bulk pack downloads after further reads = %d, want 1", f.nfullBinGets())
	}
	if f.nrangedGets() != rangedAtCrossing {
		t.Errorf("ranged GETs grew after the bulk download landed: %d -> %d", rangedAtCrossing, f.nrangedGets())
	}
}

func TestPackBulkDownloadVerifies(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)
	blobs := blobHashes(fx)
	if len(blobs) < packBulkFetchThreshold+1 {
		t.Fatalf("fixture too small: %d blobs, need > %d", len(blobs), packBulkFetchThreshold)
	}

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

	s2 := newTestStorer(t, f)
	for i := 0; i <= packBulkFetchThreshold; i++ {
		if _, err := s2.EncodedObject(plumbing.BlobObject, blobs[i]); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if f.nfullBinGets() == 0 {
		t.Error("expected a bulk-download attempt")
	}

	before := f.nfullBinGets()
	if _, err := s2.EncodedObject(plumbing.BlobObject, blobs[0]); err != nil {
		t.Fatalf("read after sticky failure: %v", err)
	}
	if f.nfullBinGets() != before {
		t.Error("bulk download retried after a sticky checksum failure")
	}
}

func TestIterUsesBulkDownload(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	t.Parallel()

	fx := buildPackFixture(t, 40, false)

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	s2 := newTestStorer(t, f)
	iter, err := s2.IterEncodedObjects(plumbing.BlobObject)
	if err != nil {
		t.Fatalf("IterEncodedObjects: %v", err)
	}
	defer iter.Close()

	count := 0
	if err := iter.ForEach(func(plumbing.EncodedObject) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if count == 0 {
		t.Fatal("iterator yielded nothing")
	}

	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("bulk pack downloads after full iteration = %d, want 1", got)
	}
	if got := f.nrangedGets(); got > packBulkFetchThreshold {
		t.Errorf("ranged GETs = %d, want <= %d (should self-convert to bulk)", got, packBulkFetchThreshold)
	}
}
