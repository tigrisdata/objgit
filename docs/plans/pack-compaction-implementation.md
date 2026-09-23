# Pack Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge small pack containers into larger ones and delete the sources after a grace period, so pack count and duplicate storage stop growing without bound.

**Architecture:** A pass bin-packs *whole* source packs into an output container and copies every payload verbatim. Because the write-side containment rule already guarantees a delta's base sits in the same container as the delta, packing whole packs preserves containment for free — no decode, no delta resolution, no compressor. Deletion is deferred through a tombstone object with a deadline, so a request that built its pack index before the merge never loses the bytes under it. A background sweeper in the daemon runs one pass per repository.

**Tech Stack:** Go 1.26, `github.com/aws/aws-sdk-go-v2/service/s3`, `github.com/go-git/go-git/v6`, `github.com/klauspost/compress/zstd`, `github.com/prometheus/client_golang`, `golang.org/x/sync/errgroup`.

**Spec:** `docs/plans/pack-compaction.md`

## Global Constraints

- Module path `github.com/tigrisdata/objgit`. Go 1.26.
- Flags are **kebab-case**, paired with `flagenv` for the `UPPER_SNAKE` environment fallback.
- `slog` uses `"err"` as the error key, never `"error"`.
- Tests are **table-driven with `tt`**. Gate any test that shells out to git with `exec.LookPath("git")`.
- Reuse the shared test helpers `runGit`, `tryGit`, and `seedRepo` from `cmd/objgitd/git_protocol_test.go`.
- Every Prometheus vector lives in `internal/metrics`, registered with `promauto` under `namespace = "objgit"`.
- A pack is immutable. Never write to an existing `packs/<id>.bin` or `packs/<id>.cue`.
- Compaction never drops an object. Every hash in a source pack appears in the output.
- Architecture notes go in `docs/architecture/`, not in `AGENTS.md`.

---

### Task 1: Teach the S3 fake about size, mtime, and delimiters

The fake returns only `Key` from `ListObjectsV2`. Compaction needs the object size and the last-modified time to make grace-period and bin-packing decisions, and the sweeper needs `CommonPrefixes` to enumerate repositories.

**Files:**
- Modify: `internal/storage/tigris/client_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `fakeObject.mtime time.Time`; `fakeS3.clock func() time.Time`; `(*fakeS3).setMtime(key string, t time.Time)`. `ListObjectsV2` now populates `types.Object.Size` and `types.Object.LastModified`, and honors `p.Delimiter` by returning `types.CommonPrefix` values.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/client_test.go`:

```go
func TestFakeListReportsSizeMtimeAndPrefixes(t *testing.T) {
	base := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	f.clock = func() time.Time { return base }
	f.put("org/repo/packs/a.bin", "hello", nil)
	f.put("org/repo/packs/a.cue", "xy", nil)
	f.put("org/other/HEAD", "z", nil)

	out, err := f.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: sp("b"), Prefix: sp("org/repo/packs/"),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(out.Contents) != 2 {
		t.Fatalf("want 2 objects, got %d", len(out.Contents))
	}
	if got := *out.Contents[0].Size; got != 5 {
		t.Errorf("size = %d, want 5", got)
	}
	if got := *out.Contents[0].LastModified; !got.Equal(base) {
		t.Errorf("mtime = %v, want %v", got, base)
	}

	out, err = f.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: sp("b"), Prefix: sp("org/"), Delimiter: sp("/"),
	})
	if err != nil {
		t.Fatalf("list with delimiter: %v", err)
	}
	if len(out.Contents) != 0 {
		t.Errorf("want no direct children, got %d", len(out.Contents))
	}
	var got []string
	for _, cp := range out.CommonPrefixes {
		got = append(got, sv(cp.Prefix))
	}
	want := []string{"org/other/", "org/repo/"}
	if !slices.Equal(got, want) {
		t.Errorf("common prefixes = %v, want %v", got, want)
	}
}
```

Add `"slices"` to the test file's imports.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestFakeListReportsSizeMtimeAndPrefixes -v`
Expected: FAIL — `out.Contents[0].Size` is nil, so the test panics on the nil dereference. `f.clock` and `f.setMtime` do not compile.

- [ ] **Step 3: Add the clock and mtime to the fake**

In `internal/storage/tigris/client_test.go`, add `mtime` to `fakeObject`:

```go
type fakeObject struct {
	body  []byte
	meta  map[string]string
	etag  string
	mtime time.Time
}
```

Add the clock field to `fakeS3` next to the other knobs:

```go
	// clock stamps fakeObject.mtime. Tests that care about the grace period
	// replace it so a deadline is deterministic instead of wall-clock bound.
	clock func() time.Time
```

Add a helper beside the other fake helpers:

```go
// now reads the fake's clock, defaulting to the wall clock.
func (f *fakeS3) now() time.Time {
	if f.clock != nil {
		return f.clock()
	}
	return time.Now()
}

// setMtime backdates one key, so a test can age an object past a grace period
// without sleeping.
func (f *fakeS3) setMtime(key string, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[key]
	if !ok {
		f.t.Fatalf("setMtime: %s is not in the fake bucket", key)
	}
	o.mtime = t
	f.objs[key] = o
}
```

Two places construct a `fakeObject`, and both need the stamp. In `putLocked`, change the stored literal to:

```go
	f.objs[key] = fakeObject{body: []byte(body), meta: m, etag: f.nextETag(), mtime: f.now()}
```

Then find the `fakeObject` literal inside `(*fakeS3).PutObject` and add `mtime: f.now()` to it the same way.

- [ ] **Step 4: Populate size, mtime, and common prefixes in the listing**

Replace the body of `(*fakeS3).ListObjectsV2` after the `f.listErr` check:

```go
	prefix := sv(p.Prefix)
	delim := sv(p.Delimiter)

	// With a delimiter, a key whose remainder past the prefix still contains
	// the delimiter collapses into a common prefix instead of a result.
	seenPrefix := map[string]struct{}{}
	var matched []string
	var prefixes []string
	for k := range f.objs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if delim != "" {
			rest := k[len(prefix):]
			if i := strings.Index(rest, delim); i >= 0 {
				cp := prefix + rest[:i+len(delim)]
				if _, dup := seenPrefix[cp]; !dup {
					seenPrefix[cp] = struct{}{}
					prefixes = append(prefixes, cp)
				}
				continue
			}
		}
		matched = append(matched, k)
	}
	sort.Strings(matched)
	sort.Strings(prefixes)

	start := 0
	if tok := sv(p.ContinuationToken); tok != "" {
		start, _ = strconv.Atoi(tok)
	}
	end := len(matched)
	if f.listMax > 0 && start+int(f.listMax) < end {
		end = start + int(f.listMax)
	}

	out := &s3.ListObjectsV2Output{IsTruncated: bp(end < len(matched))}
	for _, k := range matched[start:end] {
		o := f.objs[k]
		size := int64(len(o.body))
		mt := o.mtime
		out.Contents = append(out.Contents, types.Object{
			Key:          sp(k),
			Size:         &size,
			LastModified: &mt,
		})
	}
	// Common prefixes ride along with the first page only, which is enough for
	// every caller here and matches how S3 behaves for small listings.
	if start == 0 {
		for _, cp := range prefixes {
			out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: sp(cp)})
		}
	}
	if bv(out.IsTruncated) {
		out.NextContinuationToken = sp(strconv.Itoa(end))
	}
	return out, nil
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test ./internal/storage/tigris/ -run TestFakeListReportsSizeMtimeAndPrefixes -v`
Expected: PASS

- [ ] **Step 6: Run the whole package to catch regressions**

Run: `go test ./internal/storage/tigris/`
Expected: PASS. The listing change is additive, so existing tests keep passing.

- [ ] **Step 7: Commit**

```bash
git add internal/storage/tigris/client_test.go
git commit -m "test(storage/tigris): fake reports object size, mtime, and common prefixes"
```

---

### Task 2: `listEntries` returns size and mtime alongside the key

`listKeys` throws away the size and the last-modified time that `ListObjectsV2` already returned. Compaction needs both, and paying a second round trip for them would be waste.

**Files:**
- Modify: `internal/storage/tigris/iter.go:17-47`
- Test: `internal/storage/tigris/iter_test.go`

**Interfaces:**
- Consumes: Task 1's fake.
- Produces: `type objectEntry struct { key string; size int64; mtime time.Time }` and `func (s *Storer) listEntries(prefix string) ([]objectEntry, error)`. `listKeys` keeps its exact current signature and becomes a wrapper.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/iter_test.go`:

```go
func TestListEntriesCarriesSizeAndMtime(t *testing.T) {
	base := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	f.clock = func() time.Time { return base }
	f.put("packs/aa.bin", "0123456789", nil)
	f.put("packs/aa.cue", "ab", nil)
	s := newTestStorer(t, f)

	got, err := s.listEntries("packs/")
	if err != nil {
		t.Fatalf("listEntries: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d", len(got))
	}
	if got[0].key != "packs/aa.bin" || got[0].size != 10 {
		t.Errorf("entry 0 = %+v, want packs/aa.bin size 10", got[0])
	}
	if !got[0].mtime.Equal(base) {
		t.Errorf("mtime = %v, want %v", got[0].mtime, base)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestListEntriesCarriesSizeAndMtime -v`
Expected: FAIL — `s.listEntries undefined`.

- [ ] **Step 3: Implement `listEntries` and rewrite `listKeys` as a wrapper**

In `internal/storage/tigris/iter.go`, replace `listKeys` with:

```go
// objectEntry is one listed key plus the two facts ListObjectsV2 already
// returns with it. Compaction decides bin packing from size and grace-period
// eligibility from mtime, so throwing them away would cost a HEAD per key.
type objectEntry struct {
	key   string
	size  int64
	mtime time.Time
}

// listEntries walks one prefix fully. S3 returns Contents lexicographically
// with monotone continuation tokens, so results come back sorted.
func (s *Storer) listEntries(prefix string) ([]objectEntry, error) {
	var entries []objectEntry
	token := ""

	for {
		in := &s3.ListObjectsV2Input{
			Bucket: sp(s.bucket),
			Prefix: sp(prefix),
		}
		if token != "" {
			in.ContinuationToken = sp(token)
		}

		start := time.Now()
		page, err := s.client.ListObjectsV2(s.ctx, in)
		s.observe("ListObjectsV2", start, err)
		if err != nil {
			return nil, fmt.Errorf("tigris: list %q: %w", prefix, err)
		}

		for _, entry := range page.Contents {
			k := sv(entry.Key)
			if k == "" {
				continue
			}
			e := objectEntry{key: k}
			if entry.Size != nil {
				e.size = *entry.Size
			}
			if entry.LastModified != nil {
				e.mtime = *entry.LastModified
			}
			entries = append(entries, e)
		}
		if !bv(page.IsTruncated) || sv(page.NextContinuationToken) == "" {
			break
		}
		token = sv(page.NextContinuationToken)
	}
	return entries, nil
}

// listKeys is listEntries for the callers that want names only.
func (s *Storer) listKeys(prefix string) ([]string, error) {
	entries, err := s.listEntries(prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		keys = append(keys, e.key)
	}
	return keys, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run 'TestListEntries|TestIter' -v`
Expected: PASS

- [ ] **Step 5: Run the whole package**

Run: `go test ./internal/storage/tigris/`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/storage/tigris/iter.go internal/storage/tigris/iter_test.go
git commit -m "feat(storage/tigris): listEntries reports key size and mtime"
```

---

### Task 3: The tombstone binary format

The tombstone object records which packs to delete and when. Its format copies `packed-refs` byte for byte, so a reader of one format can read the other by eye.

**Files:**
- Create: `internal/storage/tigris/tombstones.go`
- Test: `internal/storage/tigris/tombstones_test.go`

**Interfaces:**
- Consumes: `compressBlock` and the zstd decoder from `compress.go`, as `packedrefs.go` uses them.
- Produces: `const tombstonesKey = "pack-tombstones"`; `type tombstone struct { id string; deadline time.Time }`; `func encodeTombstones(ts []tombstone) []byte`; `func decodeTombstones(raw []byte) ([]tombstone, error)`; `var errBadTombstones error`.

- [ ] **Step 1: Write the failing test**

Create `internal/storage/tigris/tombstones_test.go`:

```go
package tigris

import (
	"errors"
	"testing"
	"time"
)

func TestTombstoneRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   []tombstone
	}{
		{name: "empty", in: nil},
		{
			name: "sorted on encode",
			in: []tombstone{
				{id: "ff02", deadline: time.Unix(1756688400, 0).UTC()},
				{id: "0a1b", deadline: time.Unix(1756684800, 0).UTC()},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeTombstones(encodeTombstones(tt.in))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(got) != len(tt.in) {
				t.Fatalf("got %d entries, want %d", len(got), len(tt.in))
			}
			for i := 1; i < len(got); i++ {
				if got[i-1].id >= got[i].id {
					t.Errorf("entries are not sorted by id: %v", got)
				}
			}
			byID := map[string]time.Time{}
			for _, e := range got {
				byID[e.id] = e.deadline
			}
			for _, want := range tt.in {
				if d, ok := byID[want.id]; !ok || !d.Equal(want.deadline) {
					t.Errorf("id %s = %v (present %v), want %v", want.id, d, ok, want.deadline)
				}
			}
		})
	}
}

func TestTombstoneEncodingIsStable(t *testing.T) {
	in := []tombstone{
		{id: "0a1b", deadline: time.Unix(1756684800, 0)},
		{id: "ff02", deadline: time.Unix(1756688400, 0)},
	}
	a := encodeTombstones(in)
	b := encodeTombstones([]tombstone{in[1], in[0]})
	if string(a) != string(b) {
		t.Error("one set of tombstones must encode to identical bytes in any order")
	}
}

func TestTombstoneRejectsCorruption(t *testing.T) {
	good := encodeTombstones([]tombstone{{id: "0a1b", deadline: time.Unix(1, 0)}})
	for _, tt := range []struct {
		name string
		in   []byte
	}{
		{name: "truncated header", in: good[:8]},
		{name: "bad magic", in: append([]byte{'X', 'X', 'X'}, good[3:]...)},
		{name: "unknown version", in: append(append([]byte{}, good[:3]...), append([]byte{9}, good[4:]...)...)},
		{name: "count disagrees with body", in: func() []byte {
			b := append([]byte{}, good...)
			b[tombCountOff+3] = 9
			return b
		}()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeTombstones(tt.in); !errors.Is(err, errBadTombstones) {
				t.Errorf("err = %v, want errBadTombstones", err)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestTombstone -v`
Expected: FAIL — `undefined: tombstone`, `undefined: encodeTombstones`.

- [ ] **Step 3: Implement the format**

Create `internal/storage/tigris/tombstones.go`:

```go
package tigris

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// tombstonesKey records packs whose contents were copied into a merged
// container, and the time after which deleting them is safe. It sits at the
// root beside packed-refs, shallow, index, and config.
//
// The key deliberately does not start with packs/, so a pack listing can never
// return it, for the same reason packed-refs does not start with refs/.
const tombstonesKey = "pack-tombstones"

const (
	tombHeaderLen = 16
	tombVersion1  = 1

	// tombCodecOff is header byte 4: the codec of the body block.
	tombCodecOff = 4
	// tombCountOff is header byte 5: the entry count, big-endian uint32. It is
	// a checksum on the body and nothing else, and it catches a body truncated
	// on a line boundary, which no other check sees.
	tombCountOff = 5
)

// tombMagic opens every version of the tombstone object. Header byte 3 carries
// the version itself.
var tombMagic = [3]byte{'O', 'G', 'T'}

// errBadTombstones marks a malformed tombstone object. Corruption never
// masquerades as absence, the same posture errBadCue and errBadPackedRefs take.
//
// Reading an empty set out of a corrupt object is the dangerous failure: the
// pass would then believe no pack is pending deletion, write fresh tombstones
// for packs it just merged, and leak every source pack recorded before.
var errBadTombstones = errors.New("tigris: malformed pack-tombstones object")

// tombstone is one pack awaiting deletion, and the time it becomes safe to
// delete. The deadline is absolute rather than a duration, so changing the
// grace flag never retroactively expires a record that was already written.
type tombstone struct {
	id       string
	deadline time.Time
}

// encodeTombstones serializes ts into the tombstone format: a 16-byte
// plaintext header, then a body that is raw or one zstd frame.
//
// The body is text, one entry per line, "<pack-id>\t<unix-seconds>\n", sorted
// by pack id. Sorting means one set of tombstones always encodes to identical
// bytes, so a test can assert bytes rather than compare sets, and a person who
// decompresses the body can read it. A pack id is hex, so it can hold no tab
// and the separator is never ambiguous.
func encodeTombstones(ts []tombstone) []byte {
	sorted := make([]tombstone, len(ts))
	copy(sorted, ts)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].id < sorted[j].id })

	var body bytes.Buffer
	for _, e := range sorted {
		body.WriteString(e.id)
		body.WriteByte('\t')
		body.WriteString(strconv.FormatInt(e.deadline.Unix(), 10))
		body.WriteByte('\n')
	}

	stored, compressed := compressBlock(body.Bytes())

	out := make([]byte, tombHeaderLen, tombHeaderLen+len(stored))
	copy(out, tombMagic[:])
	out[3] = tombVersion1
	if compressed {
		out[tombCodecOff] = codecZstd
	}
	binary.BigEndian.PutUint32(out[tombCountOff:], uint32(len(sorted)))
	return append(out, stored...)
}

// decodeTombstones is encodeTombstones' inverse.
func decodeTombstones(raw []byte) ([]tombstone, error) {
	if len(raw) < tombHeaderLen {
		return nil, fmt.Errorf("%w: header truncated (%d bytes)", errBadTombstones, len(raw))
	}
	if !bytes.Equal(raw[0:3], tombMagic[:]) {
		return nil, fmt.Errorf("%w: bad magic", errBadTombstones)
	}
	if v := raw[3]; v != tombVersion1 {
		return nil, fmt.Errorf("%w: unsupported format version %d", errBadTombstones, v)
	}

	body := raw[tombHeaderLen:]
	switch codec := raw[tombCodecOff]; codec {
	case codecRaw:
	case codecZstd:
		out, err := cueDecoder().DecodeAll(body, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: body does not decompress: %w", errBadTombstones, err)
		}
		body = out
	default:
		return nil, fmt.Errorf("%w: unknown body codec %d", errBadTombstones, codec)
	}

	want := binary.BigEndian.Uint32(raw[tombCountOff:])

	var out []tombstone
	for line := range strings.SplitSeq(strings.TrimSuffix(string(body), "\n"), "\n") {
		if line == "" {
			continue
		}
		id, secs, ok := strings.Cut(line, "\t")
		if !ok {
			return nil, fmt.Errorf("%w: line %q has no separator", errBadTombstones, line)
		}
		unix, err := strconv.ParseInt(secs, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: line %q has an unreadable deadline: %w", errBadTombstones, line, err)
		}
		out = append(out, tombstone{id: id, deadline: time.Unix(unix, 0)})
	}

	if uint32(len(out)) != want {
		return nil, fmt.Errorf("%w: header claims %d entries, body holds %d", errBadTombstones, want, len(out))
	}
	return out, nil
}
```

NOTE: `cueDecoder`, `compressBlock`, `codecRaw`, and `codecZstd` all already exist in `compress.go`. If `strings.SplitSeq` is unavailable, use `for _, line := range strings.Split(...)`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run TestTombstone -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/tombstones.go internal/storage/tigris/tombstones_test.go
git commit -m "feat(storage/tigris): add the pack-tombstones binary format"
```

---

### Task 4: Extract `casPut`, and read and write the tombstone object

`commitRefs` inlines its conditional `PutObject`. The tombstone object needs exactly the same compare-and-swap, so factor the call out rather than writing it twice.

**Files:**
- Modify: `internal/storage/tigris/refcache.go:196-215`
- Modify: `internal/storage/tigris/tombstones.go`
- Test: `internal/storage/tigris/tombstones_test.go`

**Interfaces:**
- Consumes: `tombstonesKey`, `encodeTombstones`, `decodeTombstones` from Task 3; `fetchSmallETag` from `refs.go:343`; `isPreconditionFailed` from `tigris.go:358`.
- Produces: `var errCASConflict error`; `func (s *Storer) casPut(key string, body []byte, etag string) (string, error)`; `func (s *Storer) loadTombstones() ([]tombstone, string, error)`; `func (s *Storer) storeTombstones(ts []tombstone, etag string) (string, error)`.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/tombstones_test.go`:

```go
func TestTombstoneStoreLoadRoundTrip(t *testing.T) {
	f := newFakeS3(t)
	s := newTestStorer(t, f)

	got, etag, err := s.loadTombstones()
	if err != nil {
		t.Fatalf("load on empty bucket: %v", err)
	}
	if len(got) != 0 || etag != "" {
		t.Fatalf("empty bucket = %v/%q, want no entries and no etag", got, etag)
	}

	in := []tombstone{{id: "0a1b", deadline: time.Unix(1756684800, 0)}}
	etag, err = s.storeTombstones(in, "")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if etag == "" {
		t.Fatal("store returned no etag")
	}

	got, got2, err := s.loadTombstones()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 1 || got[0].id != "0a1b" {
		t.Errorf("loaded %v, want one entry with id 0a1b", got)
	}
	if got2 != etag {
		t.Errorf("etag = %q, want %q", got2, etag)
	}
}

func TestTombstoneStoreDetectsConflict(t *testing.T) {
	f := newFakeS3(t)
	s := newTestStorer(t, f)

	etag, err := s.storeTombstones([]tombstone{{id: "aa", deadline: time.Unix(1, 0)}}, "")
	if err != nil {
		t.Fatalf("first store: %v", err)
	}
	if _, err := s.storeTombstones([]tombstone{{id: "bb", deadline: time.Unix(2, 0)}}, ""); !errors.Is(err, errCASConflict) {
		t.Errorf("store with no etag over an existing object: err = %v, want errCASConflict", err)
	}
	if _, err := s.storeTombstones([]tombstone{{id: "bb", deadline: time.Unix(2, 0)}}, "stale"); !errors.Is(err, errCASConflict) {
		t.Errorf("store with a stale etag: err = %v, want errCASConflict", err)
	}
	if _, err := s.storeTombstones([]tombstone{{id: "bb", deadline: time.Unix(2, 0)}}, etag); err != nil {
		t.Errorf("store with the current etag: %v", err)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestTombstoneStore -v`
Expected: FAIL — `undefined: errCASConflict`, `s.loadTombstones undefined`.

- [ ] **Step 3: Add `casPut` and the tombstone accessors**

Append to `internal/storage/tigris/tombstones.go`:

```go
// errCASConflict marks a conditional PutObject that lost its race. The caller
// re-reads and retries, which is what makes it a distinct error rather than an
// opaque failure.
var errCASConflict = errors.New("tigris: conditional put lost its race")

// casPut writes body to key under a compare-and-swap and reports the new ETag.
//
// An empty etag means "this object must not exist yet" and sends
// If-None-Match: *. Any other value sends If-Match. A refused precondition
// comes back as errCASConflict, never as an opaque error, so a retry loop can
// tell contention apart from a real failure.
func (s *Storer) casPut(key string, body []byte, etag string) (string, error) {
	in := &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
		Body:   bytes.NewReader(body),
	}
	if etag == "" {
		in.IfNoneMatch = sp("*")
	} else {
		in.IfMatch = sp(etag)
	}

	start := time.Now()
	out, err := s.client.PutObject(s.ctx, in)
	s.observe("PutObject", start, err)
	switch {
	case err == nil:
		return sv(out.ETag), nil
	case isPreconditionFailed(err):
		return "", errCASConflict
	default:
		return "", fmt.Errorf("tigris: conditional put %s: %w", key, err)
	}
}

// loadTombstones reads the tombstone object and its compare-and-swap token. A
// missing object is an empty set with an empty token, which is what a
// repository that has never been compacted looks like.
func (s *Storer) loadTombstones() ([]tombstone, string, error) {
	raw, etag, err := s.fetchSmallETag(s.prefix + tombstonesKey)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return nil, "", nil
	default:
		return nil, "", fmt.Errorf("tigris: load tombstones: %w", err)
	}

	ts, derr := decodeTombstones(raw)
	if derr != nil {
		return nil, "", derr
	}
	return ts, etag, nil
}

// storeTombstones writes the set under a compare-and-swap against etag.
func (s *Storer) storeTombstones(ts []tombstone, etag string) (string, error) {
	return s.casPut(s.prefix+tombstonesKey, encodeTombstones(ts), etag)
}
```

Add `"github.com/aws/aws-sdk-go-v2/service/s3"` and `"github.com/go-git/go-git/v6/plumbing"` to the file's imports.

- [ ] **Step 4: Rewrite `commitRefs` to use `casPut`**

In `internal/storage/tigris/refcache.go`, replace the block that builds `in`, calls `PutObject`, and switches on the error with:

```go
		newETag, err := s.casPut(s.prefix+packedRefsKey, encodePackedRefs(next), etag)
		switch {
		case err == nil:
		case errors.Is(err, errCASConflict):
			continue // somebody else landed first; re-read and re-apply
		default:
			return fmt.Errorf("tigris: commit refs: %w", err)
		}
```

Then replace `s.refs.etag = sv(out.ETag)` with `s.refs.etag = newETag`. Remove any import that this leaves unused, and add `"errors"` if it is not already imported.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run 'TestTombstone|TestRef|TestCheckAndSet|TestPackedRefs' -v`
Expected: PASS. The existing ref compare-and-swap tests are the regression net for the refactor.

- [ ] **Step 6: Run the whole package**

Run: `go test ./internal/storage/tigris/`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/storage/tigris/tombstones.go internal/storage/tigris/tombstones_test.go internal/storage/tigris/refcache.go
git commit -m "refactor(storage/tigris): extract casPut, add tombstone load and store"
```

---

### Task 5: `surveyPacks` reads every pack's records, size, and mtime

The pass and the cold index build both list `packs/` and parse every `.cue`. Share one function so the two can never disagree about what a pack is.

**Files:**
- Modify: `internal/storage/tigris/packindex.go:553-595`
- Test: `internal/storage/tigris/compact_test.go` (create)

**Interfaces:**
- Consumes: `listEntries` from Task 2; `parseCue` from `packindex.go:207`.
- Produces: `type packSurvey struct { id string; recs []cueRecord; size int64; mtime time.Time }` and `func (s *Storer) surveyPacks() (packs []packSurvey, orphanBins []objectEntry, err error)`. `ensurePacksBuilt` keeps its exact current signature and behavior.

- [ ] **Step 1: Write the failing test**

Create `internal/storage/tigris/compact_test.go`:

```go
package tigris

import (
	"testing"
	"time"
)

func TestSurveyPacksReportsPacksAndOrphans(t *testing.T) {
	base := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	f.clock = func() time.Time { return base }
	s := newTestStorer(t, f)

	// One complete container, and one .bin whose .cue upload never landed.
	writeFakePack(t, f, s, "aa", []testObj{{typ: plumbing.BlobObject, body: []byte("one")}})
	f.put("packs/orphan.bin", "dangling", nil)

	packs, orphans, err := s.surveyPacks()
	if err != nil {
		t.Fatalf("surveyPacks: %v", err)
	}
	if len(packs) != 1 || packs[0].id != "aa" {
		t.Fatalf("packs = %+v, want one pack with id aa", packs)
	}
	if len(packs[0].recs) != 1 {
		t.Errorf("pack aa holds %d records, want 1", len(packs[0].recs))
	}
	if !packs[0].mtime.Equal(base) {
		t.Errorf("mtime = %v, want %v", packs[0].mtime, base)
	}
	if len(orphans) != 1 || orphans[0].key != "packs/orphan.bin" {
		t.Errorf("orphans = %+v, want packs/orphan.bin", orphans)
	}
}
```

Add a shared test helper at the bottom of `compact_test.go`. It writes a real container through the package's own encoder, so tests never hand-roll `.cue` bytes:

```go
type testObj struct {
	typ  plumbing.ObjectType
	body []byte
}

// hashOf computes an object id the same way the storer does. ObjectHasher.Compute
// returns an error, and threading that through every fixture would drown them.
func hashOf(t *testing.T, s *Storer, typ plumbing.ObjectType, body []byte) plumbing.Hash {
	t.Helper()
	h, err := s.oh.Compute(typ, body)
	if err != nil {
		t.Fatalf("hash %s object: %v", typ, err)
	}
	return h
}

// writeFakePack stages a container with the given objects directly into the
// fake bucket under the given id, using the package's own cue encoder so the
// bytes are exactly what a real push would leave.
func writeFakePack(t *testing.T, f *fakeS3, s *Storer, id string, objs []testObj) {
	t.Helper()
	var bin []byte
	recs := make([]cueRecord, 0, len(objs))
	for _, o := range objs {
		h := hashOf(t, s, o.typ, o.body)
		recs = append(recs, cueRecord{
			hash:   h,
			typ:    o.typ,
			codec:  codecRaw,
			offset: int64(len(bin)),
			stored: int64(len(o.body)),
			raw:    int64(len(o.body)),
		})
		bin = append(bin, o.body...)
	}
	sort.Slice(recs, func(i, j int) bool {
		return bytes.Compare(recs[i].hash.Bytes(), recs[j].hash.Bytes()) < 0
	})
	f.put(s.prefix+packPrefix+id+binSuffix, string(bin), nil)
	f.put(s.prefix+packPrefix+id+cueSuffix, string(encodeCue(s.oh.Size(), recs)), nil)
}
```

NOTE: sorting `recs` after computing offsets is safe, because each record carries its own offset. Add `"bytes"`, `"sort"`, and `"github.com/go-git/go-git/v6/plumbing"` to the imports.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestSurveyPacks -v`
Expected: FAIL — `s.surveyPacks undefined`.

- [ ] **Step 3: Implement `surveyPacks` and rewire `ensurePacksBuilt`**

In `internal/storage/tigris/packindex.go`, add above `ensurePacksBuilt`:

```go
// packSurvey is one container as compaction sees it: its records, the size of
// its .bin, and when that .bin last changed. The pass decides bin packing from
// size and grace-period eligibility from mtime.
type packSurvey struct {
	id    string
	recs  []cueRecord
	size  int64
	mtime time.Time
}

// surveyPacks lists packs/ once and parses every .cue under it.
//
// It also reports every .bin with no sibling .cue. A failed .cue upload leaves
// exactly that shape (packJob.run uploads the .bin first), and such a container
// is invisible to every reader, because the index is built from .cue files
// alone. Only compaction ever looks for them.
func (s *Storer) surveyPacks() ([]packSurvey, []objectEntry, error) {
	entries, err := s.listEntries(s.prefix + packPrefix)
	if err != nil {
		return nil, nil, fmt.Errorf("tigris: list packs: %w", err)
	}

	bins := make(map[string]objectEntry, len(entries)/2)
	cues := make(map[string]objectEntry, len(entries)/2)
	for _, e := range entries {
		name := strings.TrimPrefix(e.key, s.prefix+packPrefix)
		switch {
		case strings.HasSuffix(name, binSuffix):
			bins[strings.TrimSuffix(name, binSuffix)] = e
		case strings.HasSuffix(name, cueSuffix):
			cues[strings.TrimSuffix(name, cueSuffix)] = e
		}
	}

	hashLen := s.oh.Size()
	packs := make([]packSurvey, 0, len(cues))
	for id, cue := range cues {
		raw, err := s.fetchSmall(cue.key)
		if err != nil {
			return nil, nil, fmt.Errorf("tigris: fetch cue %s: %w", cue.key, err)
		}
		recs, err := parseCue(hashLen, raw)
		if err != nil {
			return nil, nil, fmt.Errorf("tigris: parse cue %s: %w", cue.key, err)
		}
		p := packSurvey{id: id, recs: recs}
		if b, ok := bins[id]; ok {
			p.size = b.size
			p.mtime = b.mtime
		}
		packs = append(packs, p)
	}
	sort.Slice(packs, func(i, j int) bool { return packs[i].id < packs[j].id })

	var orphans []objectEntry
	for id, b := range bins {
		if _, ok := cues[id]; !ok {
			orphans = append(orphans, b)
		}
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i].key < orphans[j].key })

	return packs, orphans, nil
}
```

Then replace the body of `ensurePacksBuilt` between the `built` check and the final lock with:

```go
	packs, _, err := s.surveyPacks()
	if err != nil {
		return err
	}

	s.packs.mu.Lock()
	defer s.packs.mu.Unlock()
	if s.packs.built {
		return nil // lost a race with a concurrent cold build on this instance
	}
	for _, p := range packs {
		s.packs.sizes[p.id] = s.packs.indexRecords(p.id, p.recs)
	}
	s.packs.built = true
	return nil
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run 'TestSurveyPacks|TestPack' -v`
Expected: PASS

- [ ] **Step 5: Run the whole package**

Run: `go test ./internal/storage/tigris/`
Expected: PASS. The existing pack-index tests are the regression net for the `ensurePacksBuilt` rewrite.

- [ ] **Step 6: Commit**

```bash
git add internal/storage/tigris/packindex.go internal/storage/tigris/compact_test.go
git commit -m "refactor(storage/tigris): share one pack survey between the index build and compaction"
```

---

### Task 6: Selection and the treadmill guard

Which packs a pass merges is pure arithmetic over the survey. Keep it a pure function so every edge case is a table row and no test needs a bucket.

**Files:**
- Create: `internal/storage/tigris/compact.go`
- Test: `internal/storage/tigris/compact_test.go`

**Interfaces:**
- Consumes: `packSurvey` from Task 5.
- Produces: `func selectForMerge(packs []packSurvey, maxPacks int, capBytes int64) [][]packSurvey`.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/compact_test.go`:

```go
func TestSelectForMerge(t *testing.T) {
	p := func(id string, size int64) packSurvey { return packSurvey{id: id, size: size} }
	const cap = 1000

	for _, tt := range []struct {
		name     string
		packs    []packSurvey
		maxPacks int
		want     [][]string // ids per output group
	}{
		{
			name:     "under the threshold does nothing",
			packs:    []packSurvey{p("a", 10), p("b", 10)},
			maxPacks: 2,
			want:     nil,
		},
		{
			name:     "small packs group into one output",
			packs:    []packSurvey{p("a", 10), p("b", 20), p("c", 30)},
			maxPacks: 2,
			want:     [][]string{{"a", "b", "c"}},
		},
		{
			name:     "a group seals at the byte cap",
			packs:    []packSurvey{p("a", 400), p("b", 400), p("c", 400)},
			maxPacks: 2,
			want:     [][]string{{"a", "b"}, {"c"}},
		},
		{
			name:     "packs over half the cap are never sources",
			packs:    []packSurvey{p("a", 900), p("b", 900), p("c", 900)},
			maxPacks: 1,
			want:     nil,
		},
		{
			name:     "no reduction means no pass",
			packs:    []packSurvey{p("a", 500), p("b", 500), p("c", 10)},
			maxPacks: 1,
			want:     nil,
		},
		{
			name:     "ties break by id so the choice is deterministic",
			packs:    []packSurvey{p("z", 10), p("a", 10), p("m", 10)},
			maxPacks: 2,
			want:     [][]string{{"a", "m", "z"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := selectForMerge(tt.packs, tt.maxPacks, cap)
			var ids [][]string
			for _, g := range got {
				var row []string
				for _, s := range g {
					row = append(row, s.id)
				}
				ids = append(ids, row)
			}
			if fmt.Sprint(ids) != fmt.Sprint(tt.want) {
				t.Errorf("groups = %v, want %v", ids, tt.want)
			}
		})
	}
}
```

Add `"fmt"` to the test imports.

NOTE on the "no reduction" row: only `c` is at or under half the cap, so there is one candidate and one output group. One output for one source reduces nothing, so the guard returns nil.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestSelectForMerge -v`
Expected: FAIL — `undefined: selectForMerge`.

- [ ] **Step 3: Implement selection**

Create `internal/storage/tigris/compact.go`:

```go
package tigris

import (
	"sort"
)

// selectForMerge groups source packs into the output containers a pass will
// build. It returns nil when no pass is worth running.
//
// Two rules keep a pass from becoming a treadmill. A pack larger than half the
// byte cap is never a source, because merging containers that are already large
// rewrites the repository to gain almost nothing. And a selection that would
// produce at least as many outputs as it consumes is abandoned outright: it
// would rewrite the same bytes on every cycle, forever, and bill egress for no
// result.
//
// Candidates sort by size and break ties by id, so two daemons that survey the
// same repository choose the same grouping. That determinism is what makes a
// duplicated pass idempotent rather than merely harmless: identical grouping
// yields byte-identical outputs, which yields one content-addressed pack id.
func selectForMerge(packs []packSurvey, maxPacks int, capBytes int64) [][]packSurvey {
	if len(packs) <= maxPacks {
		return nil
	}

	cand := make([]packSurvey, 0, len(packs))
	for _, p := range packs {
		if p.size <= capBytes/2 {
			cand = append(cand, p)
		}
	}
	sort.Slice(cand, func(i, j int) bool {
		if cand[i].size != cand[j].size {
			return cand[i].size < cand[j].size
		}
		return cand[i].id < cand[j].id
	})

	var groups [][]packSurvey
	var cur []packSurvey
	var curBytes int64
	for _, p := range cand {
		// Seal before the add, so a group never passes the cap. Deduplication
		// only removes bytes, so the sum of the source sizes is a safe upper
		// bound on what the output actually holds.
		if len(cur) > 0 && curBytes+p.size > capBytes {
			groups = append(groups, cur)
			cur, curBytes = nil, 0
		}
		cur = append(cur, p)
		curBytes += p.size
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}

	if len(groups) >= len(cand) {
		return nil
	}
	return groups
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/storage/tigris/ -run TestSelectForMerge -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/compact.go internal/storage/tigris/compact_test.go
git commit -m "feat(storage/tigris): select small packs for compaction with a treadmill guard"
```

---

### Task 7: `appendVerbatim` on a segment, and a `packWriter`-free seal

The merge writes payloads that are already encoded. `packSegment.add` re-encodes through a compressor, which is exactly the cost the design avoids. And `seal` hangs off `packWriter`, which the merge does not have.

**Files:**
- Modify: `internal/storage/tigris/packwriter.go:412-445` (add a method beside `add`)
- Modify: `internal/storage/tigris/packwriter.go:644-697` (`seal`)
- Test: `internal/storage/tigris/pack_test.go`

**Interfaces:**
- Consumes: `packSegment`, `cueRecord`.
- Produces: `func (g *packSegment) appendVerbatim(rec cueRecord, r io.Reader) error`; `func sealSegment(s *Storer, seg *packSegment) (string, error)` returning the new pack id. `(*packWriter).seal` becomes a wrapper that discards the id.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/pack_test.go`:

```go
func TestAppendVerbatimPreservesTheRecord(t *testing.T) {
	f := newFakeS3(t)
	s := newTestStorer(t, f)

	seg, err := newPackSegment(s)
	if err != nil {
		t.Fatalf("newPackSegment: %v", err)
	}
	defer seg.discard()

	body := []byte("already encoded bytes")
	src := cueRecord{
		hash:   hashOf(t, s, plumbing.BlobObject, body),
		typ:    plumbing.BlobObject,
		codec:  codecZstd,
		offset: 4096, // the source offset, which must be rewritten
		stored: int64(len(body)),
		raw:    999,
	}

	if err := seg.appendVerbatim(src, bytes.NewReader(body)); err != nil {
		t.Fatalf("appendVerbatim: %v", err)
	}
	if len(seg.recs) != 1 {
		t.Fatalf("segment holds %d records, want 1", len(seg.recs))
	}
	got := seg.recs[0]
	if got.offset != 0 {
		t.Errorf("offset = %d, want 0 (rewritten to the segment position)", got.offset)
	}
	for _, c := range []struct {
		name       string
		got, want  any
	}{
		{"hash", got.hash, src.hash},
		{"typ", got.typ, src.typ},
		{"codec", got.codec, src.codec},
		{"stored", got.stored, src.stored},
		{"raw", got.raw, src.raw},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestAppendVerbatimRejectsAShortRead(t *testing.T) {
	f := newFakeS3(t)
	s := newTestStorer(t, f)
	seg, err := newPackSegment(s)
	if err != nil {
		t.Fatalf("newPackSegment: %v", err)
	}
	defer seg.discard()

	src := cueRecord{stored: 100}
	if err := seg.appendVerbatim(src, bytes.NewReader([]byte("short"))); err == nil {
		t.Error("copying fewer bytes than the record claims must fail the merge")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestAppendVerbatim -v`
Expected: FAIL — `seg.appendVerbatim undefined`.

- [ ] **Step 3: Implement `appendVerbatim`**

In `internal/storage/tigris/packwriter.go`, add directly after `(*packSegment).add`:

```go
// appendVerbatim copies one already-encoded payload into the segment without
// decoding it, and carries its record over with a rewritten offset.
//
// This is compaction's entire write path. A merge moves payloads between
// containers, so decoding and re-encoding them would burn CPU to produce the
// bytes it already has. Every field but offset survives: codec, so a zstd frame
// stays a frame; base, so a delta stays a delta; raw and typ, so the object
// still reads as itself.
//
// The caller guarantees containment, which is what makes carrying base over
// safe. See compact.go: a merge packs whole source packs into an output, so the
// base of every delta comes with it.
func (g *packSegment) appendVerbatim(rec cueRecord, r io.Reader) error {
	n, err := io.Copy(g.file, r)
	if err != nil {
		return fmt.Errorf("copy %s into the merged container: %w", rec.hash, err)
	}
	if n != rec.stored {
		return fmt.Errorf("%s: copied %d bytes, its record claims %d", rec.hash, n, rec.stored)
	}

	rec.offset = g.offset
	g.recs = append(g.recs, rec)
	g.offset += n
	return nil
}
```

- [ ] **Step 4: Free `seal` from `packWriter`**

In the same file, rename the method and add the wrapper. Change the signature line of `func (w *packWriter) seal(seg *packSegment) error` to:

```go
// sealSegment finishes one segment: it names the pack after its own checksum,
// stages the sibling .cue, and queues both for upload. It takes ownership of
// the segment's staging files, so every error path here removes them itself.
//
// It is a function rather than a packWriter method because compaction seals
// segments too, and it has no incoming packfile to write.
func sealSegment(s *Storer, seg *packSegment) (string, error) {
```

Inside the body, replace every `w.s` with `s`, and change each `return fmt.Errorf(...)` to `return "", fmt.Errorf(...)`. The final `return nil` becomes `return id, nil`. Then add the wrapper beside it:

```go
func (w *packWriter) seal(seg *packSegment) error {
	_, err := sealSegment(w.s, seg)
	return err
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run 'TestAppendVerbatim|TestPack' -v`
Expected: PASS

- [ ] **Step 6: Run the whole package**

Run: `go test ./internal/storage/tigris/`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/storage/tigris/packwriter.go internal/storage/tigris/pack_test.go
git commit -m "feat(storage/tigris): add verbatim segment append and free seal from packWriter"
```

---

### Task 8: Merge one group of packs into one container

This is the heart of the pass: choose the surviving record for each hash, then copy payloads verbatim into a new container.

**Files:**
- Modify: `internal/storage/tigris/compact.go`
- Test: `internal/storage/tigris/compact_test.go`

**Interfaces:**
- Consumes: `selectForMerge` (Task 6), `appendVerbatim` and `sealSegment` (Task 7), `streamPack` from `packindex.go:735`, `newPackSegment` from `packwriter.go:402`.
- Produces: `func chooseRecords(group []packSurvey) map[string][]cueRecord` and `func (s *Storer) mergeGroup(group []packSurvey) (string, int, error)` returning the new pack id and the number of duplicate records dropped.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/compact_test.go`:

```go
func TestChooseRecordsDedupesAndPrefersWhole(t *testing.T) {
	h1 := plumbing.NewHash("1111111111111111111111111111111111111111")
	h2 := plumbing.NewHash("2222222222222222222222222222222222222222")
	base := plumbing.NewHash("3333333333333333333333333333333333333333")

	group := []packSurvey{
		{id: "a", recs: []cueRecord{
			{hash: h1, stored: 5, base: base}, // delta form
			{hash: h2, stored: 7},
		}},
		{id: "b", recs: []cueRecord{
			{hash: h1, stored: 50}, // whole form of the same object
		}},
	}

	got := chooseRecords(group)

	var total int
	for _, recs := range got {
		total += len(recs)
	}
	if total != 2 {
		t.Fatalf("kept %d records, want 2 (h1 once, h2 once)", total)
	}
	for _, r := range got["b"] {
		if r.hash == h1 && r.base != plumbing.ZeroHash {
			t.Error("kept the delta copy of h1, want the whole copy")
		}
	}
	if len(got["b"]) != 1 {
		t.Errorf("pack b contributes %d records, want 1 (the whole h1)", len(got["b"]))
	}
}

func TestMergeGroupProducesAReadableContainer(t *testing.T) {
	f := newFakeS3(t)
	s := newTestStorer(t, f)

	one := []byte("first object")
	two := []byte("second object")
	writeFakePack(t, f, s, "aa", []testObj{{typ: plumbing.BlobObject, body: one}})
	writeFakePack(t, f, s, "bb", []testObj{
		{typ: plumbing.BlobObject, body: two},
		{typ: plumbing.BlobObject, body: one}, // duplicated across the two packs
	})

	packs, _, err := s.surveyPacks()
	if err != nil {
		t.Fatalf("surveyPacks: %v", err)
	}

	id, deduped, err := s.mergeGroup(packs)
	if err != nil {
		t.Fatalf("mergeGroup: %v", err)
	}
	if id == "" {
		t.Fatal("mergeGroup returned no pack id")
	}
	if deduped != 1 {
		t.Errorf("deduped %d records, want 1", deduped)
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// A fresh Storer sees only what is in the bucket, so this proves the merged
	// container stands on its own.
	fresh := newTestStorer(t, f)
	for _, body := range [][]byte{one, two} {
		h := hashOf(t, s, plumbing.BlobObject, body)
		obj, err := fresh.EncodedObject(plumbing.AnyObject, h)
		if err != nil {
			t.Fatalf("read %s back: %v", h, err)
		}
		got, err := io.ReadAll(mustReader(obj))
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if !bytes.Equal(got, body) {
			t.Errorf("object %s = %q, want %q", h, got, body)
		}
	}
}
```

Add `"io"` to the test imports.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run 'TestChooseRecords|TestMergeGroup' -v`
Expected: FAIL — `undefined: chooseRecords`, `s.mergeGroup undefined`.

- [ ] **Step 3: Implement the merge**

Append to `internal/storage/tigris/compact.go`:

```go
// chooseRecords picks the surviving record for every hash in the group, keyed
// by the source pack that supplies its bytes.
//
// One hash can appear in several source packs, and only one copy goes into the
// output. Which copy is a real choice: when one is a delta and the other is
// whole, keep the whole one. Both are correct, but a kept delta can point at a
// base that is itself a delta from another source pack, so a chain grows one
// link on every compaction round. Keeping the whole form stops that growth, and
// it only ever applies to an object two source packs both hold.
//
// Deduplication never removes a hash from the output, only extra copies of one.
// That is what preserves delta containment: every base that was in a source
// pack is still somewhere in the output, so the delta that names it still
// resolves.
func chooseRecords(group []packSurvey) map[string][]cueRecord {
	type pick struct {
		packID string
		rec    cueRecord
	}
	best := make(map[plumbing.Hash]pick)

	for _, p := range group {
		for _, r := range p.recs {
			cur, seen := best[r.hash]
			if !seen {
				best[r.hash] = pick{packID: p.id, rec: r}
				continue
			}
			// Replace a delta with a whole copy, and nothing else.
			if cur.rec.base != plumbing.ZeroHash && r.base == plumbing.ZeroHash {
				best[r.hash] = pick{packID: p.id, rec: r}
			}
		}
	}

	out := make(map[string][]cueRecord, len(group))
	for _, p := range best {
		out[p.packID] = append(out[p.packID], p.rec)
	}
	// Offset order turns each source read into one forward scan of its .bin.
	for id := range out {
		recs := out[id]
		sort.Slice(recs, func(i, j int) bool { return recs[i].offset < recs[j].offset })
	}
	return out
}

// mergeGroup builds one output container from every pack in group and queues
// it for upload. It reports the new pack id and how many duplicate records it
// dropped.
//
// Each source .bin is downloaded whole, once. Sources are the smallest
// containers in the repository by construction (see selectForMerge), and one
// GET per source beats one ranged GET per object by orders of magnitude.
func (s *Storer) mergeGroup(group []packSurvey) (string, int, error) {
	chosen := chooseRecords(group)

	var total int
	for _, p := range group {
		total += len(p.recs)
	}
	var kept int
	for _, recs := range chosen {
		kept += len(recs)
	}

	seg, err := newPackSegment(s)
	if err != nil {
		return "", 0, fmt.Errorf("tigris: compact: %w", err)
	}
	sealed := false
	defer func() {
		if !sealed {
			seg.discard()
		}
	}()

	for _, p := range group {
		recs := chosen[p.id]
		if len(recs) == 0 {
			continue // every object this pack held is supplied by a sibling
		}

		src, err := s.downloadPackToTemp(p.id)
		if err != nil {
			return "", 0, err
		}
		for _, r := range recs {
			if err := seg.appendVerbatim(r, io.NewSectionReader(src, r.offset, r.stored)); err != nil {
				src.Close()
				return "", 0, fmt.Errorf("tigris: compact pack %s: %w", p.id, err)
			}
		}
		src.Close()
	}

	id, err := sealSegment(s, seg)
	if err != nil {
		return "", 0, err
	}
	sealed = true // sealSegment owns the staging files now
	return id, total - kept, nil
}

// downloadPackToTemp fetches one whole .bin into an unlinked temp file. The
// file dies with its last descriptor, so the caller only has to Close it.
//
// This deliberately bypasses the PackCache and the prefetch machinery in
// packindex.go. Those exist to make repeated reads cheap, and a source pack is
// about to be deleted, so caching it would evict something a client still
// wants.
func (s *Storer) downloadPackToTemp(id string) (*os.File, error) {
	f, err := os.CreateTemp("", "objgit-tigris-compact-*")
	if err != nil {
		return nil, fmt.Errorf("tigris: create compaction temp file: %w", err)
	}
	os.Remove(f.Name()) // unlink now; the fd keeps the data alive

	if err := s.streamPack(id)(f); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("tigris: rewind compaction temp file for %s: %w", id, err)
	}
	return f, nil
}
```

Extend the import block of `compact.go` to:

```go
import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/go-git/go-git/v6/plumbing"
)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run 'TestChooseRecords|TestMergeGroup' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/compact.go internal/storage/tigris/compact_test.go
git commit -m "feat(storage/tigris): merge whole packs into one container with verbatim copies"
```

---

### Task 9: The reap step deletes expired tombstones

**Files:**
- Modify: `internal/storage/tigris/compact.go`
- Test: `internal/storage/tigris/compact_test.go`

**Interfaces:**
- Consumes: `loadTombstones`, `storeTombstones`, `errCASConflict` (Task 4); `maxDeleteBatch` and the `DeleteObjects` idiom from `refcache.go:271`.
- Produces: `func (s *Storer) reapTombstones(now time.Time) (deleted int, err error)`.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/compact_test.go`:

```go
func TestReapTombstones(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	s := newTestStorer(t, f)

	writeFakePack(t, f, s, "expired", []testObj{{typ: plumbing.BlobObject, body: []byte("x")}})
	writeFakePack(t, f, s, "live", []testObj{{typ: plumbing.BlobObject, body: []byte("y")}})

	if _, err := s.storeTombstones([]tombstone{
		{id: "expired", deadline: now.Add(-time.Minute)},
		{id: "live", deadline: now.Add(time.Hour)},
		{id: "gone", deadline: now.Add(-time.Minute)}, // already deleted
	}, ""); err != nil {
		t.Fatalf("seed tombstones: %v", err)
	}

	deleted, err := s.reapTombstones(now)
	if err != nil {
		t.Fatalf("reapTombstones: %v", err)
	}
	if deleted != 2 {
		t.Errorf("deleted %d, want 2", deleted)
	}
	for _, k := range []string{"packs/expired.bin", "packs/expired.cue"} {
		if f.has(k) {
			t.Errorf("%s survived the reap", k)
		}
	}
	for _, k := range []string{"packs/live.bin", "packs/live.cue"} {
		if !f.has(k) {
			t.Errorf("%s was deleted before its deadline", k)
		}
	}

	left, _, err := s.loadTombstones()
	if err != nil {
		t.Fatalf("load after reap: %v", err)
	}
	if len(left) != 1 || left[0].id != "live" {
		t.Errorf("tombstones after reap = %v, want only live", left)
	}
}

func TestReapIsIdempotent(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	s := newTestStorer(t, f)

	if _, err := s.storeTombstones([]tombstone{{id: "gone", deadline: now.Add(-time.Hour)}}, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := s.reapTombstones(now); err != nil {
		t.Fatalf("first reap: %v", err)
	}
	if _, err := s.reapTombstones(now); err != nil {
		t.Fatalf("second reap over an empty set: %v", err)
	}
}
```

Add a `has` helper to the fake in `client_test.go` if one is not already present:

```go
// has reports whether the fake bucket holds a key.
func (f *fakeS3) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objs[key]
	return ok
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestReap -v`
Expected: FAIL — `s.reapTombstones undefined`.

- [ ] **Step 3: Implement the reap**

Append to `internal/storage/tigris/compact.go`:

```go
// reapTombstones deletes every pack whose deadline has passed, then rewrites
// the tombstone object without those entries. It reports how many packs it
// deleted.
//
// The order is delete first, tombstone second. If the process stops between
// the two, the tombstone names a pack that is already gone, and a DeleteObjects
// call against a missing key succeeds, so the next reap corrects the record.
// The opposite order loses the record of a pack that still exists, and nothing
// would ever delete it.
func (s *Storer) reapTombstones(now time.Time) (int, error) {
	for attempt := 0; attempt < maxRefCASRetries; attempt++ {
		ts, etag, err := s.loadTombstones()
		if err != nil {
			return 0, err
		}
		if len(ts) == 0 {
			return 0, nil
		}

		var expired, live []tombstone
		for _, e := range ts {
			if now.Before(e.deadline) {
				live = append(live, e)
				continue
			}
			expired = append(expired, e)
		}
		if len(expired) == 0 {
			return 0, nil
		}

		keys := make([]string, 0, len(expired)*2)
		for _, e := range expired {
			keys = append(keys,
				s.prefix+packPrefix+e.id+binSuffix,
				s.prefix+packPrefix+e.id+cueSuffix,
			)
		}
		if err := s.deleteKeys(keys); err != nil {
			return 0, err
		}

		switch _, err := s.storeTombstones(live, etag); {
		case err == nil:
			return len(expired), nil
		case errors.Is(err, errCASConflict):
			continue // another daemon rewrote the set; re-read and re-apply
		default:
			return 0, err
		}
	}
	return 0, fmt.Errorf("tigris: pack-tombstones contention after %d attempts", maxRefCASRetries)
}

// deleteKeys removes keys in batches of maxDeleteBatch. Deleting a key that is
// not there is not worth avoiding: S3 treats it as success, and checking first
// would cost a round trip to save one.
func (s *Storer) deleteKeys(keys []string) error {
	for start := 0; start < len(keys); start += maxDeleteBatch {
		batch := keys[start:min(start+maxDeleteBatch, len(keys))]
		ids := make([]types.ObjectIdentifier, 0, len(batch))
		for _, k := range batch {
			ids = append(ids, types.ObjectIdentifier{Key: sp(k)})
		}

		st := time.Now()
		_, err := s.client.DeleteObjects(s.ctx, &s3.DeleteObjectsInput{
			Bucket: sp(s.bucket),
			Delete: &types.Delete{Objects: ids, Quiet: bp(true)},
		})
		s.observe("DeleteObjects", st, err)
		if err != nil {
			return fmt.Errorf("tigris: delete %d pack keys: %w", len(batch), err)
		}
	}
	return nil
}
```

Extend `compact.go`'s imports with `"errors"`, `"time"`, `"github.com/aws/aws-sdk-go-v2/service/s3"`, and `"github.com/aws/aws-sdk-go-v2/service/s3/types"`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run TestReap -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/compact.go internal/storage/tigris/compact_test.go internal/storage/tigris/client_test.go
git commit -m "feat(storage/tigris): reap expired pack tombstones"
```

---

### Task 10: The backstop — subsumption and orphan cleanup

Tombstones are state that can disagree with the bucket. Two cases leak packs, and both close with data the survey already holds.

**Files:**
- Modify: `internal/storage/tigris/compact.go`
- Test: `internal/storage/tigris/compact_test.go`

**Interfaces:**
- Consumes: `surveyPacks` (Task 5), `loadTombstones`/`storeTombstones` (Task 4), `deleteKeys` (Task 9).
- Produces: `func subsumedPacks(packs []packSurvey) []string`; `func (s *Storer) recordMissingTombstones(packs []packSurvey, grace time.Duration) (int, error)`; `func (s *Storer) deleteOrphanBins(orphans []objectEntry, now time.Time, grace time.Duration) (int, error)`.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/compact_test.go`:

```go
func TestSubsumedPacks(t *testing.T) {
	h1 := plumbing.NewHash("1111111111111111111111111111111111111111")
	h2 := plumbing.NewHash("2222222222222222222222222222222222222222")
	h3 := plumbing.NewHash("3333333333333333333333333333333333333333")

	for _, tt := range []struct {
		name  string
		packs []packSurvey
		want  []string
	}{
		{
			name: "a merged pack subsumes both sources",
			packs: []packSurvey{
				{id: "src1", recs: []cueRecord{{hash: h1}}},
				{id: "src2", recs: []cueRecord{{hash: h2}}},
				{id: "merged", recs: []cueRecord{{hash: h1}, {hash: h2}}},
			},
			want: []string{"src1", "src2"},
		},
		{
			name: "a pack holding a unique object is never subsumed",
			packs: []packSurvey{
				{id: "a", recs: []cueRecord{{hash: h1}, {hash: h3}}},
				{id: "b", recs: []cueRecord{{hash: h1}}},
			},
			want: []string{"b"},
		},
		{
			name:  "one pack alone is never subsumed",
			packs: []packSurvey{{id: "only", recs: []cueRecord{{hash: h1}}}},
			want:  nil,
		},
		{
			name: "an empty pack is not reported",
			packs: []packSurvey{
				{id: "empty", recs: nil},
				{id: "a", recs: []cueRecord{{hash: h1}}},
			},
			want: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := subsumedPacks(tt.packs)
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("subsumed = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecordMissingTombstonesUsesTheNewestPackClock(t *testing.T) {
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	h1 := plumbing.NewHash("1111111111111111111111111111111111111111")

	f := newFakeS3(t)
	s := newTestStorer(t, f)

	packs := []packSurvey{
		{id: "ancient", recs: []cueRecord{{hash: h1}}, mtime: old},
		{id: "merged", recs: []cueRecord{{hash: h1}}, mtime: recent},
	}

	n, err := s.recordMissingTombstones(packs, time.Hour)
	if err != nil {
		t.Fatalf("recordMissingTombstones: %v", err)
	}
	if n != 1 {
		t.Fatalf("wrote %d tombstones, want 1", n)
	}

	ts, _, err := s.loadTombstones()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(ts) != 1 || ts[0].id != "ancient" {
		t.Fatalf("tombstones = %v, want one for ancient", ts)
	}
	// The deadline must come from the newest pack, not from the ancient one.
	// Using the victim's own mtime would give it no grace at all.
	if want := recent.Add(time.Hour); !ts[0].deadline.Equal(want) {
		t.Errorf("deadline = %v, want %v", ts[0].deadline, want)
	}
}

func TestDeleteOrphanBinsRespectsGrace(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	s := newTestStorer(t, f)
	f.put("packs/old.bin", "stale", nil)
	f.put("packs/new.bin", "inflight", nil)

	orphans := []objectEntry{
		{key: "packs/old.bin", mtime: now.Add(-2 * time.Hour)},
		{key: "packs/new.bin", mtime: now.Add(-time.Minute)},
	}

	n, err := s.deleteOrphanBins(orphans, now, time.Hour)
	if err != nil {
		t.Fatalf("deleteOrphanBins: %v", err)
	}
	if n != 1 {
		t.Errorf("deleted %d, want 1", n)
	}
	if f.has("packs/old.bin") {
		t.Error("the aged orphan survived")
	}
	if !f.has("packs/new.bin") {
		t.Error("an upload still in flight was deleted")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run 'TestSubsumed|TestRecordMissing|TestDeleteOrphan' -v`
Expected: FAIL — `undefined: subsumedPacks`.

- [ ] **Step 3: Implement the backstop**

Append to `internal/storage/tigris/compact.go`:

```go
// subsumedPacks reports every pack whose every object also lives in some other
// pack. Such a pack can be deleted without losing anything.
//
// This is the backstop that makes the tombstone object an optimization rather
// than a correctness dependency. A crash between uploading a merged container
// and writing its tombstone leaves the sources subsumed and unrecorded, and
// without this nothing would ever delete them.
//
// A pack with no records is never reported. An empty pack is vacuously
// subsumed, and deleting one gains nothing while giving the rule a special case
// that is easy to get wrong.
func subsumedPacks(packs []packSurvey) []string {
	// One entry per distinct object, capped at two: all this needs to know is
	// whether a hash lives in more than one pack.
	count := make(map[plumbing.Hash]uint8)
	for _, p := range packs {
		seen := make(map[plumbing.Hash]struct{}, len(p.recs))
		for _, r := range p.recs {
			// Count each pack once per hash, so a pack holding a duplicate
			// internally cannot make itself look covered.
			if _, dup := seen[r.hash]; dup {
				continue
			}
			seen[r.hash] = struct{}{}
			if count[r.hash] < 2 {
				count[r.hash]++
			}
		}
	}

	var out []string
	for _, p := range packs {
		if len(p.recs) == 0 {
			continue
		}
		covered := true
		for _, r := range p.recs {
			if count[r.hash] < 2 {
				covered = false
				break
			}
		}
		if covered {
			out = append(out, p.id)
		}
	}
	sort.Strings(out)
	return out
}

// recordMissingTombstones writes a tombstone for every subsumed pack that has
// none. It reports how many it added.
//
// CAUTION: The deadline is measured from the newest pack in the repository,
// never from the subsumed pack's own mtime. A source pack can be far older than
// the merge that subsumed it, so its own clock would give it no grace at all,
// and a reader holding an index built moments ago would lose the bytes under
// it. The newest pack's mtime is always at or after the merge that made this
// pack redundant, so it is a safe lower bound on when the replacement appeared.
func (s *Storer) recordMissingTombstones(packs []packSurvey, grace time.Duration) (int, error) {
	subsumed := subsumedPacks(packs)
	if len(subsumed) == 0 {
		return 0, nil
	}

	var newest time.Time
	for _, p := range packs {
		if p.mtime.After(newest) {
			newest = p.mtime
		}
	}
	deadline := newest.Add(grace)

	for attempt := 0; attempt < maxRefCASRetries; attempt++ {
		ts, etag, err := s.loadTombstones()
		if err != nil {
			return 0, err
		}

		have := make(map[string]struct{}, len(ts))
		for _, e := range ts {
			have[e.id] = struct{}{}
		}

		added := 0
		for _, id := range subsumed {
			if _, ok := have[id]; ok {
				continue
			}
			ts = append(ts, tombstone{id: id, deadline: deadline})
			added++
		}
		if added == 0 {
			return 0, nil
		}

		switch _, err := s.storeTombstones(ts, etag); {
		case err == nil:
			return added, nil
		case errors.Is(err, errCASConflict):
			continue
		default:
			return 0, err
		}
	}
	return 0, fmt.Errorf("tigris: pack-tombstones contention after %d attempts", maxRefCASRetries)
}

// deleteOrphanBins removes a .bin that has no .cue, once it is older than the
// grace period. It reports how many it deleted.
//
// No tombstone is needed, because no reader can hold a reference to one: the
// pack index is built from .cue files alone, so a container with no .cue is
// invisible. The age test is the only thing that divides a true orphan from an
// upload that is still in flight, since packJob.run uploads the .bin first.
func (s *Storer) deleteOrphanBins(orphans []objectEntry, now time.Time, grace time.Duration) (int, error) {
	var keys []string
	for _, o := range orphans {
		if now.Sub(o.mtime) > grace {
			keys = append(keys, o.key)
		}
	}
	if len(keys) == 0 {
		return 0, nil
	}
	if err := s.deleteKeys(keys); err != nil {
		return 0, err
	}
	return len(keys), nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run 'TestSubsumed|TestRecordMissing|TestDeleteOrphan' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/compact.go internal/storage/tigris/compact_test.go
git commit -m "feat(storage/tigris): back tombstones with subsumption and orphan cleanup"
```

---

### Task 11: `Compact` orchestration and its metrics

**Files:**
- Modify: `internal/storage/tigris/compact.go`
- Modify: `internal/metrics/metrics.go`
- Test: `internal/storage/tigris/compact_test.go`

**Interfaces:**
- Consumes: every function from Tasks 5 through 10.
- Produces: `type CompactOptions struct { MaxPacks int; Grace time.Duration; Now func() time.Time }`; `type CompactStats struct { PacksMerged, PacksWritten, PacksDeleted, ObjectsDeduped, OrphansDeleted int }`; `func (s *Storer) Compact(opt CompactOptions) (CompactStats, error)`; `func metrics.ObserveCompact(result string, dur time.Duration, st CompactCounts)`.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/compact_test.go`:

```go
func TestCompactEndToEndOverTheFake(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	f.clock = func() time.Time { return now }
	s := newTestStorer(t, f)

	bodies := [][]byte{}
	for i := range 5 {
		b := []byte(fmt.Sprintf("object number %d", i))
		bodies = append(bodies, b)
		writeFakePack(t, f, s, fmt.Sprintf("p%d", i), []testObj{{typ: plumbing.BlobObject, body: b}})
	}

	opt := CompactOptions{MaxPacks: 2, Grace: time.Hour, Now: func() time.Time { return now }}
	st, err := s.Compact(opt)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if st.PacksMerged != 5 || st.PacksWritten != 1 {
		t.Errorf("stats = %+v, want 5 merged into 1", st)
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The sources are tombstoned, not yet deleted.
	for i := range 5 {
		if !f.has(fmt.Sprintf("packs/p%d.bin", i)) {
			t.Errorf("source p%d was deleted before its grace period", i)
		}
	}
	ts, _, err := s.loadTombstones()
	if err != nil {
		t.Fatalf("load tombstones: %v", err)
	}
	if len(ts) != 5 {
		t.Errorf("wrote %d tombstones, want 5", len(ts))
	}

	// Every object still reads, from a Storer with no memory of the merge.
	fresh := newTestStorer(t, f)
	for _, b := range bodies {
		h := hashOf(t, s, plumbing.BlobObject, b)
		if _, err := fresh.EncodedObject(plumbing.AnyObject, h); err != nil {
			t.Errorf("object %s is unreadable after compaction: %v", h, err)
		}
	}

	// After the grace period, a second pass deletes the sources.
	later := now.Add(2 * time.Hour)
	opt.Now = func() time.Time { return later }
	if _, err := s.Compact(opt); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	for i := range 5 {
		if f.has(fmt.Sprintf("packs/p%d.bin", i)) {
			t.Errorf("source p%d survived past its deadline", i)
		}
	}
	after := newTestStorer(t, f)
	for _, b := range bodies {
		h := hashOf(t, s, plumbing.BlobObject, b)
		if _, err := after.EncodedObject(plumbing.AnyObject, h); err != nil {
			t.Errorf("object %s is unreadable after the reap: %v", h, err)
		}
	}
}

func TestCompactUnderThresholdStillCleansOrphans(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	f.clock = func() time.Time { return now }
	s := newTestStorer(t, f)

	writeFakePack(t, f, s, "only", []testObj{{typ: plumbing.BlobObject, body: []byte("x")}})
	f.put("packs/orphan.bin", "dangling", nil)
	f.setMtime("packs/orphan.bin", now.Add(-2*time.Hour))

	st, err := s.Compact(CompactOptions{MaxPacks: 16, Grace: time.Hour, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if st.PacksMerged != 0 {
		t.Errorf("merged %d packs under the threshold, want 0", st.PacksMerged)
	}
	if st.OrphansDeleted != 1 {
		t.Errorf("deleted %d orphans, want 1", st.OrphansDeleted)
	}
	if f.has("packs/orphan.bin") {
		t.Error("the orphan survived a pass that did no merging")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run TestCompact -v`
Expected: FAIL — `undefined: CompactOptions`.

- [ ] **Step 3: Implement `Compact`**

Append to `internal/storage/tigris/compact.go`:

```go
// CompactOptions configures one compaction pass.
type CompactOptions struct {
	// MaxPacks is the pack count above which a pass merges. At or below it, a
	// pass still reaps, cleans orphans, and runs the backstop.
	MaxPacks int
	// Grace is how long a merged-away pack survives before deletion. It must
	// exceed the longest request: a Storer builds its pack index once per
	// request, so a reader's view is never more stale than the request holding
	// it, and a large clone runs for many minutes.
	Grace time.Duration
	// Now is the clock, overridable so a test can cross a deadline without
	// sleeping. A nil value means time.Now.
	Now func() time.Time
}

func (o CompactOptions) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

// CompactStats reports what one pass did.
type CompactStats struct {
	PacksMerged    int // source packs read
	PacksWritten   int // output containers produced
	PacksDeleted   int // packs whose grace period expired this pass
	ObjectsDeduped int // duplicate records dropped during the merge
	OrphansDeleted int // .bin files with no .cue
}

// Compact runs one compaction pass over this Storer's repository.
//
// The order matters. The reap runs first, so a crash mid-pass still frees
// space on the next cycle. The survey then feeds three steps that run at every
// pack count — orphan cleanup, the subsumption backstop, and selection —
// because a failed upload leaves an orphan whatever the pack count is, and
// gating them behind the merge threshold would let a quiet repository leak
// forever.
//
// Compaction never writes a reference, so it cannot interact with the
// packed-refs compare-and-swap, and it needs no lock against a concurrent push:
// a pack that lands after the survey is simply not in this pass's input, and a
// pack that was surveyed cannot change, because packs are immutable.
func (s *Storer) Compact(opt CompactOptions) (CompactStats, error) {
	var st CompactStats
	now := opt.now()

	deleted, err := s.reapTombstones(now)
	if err != nil {
		return st, err
	}
	st.PacksDeleted = deleted

	packs, orphans, err := s.surveyPacks()
	if err != nil {
		return st, err
	}

	dropped, err := s.deleteOrphanBins(orphans, now, opt.Grace)
	if err != nil {
		return st, err
	}
	st.OrphansDeleted = dropped

	if _, err := s.recordMissingTombstones(packs, opt.Grace); err != nil {
		return st, err
	}

	groups := selectForMerge(packs, opt.MaxPacks, s.mergeCapBytes())
	if len(groups) == 0 {
		return st, nil
	}

	var sources []tombstone
	deadline := now.Add(opt.Grace)
	for _, g := range groups {
		_, deduped, err := s.mergeGroup(g)
		if err != nil {
			return st, err
		}
		st.PacksMerged += len(g)
		st.PacksWritten++
		st.ObjectsDeduped += deduped
		for _, p := range g {
			sources = append(sources, tombstone{id: p.id, deadline: deadline})
		}
	}

	// The merged containers must be durable before anything records their
	// sources as deletable. flush surfaces the first upload failure, and a
	// failure here leaves the sources untombstoned, which is the safe side.
	if err := s.up.flush(); err != nil {
		return st, fmt.Errorf("tigris: compact: %w", err)
	}

	if err := s.addTombstones(sources); err != nil {
		return st, err
	}
	return st, nil
}

// mergeCapBytes is the byte cap one merged container may reach.
func (s *Storer) mergeCapBytes() int64 {
	if s.maxPackBytes > 0 {
		return s.maxPackBytes
	}
	return maxPackBytes
}

// addTombstones records sources as pending deletion, under a compare-and-swap.
func (s *Storer) addTombstones(add []tombstone) error {
	if len(add) == 0 {
		return nil
	}
	for attempt := 0; attempt < maxRefCASRetries; attempt++ {
		ts, etag, err := s.loadTombstones()
		if err != nil {
			return err
		}
		have := make(map[string]struct{}, len(ts))
		for _, e := range ts {
			have[e.id] = struct{}{}
		}
		for _, e := range add {
			if _, ok := have[e.id]; !ok {
				ts = append(ts, e)
			}
		}

		switch _, err := s.storeTombstones(ts, etag); {
		case err == nil:
			return nil
		case errors.Is(err, errCASConflict):
			continue
		default:
			return err
		}
	}
	return fmt.Errorf("tigris: pack-tombstones contention after %d attempts", maxRefCASRetries)
}
```

- [ ] **Step 4: Add the metrics**

In `internal/metrics/metrics.go`, add to the `var` block:

```go
	compactRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "compact",
		Name:      "runs_total",
		Help:      "Total pack compaction passes by outcome.",
	}, []string{"result"})

	compactDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "compact",
		Name:      "duration_seconds",
		Help:      "Wall-clock duration of one pack compaction pass.",
		Buckets:   prometheus.ExponentialBuckets(0.1, 3, 8),
	})

	compactPacks = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "compact",
		Name:      "packs_total",
		Help:      "Pack containers touched by compaction, by disposition.",
	}, []string{"disposition"})

	compactObjectsDeduped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "compact",
		Name:      "objects_deduped_total",
		Help:      "Duplicate object records dropped while merging containers.",
	})
```

And add the helper beside the other exported helpers:

```go
// CompactCounts mirrors tigris.CompactStats without importing it, so the
// storage package keeps its one-way dependency on metrics.
type CompactCounts struct {
	Merged, Written, Deleted, Deduped, Orphans int
}

// ObserveCompact records one compaction pass. result is "ok" or "error".
func ObserveCompact(result string, dur time.Duration, c CompactCounts) {
	compactRuns.WithLabelValues(result).Inc()
	compactDuration.Observe(dur.Seconds())
	compactObjectsDeduped.Add(float64(c.Deduped))
	for disposition, n := range map[string]int{
		"merged":  c.Merged,
		"written": c.Written,
		"deleted": c.Deleted,
		"orphan":  c.Orphans,
	} {
		compactPacks.WithLabelValues(disposition).Add(float64(n))
	}
}
```

NOTE: `Compact` itself does not call this. The sweeper does, in Task 13, which keeps `internal/storage/tigris` free of a metrics import exactly as it is today.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run TestCompact -v && go build ./...`
Expected: PASS, and a clean build.

- [ ] **Step 6: Run the whole suite**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/storage/tigris/compact.go internal/storage/tigris/compact_test.go internal/metrics/metrics.go
git commit -m "feat(storage/tigris): add the Compact pass and its metrics"
```

---

### Task 12: Repository enumeration and the compaction lease

The sweeper needs the list of repositories in the bucket, and a way to avoid doing the same work on every daemon at once.

**Files:**
- Modify: `internal/storage/tigris/compact.go`
- Test: `internal/storage/tigris/compact_test.go`

**Interfaces:**
- Consumes: Task 1's delimiter support in the fake.
- Produces: `func (s *Storer) ListRepoPrefixes() ([]string, error)`; `func (s *Storer) TakeCompactLease(owner string, ttl time.Duration, now time.Time) (bool, error)`; `func (s *Storer) ReleaseCompactLease() error`.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/tigris/compact_test.go`:

```go
func TestListRepoPrefixes(t *testing.T) {
	f := newFakeS3(t)
	s := newTestStorer(t, f)
	f.put("acme/web.git/HEAD", "ref: refs/heads/main\n", nil)
	f.put("acme/api.git/packed-refs", "x", nil)
	f.put("other/tools.git/HEAD", "y", nil)

	got, err := s.ListRepoPrefixes()
	if err != nil {
		t.Fatalf("ListRepoPrefixes: %v", err)
	}
	want := []string{"acme/api.git", "acme/web.git", "other/tools.git"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("prefixes = %v, want %v", got, want)
	}
}

func TestCompactLease(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	a := newTestStorer(t, f)
	b := newTestStorer(t, f)

	ok, err := a.TakeCompactLease("daemon-a", time.Hour, now)
	if err != nil || !ok {
		t.Fatalf("first take = %v/%v, want true/nil", ok, err)
	}
	ok, err = b.TakeCompactLease("daemon-b", time.Hour, now)
	if err != nil {
		t.Fatalf("contended take: %v", err)
	}
	if ok {
		t.Error("two daemons took the same lease at once")
	}

	// An expired lease is available again, even though nobody released it.
	ok, err = b.TakeCompactLease("daemon-b", time.Hour, now.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("take after expiry: %v", err)
	}
	if !ok {
		t.Error("an expired lease must be available")
	}

	if err := b.ReleaseCompactLease(); err != nil {
		t.Fatalf("release: %v", err)
	}
	ok, err = a.TakeCompactLease("daemon-a", time.Hour, now)
	if err != nil || !ok {
		t.Fatalf("take after release = %v/%v, want true/nil", ok, err)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/storage/tigris/ -run 'TestListRepoPrefixes|TestCompactLease' -v`
Expected: FAIL — `s.ListRepoPrefixes undefined`.

- [ ] **Step 3: Implement enumeration and the lease**

Append to `internal/storage/tigris/compact.go`:

```go
// compactLeaseKey stops two daemons from compacting one repository at the same
// time. It sits at the root of the repository prefix.
const compactLeaseKey = "compact-lease"

// ListRepoPrefixes reports every repository path in the bucket, relative to
// this Storer's prefix and with no trailing slash.
//
// Two levels of delimiter listing, because RepoRef.Path is path.Join(OrgID,
// Name) and therefore always exactly two segments. One call returns the
// organization prefixes, and one call per organization returns its
// repositories. Listing every key instead would walk every object in the
// bucket to learn a handful of names.
func (s *Storer) ListRepoPrefixes() ([]string, error) {
	orgs, err := s.listCommonPrefixes(s.prefix)
	if err != nil {
		return nil, err
	}

	var repos []string
	for _, org := range orgs {
		found, err := s.listCommonPrefixes(org)
		if err != nil {
			return nil, err
		}
		for _, r := range found {
			repos = append(repos, strings.TrimSuffix(strings.TrimPrefix(r, s.prefix), "/"))
		}
	}
	sort.Strings(repos)
	return repos, nil
}

// listCommonPrefixes returns the immediate child "directories" of one prefix.
func (s *Storer) listCommonPrefixes(prefix string) ([]string, error) {
	var out []string
	token := ""
	for {
		in := &s3.ListObjectsV2Input{
			Bucket:    sp(s.bucket),
			Prefix:    sp(prefix),
			Delimiter: sp("/"),
		}
		if token != "" {
			in.ContinuationToken = sp(token)
		}

		start := time.Now()
		page, err := s.client.ListObjectsV2(s.ctx, in)
		s.observe("ListObjectsV2", start, err)
		if err != nil {
			return nil, fmt.Errorf("tigris: list prefixes under %q: %w", prefix, err)
		}
		for _, cp := range page.CommonPrefixes {
			if p := sv(cp.Prefix); p != "" {
				out = append(out, p)
			}
		}
		if !bv(page.IsTruncated) || sv(page.NextContinuationToken) == "" {
			break
		}
		token = sv(page.NextContinuationToken)
	}
	return out, nil
}

// TakeCompactLease claims the right to compact this repository, and reports
// whether the claim succeeded.
//
// NOTE: The lease saves cost. It is not a correctness requirement. Selection is
// deterministic, so two daemons that compact one repository at the same time
// build byte-identical containers, which get one content-addressed id, which
// makes their PutObject calls idempotent. A fault here wastes work and cannot
// damage a repository.
func (s *Storer) TakeCompactLease(owner string, ttl time.Duration, now time.Time) (bool, error) {
	key := s.prefix + compactLeaseKey

	raw, etag, err := s.fetchSmallETag(key)
	switch {
	case err == nil:
		// Somebody holds it. Take it over only once it has expired.
		if held, ok := parseLeaseExpiry(raw); ok && now.Before(held) {
			return false, nil
		}
	case errors.Is(err, plumbing.ErrObjectNotFound):
		etag = "" // free: claim it with If-None-Match: *
	default:
		return false, fmt.Errorf("tigris: read compaction lease: %w", err)
	}

	body := fmt.Sprintf("%s\t%d\n", owner, now.Add(ttl).Unix())
	switch _, err := s.casPut(key, []byte(body), etag); {
	case err == nil:
		return true, nil
	case errors.Is(err, errCASConflict):
		return false, nil // another daemon claimed it between the read and the write
	default:
		return false, err
	}
}

// ReleaseCompactLease drops the lease. A missing key is success.
func (s *Storer) ReleaseCompactLease() error {
	return s.removeSimple(s.prefix + compactLeaseKey)
}

// parseLeaseExpiry reads the deadline out of a lease body. An unreadable body
// reports false, which makes the lease available: a lease nobody can parse is
// worse than no lease at all, because it would block compaction forever.
func parseLeaseExpiry(raw []byte) (time.Time, bool) {
	_, secs, ok := strings.Cut(strings.TrimSpace(string(raw)), "\t")
	if !ok {
		return time.Time{}, false
	}
	unix, err := strconv.ParseInt(secs, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(unix, 0), true
}
```

Add `"strconv"` and `"strings"` to `compact.go`'s imports.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/storage/tigris/ -run 'TestListRepoPrefixes|TestCompactLease' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/compact.go internal/storage/tigris/compact_test.go
git commit -m "feat(storage/tigris): enumerate repositories and lease compaction"
```

---

### Task 13: The sweeper

**Files:**
- Create: `internal/compactor/compactor.go`
- Test: `internal/compactor/compactor_test.go`

**Interfaces:**
- Consumes: `(*tigris.Storer).ListRepoPrefixes`, `.Scoped`, `.Compact`, `.TakeCompactLease`, `.ReleaseCompactLease`, `tigris.CompactOptions`, `tigris.CompactStats`; `metrics.ObserveCompact` and `metrics.CompactCounts` from Task 11.
- Produces: `type Sweeper struct { Base *tigris.Storer; Owner string; Interval time.Duration; Opts tigris.CompactOptions; Rand *rand.Rand }`; `func (sw *Sweeper) Run(ctx context.Context) error`; `func (sw *Sweeper) SweepOnce(ctx context.Context) error`.

- [ ] **Step 1: Write the failing test**

Create `internal/compactor/compactor_test.go`:

```go
package compactor

import (
	"context"
	"testing"
	"time"
)

func TestJitteredHoldsWithinBounds(t *testing.T) {
	for _, tt := range []struct {
		name     string
		interval time.Duration
	}{
		{name: "hour", interval: time.Hour},
		{name: "minute", interval: time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lo := time.Duration(float64(tt.interval) * 0.75)
			hi := time.Duration(float64(tt.interval) * 1.25)
			for range 200 {
				got := jittered(tt.interval)
				if got < lo || got > hi {
					t.Fatalf("jittered(%v) = %v, want within [%v, %v]", tt.interval, got, lo, hi)
				}
			}
		})
	}
}

func TestSweepOnceStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sw := &Sweeper{Interval: time.Hour}
	if err := sw.Run(ctx); err != nil {
		t.Errorf("Run after cancel = %v, want nil", err)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/compactor/ -v`
Expected: FAIL — the package does not exist.

- [ ] **Step 3: Implement the sweeper**

Create `internal/compactor/compactor.go`:

```go
// Package compactor runs pack compaction over every repository in the bucket,
// on a timer, inside the daemon.
//
// The pass itself lives in internal/storage/tigris. This package owns only
// when it runs, over which repositories, and who is allowed to run it.
package compactor

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/storage/tigris"
)

// leaseTTL bounds how long one daemon's claim on a repository survives. It is
// long enough for a large merge and short enough that a daemon killed mid-pass
// does not block the repository for a whole shift.
const leaseTTL = 30 * time.Minute

// Sweeper compacts every repository in one bucket, on an interval.
type Sweeper struct {
	// Base is the root Storer. The sweeper Scopes it per repository, exactly as
	// repofs does per request.
	Base *tigris.Storer
	// Owner names this daemon in a lease body. Any stable string works.
	Owner string
	// Interval is the nominal time between sweeps. Each wait is jittered, so a
	// fleet started at one moment does not stay in lockstep.
	Interval time.Duration
	// Opts configures each pass.
	Opts tigris.CompactOptions
}

// Run sweeps until ctx is done. It returns nil on cancellation, so an errgroup
// treats a clean shutdown as success.
func (sw *Sweeper) Run(ctx context.Context) error {
	slog.Info("pack compaction sweeper started",
		"interval", sw.Interval, "max_packs", sw.Opts.MaxPacks, "grace", sw.Opts.Grace)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(jittered(sw.Interval)):
		}

		if err := sw.SweepOnce(ctx); err != nil {
			// A sweep failure is never fatal to the daemon. The next tick
			// retries, and a repository that keeps failing shows up in the
			// error counter rather than taking the process down.
			slog.Error("pack compaction sweep failed", "err", err)
		}
	}
}

// SweepOnce compacts every repository once.
func (sw *Sweeper) SweepOnce(ctx context.Context) error {
	repos, err := sw.Base.ListRepoPrefixes()
	if err != nil {
		return err
	}

	for _, repo := range repos {
		if ctx.Err() != nil {
			return nil
		}
		sw.sweepRepo(repo)
	}
	return nil
}

// sweepRepo runs one pass under the repository's lease. One repository at a
// time: compaction is bulk I/O, and it must not compete with traffic serving a
// client.
func (sw *Sweeper) sweepRepo(repo string) {
	s := sw.Base.Scoped(repo)

	taken, err := s.TakeCompactLease(sw.Owner, leaseTTL, time.Now())
	if err != nil {
		slog.Warn("could not take the compaction lease", "repo", repo, "err", err)
		return
	}
	if !taken {
		slog.Debug("another daemon holds the compaction lease", "repo", repo)
		return
	}
	defer func() {
		if err := s.ReleaseCompactLease(); err != nil {
			slog.Warn("could not release the compaction lease", "repo", repo, "err", err)
		}
	}()

	start := time.Now()
	st, err := s.Compact(sw.Opts)
	dur := time.Since(start)

	result := "ok"
	if err != nil {
		result = "error"
		slog.Error("pack compaction failed", "repo", repo, "dur", dur, "err", err)
	}
	metrics.ObserveCompact(result, dur, metrics.CompactCounts{
		Merged:  st.PacksMerged,
		Written: st.PacksWritten,
		Deleted: st.PacksDeleted,
		Deduped: st.ObjectsDeduped,
		Orphans: st.OrphansDeleted,
	})

	if err == nil && (st.PacksMerged > 0 || st.PacksDeleted > 0 || st.OrphansDeleted > 0) {
		slog.Info("compacted packs", "repo", repo, "dur", dur,
			"merged", st.PacksMerged, "written", st.PacksWritten,
			"deleted", st.PacksDeleted, "deduped", st.ObjectsDeduped,
			"orphans", st.OrphansDeleted)
	}
}

// jittered spreads a fleet's sweeps over plus or minus 25 percent of the
// interval, so N daemons started together do not all wake at one moment and
// contend for every lease in the bucket.
func jittered(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	spread := float64(d) * 0.25
	return time.Duration(float64(d) - spread + rand.Float64()*2*spread)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/compactor/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/compactor/
git commit -m "feat(compactor): add the background pack compaction sweeper"
```

---

### Task 14: Wire the sweeper into the daemon

**Files:**
- Modify: `cmd/objgitd/main.go:36-50` (flags) and `cmd/objgitd/main.go:151` (errgroup)

**Interfaces:**
- Consumes: `compactor.Sweeper` (Task 13), `tigris.CompactOptions` (Task 11).
- Produces: the flags `-compact-interval`, `-compact-max-packs`, `-compact-grace`.

- [ ] **Step 1: Add the flags**

In the `var` block in `cmd/objgitd/main.go`, after `packedRefs`:

```go
	compactInterval = flag.Duration("compact-interval", 0, "time between background pack compaction sweeps; 0 disables compaction entirely")
	compactMaxPacks = flag.Int("compact-max-packs", 16, "pack count above which a repository's smallest containers are merged")
	compactGrace    = flag.Duration("compact-grace", time.Hour, "how long a merged-away pack survives before deletion; must exceed the longest clone")
```

CAUTION: The interval defaults to `0`, which turns compaction off. This flag is the only one in the daemon that deletes data, so it is opt-in for one release, in the same way `-allow-push` and `-allow-hooks` are. Turn it on by default only after a release has run it in production.

- [ ] **Step 2: Start the sweeper on the errgroup**

After the `-metrics-bind` block and before the `-http-bind` block in `main.go`, add:

```go
	if *compactInterval > 0 {
		host, err := os.Hostname()
		if err != nil {
			host = "objgitd"
		}
		sweeper := &compactor.Sweeper{
			Base:     base,
			Owner:    fmt.Sprintf("%s/%d", host, os.Getpid()),
			Interval: *compactInterval,
			Opts: tigris.CompactOptions{
				MaxPacks: *compactMaxPacks,
				Grace:    *compactGrace,
			},
		}
		g.Go(func() error { return sweeper.Run(gCtx) })
	}
```

NOTE: `base` is the root `*tigris.Storer` built at `main.go:126`. The sweeper takes it directly, not the `tigrisBase{s: base}` adapter the resolver wraps it in, because it needs `Compact` and `ListRepoPrefixes`, which that adapter does not expose. Add `"fmt"` and the `internal/compactor` import if they are not already present.

- [ ] **Step 3: Add the flags to the startup log**

In the `slog.Info` call that already logs `bucket`, `allow_push`, `allow_hooks`, and `pack_cache_bytes`, add:

```go
		"compact_interval", *compactInterval,
```

- [ ] **Step 4: Build and confirm the flags exist**

Run: `go build ./... && go run ./cmd/objgitd -h 2>&1 | grep compact`
Expected: the three `-compact-*` flags are listed with their defaults.

- [ ] **Step 5: Confirm the environment fallback works**

Run: `COMPACT_MAX_PACKS=4 go run ./cmd/objgitd -h 2>&1 | grep compact-max-packs`
Expected: the flag is listed. `flagenv` maps `-compact-max-packs` to `COMPACT_MAX_PACKS` with no extra wiring, because it walks the whole flag set.

- [ ] **Step 6: Run the whole suite**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add cmd/objgitd/main.go
git commit -m "feat(objgitd): run the pack compaction sweeper behind -compact-interval"
```

---

### Task 15: The end-to-end regression test over a real git pack

Every test so far uses hand-built containers. This one pushes a **real pack
built by real git, containing real deltas**, splits it across several
containers, compacts, and proves every object still reads back byte-identical.
It is the regression net for the containment invariant: if a merge ever
stranded a delta from its base, the read below fails outright.

**Files:**
- Modify: `internal/storage/tigris/compact_test.go`
- Modify: `internal/storage/tigris/livebucket_test.go`

**Interfaces:**
- Consumes: `buildPackFixture(t, numFiles int, withDeltaEdit bool) packFixture` and `writePack(t, s *Storer, fx packFixture)` from `pack_test.go:672,760`; `packFixture.hashes []plumbing.Hash`; `withMaxPackBytes` from `tigris.go:233`.

- [ ] **Step 1: Write the test**

Add to `internal/storage/tigris/compact_test.go`:

```go
// TestCompactionPreservesARealPushWithDeltas is the containment invariant's
// regression net. buildPackFixture's withDeltaEdit arm makes git emit a real
// delta chain, and withMaxPackBytes splits the push across several containers,
// so the merge has to move deltas between containers to be wrong.
func TestCompactionPreservesARealPushWithDeltas(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	f := newFakeS3(t)
	f.clock = func() time.Time { return now }
	// A small cap turns one push into many containers, which is what gives
	// compaction something to merge.
	s := newTestStorer(t, f, withMaxPackBytes(8<<10))

	fx := buildPackFixture(t, 40, true)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush the push: %v", err)
	}

	before, _, err := s.surveyPacks()
	if err != nil {
		t.Fatalf("survey before: %v", err)
	}
	if len(before) < 3 {
		t.Fatalf("the push made %d containers, want at least 3 for this test to mean anything", len(before))
	}

	// readAll proves every object in the fixture reads back and still hashes to
	// its own name. A stranded delta fails the read; a corrupt merge fails the
	// hash.
	readAll := func(when string) {
		t.Helper()
		fresh := newTestStorer(t, f, withMaxPackBytes(8<<10))
		for _, h := range fx.hashes {
			obj, err := fresh.EncodedObject(plumbing.AnyObject, h)
			if err != nil {
				t.Fatalf("%s: object %s is unreadable: %v", when, h, err)
			}
			body, err := io.ReadAll(mustReader(obj))
			if err != nil {
				t.Fatalf("%s: read %s: %v", when, h, err)
			}
			if got := hashOf(t, fresh, obj.Type(), body); got != h {
				t.Fatalf("%s: object %s rebuilt as %s", when, h, got)
			}
		}
	}
	readAll("before compaction")

	opt := CompactOptions{MaxPacks: 2, Grace: time.Hour, Now: func() time.Time { return now }}
	st, err := s.Compact(opt)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if st.PacksMerged == 0 {
		t.Fatal("the pass merged nothing, so this test proves nothing")
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush the merge: %v", err)
	}

	// The overlap window: sources and merged containers both present.
	readAll("during the overlap window")

	// Cross the grace period so the next pass reaps the sources.
	later := now.Add(2 * time.Hour)
	opt.Now = func() time.Time { return later }
	if _, err := s.Compact(opt); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush the reap: %v", err)
	}

	after, _, err := s.surveyPacks()
	if err != nil {
		t.Fatalf("survey after: %v", err)
	}
	if len(after) >= len(before) {
		t.Errorf("container count went from %d to %d, want a reduction", len(before), len(after))
	}
	readAll("after the reap")
}
```

Add `"os/exec"`, `"io"`, and `"time"` to `compact_test.go`'s imports if they are not already there.

- [ ] **Step 2: Run the test**

Run: `go test ./internal/storage/tigris/ -run TestCompactionPreservesARealPushWithDeltas -v`
Expected: PASS, and the log shows a container count that drops.

- [ ] **Step 3: Add the live-bucket arm**

Append to `internal/storage/tigris/livebucket_test.go`, following the skip idiom already at the top of that file:

```go
// TestLiveBucketCompaction runs the same push-compact-read cycle against a
// real bucket. It exists because the fake cannot reproduce the two facts this
// design leans on hardest: that a conditional PutObject really is atomic, and
// that DeleteObjects against a missing key really does succeed.
func TestLiveBucketCompaction(t *testing.T) {
	bucket := os.Getenv("OBJGIT_TIGRIS_LIVE_BUCKET")
	if bucket == "" {
		t.Skip("OBJGIT_TIGRIS_LIVE_BUCKET not set; skipping live-bucket verification")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	ctx := context.Background()
	prefix := "compact-test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	root, err := New(ctx, bucket, withMaxPackBytes(8<<10))
	if err != nil {
		t.Fatalf("live construct: %v", err)
	}
	s := root.Scoped(prefix)

	fx := buildPackFixture(t, 40, true)
	writePack(t, s, fx)
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Grace of zero: the sources are deletable the moment they are tombstoned,
	// so one extra pass reaps them and the test needs no wait.
	opt := CompactOptions{MaxPacks: 2, Grace: 0}
	if _, err := s.Compact(opt); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.up.flush(); err != nil {
		t.Fatalf("flush the merge: %v", err)
	}
	if _, err := s.Compact(opt); err != nil {
		t.Fatalf("reap pass: %v", err)
	}

	fresh := root.Scoped(prefix)
	for _, h := range fx.hashes {
		if _, err := fresh.EncodedObject(plumbing.AnyObject, h); err != nil {
			t.Errorf("object %s is unreadable after live compaction: %v", h, err)
		}
	}

	t.Cleanup(func() {
		keys, err := s.listKeys(s.prefix)
		if err != nil {
			t.Logf("cleanup list failed, %s may hold stray keys: %v", prefix, err)
			return
		}
		if err := s.deleteKeys(keys); err != nil {
			t.Logf("cleanup delete failed, %s may hold stray keys: %v", prefix, err)
		}
	})
}
```

CAUTION: This test writes to a real bucket under its own timestamped prefix and deletes those keys in `t.Cleanup`. Point `OBJGIT_TIGRIS_LIVE_BUCKET` at a scratch bucket, never at one holding real repositories.

- [ ] **Step 4: Run the live arm if a scratch bucket is available**

Run: `OBJGIT_TIGRIS_LIVE_BUCKET=<scratch-bucket> go test ./internal/storage/tigris/ -run TestLiveBucketCompaction -v`
Expected: PASS, or SKIP when the variable is unset.

- [ ] **Step 5: Run the whole suite**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/storage/tigris/compact_test.go internal/storage/tigris/livebucket_test.go
git commit -m "test(storage/tigris): prove compaction preserves a real delta-bearing push"
```

---


### Task 16: Documentation

**Files:**
- Modify: `docs/architecture/tigris-storer.md` (the "Remaining gap" section at the end)
- Modify: `docs/reference/tigris-backend.md:476-503` (the build order)
- Modify: `AGENTS.md` (the "Where the code lives" table)

- [ ] **Step 1: Replace the "Remaining gap" section in the architecture page**

In `docs/architecture/tigris-storer.md`, replace the `## Remaining gap` section with:

```markdown
## Compaction (`compact.go`, `tombstones.go`)

A pass merges the smallest containers into larger ones, then deletes the
sources after a grace period. `internal/compactor` runs one pass per
repository on a timer. `-compact-interval` turns it on, and `0` turns it off.

The rewrite merges **whole packs, not objects**. A merge bin-packs entire
source containers into an output and deduplicates hashes inside it, which
gives one invariant: the output holds the union of its sources' hashes.
Deduplication collapses copies and never removes a hash.

That invariant buys three things. Delta containment holds with no check,
because the write-side rule already puts every delta's base in the same
container. Every payload is a verbatim byte copy, so compaction runs no
compressor and resolves no delta. And the sum of the source sizes bounds the
output, so bin packing decides from the survey alone.

Two rules stop a pass becoming a treadmill. A pack above half the byte cap is
never a source, and a selection producing at least as many outputs as inputs
is abandoned. Without them a repository of near-cap containers is rewritten on
every cycle for no gain.

CAUTION: A merged-away pack is not deleted at once. A request builds its pack
index once, so a clone that started before the merge still names the old
container. `pack-tombstones` records each source with a deadline, and a later
pass deletes it. `-compact-grace` must exceed the longest clone.

The tombstone object is an optimization, not a correctness dependency. Every
pass recomputes which packs are subsumed, from records the survey already
loaded, and writes a tombstone for any subsumed pack that lacks one. A crash
between the upload and the tombstone write therefore heals on the next cycle.
The deadline comes from the **newest** pack's `LastModified`, never the
subsumed pack's own: a source can be far older than the merge that replaced
it, and its own clock would give it no grace at all.

The same pass deletes a `.bin` that has no `.cue`, once it ages past the grace
period. A failed `.cue` upload leaves exactly that, and no reader can ever see
it, because the index is built from `.cue` files alone.

Compaction needs no lock against a push. A pack landing after the survey is
not in that pass's input, a surveyed pack cannot change because packs are
immutable, and through the overlap window both copies of a duplicated object
are byte-identical, so whichever entry wins the index merge is correct.

The `compact-lease` key stops two daemons doing one repository twice. It saves
cost and is not required for correctness: selection is deterministic, so two
daemons build byte-identical containers, which get one content-addressed id,
which makes their uploads idempotent.

## Remaining gap

There is no reachability-based garbage collection. A force-push or a deleted
branch leaves its objects in the bucket forever. Compaction copies every
object forward by design, so adding that filter means adding a second safety
mechanism: a push that has uploaded its objects but not yet committed its ref
looks unreachable.
```

- [ ] **Step 2: Update the build order**

In `docs/reference/tigris-backend.md`, replace item 5 with:

```markdown
5. **Done.** Add repacking. `internal/compactor` merges the smallest
   containers and deletes the sources after a grace period, behind
   `-compact-interval`. The same pass deletes a `.bin` that has no `.cue`,
   which a failed upload leaves behind. Garbage collection by reachability is
   still open. See
   [the storer page](../architecture/tigris-storer.md#compaction-compactgo-tombstonesgo).
```

Change the first line of the section from `Items 1 to 4 and item 6 are complete.` to `Items 1 to 7 are complete.`

- [ ] **Step 3: Add the new package to the index**

In `AGENTS.md`, add a row to the "Where the code lives" table after `internal/bundler`:

```markdown
| `internal/compactor`          | The timer that runs pack compaction over every repository.             |
```

- [ ] **Step 4: Check the prose**

Read the three edits once against the `simple-english` rules: sentences at or under 25 words, no `should`, no semicolons, no contractions, and a condition before its command.

- [ ] **Step 5: Commit**

```bash
git add docs/architecture/tigris-storer.md docs/reference/tigris-backend.md AGENTS.md
git commit -m "docs: describe pack compaction and close build order item 5"
```

---

## Self-Review Notes

Checked against `docs/plans/pack-compaction.md`:

| Spec section | Task |
| --- | --- |
| Rewrite merges whole packs | 7, 8 |
| Prefer the whole copy over the delta copy | 8 (`chooseRecords`) |
| Selection by pack count, smallest first | 6 |
| Treadmill guard | 6 |
| No recompression | 7 (`appendVerbatim` carries `codec`) |
| Tombstone object and format | 3, 4 |
| Reap step, delete before tombstone rewrite | 9 |
| Backstop: subsumption, deadline from the newest pack | 10 |
| Backstop: orphan `.bin` | 10 |
| Sweeper, jitter, two-level enumeration | 12, 13 |
| Lease | 12, 13 |
| Concurrency with a live push | documented in 16; the overlap window is exercised in 15 |
| Flags | 14 |
| Metrics | 11 |
| Testing (every listed case) | 3, 6, 8, 9, 10, 11, 15 |

Two items in the spec's testing list are folded rather than dropped. "A refused compare-and-swap on the tombstone retries" is covered by `TestTombstoneStoreDetectsConflict` in Task 4, plus the retry loops in Tasks 9 through 11. "A delta still reads after a merge" is covered in Task 15 by pushing a real git pack whose deltas git itself produced, then re-hashing every object after the merge and again after the reap. That is stronger than a synthetic delta, because a stranded base fails the read outright and a corrupt payload fails the hash.

One deliberate simplification is worth recording. The spec says a subsumed pack's deadline comes from "the newest pack that subsumes it". Task 10 uses the newest pack in the whole repository instead. That value is always at or after the true newest subsuming pack, so the grace it grants is never shorter than required, and it costs one pass over the survey rather than a per-pack cover set.
