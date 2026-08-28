# Packed refs with compare-and-swap — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace one-loose-object-per-ref with a single `packed-refs` object written under an `If-Match` ETag, so a push of any number of refs costs one `PutObject` call and an advertisement costs two calls.

**Architecture:** A new `refCache` on `*tigris.Storer` memoizes the merged ref view for one request, built from one `GetObject packed-refs` plus one `ListObjectsV2` on the legacy `refs/` prefix. Every write funnels into one `commitRefs` method whose single conditional `PutObject` is the commit point. A `refUpdater` interface lets `cmd/objgitd/receivepack.go` hand a whole push over in one call.

**Tech Stack:** Go 1.26, `github.com/aws/aws-sdk-go-v2/service/s3`, `github.com/go-git/go-git/v6`, `github.com/klauspost/compress/zstd`, `github.com/prometheus/client_golang`.

**Spec:** `docs/superpowers/specs/2026-08-28-packed-refs-cas-design.md`

## Global Constraints

- Module path is `github.com/tigrisdata/objgit` (go.mod), Go 1.26.3. `AGENTS.md` states a different path and is stale; Task 8 corrects it.
- Design target is 10,000 to 100,000 refs. Do not build sharding or a write-ahead log.
- `slog` uses `"err"` as the error key, never `"error"`.
- Tests are table-driven with `tt` as the loop variable. See `refs_test.go`.
- Reuse the shared test helpers `newFakeS3`, `newTestStorer`, `countingObserver`, and `hashRef`. Do not write new ones that duplicate them.
- Flags are kebab-case, paired with `flagenv` for the environment fallback.
- **`WithPackedRefs` gates writes only. Reads are never gated.** This mirrors `WithPackCompression` (`tigris.go:150`) and is what makes a rollback safe.
- The default for `WithPackedRefs` stays **off** for the whole of this plan. Task 8 documents the flip; it does not perform it.
- Every existing test in `internal/storage/tigris` and `cmd/objgitd` must still pass after every task, except the two the plan names explicitly (Task 4 and Task 6).
- Commit after every task. Sign off exactly: `Signed-off-by: Xe Iaso <xe@tigrisdata.com>`

---

### Task 1: Prove Tigris honors conditional writes

The spec's risk table makes this the gate on everything else. If Tigris does not return 412 for these two headers, the whole design is wrong and no other task is worth starting.

**Files:**
- Modify: `internal/storage/tigris/livebucket_test.go` (append a new test function)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing consumed by code. It produces the go-ahead for Tasks 2 through 8.

- [ ] **Step 1: Write the live-bucket probe**

Append to `internal/storage/tigris/livebucket_test.go`. The `OBJGIT_TIGRIS_LIVE_BUCKET` gate copies `TestLiveBucketRoundTrip` at the top of that file.

```go
// TestLiveBucketConditionalWrites is the gate on the packed-refs design (see
// docs/superpowers/specs/2026-08-28-packed-refs-cas-design.md). Every other
// part of that design assumes real Tigris rejects a failed precondition with
// an error whose code is "PreconditionFailed". Nothing else in this repository
// sends a conditional header, so this is the only place that claim is checked.
//
// CAUTION: conditional operations evaluate against the latest state only on
// Single-region and Multi-region buckets. A Global or Dual-region bucket reads
// eventually, and this test can pass there while production races.
// See https://www.tigrisdata.com/docs/concepts/consistency/
func TestLiveBucketConditionalWrites(t *testing.T) {
	bucket := os.Getenv("OBJGIT_TIGRIS_LIVE_BUCKET")
	if bucket == "" {
		t.Skip("OBJGIT_TIGRIS_LIVE_BUCKET not set; skipping conditional-write verification")
	}

	ctx := context.Background()
	s, err := New(ctx, bucket, WithObserver(func(op string, dur time.Duration, oerr error) {
		t.Logf("s3 %-14s dur=%-12s err=%v", op, dur, oerr)
	}))
	if err != nil {
		t.Fatalf("live construct: %v", err)
	}

	key := "conditional-write-probe-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	t.Cleanup(func() {
		if derr := s.removeSimple(key); derr != nil {
			t.Logf("probe cleanup failed (harmless, key %q): %v", key, derr)
		}
	})

	// 1. Create-if-absent succeeds on a key that does not exist, and reports an
	//    ETag we can compare against later.
	first, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      sp(bucket),
		Key:         sp(key),
		Body:        strings.NewReader("one"),
		IfNoneMatch: sp("*"),
	})
	if err != nil {
		t.Fatalf("IfNoneMatch:* on an absent key must succeed, got: %v", err)
	}
	etag := sv(first.ETag)
	if etag == "" {
		t.Fatal("PutObject returned no ETag; packed-refs has no CAS token without one")
	}
	t.Logf("live create-if-absent succeeded, etag=%s", etag)

	// 2. Create-if-absent now fails, because the key exists.
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      sp(bucket),
		Key:         sp(key),
		Body:        strings.NewReader("two"),
		IfNoneMatch: sp("*"),
	})
	if !isPreconditionFailed(err) {
		t.Fatalf("IfNoneMatch:* on a present key must fail the precondition, got: %v", err)
	}
	t.Logf("live create-if-absent correctly refused the second write")

	// 3. Compare-and-swap succeeds against the current ETag.
	second, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  sp(bucket),
		Key:     sp(key),
		Body:    strings.NewReader("three"),
		IfMatch: sp(etag),
	})
	if err != nil {
		t.Fatalf("IfMatch against the current ETag must succeed, got: %v", err)
	}
	if sv(second.ETag) == etag {
		t.Error("ETag did not change across a write; it cannot serve as a CAS token")
	}

	// 4. Compare-and-swap fails against the now-stale ETag. This is the case
	//    commitRefs retries on, and the reason CheckAndSetReference becomes
	//    atomic.
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  sp(bucket),
		Key:     sp(key),
		Body:    strings.NewReader("four"),
		IfMatch: sp(etag),
	})
	if !isPreconditionFailed(err) {
		t.Fatalf("IfMatch against a stale ETag must fail the precondition, got: %v", err)
	}
	t.Logf("live compare-and-swap correctly refused a stale ETag")
}
```

- [ ] **Step 2: Add the error classifier the test uses**

In `internal/storage/tigris/tigris.go`, directly below `isNotFound`:

```go
// isPreconditionFailed reports whether err is a rejected conditional write.
// Tigris and S3 both answer a failed If-Match or If-None-Match with HTTP 412
// and the code "PreconditionFailed". Two writers racing on packed-refs is the
// normal way to see this, so commitRefs treats it as retryable and not as a
// failure. Callers must never map it to absence the way isNotFound does.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "PreconditionFailed"
}
```

- [ ] **Step 3: Confirm it compiles and skips without a bucket**

Run: `go test -run TestLiveBucketConditionalWrites ./internal/storage/tigris/ -v`
Expected: `SKIP` with "OBJGIT_TIGRIS_LIVE_BUCKET not set".

If `strconv` is not yet imported by `livebucket_test.go`, it is already there (line 10 of the current file). Do not add a duplicate import.

- [ ] **Step 4: Run it against a real bucket**

Run: `OBJGIT_TIGRIS_LIVE_BUCKET=<a writable single-region or multi-region bucket> go test -run TestLiveBucketConditionalWrites ./internal/storage/tigris/ -v`
Expected: PASS, with all four log lines.

**WARNING: If this fails, stop. Do not start Task 2.** Report which of the four steps failed and what error code came back instead. The design needs revising, not implementing.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/tigris/livebucket_test.go internal/storage/tigris/tigris.go
git commit -m "test(storage/tigris): prove Tigris honors conditional writes

The packed-refs design rests on a failed If-Match or If-None-Match
coming back as PreconditionFailed. Nothing in this repository sent a
conditional header before, so pin the claim with a live-bucket test and
add the isPreconditionFailed classifier it needs.

Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 2: The packed-refs binary format

Pure functions over byte slices. No S3, no Storer, so this task is testable on its own and needs none of the test-fake work in Task 3.

**Files:**
- Create: `internal/storage/tigris/packedrefs.go`
- Create: `internal/storage/tigris/packedrefs_test.go`

**Interfaces:**
- Consumes: `encodeRefValue` and `decodeRefValue` from `refs.go:26` and `refs.go:33`, unchanged. `compressBlock` and `cueDecoder` from `compress.go:161` and `compress.go:107`. `codecRaw` and `codecZstd` from `compress.go:12`.
- Produces:
  - `const packedRefsKey = "packed-refs"`
  - `var errBadPackedRefs error`
  - `func encodePackedRefs(refs map[plumbing.ReferenceName]*plumbing.Reference) []byte`
  - `func decodePackedRefs(b []byte) (map[plumbing.ReferenceName]*plumbing.Reference, error)`

- [ ] **Step 1: Write the failing tests**

Create `internal/storage/tigris/packedrefs_test.go`:

```go
package tigris

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

// refSet builds a name-keyed map the way the cache holds one.
func refSet(refs ...*plumbing.Reference) map[plumbing.ReferenceName]*plumbing.Reference {
	out := make(map[plumbing.ReferenceName]*plumbing.Reference, len(refs))
	for _, r := range refs {
		out[r.Name()] = r
	}
	return out
}

func TestPackedRefsRoundTrip(t *testing.T) {
	t.Parallel()

	sym := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.ReferenceName("refs/heads/main"))

	many := map[plumbing.ReferenceName]*plumbing.Reference{}
	for i := 0; i < 20000; i++ {
		n := plumbing.ReferenceName(fmt.Sprintf("refs/tags/v1.%d.0", i))
		many[n] = plumbing.NewHashReference(n, mustHash(headAB))
	}

	tests := []struct {
		name string
		in   map[plumbing.ReferenceName]*plumbing.Reference
	}{
		{name: "empty", in: refSet()},
		{name: "one hash ref", in: refSet(hashRef("refs/heads/main", headAB))},
		{name: "symbolic ref", in: refSet(sym)},
		{
			name: "mixed",
			in: refSet(
				sym,
				hashRef("refs/heads/main", headAB),
				hashRef("refs/tags/v1.0.0", headCD),
			),
		},
		{name: "twenty thousand tags", in: many},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			back, err := decodePackedRefs(encodePackedRefs(tt.in))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(back) != len(tt.in) {
				t.Fatalf("decoded %d refs, want %d", len(back), len(tt.in))
			}
			for name, want := range tt.in {
				got, ok := back[name]
				if !ok {
					t.Fatalf("ref %s vanished across the round trip", name)
				}
				if got.Type() != want.Type() {
					t.Errorf("ref %s changed type: %v vs %v", name, got.Type(), want.Type())
				}
				if got.Type() == plumbing.SymbolicReference {
					if got.Target() != want.Target() {
						t.Errorf("ref %s target: %s vs %s", name, got.Target(), want.Target())
					}
					continue
				}
				if got.Hash() != want.Hash() {
					t.Errorf("ref %s hash: %s vs %s", name, got.Hash(), want.Hash())
				}
			}
		})
	}
}

// TestPackedRefsEncodingIsStable pins the property that lets every other test
// assert bytes instead of sets: one set of refs always encodes identically,
// whatever order the map iterates in.
func TestPackedRefsEncodingIsStable(t *testing.T) {
	t.Parallel()

	in := refSet(
		hashRef("refs/tags/v2", headCD),
		hashRef("refs/heads/main", headAB),
		hashRef("refs/tags/v1", headAB),
	)

	first := encodePackedRefs(in)
	for i := 0; i < 20; i++ {
		if !bytes.Equal(encodePackedRefs(in), first) {
			t.Fatal("encoding is not deterministic across map iteration orders")
		}
	}
}

// TestPackedRefsBodyIsSortedText pins the on-the-wire shape, so a person
// debugging a bucket by hand knows what to expect.
func TestPackedRefsBodyIsSortedText(t *testing.T) {
	t.Parallel()

	// Small enough that compressBlock refuses to compress it, so the body is
	// readable straight out of the encoder.
	in := refSet(
		hashRef("refs/tags/v1", headCD),
		hashRef("refs/heads/main", headAB),
	)
	raw := encodePackedRefs(in)

	if got := string(raw[:3]); got != "OGR" {
		t.Errorf("magic = %q, want %q", got, "OGR")
	}
	if raw[3] != refsVersion1 {
		t.Errorf("version = %d, want %d", raw[3], refsVersion1)
	}
	if raw[refsCodecOff] != codecRaw {
		t.Fatalf("a two-ref body must not compress; codec = %d", raw[refsCodecOff])
	}

	want := "refs/heads/main\t" + headAB + "\n" + "refs/tags/v1\t" + headCD + "\n"
	if got := string(raw[refsHeaderLen:]); got != want {
		t.Errorf("body =\n%q\nwant\n%q", got, want)
	}
}

func TestPackedRefsCompressesLargeBodies(t *testing.T) {
	t.Parallel()

	in := map[plumbing.ReferenceName]*plumbing.Reference{}
	for i := 0; i < 5000; i++ {
		n := plumbing.ReferenceName(fmt.Sprintf("refs/tags/release-2026-08-%d", i))
		in[n] = plumbing.NewHashReference(n, mustHash(headAB))
	}

	raw := encodePackedRefs(in)
	if raw[refsCodecOff] != codecZstd {
		t.Fatalf("5000 refs of shared-prefix names must compress; codec = %d", raw[refsCodecOff])
	}
	if _, err := decodePackedRefs(raw); err != nil {
		t.Fatalf("compressed body will not decode: %v", err)
	}
}

// TestPackedRefsRejectsCorruption pins the posture errBadCue already takes for
// pack indexes: corruption is an error and never masquerades as absence. This
// is deliberately different from the loose-ref path, which logs and skips one
// malformed key (see TestMalformedRefEntriesAreSkipped). One bad line makes the
// whole packed object suspect, so nothing in it is trusted.
func TestPackedRefsRejectsCorruption(t *testing.T) {
	t.Parallel()

	good := encodePackedRefs(refSet(hashRef("refs/heads/main", headAB)))

	badMagic := bytes.Clone(good)
	badMagic[0] = 'X'

	badVersion := bytes.Clone(good)
	badVersion[3] = 99

	badCount := bytes.Clone(good)
	badCount[refsCountOff+3] = 77

	noTab := append(bytes.Clone(good[:refsHeaderLen]), []byte("refs/heads/main "+headAB+"\n")...)
	noTab[refsCountOff+3] = 1

	noNewline := append(bytes.Clone(good[:refsHeaderLen]), []byte("refs/heads/main\t"+headAB)...)
	noNewline[refsCountOff+3] = 1

	badValue := append(bytes.Clone(good[:refsHeaderLen]), []byte("refs/heads/main\tnot-a-hash\n")...)
	badValue[refsCountOff+3] = 1

	tests := []struct {
		name string
		in   []byte
		want error
	}{
		{name: "truncated header", in: good[:5], want: errBadPackedRefs},
		{name: "wrong magic", in: badMagic, want: errBadPackedRefs},
		{name: "future version", in: badVersion, want: errBadPackedRefs},
		{name: "count disagrees with body", in: badCount, want: errBadPackedRefs},
		{name: "line without a tab", in: noTab, want: errBadPackedRefs},
		{name: "body without a trailing newline", in: noNewline, want: errBadPackedRefs},
		{name: "unparseable value", in: badValue, want: errMalformedRef},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := decodePackedRefs(tt.in); !errors.Is(err, tt.want) {
				t.Errorf("want %v, got %v", tt.want, err)
			}
		})
	}
}

// TestPackedRefsRejectsNamesWithSeparators pins the assumption the tab
// separator rests on. git-check-ref-format forbids whitespace in a ref name,
// so a name that carries a tab or a newline can only come from a caller bug or
// a hostile client. Refusing to encode it is what stops such a name from
// forging extra lines in the body.
func TestPackedRefsRejectsNamesWithSeparators(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"refs/heads/a\tb", "refs/heads/a\nb"} {
		t.Run(strings.ReplaceAll(bad, "\t", "TAB"), func(t *testing.T) {
			t.Parallel()

			in := refSet(plumbing.NewHashReference(plumbing.ReferenceName(bad), mustHash(headAB)))
			if _, err := decodePackedRefs(encodePackedRefs(in)); err == nil {
				t.Error("a ref name holding a separator encoded without complaint")
			}
		})
	}
}
```

- [ ] **Step 2: Add the one missing test helper**

`hashRef` and the `headAB`/`headCD` fixtures already exist in `refs_test.go:11-22`. Only `mustHash` is new. Add it to `refs_test.go`, next to `hashRef`:

```go
func mustHash(hexval string) plumbing.Hash {
	h, ok := plumbing.FromHex(hexval)
	if !ok {
		panic("refs_test bug: bad hex fixture " + hexval)
	}
	return h
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -run 'TestPackedRefs' ./internal/storage/tigris/`
Expected: FAIL to build, with `undefined: encodePackedRefs`, `undefined: decodePackedRefs`, `undefined: refsVersion1`, `undefined: refsCodecOff`, `undefined: refsHeaderLen`, `undefined: refsCountOff`, `undefined: errBadPackedRefs`.

- [ ] **Step 4: Write the implementation**

Create `internal/storage/tigris/packedrefs.go`:

```go
package tigris

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/go-git/go-git/v6/plumbing"
)

// packedRefsKey holds every ref for one repository in one object. It sits at
// the root beside shallow, index, and config. The name comes from the git file
// with the same role, though the encoding differs: this one is a versioned
// header over an optionally-compressed body, and it can hold a symbolic ref,
// which git's own packed-refs cannot.
//
// The key deliberately does not start with refs/, so listKeys(prefix +
// refPrefix) can never return it and mistake it for a loose ref.
const packedRefsKey = "packed-refs"

const (
	refsHeaderLen = 16
	refsVersion1  = 1

	// refsCodecOff is header byte 4: the codec of the body block.
	refsCodecOff = 4
	// refsCountOff is header byte 5: the ref count, big-endian uint32. It is a
	// checksum on the body and nothing else — decodePackedRefs uses it to catch
	// a body that was truncated on a line boundary, which no other check sees.
	refsCountOff = 5
)

// refsMagic opens every version of the packed-refs object. Header byte 3
// carries the version itself.
var refsMagic = [3]byte{'O', 'G', 'R'}

// errBadPackedRefs marks a malformed packed-refs object. Corruption never
// masquerades as absence, the same posture errBadCue takes for a pack index.
//
// This is stricter than the loose-ref path on purpose. listLooseRefs logs and
// skips one malformed key, because the other keys are independent objects that
// are still trustworthy. Here every ref shares one object, so one bad line
// means the bytes are not what this code wrote, and none of them are trusted.
var errBadPackedRefs = errors.New("tigris: malformed packed-refs object")

// encodePackedRefs serializes refs into the packed-refs format: a 16-byte
// plaintext header, then a body that is raw or one zstd frame.
//
// The body is text, one ref per line, "<name>\t<value>\n", sorted by name.
// Sorting buys three things. Tag names that share a prefix become adjacent, so
// zstd collapses them instead of meeting them scattered. One set of refs always
// encodes to identical bytes, so a test can assert bytes rather than compare
// sets. And the decompressed body is greppable when someone is reading a bucket
// by hand.
//
// The value is whatever encodeRefValue produces, so a symbolic ref survives as
// "ref: <target>" and the loose format and this one can never drift apart.
//
// A later version can add a third tab-separated field for a tag's peeled hash,
// which would spare a ref advertisement from opening every annotated tag
// object. A v1 reader refuses such a line rather than misreading it: the cut
// below splits on the first tab only, so the extra field lands inside the value
// and decodeRefValue rejects it. That is the right direction to fail, and it is
// why adding the field is a version bump and not a new key.
func encodePackedRefs(refs map[plumbing.ReferenceName]*plumbing.Reference) []byte {
	names := make([]string, 0, len(refs))
	for n := range refs {
		names = append(names, n.String())
	}
	sort.Strings(names)

	var body bytes.Buffer
	for _, n := range names {
		body.WriteString(n)
		body.WriteByte('\t')
		body.WriteString(encodeRefValue(refs[plumbing.ReferenceName(n)])) // already ends in "\n"
	}

	stored, compressed := compressBlock(body.Bytes())

	out := make([]byte, refsHeaderLen, refsHeaderLen+len(stored))
	copy(out, refsMagic[:])
	out[3] = refsVersion1
	if compressed {
		out[refsCodecOff] = codecZstd
	}
	binary.BigEndian.PutUint32(out[refsCountOff:], uint32(len(names)))
	return append(out, stored...)
}

// decodePackedRefs is encodePackedRefs's inverse.
//
// It reuses cueDecoder rather than adding a third process-wide zstd decoder.
// The bound that decoder carries (cueMaxDecoded, 256 MiB) is orders of
// magnitude above any honest packed-refs body: 100,000 refs come to about
// 4.5 MB. Its only job either way is to turn a hostile input into an error
// instead of an out-of-memory kill.
func decodePackedRefs(b []byte) (map[plumbing.ReferenceName]*plumbing.Reference, error) {
	if len(b) < refsHeaderLen {
		return nil, fmt.Errorf("%w: %d bytes is shorter than the %d-byte header", errBadPackedRefs, len(b), refsHeaderLen)
	}
	if !bytes.Equal(b[:3], refsMagic[:]) {
		return nil, fmt.Errorf("%w: magic %q is not %q", errBadPackedRefs, b[:3], refsMagic[:])
	}
	if v := b[3]; v != refsVersion1 {
		return nil, fmt.Errorf("%w: version %d is not supported by this binary", errBadPackedRefs, v)
	}

	body := b[refsHeaderLen:]
	switch codec := b[refsCodecOff]; codec {
	case codecRaw:
	case codecZstd:
		dec, err := cueDecoder().DecodeAll(body, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: body will not decompress: %w", errBadPackedRefs, err)
		}
		body = dec
	default:
		return nil, fmt.Errorf("%w: unknown body codec %d", errBadPackedRefs, codec)
	}

	want := binary.BigEndian.Uint32(b[refsCountOff:])
	out := make(map[plumbing.ReferenceName]*plumbing.Reference, want)

	for len(body) > 0 {
		line, rest, ok := bytes.Cut(body, []byte("\n"))
		if !ok {
			return nil, fmt.Errorf("%w: trailing %d bytes with no newline", errBadPackedRefs, len(body))
		}
		body = rest

		rawName, rawValue, ok := bytes.Cut(line, []byte("\t"))
		if !ok {
			return nil, fmt.Errorf("%w: line %q holds no tab", errBadPackedRefs, line)
		}
		name := plumbing.ReferenceName(rawName)

		// decodeRefValue returns errMalformedRef, which callers already handle.
		// Do not rewrap it as errBadPackedRefs: the two say different things
		// about which layer is broken.
		ref, err := decodeRefValue(name, string(rawValue))
		if err != nil {
			return nil, err
		}
		out[name] = ref
	}

	// A body truncated exactly on a line boundary decodes cleanly and silently
	// loses refs. The count is the only thing that catches it.
	if uint32(len(out)) != want {
		return nil, fmt.Errorf("%w: header claims %d refs, body holds %d", errBadPackedRefs, want, len(out))
	}
	return out, nil
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -run 'TestPackedRefs' ./internal/storage/tigris/ -v`
Expected: PASS, every subtest.

`TestPackedRefsRejectsNamesWithSeparators` passes because a forged line either fails the tab split, fails `decodeRefValue`, or throws the count off. Any of the three is an error, which is what the test asserts. No explicit name validation is needed in the encoder.

- [ ] **Step 6: Run the whole package to confirm nothing regressed**

Run: `go test ./internal/storage/tigris/`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/storage/tigris/packedrefs.go internal/storage/tigris/packedrefs_test.go internal/storage/tigris/refs_test.go
git commit -m "feat(storage/tigris): add the packed-refs object format

A versioned header over sorted \"<name>\\t<value>\\n\" text, compressed
when it pays. Sorting makes the encoding deterministic and lets zstd
collapse shared tag prefixes. Reuses encodeRefValue and decodeRefValue,
so the loose format and this one cannot drift apart.

Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 3: Teach the test fake about ETags and conditional writes

The spec calls this the one piece of test infrastructure the change needs. It buys the whole compare-and-swap path with no bucket.

**Files:**
- Modify: `internal/storage/tigris/client_test.go:31-35` (the `fakeObject` struct), `:212-220` (`put`), `:298-333` (`GetObject`), `:355-390` (`PutObject`)
- Create: `internal/storage/tigris/conditional_test.go`

**Interfaces:**
- Consumes: `isPreconditionFailed` from Task 1.
- Produces:
  - `fakeObject` gains an `etag string` field.
  - `fakeS3.GetObject` sets `GetObjectOutput.ETag`.
  - `fakeS3.PutObject` honors `IfMatch` and `IfNoneMatch`, and sets `PutObjectOutput.ETag`.
  - `func preconditionFailed() error`
  - `fakeS3.etagOf(key string) string`

- [ ] **Step 1: Write the failing test**

Create `internal/storage/tigris/conditional_test.go`:

```go
package tigris

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestFakeS3HonorsConditionalWrites pins the fake against the behavior
// TestLiveBucketConditionalWrites observed on real Tigris. Every
// compare-and-swap test in this package trusts the fake, so the fake gets its
// own test.
func TestFakeS3HonorsConditionalWrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFakeS3(t)
	const key = "packed-refs"

	body := func(s string) *s3.PutObjectInput {
		return &s3.PutObjectInput{Bucket: sp("test-bucket"), Key: sp(key), Body: newReader(s)}
	}

	t.Run("create-if-absent succeeds then refuses", func(t *testing.T) {
		in := body("one")
		in.IfNoneMatch = sp("*")
		out, err := f.PutObject(ctx, in)
		if err != nil {
			t.Fatalf("first create-if-absent: %v", err)
		}
		if sv(out.ETag) == "" {
			t.Fatal("PutObject returned no ETag")
		}

		again := body("two")
		again.IfNoneMatch = sp("*")
		if _, err := f.PutObject(ctx, again); !isPreconditionFailed(err) {
			t.Fatalf("second create-if-absent must fail the precondition, got %v", err)
		}
		if got := string(f.get(t, key).body); got != "one" {
			t.Errorf("refused write still landed: body = %q", got)
		}
	})

	t.Run("GetObject reports the ETag", func(t *testing.T) {
		out, err := f.GetObject(ctx, &s3.GetObjectInput{Bucket: sp("test-bucket"), Key: sp(key)})
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if sv(out.ETag) != f.etagOf(key) {
			t.Errorf("GetObject ETag %q disagrees with the fake's %q", sv(out.ETag), f.etagOf(key))
		}
	})

	t.Run("compare-and-swap succeeds on a fresh ETag and refuses a stale one", func(t *testing.T) {
		stale := f.etagOf(key)

		in := body("three")
		in.IfMatch = sp(stale)
		out, err := f.PutObject(ctx, in)
		if err != nil {
			t.Fatalf("swap on a fresh ETag: %v", err)
		}
		if sv(out.ETag) == stale {
			t.Error("ETag did not change across a write")
		}

		again := body("four")
		again.IfMatch = sp(stale)
		if _, err := f.PutObject(ctx, again); !isPreconditionFailed(err) {
			t.Fatalf("swap on a stale ETag must fail the precondition, got %v", err)
		}
		if got := string(f.get(t, key).body); got != "three" {
			t.Errorf("refused swap still landed: body = %q", got)
		}
	})

	t.Run("compare-and-swap on an absent key refuses", func(t *testing.T) {
		in := &s3.PutObjectInput{Bucket: sp("test-bucket"), Key: sp("nothing-here"), Body: newReader("x"), IfMatch: sp("whatever")}
		if _, err := f.PutObject(ctx, in); !isPreconditionFailed(err) {
			t.Fatalf("If-Match on an absent key must fail the precondition, got %v", err)
		}
	})
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -run TestFakeS3HonorsConditionalWrites ./internal/storage/tigris/`
Expected: FAIL to build, with `undefined: newReader` and `f.etagOf undefined`.

- [ ] **Step 3: Modify the fake**

In `internal/storage/tigris/client_test.go`, replace the `fakeObject` struct (line 31-35) with:

```go
// fakeObject is one object in the fake bucket. etag is the fake's CAS token;
// it is a plain counter rather than a content digest, which keeps every caller
// honest about treating an ETag as opaque.
type fakeObject struct {
	body []byte
	meta map[string]string
	etag string
}
```

Add an `etagSeq int` field to `fakeS3`, next to `puts` (line 56):

```go
	puts    int
	deletes int
	etagSeq int   // monotone source of fake ETags
	listMax int64 // ListObjectsV2 page size knob; 0 = unlimited
```

Add these helpers next to `nputs` (after line 243):

```go
// nextETag mints a fresh opaque token. Callers hold f.mu.
func (f *fakeS3) nextETag() string {
	f.etagSeq++
	return `"` + strconv.Itoa(f.etagSeq) + `"`
}

// etagOf reports one key's current token, or "" when the key is absent.
func (f *fakeS3) etagOf(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.objs[key].etag
}

// preconditionFailed is what Tigris returns for a refused If-Match or
// If-None-Match. TestLiveBucketConditionalWrites pins that this is the real
// code, and isPreconditionFailed is what matches it.
func preconditionFailed() error {
	return &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "at least one of the preconditions you specified did not hold"}
}

// newReader is a tiny shim so a test can build a PutObjectInput body without
// importing strings at every call site.
func newReader(s string) io.Reader { return strings.NewReader(s) }
```

In `put` (line 212), mint an ETag so seeded objects have one:

```go
func (f *fakeS3) put(key, body string, meta map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := make(map[string]string, len(meta))
	for k, v := range meta {
		m[k] = v
	}
	f.objs[key] = fakeObject{body: []byte(body), meta: m, etag: f.nextETag()}
}
```

In `GetObject`, add the ETag to the output. Replace the output construction (lines 323-326) with:

```go
	out := &s3.GetObjectOutput{
		ContentLength: ip(int64(len(body))),
		Metadata:      o.meta,
		ETag:          sp(o.etag),
	}
```

In `PutObject`, insert the precondition check and the ETag mint. Replace the body from `f.mu.Lock()` at line 372 through the `return` at line 389 with:

```go
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.putErr != nil {
		return nil, f.putErr
	}

	// Preconditions are evaluated before the write, and a refusal still counts
	// as a request — which is what real S3 bills and what a call-count test
	// must see.
	key := sv(p.Key)
	cur, exists := f.objs[key]
	switch {
	case sv(p.IfNoneMatch) == "*" && exists:
		return nil, preconditionFailed()
	case sv(p.IfMatch) != "" && (!exists || cur.etag != sv(p.IfMatch)):
		return nil, preconditionFailed()
	}

	var buf bytes.Buffer
	if p.Body != nil {
		if _, err := io.Copy(&buf, p.Body); err != nil {
			f.t.Fatalf("fake PutObject copy failed: %v", err)
		}
	}
	meta := make(map[string]string, len(p.Metadata))
	for k, v := range p.Metadata {
		meta[k] = v
	}
	etag := f.nextETag()
	f.objs[key] = fakeObject{body: buf.Bytes(), meta: meta, etag: etag}
	return &s3.PutObjectOutput{ETag: sp(etag)}, nil
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -run TestFakeS3HonorsConditionalWrites ./internal/storage/tigris/ -v`
Expected: PASS, all four subtests.

- [ ] **Step 5: Run the whole package**

Run: `go test ./internal/storage/tigris/`
Expected: PASS. Nothing sends a conditional header yet, so every existing test takes the unconditional path unchanged.

- [ ] **Step 6: Commit**

```bash
git add internal/storage/tigris/client_test.go internal/storage/tigris/conditional_test.go
git commit -m "test(storage/tigris): give the fake bucket ETags and preconditions

Tracks an opaque token per key, reports it from GetObject and
PutObject, and refuses a failed If-Match or If-None-Match with
PreconditionFailed. This covers the whole compare-and-swap path
without a live bucket.

Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 4: The read path — `refCache` and the merge rule

Reads become unconditional: every read merges `packed-refs` with the legacy loose layer, whether or not writes are enabled. That asymmetry is the whole rollback story.

**Files:**
- Create: `internal/storage/tigris/refcache.go`
- Create: `internal/storage/tigris/refcache_test.go`
- Modify: `internal/storage/tigris/tigris.go:83-113` (`Storer` fields), `:203-231` (`New`), `:251-264` (`Scoped`)
- Modify: `internal/storage/tigris/refs.go:83-98` (`Reference`), `:100-127` (`listLooseRefs`), `:129-135` (`IterReferences`), `:144-150` (`CountLooseRefs`), `:212-228` (`fetchSmall`)
- Modify: `internal/storage/tigris/refs_test.go:134-178` (`TestIterReferencesSortedAndComplete`)

**Interfaces:**
- Consumes: `decodePackedRefs`, `packedRefsKey`, `errBadPackedRefs` from Task 2. `listLooseRefs` from `refs.go:103`, unchanged.
- Produces:
  - `type refCache struct` with fields `mu`, `built`, `etag`, `packed`, `loose`
  - `func newRefCache() *refCache`
  - `func (s *Storer) ensureRefsBuilt() error`
  - `func (s *Storer) refView() (map[plumbing.ReferenceName]*plumbing.Reference, error)` — the merged read view; callers must not mutate it
  - `func (s *Storer) fetchSmallETag(key string) ([]byte, string, error)`
  - `func (s *Storer) looseReference(n plumbing.ReferenceName) (*plumbing.Reference, error)`
  - `func WithPackedRefs(enabled bool) Option`
  - `Storer.refs *refCache` and `Storer.packedRefs bool`

**WARNING: `listLooseRefs` currently loads each key by calling `s.Reference`
(`refs.go:113`). Step 6 reroutes `Reference` through the cache, and
`ensureRefsBuilt` calls `listLooseRefs` while holding the cache lock. Leave
that call in place and the storer deadlocks on its own mutex the first time
anything reads a ref. Step 5a breaks the cycle and must not be skipped.**

- [ ] **Step 1: Write the failing tests**

Create `internal/storage/tigris/refcache_test.go`:

```go
package tigris

import (
	"errors"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

// seedPackedRefs writes a packed-refs object straight into the fake bucket,
// under the given storer's prefix.
func seedPackedRefs(t *testing.T, f *fakeS3, s *Storer, refs ...*plumbing.Reference) {
	t.Helper()
	f.put(s.prefix+packedRefsKey, string(encodePackedRefs(refSet(refs...))), nil)
}

func TestRefViewMergesPackedAndLoose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		packed []*plumbing.Reference
		loose  map[string]string // key suffix under refs/ -> stored body
		want   map[string]string // ref name -> expected hash hex
	}{
		{
			name: "packed only",
			packed: []*plumbing.Reference{
				hashRef("refs/heads/main", headAB),
				hashRef("refs/tags/v1", headCD),
			},
			want: map[string]string{"refs/heads/main": headAB, "refs/tags/v1": headCD},
		},
		{
			name:  "loose only, the state of a bucket before any packed write",
			loose: map[string]string{"refs/heads/main": headAB + "\n"},
			want:  map[string]string{"refs/heads/main": headAB},
		},
		{
			name: "a loose ref supplies a name packed does not hold",
			packed: []*plumbing.Reference{
				hashRef("refs/heads/main", headAB),
			},
			loose: map[string]string{"refs/heads/topic": headCD + "\n"},
			want:  map[string]string{"refs/heads/main": headAB, "refs/heads/topic": headCD},
		},
		{
			// The load-bearing case. A loose key can only exist if something
			// wrote it after the last packed write, so it is newer and must
			// win. Get this backwards and a fleet running two releases at once
			// swallows every push made by the older binaries.
			name: "a loose ref beats a packed ref with the same name",
			packed: []*plumbing.Reference{
				hashRef("refs/heads/main", headAB),
			},
			loose: map[string]string{"refs/heads/main": headCD + "\n"},
			want:  map[string]string{"refs/heads/main": headCD},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			s := newTestStorer(t, f)
			if len(tt.packed) > 0 {
				seedPackedRefs(t, f, s, tt.packed...)
			}
			for name, body := range tt.loose {
				f.put(refPrefix+name, body, nil)
			}

			view, err := s.refView()
			if err != nil {
				t.Fatalf("refView: %v", err)
			}
			if len(view) != len(tt.want) {
				t.Fatalf("view holds %d refs, want %d: %v", len(view), len(tt.want), view)
			}
			for name, hex := range tt.want {
				got, ok := view[plumbing.ReferenceName(name)]
				if !ok {
					t.Fatalf("ref %s missing from the view", name)
				}
				if got.Hash().String() != hex {
					t.Errorf("ref %s = %s, want %s", name, got.Hash(), hex)
				}
			}
		})
	}
}

// TestRefViewCostsTwoCalls is one of the two headline assertions of this whole
// change. Before it, one advertisement cost one ListObjectsV2 page per 1000
// refs plus one GetObject per ref.
func TestRefViewCostsTwoCalls(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	obs, snapshot := countingObserver()
	s := newTestStorer(t, f, obs)

	many := make([]*plumbing.Reference, 0, 500)
	for i := 0; i < 500; i++ {
		many = append(many, hashRef("refs/tags/v"+strconv.Itoa(i), headAB))
	}
	seedPackedRefs(t, f, s, many...)

	it, err := s.IterReferences()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	n := 0
	if err := it.ForEach(func(*plumbing.Reference) error { n++; return nil }); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if n != 500 {
		t.Fatalf("walked %d refs, want 500", n)
	}

	seen := snapshot()
	if seen["GetObject"] != 1 {
		t.Errorf("GetObject calls = %d, want 1 (map: %v)", seen["GetObject"], seen)
	}
	if seen["ListObjectsV2"] != 1 {
		t.Errorf("ListObjectsV2 calls = %d, want 1 (map: %v)", seen["ListObjectsV2"], seen)
	}
}

// TestRefCacheIsMemoizedPerStorer pins that the cache builds once, and that a
// Scoped storer gets its own.
func TestRefCacheIsMemoizedPerStorer(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	obs, snapshot := countingObserver()
	base := newTestStorer(t, f, obs)
	s := base.Scoped("acme/widgets")
	seedPackedRefs(t, f, s, hashRef("refs/heads/main", headAB))

	for i := 0; i < 5; i++ {
		if _, err := s.Reference(plumbing.ReferenceName("refs/heads/main")); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if got := snapshot()["GetObject"]; got != 1 {
		t.Errorf("five reads made %d GetObject calls, want 1", got)
	}

	// A sibling repository must not see this one's refs, and must build its own
	// cache.
	other := base.Scoped("acme/gadgets")
	if _, err := other.Reference(plumbing.ReferenceName("refs/heads/main")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("sibling saw another repository's ref: %v", err)
	}
}

// TestRefCacheIsNotStickyOnError copies ensurePacksBuilt's posture: a
// transient failure is retried on the next call, not remembered forever.
func TestRefCacheIsNotStickyOnError(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	seedPackedRefs(t, f, s, hashRef("refs/heads/main", headAB))

	f.listErr = errors.New("transient list failure")
	if _, err := s.refView(); err == nil {
		t.Fatal("a failing list must surface as an error")
	}

	f.listErr = nil
	view, err := s.refView()
	if err != nil {
		t.Fatalf("the cache stayed poisoned after the failure cleared: %v", err)
	}
	if _, ok := view[plumbing.ReferenceName("refs/heads/main")]; !ok {
		t.Error("retry produced an empty view")
	}
}

// TestRefViewSurfacesCorruption pins that a corrupt packed-refs object is an
// error and never an empty ref set. An empty set would make a repository look
// brand new, and git would then happily accept a force-push over the top.
func TestRefViewSurfacesCorruption(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f)
	f.put(packedRefsKey, "this is not a packed-refs object", nil)

	if _, err := s.refView(); !errors.Is(err, errBadPackedRefs) {
		t.Fatalf("want errBadPackedRefs, got %v", err)
	}
}
```

`refcache_test.go` imports `errors`, `strconv`, `testing`, and
`github.com/go-git/go-git/v6/plumbing`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestRefView|TestRefCache' ./internal/storage/tigris/`
Expected: FAIL to build, with `s.refView undefined`.

- [ ] **Step 3: Add the ETag-aware small fetch**

In `internal/storage/tigris/refs.go`, replace `fetchSmall` (lines 212-228) with:

```go
// fetchSmall GETs one whole ancillary object and returns its body. Misses
// normalize to plumbing.ErrObjectNotFound so every caller maps absence the
// same way object reads do.
func (s *Storer) fetchSmall(key string) ([]byte, error) {
	body, _, err := s.fetchSmallETag(key)
	return body, err
}

// fetchSmallETag is fetchSmall plus the object's ETag. Only packed-refs wants
// the ETag, which it uses as its compare-and-swap token, so the plain wrapper
// above keeps the other three callers (refs, shallow, index, config) untouched.
func (s *Storer) fetchSmallETag(key string) ([]byte, string, error) {
	start := time.Now()
	out, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(key),
	})
	s.observe("GetObject", start, err)
	switch {
	case err == nil:
	case isNotFound(err):
		return nil, "", plumbing.ErrObjectNotFound
	default:
		return nil, "", err
	}
	defer out.Body.Close()
	body, rerr := io.ReadAll(out.Body)
	return body, sv(out.ETag), rerr
}
```

- [ ] **Step 4: Write the cache**

Create `internal/storage/tigris/refcache.go`:

```go
package tigris

import (
	"errors"
	"fmt"
	"sync"

	"github.com/go-git/go-git/v6/plumbing"
)

// refCache is one Storer's memoized view of every ref in its repository. It
// mirrors packIndex: built once per instance, and not sticky on error.
//
// repofs calls Scoped once per request (internal/repofs/repofs.go:94), so one
// cache serves one request. That is the same lifetime ensurePacksBuilt relies
// on, and it is what turns a push's N existence checks into N map lookups.
//
// packed and loose stay separate rather than pre-merged. A write needs them
// apart: it rewrites packed, and it must know which loose keys it has to fold
// and then delete.
type refCache struct {
	mu    sync.Mutex
	built bool
	// etag is the compare-and-swap token for packedRefsKey. An empty string
	// means the object does not exist, and a writer must use If-None-Match
	// instead of If-Match.
	etag   string
	packed map[plumbing.ReferenceName]*plumbing.Reference
	loose  map[plumbing.ReferenceName]*plumbing.Reference
}

func newRefCache() *refCache { return &refCache{} }

// ensureRefsBuilt fills the cache once. Two calls: one GetObject for
// packed-refs, and one ListObjectsV2 for the legacy loose layer.
//
// The list runs every time. No flag records that a repository has been folded
// and that the list can be skipped, and that is deliberate: one cheap round
// trip is the price of a safety net that catches anything writing a loose ref
// outside this code, including an older binary during a rolling deploy.
func (s *Storer) ensureRefsBuilt() error {
	s.refs.mu.Lock()
	defer s.refs.mu.Unlock()
	if s.refs.built {
		return nil
	}

	body, etag, err := s.fetchSmallETag(s.prefix + packedRefsKey)
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		body, etag = nil, ""
	default:
		return fmt.Errorf("tigris: load packed refs: %w", err)
	}

	packed := map[plumbing.ReferenceName]*plumbing.Reference{}
	if len(body) > 0 {
		// A corrupt object is an error and never an empty ref set. An empty set
		// makes a repository look brand new, and git would then accept a
		// force-push straight over the top of it.
		packed, err = decodePackedRefs(body)
		if err != nil {
			return err
		}
	}

	looseRefs, err := s.listLooseRefs()
	if err != nil {
		return err
	}
	loose := make(map[plumbing.ReferenceName]*plumbing.Reference, len(looseRefs))
	for _, r := range looseRefs {
		loose[r.Name()] = r
	}

	s.refs.etag = etag
	s.refs.packed = packed
	s.refs.loose = loose
	s.refs.built = true
	return nil
}

// refView returns the merged read view: packed, with the legacy loose layer on
// top. The caller must not mutate the result.
//
// A loose ref wins. That reads backwards, and it is only sound because of the
// invariant commitRefs holds: a write through the packed path deletes the loose
// keys for every name it touched before it reports success. So a loose key can
// exist only if something wrote it after the last packed write, which makes it
// newer.
func (s *Storer) refView() (map[plumbing.ReferenceName]*plumbing.Reference, error) {
	if err := s.ensureRefsBuilt(); err != nil {
		return nil, err
	}

	s.refs.mu.Lock()
	defer s.refs.mu.Unlock()

	out := make(map[plumbing.ReferenceName]*plumbing.Reference, len(s.refs.packed)+len(s.refs.loose))
	for n, r := range s.refs.packed {
		out[n] = r
	}
	for n, r := range s.refs.loose {
		out[n] = r
	}
	return out, nil
}

// invalidateRefs drops the cache so the next read rebuilds it. Called after a
// refused compare-and-swap, and after any write that took the loose path.
func (s *Storer) invalidateRefs() {
	s.refs.mu.Lock()
	defer s.refs.mu.Unlock()
	s.refs.built = false
	s.refs.etag = ""
	s.refs.packed = nil
	s.refs.loose = nil
}
```

- [ ] **Step 4a: Break the loose-loader cycle**

`listLooseRefs` loads each key by calling `s.Reference`. Step 6 makes
`Reference` read the cache, and `ensureRefsBuilt` calls `listLooseRefs` while
holding `s.refs.mu` — so leaving this alone deadlocks the storer against
itself. Split the single-key load out and have `listLooseRefs` use it.

In `internal/storage/tigris/refs.go`, replace `listLooseRefs` (lines 100-127)
with:

```go
// looseReference loads one refs/<name> object directly, with no cache in the
// path. ensureRefsBuilt is what fills the cache, and it calls listLooseRefs
// below to do so, so nothing on this path may consult the cache: Reference
// does, and routing through it would have ensureRefsBuilt wait on the lock it
// already holds.
func (s *Storer) looseReference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	body, err := s.fetchSmall(s.prefix + refKey(n))
	switch {
	case err == nil:
	case errors.Is(err, plumbing.ErrObjectNotFound):
		return nil, plumbing.ErrReferenceNotFound
	default:
		return nil, fmt.Errorf("tigris: load ref %s: %w", n.String(), err)
	}
	return decodeRefValue(n, string(body))
}

// listLooseRefs walks the legacy loose layer, which ensureRefsBuilt merges
// under the packed object. Malformed entries log-and-skip: each loose key is an
// independent object, so one bad one says nothing about its neighbors. That is
// deliberately gentler than decodePackedRefs, where every ref shares one object
// and a single bad line makes all of them untrustworthy.
//
// Vanished-mid-list keys behave like the object iterator's race rule.
func (s *Storer) listLooseRefs() ([]*plumbing.Reference, error) {
	keys, err := s.listKeys(s.prefix + refPrefix)
	if err != nil {
		return nil, err
	}

	var refs []*plumbing.Reference
	for _, k := range keys {
		name := plumbing.ReferenceName(strings.TrimPrefix(k, s.prefix+refPrefix))

		ref, rerr := s.looseReference(name)
		switch {
		case rerr == nil:
			refs = append(refs, ref)
		case errors.Is(rerr, plumbing.ErrReferenceNotFound):
			continue
		case errors.Is(rerr, errMalformedRef):
			slog.Warn("skipping malformed loose ref", "err", rerr, "key", k)
			continue
		default:
			return nil, rerr
		}
	}
	return refs, nil
}
```

Note the `slog.Warn` argument order changed so `err` leads, matching the
convention in `AGENTS.md`. The key stays `"err"`.

- [ ] **Step 5: Wire the Storer**

In `internal/storage/tigris/tigris.go`, add two fields to `Storer`, after `packs` (line 95):

```go
	packs *packIndex // pack containers this Storer knows about; see packindex.go
	// refs is this Storer's memoized ref view; see refcache.go. Reads always
	// consult it. Writes only go through it when packedRefs is set.
	refs *refCache
	// packedRefs enables writing the packed-refs object. Reading it is
	// unconditional — see WithPackedRefs for why the two differ.
	packedRefs bool
```

Add the option next to `WithPackCompression` (after line 161):

```go
// WithPackedRefs controls whether ref writes go to the single packed-refs
// object under a compare-and-swap, instead of one loose object per ref.
// Reading packed-refs is never gated: a Storer merges the packed object and
// the loose layer no matter how this is set.
//
// The asymmetry is the same rollback story WithPackCompression tells. A binary
// that cannot read packed-refs sees every ref written through it vanish, which
// makes a repository look empty rather than failing loudly. Shipping the
// reader in one release and turning writes on in a later one makes that window
// empty.
func WithPackedRefs(enabled bool) Option {
	return func(s *Storer) { s.packedRefs = enabled }
}
```

In `New`, build the cache next to the pack index (line 228):

```go
	s.up = newUploader(s)
	s.packs = newPackIndex()
	s.refs = newRefCache()
	s.fetchSem = make(chan struct{}, maxLivePackFetches)
```

In `Scoped`, give the copy its own cache (line 261), and extend the doc comment's list:

```go
	cp.up = newUploader(&cp)
	cp.packs = newPackIndex()
	cp.refs = newRefCache()
	return &cp
```

In `Scoped`'s doc comment, change "gets its own uploader and pack index (see upload.go, packindex.go)" to "gets its own uploader, pack index, and ref cache (see upload.go, packindex.go, refcache.go)".

- [ ] **Step 6: Route the three read methods through the cache**

In `internal/storage/tigris/refs.go`, replace `Reference` (lines 83-98):

```go
func (s *Storer) Reference(n plumbing.ReferenceName) (*plumbing.Reference, error) {
	view, err := s.refView()
	if err != nil {
		return nil, err
	}
	ref, ok := view[n]
	if !ok {
		return nil, plumbing.ErrReferenceNotFound
	}
	return ref, nil
}
```

Replace `IterReferences` (lines 129-135). The merged view is a map, so the sort is now explicit rather than inherited from S3's listing order:

```go
// IterReferences walks every ref, sorted by name. The order used to come free
// from S3's lexicographic listing; the merged view is a map, so it is sorted
// here instead. Callers depend on it: a ref advertisement is more compressible
// in name order, and a stable order keeps test assertions exact.
func (s *Storer) IterReferences() (storer.ReferenceIter, error) {
	view, err := s.refView()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(view))
	for n := range view {
		names = append(names, n.String())
	}
	sort.Strings(names)

	refs := make([]*plumbing.Reference, 0, len(names))
	for _, n := range names {
		refs = append(refs, view[plumbing.ReferenceName(n)])
	}
	return storer.NewReferenceSliceIter(refs), nil
}
```

Add `"sort"` to the imports in `refs.go`.

Replace `CountLooseRefs` (lines 144-150). Its meaning narrows to exactly what its name says, which is also what go-git wants the number to mean:

```go
// CountLooseRefs reports how many legacy loose refs are left. After a fold it
// returns 0, which correctly tells go-git there is nothing to pack. It
// deliberately does not count packed refs: go-git reads this number to decide
// whether compaction is worth doing, and packed refs are already compacted.
func (s *Storer) CountLooseRefs() (int, error) {
	if err := s.ensureRefsBuilt(); err != nil {
		return 0, err
	}
	s.refs.mu.Lock()
	defer s.refs.mu.Unlock()
	return len(s.refs.loose), nil
}
```

Also delete the now-stale concurrency note at `refs.go:22-24` ("Concurrency note: CheckAndSetReference compares then writes non-atomically...") and replace it with a pointer to the new layout:

```go
// Two layers hold refs. packed-refs holds them all in one object under a
// compare-and-swap (see refcache.go and packedrefs.go); refs/<name> is the
// legacy loose layer, which is read-only and folded away on the first packed
// write. A loose ref wins over a packed ref with the same name — refView
// explains why.
```

- [ ] **Step 7: Update the one existing test this changes**

`TestIterReferencesSortedAndComplete` (`refs_test.go:134`) asserts that `CountLooseRefs` equals the walked ref count. That still holds here, because writes are still loose in this task. But its comment says the order is "S3-sorted", which is no longer where the order comes from. Change the comment on the `want` line (line 161):

```go
	want := []string{"refs/heads/alpha", "refs/heads/zeta", "refs/tags/v1"} // IterReferences sorts by name
```

- [ ] **Step 8: Run the new tests**

Run: `go test -run 'TestRefView|TestRefCache' ./internal/storage/tigris/ -v`
Expected: PASS, every subtest.

- [ ] **Step 9: Run everything**

Run: `go test ./...`
Expected: PASS. Reads now make one extra `GetObject` that misses, which no existing test counts.

- [ ] **Step 10: Commit**

```bash
git add internal/storage/tigris/refcache.go internal/storage/tigris/refcache_test.go internal/storage/tigris/refs.go internal/storage/tigris/tigris.go internal/storage/tigris/refs_test.go
git commit -m "feat(storage/tigris): read refs through a memoized packed view

One GetObject for packed-refs plus one ListObjectsV2 for the legacy
loose layer, built once per request and merged with loose winning. An
advertisement of 500 refs drops from 501 calls to two. Reads are
unconditional; WithPackedRefs gates writes only, so a rollback still
sees every ref.

Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 5: The compare-and-swap write path

**Files:**
- Modify: `internal/storage/tigris/refcache.go` (append `commitRefs` and friends)
- Modify: `internal/storage/tigris/tigris.go:54-69` (error vars)
- Modify: `internal/storage/tigris/refs.go:48-81` (`SetReference`, `CheckAndSetReference`), `:137-142` (`RemoveReference`)
- Modify: `internal/metrics/metrics.go`
- Modify: `cmd/objgitd/main.go` (wire the metric)
- Create: `internal/storage/tigris/refcommit_test.go`

**Interfaces:**
- Consumes: `encodePackedRefs` (Task 2), `isPreconditionFailed` (Task 1), `preconditionFailed` in tests (Task 3), `refCache` and `refView` (Task 4), `s.up.flush()` from `upload.go`.
- Produces:
  - `const maxRefCASRetries = 8`
  - `var ErrRefContention error`
  - `type refExpectation struct { name plumbing.ReferenceName; old *plumbing.Reference }`
  - `func (s *Storer) commitRefs(sets []*plumbing.Reference, removes []plumbing.ReferenceName, expect []refExpectation) error`
  - `func (s *Storer) setLooseReference(ref *plumbing.Reference) error` — the old `SetReference` body
  - `func WithRefCASObserver(fn func()) Option`
  - `metrics.ObserveRefCASRetry()`

- [ ] **Step 1: Write the failing tests**

Create `internal/storage/tigris/refcommit_test.go`:

```go
package tigris

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage"
)

// packedTestStorer builds a storer with packed-ref writes enabled.
func packedTestStorer(t *testing.T, f *fakeS3, opts ...Option) *Storer {
	t.Helper()
	return newTestStorer(t, f, append([]Option{WithPackedRefs(true)}, opts...)...)
}

// TestCommitRefsCostsOnePut is the second headline assertion of this change.
// Before it, a push of 500 refs cost 500 GetObject calls and 500 PutObject
// calls.
func TestCommitRefsCostsOnePut(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	obs, snapshot := countingObserver()
	s := packedTestStorer(t, f, obs)

	sets := make([]*plumbing.Reference, 0, 500)
	for i := 0; i < 500; i++ {
		sets = append(sets, hashRef("refs/tags/v"+strconv.Itoa(i), headAB))
	}
	if err := s.commitRefs(sets, nil, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got := snapshot()["PutObject"]; got != 1 {
		t.Errorf("PutObject calls = %d, want 1 (map: %v)", got, snapshot())
	}

	// And every ref really landed.
	view, err := s.refView()
	if err != nil {
		t.Fatalf("refView: %v", err)
	}
	if len(view) != 500 {
		t.Errorf("view holds %d refs, want 500", len(view))
	}
}

func TestCommitRefsAppliesSetsAndRemoves(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	seedPackedRefs(t, f, s,
		hashRef("refs/heads/main", headAB),
		hashRef("refs/heads/doomed", headAB),
	)

	sym := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.ReferenceName("refs/heads/main"))
	sets := []*plumbing.Reference{hashRef("refs/heads/main", headCD), sym}
	removes := []plumbing.ReferenceName{plumbing.ReferenceName("refs/heads/doomed")}

	if err := s.commitRefs(sets, removes, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}

	main, err := s.Reference(plumbing.ReferenceName("refs/heads/main"))
	if err != nil {
		t.Fatalf("read main: %v", err)
	}
	if main.Hash().String() != headCD {
		t.Errorf("main = %s, want %s", main.Hash(), headCD)
	}
	head, err := s.Reference(plumbing.HEAD)
	if err != nil {
		t.Fatalf("read HEAD: %v", err)
	}
	if head.Type() != plumbing.SymbolicReference {
		t.Errorf("HEAD lost its symbolic nature: %+v", head)
	}
	if _, err := s.Reference(plumbing.ReferenceName("refs/heads/doomed")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("removed ref survived: %v", err)
	}
}

// TestCommitRefsUsesCreateIfAbsentOnAFreshRepo pins that the first ever write
// sends If-None-Match, not If-Match against an empty ETag.
func TestCommitRefsUsesCreateIfAbsentOnAFreshRepo(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)

	// Another writer wins the race to create the object. Our create-if-absent
	// must be refused, then retried as a compare-and-swap and succeed.
	f.putHook = func(p *putSnapshot) {
		if p.n == 1 {
			f.putLocked(packedRefsKey, string(encodePackedRefs(refSet(hashRef("refs/heads/other", headCD)))), nil)
		}
	}

	if err := s.commitRefs([]*plumbing.Reference{hashRef("refs/heads/main", headAB)}, nil, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}

	view, err := s.refView()
	if err != nil {
		t.Fatalf("refView: %v", err)
	}
	if _, ok := view[plumbing.ReferenceName("refs/heads/main")]; !ok {
		t.Error("our ref did not land")
	}
	if _, ok := view[plumbing.ReferenceName("refs/heads/other")]; !ok {
		t.Error("the racing writer's ref was clobbered instead of merged")
	}
}

func TestCommitRefsRetriesAndReportsContention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stealFirst int  // rewrite packed-refs under us before this many attempts
		wantErr    error
		wantLanded bool
	}{
		{name: "one lost race then success", stealFirst: 1, wantLanded: true},
		{name: "three lost races then success", stealFirst: 3, wantLanded: true},
		{name: "never wins", stealFirst: maxRefCASRetries + 1, wantErr: ErrRefContention},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFakeS3(t)
			s := packedTestStorer(t, f)
			seedPackedRefs(t, f, s, hashRef("refs/heads/other", headCD))

			var retries int
			f.putHook = func(p *putSnapshot) {
				if p.n <= tt.stealFirst {
					// A competing writer lands first, invalidating our ETag.
					f.putLocked(packedRefsKey, string(encodePackedRefs(refSet(
						hashRef("refs/heads/other", headCD),
						hashRef("refs/heads/racer"+strconv.Itoa(p.n), headAB),
					))), nil)
				}
			}
			s.refCASObserver = func() { retries++ }

			err := s.commitRefs([]*plumbing.Reference{hashRef("refs/heads/main", headAB)}, nil, nil)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("want %v, got %v", tt.wantErr, err)
				}
				if retries != maxRefCASRetries-1 {
					t.Errorf("observed %d retries, want %d", retries, maxRefCASRetries-1)
				}
				return
			}
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			if retries != tt.stealFirst {
				t.Errorf("observed %d retries, want %d", retries, tt.stealFirst)
			}

			view, verr := s.refView()
			if verr != nil {
				t.Fatalf("refView: %v", verr)
			}
			if _, ok := view[plumbing.ReferenceName("refs/heads/main")]; !ok {
				t.Error("our ref did not land after the retries")
			}
			// The competing writer's last ref must survive: a retry re-reads and
			// re-applies rather than overwriting.
			last := plumbing.ReferenceName("refs/heads/racer" + strconv.Itoa(tt.stealFirst))
			if _, ok := view[last]; !ok {
				t.Errorf("retry clobbered the competing writer's ref %s", last)
			}
		})
	}
}

// TestCommitRefsRevalidatesExpectations is the race that refs.go documented as
// a known hole before this change. A competing writer moves the ref between
// our read and our write, and the result must be go-git's own
// ErrReferenceHasChanged — not ErrRefContention, which means something else.
func TestCommitRefsRevalidatesExpectations(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	seedPackedRefs(t, f, s, hashRef("refs/heads/main", headAB))

	f.putHook = func(p *putSnapshot) {
		if p.n == 1 {
			// Somebody else moved main to headCD first.
			f.putLocked(packedRefsKey, string(encodePackedRefs(refSet(hashRef("refs/heads/main", headCD)))), nil)
		}
	}

	expect := []refExpectation{{
		name: plumbing.ReferenceName("refs/heads/main"),
		old:  hashRef("refs/heads/main", headAB),
	}}
	err := s.commitRefs([]*plumbing.Reference{hashRef("refs/heads/main", headAB)}, nil, expect)

	if !errors.Is(err, storage.ErrReferenceHasChanged) {
		t.Fatalf("want ErrReferenceHasChanged, got %v", err)
	}
	if errors.Is(err, ErrRefContention) {
		t.Error("a genuine ref conflict was reported as raw contention")
	}

	// The competing writer's value must stand.
	cur, rerr := s.Reference(plumbing.ReferenceName("refs/heads/main"))
	if rerr != nil {
		t.Fatalf("read after refusal: %v", rerr)
	}
	if cur.Hash().String() != headCD {
		t.Errorf("refused commit mutated the ref: %s", cur.Hash())
	}
}

// TestCommitRefsFlushesOnce pins the invariant refs.go protects — a ref never
// names an object whose upload has not finished — and pins that a batch pays
// for one flush rather than one per ref.
func TestCommitRefsFlushesOnce(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)

	obj := plumbing.NewMemoryObject(s.oh)
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(5)
	if _, err := obj.Write([]byte("hello")); err != nil {
		t.Fatalf("buffer: %v", err)
	}
	h, err := s.SetEncodedObject(obj)
	if err != nil {
		t.Fatalf("SetEncodedObject: %v", err)
	}

	sets := []*plumbing.Reference{
		plumbing.NewHashReference(plumbing.ReferenceName("refs/heads/a"), h),
		plumbing.NewHashReference(plumbing.ReferenceName("refs/heads/b"), h),
	}
	if err := s.commitRefs(sets, nil, nil); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The object must be readable, which is only true if the flush happened
	// before the ref write.
	if err := s.HasEncodedObject(h); err != nil {
		t.Errorf("ref points at an object that is not there: %v", err)
	}
}

// TestPackedWritesAreGated pins that WithPackedRefs off keeps every write on
// the loose path, which is what makes a rollback safe.
func TestPackedWritesAreGated(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f) // default: off

	if err := s.SetReference(hashRef("refs/heads/main", headAB)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, ok := f.objs["refs/refs/heads/main"]; !ok {
		t.Error("write did not land as a loose ref")
	}
	if _, ok := f.objs[packedRefsKey]; ok {
		t.Error("packed-refs was written while WithPackedRefs was off")
	}
}

// TestConcurrentCommitsAllLand runs real goroutines through the retry loop. It
// is the closest a fake gets to the production race, and it is what proves the
// loop converges rather than losing writes.
func TestConcurrentCommitsAllLand(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	base := newTestStorer(t, f, WithPackedRefs(true))

	const writers = 8
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each writer gets its own Storer, exactly as each request does.
			s := base.Scoped("")
			errs[i] = s.commitRefs([]*plumbing.Reference{
				hashRef("refs/heads/w"+strconv.Itoa(i), headAB),
			}, nil, nil)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}

	fresh := base.Scoped("")
	view, err := fresh.refView()
	if err != nil {
		t.Fatalf("refView: %v", err)
	}
	if len(view) != writers {
		t.Errorf("view holds %d refs, want %d — the loop lost writes: %v", len(view), writers, view)
	}
}
```

- [ ] **Step 2: Add the fake's put hook**

The retry tests need to change the bucket between attempts. Add to `fakeS3` in `client_test.go`, next to `putDelay` (line 51):

```go
	// putHook fires inside PutObject after the precondition check and before
	// the write, with the attempt number. It exists so a test can land a
	// competing write between a storer's read and its retry, which is the only
	// way to exercise the compare-and-swap loop deterministically.
	putHook func(*putSnapshot)
```

Add the snapshot type and call it from `PutObject`. In `PutObject`, immediately after `f.puts++` and the `f.putErr` check, insert:

```go
	if f.putHook != nil {
		// Called with the lock held, so a hook may use f.objs directly. It must
		// not call back into a Storer.
		f.putHook(&putSnapshot{n: f.puts, key: sv(p.Key)})
	}
```

And define next to `preconditionFailed`:

```go
// putSnapshot is what a putHook sees: which attempt this is, and which key it
// targets.
type putSnapshot struct {
	n   int
	key string
}
```

**Note:** `f.put` takes `f.mu` itself, so a hook cannot call it while the lock is held. Change `put` to delegate to an unlocked helper, and have the hook path use the unlocked one:

```go
func (f *fakeS3) put(key, body string, meta map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putLocked(key, body, meta)
}

// putLocked is put without the lock, for a putHook, which already holds it.
func (f *fakeS3) putLocked(key, body string, meta map[string]string) {
	m := make(map[string]string, len(meta))
	for k, v := range meta {
		m[k] = v
	}
	f.objs[key] = fakeObject{body: []byte(body), meta: m, etag: f.nextETag()}
}
```

The tests in Step 1 already call `f.putLocked` inside their hooks, for exactly this reason.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -run 'TestCommitRefs|TestPackedWrites|TestConcurrentCommits' ./internal/storage/tigris/`
Expected: FAIL to build, with `s.commitRefs undefined`, `undefined: ErrRefContention`, `undefined: refExpectation`, `undefined: maxRefCASRetries`, `s.refCASObserver undefined`.

- [ ] **Step 4: Add the error and the retry bound**

In `internal/storage/tigris/tigris.go`, add to the `var` block (after line 63):

```go
	// ErrRefContention marks a ref write that lost maxRefCASRetries
	// compare-and-swap races in a row without any of its own expectations
	// failing. It is deliberately not storage.ErrReferenceHasChanged: nothing
	// the caller asked for was violated, so a retry is reasonable, where a
	// changed ref means the caller's view of the world is stale.
	ErrRefContention = errors.New("tigris: packed-refs contention: too many failed compare-and-swap attempts")
```

- [ ] **Step 5: Write the commit loop**

Append to `internal/storage/tigris/refcache.go`:

```go
// maxRefCASRetries bounds the compare-and-swap loop. Eight is far above what
// honest traffic reaches: a loser re-reads one object and re-uploads one
// object, so a round trip is milliseconds, and losing eight in a row means
// something is wrong that another attempt will not fix.
const maxRefCASRetries = 8

// refExpectation is one CheckAndSetReference precondition: the caller believes
// that name currently holds old. A nil old means "the ref must not exist".
type refExpectation struct {
	name plumbing.ReferenceName
	old  *plumbing.Reference
}

// commitRefs applies a whole batch of ref mutations in one conditional
// PutObject. That single call is the commit point: either every mutation in
// the batch lands or none of them do.
//
// The flush at the top runs once for the batch, and not once per ref. It holds
// the invariant refs.go protects — a ref must never name an object whose
// upload has not finished — and one flush per batch instead of one per ref is
// most of why this path is faster than the loose one.
func (s *Storer) commitRefs(sets []*plumbing.Reference, removes []plumbing.ReferenceName, expect []refExpectation) error {
	if len(sets) == 0 && len(removes) == 0 {
		return nil
	}
	if err := s.up.flush(); err != nil {
		return fmt.Errorf("tigris: commit refs: %w", err)
	}

	for attempt := 0; attempt < maxRefCASRetries; attempt++ {
		if attempt > 0 {
			s.invalidateRefs()
			if s.refCASObserver != nil {
				s.refCASObserver()
			}
		}
		if err := s.ensureRefsBuilt(); err != nil {
			return err
		}

		s.refs.mu.Lock()
		etag := s.refs.etag
		next := make(map[plumbing.ReferenceName]*plumbing.Reference, len(s.refs.packed)+len(s.refs.loose)+len(sets))
		for n, r := range s.refs.packed {
			next[n] = r
		}
		// Fold the legacy loose layer in. After this commit lands, these names
		// live in packed-refs, and Task 6 deletes the keys.
		folded := make([]plumbing.ReferenceName, 0, len(s.refs.loose))
		for n, r := range s.refs.loose {
			next[n] = r
			folded = append(folded, n)
		}
		s.refs.mu.Unlock()

		// Expectations are checked against the merged pre-write view, which is
		// exactly what next holds before the mutations below are applied.
		if err := checkRefExpectations(next, expect); err != nil {
			return err
		}

		for _, r := range sets {
			next[r.Name()] = r
		}
		for _, n := range removes {
			delete(next, n)
		}

		in := &s3.PutObjectInput{
			Bucket: sp(s.bucket),
			Key:    sp(s.prefix + packedRefsKey),
			Body:   bytes.NewReader(encodePackedRefs(next)),
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
		case isPreconditionFailed(err):
			continue // somebody else landed first; re-read and re-apply
		default:
			return fmt.Errorf("tigris: commit refs: %w", err)
		}

		// The commit landed. Adopt it in place rather than re-reading, and hand
		// the folded names to the loose-key cleanup.
		s.refs.mu.Lock()
		s.refs.etag = sv(out.ETag)
		s.refs.packed = next
		s.refs.loose = map[plumbing.ReferenceName]*plumbing.Reference{}
		s.refs.built = true
		s.refs.mu.Unlock()

		return s.dropFoldedLooseRefs(folded, removes)
	}
	return fmt.Errorf("%w after %d attempts", ErrRefContention, maxRefCASRetries)
}

// checkRefExpectations compares a caller's CheckAndSetReference preconditions
// against the pre-write view. It runs on every attempt, not once: a retry
// happens precisely because somebody else wrote, so the expectation must be
// re-tested against what they wrote.
func checkRefExpectations(view map[plumbing.ReferenceName]*plumbing.Reference, expect []refExpectation) error {
	for _, e := range expect {
		cur, ok := view[e.name]
		switch {
		case e.old == nil:
			// A nil old is lenient, matching the in-memory storer: a missing
			// current reference falls through to creation.
		case !ok:
			// Also lenient, and for the same reason.
		case cur.Hash() != e.old.Hash():
			return storage.ErrReferenceHasChanged
		}
	}
	return nil
}

// dropFoldedLooseRefs is defined in Task 6. Until then it is a stub.
func (s *Storer) dropFoldedLooseRefs(folded, removed []plumbing.ReferenceName) error {
	return nil
}
```

Add the imports `bytes`, `time`, `github.com/aws/aws-sdk-go-v2/service/s3`, and `github.com/go-git/go-git/v6/storage` to `refcache.go`.

- [ ] **Step 6: Add the retry observer hook**

In `tigris.go`, add a field next to `payloadObserver` (line 93):

```go
	// refCASObserver fires once per retried packed-refs compare-and-swap; see
	// WithRefCASObserver.
	refCASObserver func()
```

And the option, after `WithPayloadObserver`:

```go
// WithRefCASObserver installs a callback fired once for every retried
// packed-refs compare-and-swap. Contention is the only way the packed-ref
// design degrades quietly, so it is the one thing worth a metric of its own.
// A callback rather than a direct metrics call, for the same reason
// WithPayloadObserver is one: this package stays free of any Prometheus
// import. Wire metrics.ObserveRefCASRetry here from main.
//
// The callback must be safe for concurrent use.
func WithRefCASObserver(fn func()) Option {
	return func(s *Storer) { s.refCASObserver = fn }
}
```

- [ ] **Step 7: Route the three write methods**

In `refs.go`, rename the existing `SetReference` body to `setLooseReference` and add the gate:

```go
// SetReference writes one ref. With WithPackedRefs set it goes through
// commitRefs, which is one conditional PutObject; otherwise it writes one
// loose object, which is what every release before packed refs did.
func (s *Storer) SetReference(ref *plumbing.Reference) error {
	if ref == nil {
		return nil // tolerated identically by the in-memory storer
	}
	if s.packedRefs {
		return s.commitRefs([]*plumbing.Reference{ref}, nil, nil)
	}
	return s.setLooseReference(ref)
}

// setLooseReference writes one refs/<name> object. It flushes every upload
// queued through this Storer first, so a ref can never point at an object that
// failed — or hasn't yet finished — its asynchronous upload (see upload.go).
func (s *Storer) setLooseReference(ref *plumbing.Reference) error {
	if err := s.up.flush(); err != nil {
		return fmt.Errorf("tigris: set ref %s: %w", ref.Name().String(), err)
	}
	start := time.Now()
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(s.prefix + refKey(ref.Name())),
		Body:   strings.NewReader(encodeRefValue(ref)),
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: set ref %s: %w", ref.Name().String(), err)
	}
	// The cache no longer matches the bucket.
	s.invalidateRefs()
	return nil
}
```

Replace `CheckAndSetReference` (lines 68-81):

```go
// CheckAndSetReference writes newRef only if the ref currently holds old.
//
// With WithPackedRefs set, the compare and the write are one conditional
// request, so the check cannot go stale between them. Without it, the two
// steps race exactly as every release before packed refs did.
func (s *Storer) CheckAndSetReference(newRef, old *plumbing.Reference) error {
	if newRef == nil {
		return nil
	}
	if s.packedRefs {
		expect := []refExpectation{{name: newRef.Name(), old: old}}
		return s.commitRefs([]*plumbing.Reference{newRef}, nil, expect)
	}

	if old != nil {
		current, err := s.Reference(newRef.Name())
		if err == nil && current.Hash() != old.Hash() {
			return storage.ErrReferenceHasChanged
		}
		// Missing current reference falls through to creation, mirroring the
		// in-memory storer's lenient behavior.
	}
	return s.setLooseReference(newRef)
}
```

Replace `RemoveReference` (lines 137-142):

```go
func (s *Storer) RemoveReference(n plumbing.ReferenceName) error {
	if s.packedRefs {
		return s.commitRefs(nil, []plumbing.ReferenceName{n}, nil)
	}
	if err := s.removeSimple(s.prefix + refKey(n)); err != nil {
		return fmt.Errorf("tigris: remove ref %s: %w", n.String(), err)
	}
	s.invalidateRefs()
	return nil
}
```

**Note on `commitRefs` returning early for an empty batch:** `RemoveReference` passes one name, so it is never empty. But `commitRefs(nil, nil, nil)` returning `nil` means a removal of a nonexistent ref still writes. That is correct and matches the old behavior, which tolerated absence.

- [ ] **Step 8: Add the metric**

In `internal/metrics/metrics.go`, add to the `var` block next to `packPayloadRatio`:

```go
	refCASRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "objgit_ref_cas_retries_total",
		Help: "Packed-refs compare-and-swap attempts that lost a race and were retried.",
	})
```

And the observer, next to `ObservePackPayload`:

```go
// ObserveRefCASRetry records one retried packed-refs compare-and-swap. Wire it
// into tigris.WithRefCASObserver. A rising rate here is the one quiet way the
// packed-ref write path degrades: every retry re-reads and re-writes the whole
// object, so sustained contention shows up as latency long before it shows up
// as an error.
func ObserveRefCASRetry() {
	refCASRetries.Inc()
}
```

- [ ] **Step 9: Wire it in main**

In `cmd/objgitd/main.go`, find where `tigris.WithPayloadObserver(metrics.ObservePackPayload)` is passed to `tigris.New` and add alongside it:

```go
		tigris.WithRefCASObserver(metrics.ObserveRefCASRetry),
```

- [ ] **Step 10: Run the tests**

Run: `go test -run 'TestCommitRefs|TestPackedWrites|TestConcurrentCommits' ./internal/storage/tigris/ -v`
Expected: PASS, every subtest.

- [ ] **Step 11: Run everything**

Run: `go build ./... && go test ./...`
Expected: PASS. `WithPackedRefs` is off by default, so every existing ref test still takes the loose path.

- [ ] **Step 12: Commit**

```bash
git add internal/storage/tigris/refcache.go internal/storage/tigris/refcommit_test.go internal/storage/tigris/refs.go internal/storage/tigris/tigris.go internal/storage/tigris/client_test.go internal/metrics/metrics.go cmd/objgitd/main.go
git commit -m "feat(storage/tigris): write refs under a compare-and-swap

One conditional PutObject commits a whole batch of ref mutations, so a
push of 500 refs costs one call instead of 500. Retrying a refused
precondition re-reads and re-applies, which also makes
CheckAndSetReference atomic and closes the race refs.go documented.

A failed caller expectation stays storage.ErrReferenceHasChanged. Only
exhausting the retries gives the new ErrRefContention: the two say
different things and callers act on them differently.

Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 6: The legacy fold and its loose-key cleanup

**Files:**
- Modify: `internal/storage/tigris/refcache.go` (replace the `dropFoldedLooseRefs` stub)
- Modify: `internal/storage/tigris/tigris.go:74-80` (`s3API`)
- Modify: `internal/storage/tigris/refs.go` (`PackRefs`)
- Modify: `internal/storage/tigris/client_test.go` (add `DeleteObjects` to the fake)
- Modify: `internal/storage/tigris/refs_test.go:235-241` (`TestPackRefsIsDeliberateNoOp`)
- Create: `internal/storage/tigris/reffold_test.go`

**Interfaces:**
- Consumes: `commitRefs` (Task 5), `refCache` (Task 4).
- Produces:
  - `s3API` gains `DeleteObjects`.
  - `const maxDeleteBatch = 1000`
  - `func (s *Storer) dropFoldedLooseRefs(folded, removed []plumbing.ReferenceName) error` — real implementation
  - `func (s *Storer) PackRefs() error` — folds instead of doing nothing
  - `fakeS3.DeleteObjects`, and `fakeS3.batchDeletes int`

- [ ] **Step 1: Write the failing tests**

Create `internal/storage/tigris/reffold_test.go`:

```go
package tigris

import (
	"errors"
	"strconv"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

// TestFoldMovesLooseRefsIntoPacked pins the one-shot migration: the first
// ref-mutating operation on a repository folds every loose ref into its own
// commit, and then deletes the loose keys.
func TestFoldMovesLooseRefsIntoPacked(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	for i := 0; i < 2500; i++ {
		f.put(refPrefix+"refs/tags/v"+strconv.Itoa(i), headAB+"\n", nil)
	}

	if err := s.SetReference(hashRef("refs/heads/main", headCD)); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Every loose key is gone from the bucket.
	for i := 0; i < 2500; i++ {
		if _, ok := f.objs[refPrefix+"refs/tags/v"+strconv.Itoa(i)]; ok {
			t.Fatalf("loose key for v%d survived the fold", i)
		}
	}

	// And every ref is still readable, out of packed-refs.
	fresh := packedTestStorer(t, f)
	view, err := fresh.refView()
	if err != nil {
		t.Fatalf("refView: %v", err)
	}
	if len(view) != 2501 {
		t.Fatalf("view holds %d refs, want 2501", len(view))
	}
	if got := view[plumbing.ReferenceName("refs/tags/v42")]; got == nil || got.Hash().String() != headAB {
		t.Errorf("folded ref v42 = %v", got)
	}

	// 2500 keys is three DeleteObjects calls at 1000 per call.
	if got := f.nbatchDeletes(); got != 3 {
		t.Errorf("batch deletes = %d, want 3", got)
	}
	if got := f.ndeletes(); got != 0 {
		t.Errorf("the fold used %d single-key deletes; it must batch", got)
	}
}

func TestFoldedRefCountDropsToZero(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	f.put(refPrefix+"refs/heads/main", headAB+"\n", nil)

	before, err := s.CountLooseRefs()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if before != 1 {
		t.Fatalf("loose count before the fold = %d, want 1", before)
	}

	if err := s.PackRefs(); err != nil {
		t.Fatalf("PackRefs: %v", err)
	}

	after, err := packedTestStorer(t, f).CountLooseRefs()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if after != 0 {
		t.Errorf("loose count after the fold = %d, want 0", after)
	}
}

// TestPackRefsIsGated pins that PackRefs stays a no-op while writes are off,
// so release 1 never creates a packed-refs object.
func TestPackRefsIsGated(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := newTestStorer(t, f) // default: off
	f.put(refPrefix+"refs/heads/main", headAB+"\n", nil)

	if err := s.PackRefs(); err != nil {
		t.Fatalf("PackRefs must succeed vacuously when gated, got %v", err)
	}
	if _, ok := f.objs[packedRefsKey]; ok {
		t.Error("PackRefs wrote packed-refs while WithPackedRefs was off")
	}
	if _, ok := f.objs[refPrefix+"refs/heads/main"]; !ok {
		t.Error("PackRefs deleted a loose ref while gated")
	}
}

// TestFoldStoppedBeforeCleanupKeepsRefsReadable is the crash case. The commit
// PUT lands and the delete fails. The loose keys still win under the merge
// rule, which is the correct result, and the next write retries the delete.
// The order is PUT and then DELETE precisely so that this is the failure mode
// rather than a lost ref.
func TestFoldStoppedBeforeCleanupKeepsRefsReadable(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	f.put(refPrefix+"refs/heads/main", headAB+"\n", nil)
	f.batchDelErr = errors.New("delete failed mid-fold")

	err := s.SetReference(hashRef("refs/heads/topic", headCD))
	if err == nil {
		t.Fatal("a failed loose-key delete must surface, not be swallowed")
	}

	// packed-refs holds both refs, and the loose key is still there.
	if _, ok := f.objs[packedRefsKey]; !ok {
		t.Fatal("the commit PUT did not land before the delete was attempted")
	}
	if _, ok := f.objs[refPrefix+"refs/heads/main"]; !ok {
		t.Fatal("the loose key vanished despite the delete failing")
	}

	// Both refs read correctly from a fresh storer, loose winning for main.
	fresh := packedTestStorer(t, f)
	view, verr := fresh.refView()
	if verr != nil {
		t.Fatalf("refView: %v", verr)
	}
	if got := view[plumbing.ReferenceName("refs/heads/main")]; got == nil || got.Hash().String() != headAB {
		t.Errorf("main = %v, want %s", got, headAB)
	}
	if got := view[plumbing.ReferenceName("refs/heads/topic")]; got == nil || got.Hash().String() != headCD {
		t.Errorf("topic = %v, want %s", got, headCD)
	}

	// The next write, with deletes working again, finishes the job.
	f.batchDelErr = nil
	if err := fresh.SetReference(hashRef("refs/heads/third", headAB)); err != nil {
		t.Fatalf("retry write: %v", err)
	}
	if _, ok := f.objs[refPrefix+"refs/heads/main"]; ok {
		t.Error("the retry did not finish deleting the loose key")
	}
}

// TestRemoveDeletesTheLooseKeyToo pins that a removal cannot resurrect from
// the loose layer.
func TestRemoveDeletesTheLooseKeyToo(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	s := packedTestStorer(t, f)
	f.put(refPrefix+"refs/heads/doomed", headAB+"\n", nil)

	if err := s.RemoveReference(plumbing.ReferenceName("refs/heads/doomed")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, ok := f.objs[refPrefix+"refs/heads/doomed"]; ok {
		t.Fatal("the loose key survived the removal and will resurrect the ref")
	}

	fresh := packedTestStorer(t, f)
	if _, err := fresh.Reference(plumbing.ReferenceName("refs/heads/doomed")); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		t.Errorf("removed ref came back: %v", err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestFold|TestPackRefsIsGated|TestRemoveDeletesTheLooseKey|TestFoldedRefCount' ./internal/storage/tigris/`
Expected: FAIL to build, with `f.nbatchDeletes undefined` and `f.batchDelErr undefined`.

- [ ] **Step 3: Add `DeleteObjects` to the interface and the fake**

In `tigris.go`, add to `s3API` (after line 79):

```go
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
```

In `client_test.go`, add two fields to `fakeS3` next to `deletes`:

```go
	batchDeletes int   // DeleteObjects calls
	batchDelErr  error // injected DeleteObjects failure
```

Add the counter next to `ndeletes`:

```go
func (f *fakeS3) nbatchDeletes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batchDeletes
}
```

And the method, next to `DeleteObject`:

```go
// DeleteObjects removes every named key. Real S3 caps a request at 1000 keys
// and reports per-key failures in the response; the fake enforces the cap so a
// caller that ignores it fails a test rather than production, and returns a
// whole-call error for injected failures.
func (f *fakeS3) DeleteObjects(_ context.Context, p *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchDeletes++
	if f.batchDelErr != nil {
		return nil, f.batchDelErr
	}
	if p.Delete == nil {
		return &s3.DeleteObjectsOutput{}, nil
	}
	if n := len(p.Delete.Objects); n > maxDeleteBatch {
		f.t.Fatalf("fake DeleteObjects: %d keys exceeds the %d-key limit", n, maxDeleteBatch)
	}
	for _, o := range p.Delete.Objects {
		delete(f.objs, sv(o.Key))
	}
	return &s3.DeleteObjectsOutput{}, nil
}
```

- [ ] **Step 4: Implement the cleanup**

In `refcache.go`, replace the `dropFoldedLooseRefs` stub:

```go
// maxDeleteBatch is the most keys one DeleteObjects request accepts. It is an
// S3 protocol limit, not a tuning knob.
const maxDeleteBatch = 1000

// dropFoldedLooseRefs deletes the legacy loose keys that the commit just
// superseded: the folded ones, whose values now live in packed-refs, and the
// removed ones, whose values must not exist anywhere.
//
// It runs after the commit PUT, never before. That order is what makes a
// crash safe. Stop between the two and the loose keys still win under the
// merge rule, which is the correct value, and the next write folds and deletes
// them again. Reverse the order and a ref that existed only as a loose key is
// gone for good if the PUT then fails.
//
// A failure here surfaces to the caller rather than being logged and dropped.
// For a removal the reason is hard: a surviving loose key resurrects the ref.
// For a fold it is softer — the refs are all still correct — but a caller that
// sees success has no other way to learn the cleanup is outstanding.
func (s *Storer) dropFoldedLooseRefs(folded, removed []plumbing.ReferenceName) error {
	seen := make(map[plumbing.ReferenceName]struct{}, len(folded)+len(removed))
	keys := make([]string, 0, len(folded)+len(removed))
	for _, n := range append(append([]plumbing.ReferenceName{}, folded...), removed...) {
		if _, dup := seen[n]; dup {
			continue
		}
		seen[n] = struct{}{}
		keys = append(keys, s.prefix+refKey(n))
	}
	if len(keys) == 0 {
		return nil
	}

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
			// The cache says these names are packed and no longer loose, which
			// is now a lie: the keys are still there and will win the merge.
			s.invalidateRefs()
			return fmt.Errorf("tigris: delete %d folded loose refs: %w", len(batch), err)
		}
	}
	return nil
}
```

Add `"github.com/aws/aws-sdk-go-v2/service/s3/types"` to `refcache.go`'s imports.

`bp` is a test helper in `client_test.go` today (`client_test.go:464`). Move it to `tigris.go` next to `sp`, so production code can use it:

```go
func bp(v bool) *bool { return &v }
```

and delete the copy at `client_test.go:464`.

**Note:** removing a ref that has *no* loose key still issues a DeleteObjects for its name. S3 treats deleting an absent key as success, so this is harmless, and checking first would cost a round trip to save one.

- [ ] **Step 5: Give `PackRefs` its meaning**

In `refs.go`, replace `PackRefs` (lines 152-156):

```go
// PackRefs folds the legacy loose refs into packed-refs and deletes them.
//
// It used to be a no-op, on the reasoning that every ref stayed individually
// addressable so compaction bought nothing. Packed refs change that: the fold
// is what turns an advertisement from one GetObject per ref into one for the
// whole repository. Exposing it here gives an operator a way to pay that cost
// on demand instead of on whichever push happens to be first.
//
// It is a no-op while WithPackedRefs is off, so a release that can only read
// the new format never creates it.
func (s *Storer) PackRefs() error {
	if !s.packedRefs {
		return nil
	}
	if err := s.ensureRefsBuilt(); err != nil {
		return err
	}

	s.refs.mu.Lock()
	loose := make([]*plumbing.Reference, 0, len(s.refs.loose))
	for _, r := range s.refs.loose {
		loose = append(loose, r)
	}
	s.refs.mu.Unlock()

	if len(loose) == 0 {
		return nil
	}
	// commitRefs folds the whole loose layer on any write, so handing it the
	// loose refs as sets is enough — and it keeps one commit path rather than
	// two.
	return s.commitRefs(loose, nil, nil)
}
```

- [ ] **Step 6: Replace the obsolete no-op test**

In `refs_test.go`, replace `TestPackRefsIsDeliberateNoOp` (lines 235-241) with:

```go
// TestPackRefsIsANoOpOnAnEmptyRepo keeps the vacuous case pinned. PackRefs is
// no longer a no-op in general — see TestFoldedRefCountDropsToZero and
// TestPackRefsIsGated — but it must still succeed with nothing to fold.
func TestPackRefsIsANoOpOnAnEmptyRepo(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	if err := newTestStorer(t, f, WithPackedRefs(true)).PackRefs(); err != nil {
		t.Errorf("PackRefs must succeed vacuously, got %v", err)
	}
	if _, ok := f.objs[packedRefsKey]; ok {
		t.Error("PackRefs wrote an object with nothing to fold")
	}
}
```

- [ ] **Step 7: Run the tests**

Run: `go test -run 'TestFold|TestPackRefs|TestRemoveDeletesTheLooseKey' ./internal/storage/tigris/ -v`
Expected: PASS, every subtest.

- [ ] **Step 8: Run everything**

Run: `go build ./... && go test ./...`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/storage/tigris/refcache.go internal/storage/tigris/refs.go internal/storage/tigris/tigris.go internal/storage/tigris/client_test.go internal/storage/tigris/reffold_test.go internal/storage/tigris/refs_test.go
git commit -m "feat(storage/tigris): fold legacy loose refs on the first write

The first ref-mutating operation folds every refs/<name> key into its
own commit PUT, then batch-deletes them 1000 at a time. PUT before
DELETE, so a crash between the two leaves the loose keys winning — the
correct value — instead of losing a ref that existed only loose.

PackRefs stops being a no-op and runs the fold on demand.

Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 7: The batch seam in receive-pack

**Files:**
- Modify: `internal/storage/tigris/refs.go` (add `UpdateReferences`)
- Modify: `cmd/objgitd/receivepack.go:287-324` (`updateReferences`)
- Create: `cmd/objgitd/refbatch_test.go`

**Interfaces:**
- Consumes: `commitRefs` (Task 5).
- Produces:
  - `func (s *Storer) UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error`
  - `type refUpdater interface` in `cmd/objgitd`
  - `func updateReferencesBatched(...)` and `func updateReferencesOneByOne(...)`

- [ ] **Step 1: Write the failing test**

Create `cmd/objgitd/refbatch_test.go`:

```go
package main

import (
	"errors"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
)

// batchBase is memBase with a batch ref-update surface on every repository, so
// an end-to-end push takes the refUpdater path. memBase itself hands out a bare
// memory.Storage, which has no such surface and would take the fallback.
type batchBase struct {
	mu    sync.Mutex
	repos map[string]*batchingStorer
}

func newBatchBase() *batchBase { return &batchBase{repos: map[string]*batchingStorer{}} }

func (b *batchBase) Scoped(prefix string) storage.Storer {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.repos[prefix]
	if !ok {
		st = &batchingStorer{Storer: memory.NewStorage()}
		b.repos[prefix] = st
	}
	return st
}

// only returns the single storer this base handed out. Asserting on "the one
// repository" keeps the test independent of how RepoRef.Path formats a prefix.
func (b *batchBase) only(t *testing.T) *batchingStorer {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.repos) != 1 {
		t.Fatalf("want exactly one repository, got %d", len(b.repos))
	}
	for _, st := range b.repos {
		return st
	}
	return nil
}

// batchingStorer is a memory.Storage that also records batch ref updates, so a
// test can see which path updateReferences took and with what.
type batchingStorer struct {
	storage.Storer
	// mu guards the recorded fields. The end-to-end test in this file reads
	// them from the test goroutine while a server goroutine may still be
	// unwinding, which -race notices without it.
	mu      sync.Mutex
	calls   int
	sets    []*plumbing.Reference
	removes []plumbing.ReferenceName
	err     error
}

// record reports what the storer has seen: batch calls, total sets, total
// removes.
func (b *batchingStorer) record() (calls, sets, removes int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls, len(b.sets), len(b.removes)
}

func (b *batchingStorer) UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error {
	b.mu.Lock()
	b.calls++
	b.sets = append(b.sets, sets...)
	b.removes = append(b.removes, removes...)
	failWith := b.err
	b.mu.Unlock()

	if failWith != nil {
		return failWith
	}
	for _, r := range sets {
		if err := b.Storer.SetReference(r); err != nil {
			return err
		}
	}
	for _, n := range removes {
		if err := b.Storer.RemoveReference(n); err != nil {
			return err
		}
	}
	return nil
}

func hashOf(t *testing.T, hex string) plumbing.Hash {
	t.Helper()
	h, ok := plumbing.FromHex(hex)
	if !ok {
		t.Fatalf("bad hex fixture %q", hex)
	}
	return h
}

func TestUpdateReferencesBatchesWhenTheStorerCan(t *testing.T) {
	t.Parallel()

	const (
		hexA = "1111111111111111111111111111111111111111"
		hexB = "2222222222222222222222222222222222222222"
	)

	tests := []struct {
		name        string
		seed        map[string]string // existing refs
		commands    []*packp.Command
		batchErr    error
		wantCalls   int
		wantSets    int
		wantRemoves int
		wantStatus  map[string]error // ref name -> expected status
	}{
		{
			name: "many creates go in one call",
			commands: []*packp.Command{
				{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
				{Name: "refs/tags/v2", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
				{Name: "refs/tags/v3", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
			},
			wantCalls: 1,
			wantSets:  3,
			wantStatus: map[string]error{
				"refs/tags/v1": nil, "refs/tags/v2": nil, "refs/tags/v3": nil,
			},
		},
		{
			name: "creates, updates and deletes share the call",
			seed: map[string]string{"refs/heads/main": hexA, "refs/heads/old": hexA},
			commands: []*packp.Command{
				{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexB)},
				{Name: "refs/heads/main", Old: hashOf(t, hexA), New: hashOf(t, hexB)},
				{Name: "refs/heads/old", Old: hashOf(t, hexA), New: plumbing.ZeroHash},
			},
			wantCalls:   1,
			wantSets:    2,
			wantRemoves: 1,
			wantStatus: map[string]error{
				"refs/tags/v1": nil, "refs/heads/main": nil, "refs/heads/old": nil,
			},
		},
		{
			name: "a rejected command never reaches the batch",
			seed: map[string]string{"refs/heads/main": hexA},
			commands: []*packp.Command{
				// Create over an existing ref: rejected before the batch.
				{Name: "refs/heads/main", Old: plumbing.ZeroHash, New: hashOf(t, hexB)},
				{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
			},
			wantCalls: 1,
			wantSets:  1,
			wantStatus: map[string]error{
				"refs/heads/main": transport.ErrUpdateReference,
				"refs/tags/v1":    nil,
			},
		},
		{
			// The behavior change this task introduces. One PutObject means the
			// batch is all-or-nothing, so a commit failure fails every command
			// in the push rather than the first N succeeding.
			name: "a failed commit fails every command in the batch",
			commands: []*packp.Command{
				{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
				{Name: "refs/tags/v2", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
			},
			batchErr:  errors.New("commit refused"),
			wantCalls: 1,
			wantSets:  2,
			wantStatus: map[string]error{
				"refs/tags/v1": errors.New("commit refused"),
				"refs/tags/v2": errors.New("commit refused"),
			},
		},
		{
			name:       "nothing staged makes no call at all",
			seed:       map[string]string{"refs/heads/main": hexA},
			commands:   []*packp.Command{{Name: "refs/heads/main", Old: plumbing.ZeroHash, New: hashOf(t, hexB)}},
			wantCalls:  0,
			wantStatus: map[string]error{"refs/heads/main": transport.ErrUpdateReference},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mem := memory.NewStorage()
			for name, hex := range tt.seed {
				ref := plumbing.NewHashReference(plumbing.ReferenceName(name), hashOf(t, hex))
				if err := mem.SetReference(ref); err != nil {
					t.Fatalf("seed %s: %v", name, err)
				}
			}
			st := &batchingStorer{Storer: mem, err: tt.batchErr}

			cmdStatus := map[plumbing.ReferenceName]error{}
			var firstErr error
			updateReferences(st, &packp.UpdateRequests{Commands: tt.commands}, cmdStatus, &firstErr)

			calls, sets, removes := st.record()
			if calls != tt.wantCalls {
				t.Errorf("UpdateReferences calls = %d, want %d", calls, tt.wantCalls)
			}
			if sets != tt.wantSets {
				t.Errorf("batched sets = %d, want %d", sets, tt.wantSets)
			}
			if removes != tt.wantRemoves {
				t.Errorf("batched removes = %d, want %d", removes, tt.wantRemoves)
			}
			for name, want := range tt.wantStatus {
				got, ok := cmdStatus[plumbing.ReferenceName(name)]
				if !ok {
					t.Errorf("no status recorded for %s", name)
					continue
				}
				switch {
				case want == nil && got != nil:
					t.Errorf("%s: want success, got %v", name, got)
				case want != nil && got == nil:
					t.Errorf("%s: want %v, got success", name, want)
				case want != nil && got != nil && !errors.Is(got, want) && got.Error() != want.Error():
					t.Errorf("%s: want %v, got %v", name, want, got)
				}
			}
		})
	}
}

// TestUpdateReferencesFallsBackWithoutTheInterface pins that a storer with no
// batch surface still works, one ref at a time.
func TestUpdateReferencesFallsBackWithoutTheInterface(t *testing.T) {
	t.Parallel()

	const hexA = "1111111111111111111111111111111111111111"
	mem := memory.NewStorage()

	cmdStatus := map[plumbing.ReferenceName]error{}
	var firstErr error
	updateReferences(mem, &packp.UpdateRequests{Commands: []*packp.Command{
		{Name: "refs/tags/v1", Old: plumbing.ZeroHash, New: hashOf(t, hexA)},
	}}, cmdStatus, &firstErr)

	if firstErr != nil {
		t.Fatalf("fallback path errored: %v", firstErr)
	}
	if _, err := mem.Reference(plumbing.ReferenceName("refs/tags/v1")); err != nil {
		t.Errorf("fallback did not write the ref: %v", err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -run 'TestUpdateReferences' ./cmd/objgitd/`
Expected: FAIL. `updateReferences` ignores the interface, so `st.calls` is 0 where the test wants 1.

- [ ] **Step 3: Add `UpdateReferences` to the storer**

In `internal/storage/tigris/refs.go`, next to `SetReference`:

```go
// UpdateReferences applies a whole batch of ref mutations at once. With
// WithPackedRefs set that is one conditional PutObject for the entire batch,
// which is the difference between one round trip and one per ref on a push of
// many tags.
//
// The batch is all-or-nothing: the single commit PUT either lands or it does
// not. cmd/objgitd/receivepack.go relies on that, and reports one shared error
// for every command in a failed push.
//
// Without WithPackedRefs it walks the loose path, so the method is always safe
// to call and the transport never needs to know which mode it is in.
func (s *Storer) UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error {
	if s.packedRefs {
		return s.commitRefs(sets, removes, nil)
	}
	for _, r := range sets {
		if err := s.setLooseReference(r); err != nil {
			return err
		}
	}
	for _, n := range removes {
		if err := s.RemoveReference(n); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Split `updateReferences`**

In `cmd/objgitd/receivepack.go`, replace `updateReferences` (lines 287-324) with:

```go
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
```

- [ ] **Step 4a: Add the end-to-end call-count test**

Append to `internal/storage/tigris/refcommit_test.go`:

```go
// TestUpdateReferencesCostsOnePut is the storer-side half of the headline
// claim, matching TestRefViewCostsTwoCalls on the read side.
func TestUpdateReferencesCostsOnePut(t *testing.T) {
	t.Parallel()

	f := newFakeS3(t)
	obs, snapshot := countingObserver()
	s := packedTestStorer(t, f, obs)

	sets := make([]*plumbing.Reference, 0, 500)
	for i := 0; i < 500; i++ {
		sets = append(sets, hashRef("refs/tags/v"+strconv.Itoa(i), headAB))
	}
	if err := s.UpdateReferences(sets, nil); err != nil {
		t.Fatalf("UpdateReferences: %v", err)
	}

	seen := snapshot()
	if seen["PutObject"] != 1 {
		t.Errorf("PutObject calls = %d, want 1 (map: %v)", seen["PutObject"], seen)
	}
	if seen["GetObject"] != 1 {
		t.Errorf("GetObject calls = %d, want 1 (map: %v)", seen["GetObject"], seen)
	}
}
```

- [ ] **Step 4b: Add the end-to-end push test**

The unit test above proves `updateReferences` batches. This proves a real git
client sends one push's commands as one `UpdateRequests`, so the seam actually
fires end to end rather than being bypassed by the transport.

It deliberately does **not** assert S3 call counts. `cmd/objgitd` tests run on
`memory.Storage`, and the fake bucket is an unexported test type inside
`internal/storage/tigris`. Call counts live where the fake lives:
`TestUpdateReferencesCostsOnePut` and `TestRefViewCostsTwoCalls`.

Append to `cmd/objgitd/refbatch_test.go`:

```go
// TestPushOfManyTagsBatchesEndToEnd drives a real git client through Smart
// HTTP and asserts the whole push arrived as one batch.
func TestPushOfManyTagsBatchesEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	bb := newBatchBase()
	ts := httptest.NewServer((&daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: bb},
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}).httpHandler())
	t.Cleanup(ts.Close)

	const tags = 200
	work := seedRepo(t)
	for i := 0; i < tags; i++ {
		runGit(t, work, "tag", "v"+strconv.Itoa(i))
	}
	runGit(t, work, "remote", "add", "origin", ts.URL+"/acme/widgets.git")
	runGit(t, work, "push", "--tags", "origin", "main")

	calls, sets, removes := bb.only(t).record()
	if calls != 1 {
		t.Errorf("UpdateReferences calls = %d, want 1: git split the push, or the seam was bypassed", calls)
	}
	// 200 tags plus refs/heads/main.
	if sets != tags+1 {
		t.Errorf("batched sets = %d, want %d", sets, tags+1)
	}
	if removes != 0 {
		t.Errorf("batched removes = %d, want 0", removes)
	}

	// And the refs really landed, not just got counted.
	st := bb.only(t)
	for _, name := range []string{"refs/heads/main", "refs/tags/v0", "refs/tags/v199"} {
		if _, err := st.Reference(plumbing.ReferenceName(name)); err != nil {
			t.Errorf("ref %s did not land: %v", name, err)
		}
	}
}
```

The imports `refbatch_test.go` needs, on top of those in Step 1: `net/http/httptest`,
`os/exec`, `strconv`, `sync`, `github.com/go-git/go-billy/v6/memfs`, and the
repository's own `internal/auth` and `internal/repofs`. Copy the exact `memfs`
and `auth` import paths from the top of `cmd/objgitd/http_test.go` rather than
guessing the major version.

**Note:** repository initialization writes `HEAD` through `SetReference`, not
`UpdateReferences`, so it does not add to `calls`.

- [ ] **Step 5: Run the tests**

Run: `go test -run 'TestUpdateReferences|TestPushOfManyTags' ./cmd/objgitd/ ./internal/storage/tigris/ -v`
Expected: PASS, every subtest. Confirm `TestPushOfManyTagsBatchesEndToEnd` did
not skip — it needs `git` on PATH.

- [ ] **Step 5a: Run the batch tests under the race detector**

Run: `go test -race -run 'TestUpdateReferences|TestPushOfManyTags' ./cmd/objgitd/`
Expected: PASS with no race reports. `batchingStorer` is read from the test
goroutine while a server goroutine may still be unwinding, which is why it
carries a mutex.

- [ ] **Step 6: Run everything, including the protocol tests**

Run: `go build ./... && go test ./...`
Expected: PASS. The protocol tests need `git` on PATH and skip themselves without it — confirm with `which git` that they actually ran.

- [ ] **Step 7: Commit**

```bash
git add internal/storage/tigris/refs.go internal/storage/tigris/refcommit_test.go cmd/objgitd/receivepack.go cmd/objgitd/refbatch_test.go
git commit -m "feat(objgitd): hand a whole push's ref updates over in one call

updateReferences now validates every command against the storer, then
applies the batch through the optional refUpdater interface. Validation
got nearly free on the way: the tigris storer answers every existence
check out of its per-request ref cache instead of a GetObject.

The batch is all-or-nothing, so a commit failure fails every command in
the push. That is git push --atomic semantics, and it is a change from
the first-N-succeed behavior of the one-by-one path, which is kept for
storers with no batch surface.

Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

### Task 8: Documentation, and the release-2 flip

This task writes down what changed and prepares the flip. **It does not flip the default.** That is release 2's commit, made once the fleet runs a binary that can read packed refs.

**Files:**
- Modify: `docs/architecture/tigris-storer.md`
- Modify: `docs/architecture/transports.md`
- Modify: `internal/storage/tigris/tigris.go:6-21` (the package doc's layout diagram)
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: everything above.
- Produces: no code.

- [ ] **Step 1: Update the package doc's layout list**

In `internal/storage/tigris/tigris.go`, the layout comment at lines 9-20 lists the bucket keys. Add `packed-refs` and mark `refs/` as legacy:

```go
//	objects/<hex>       loose object keyed by content hash; user metadata
//	                    carries the git type (git-type) and size (git-size)
//	packed-refs         every ref in one object, under a compare-and-swap; see
//	                    packedrefs.go for the format and refcache.go for the
//	                    read and write paths
//	refs/<name>         one legacy loose ref (hash hex, or "ref: target" for
//	                    symbolics). Read-only: the first packed write folds
//	                    these in and deletes them. A loose ref still wins over
//	                    a packed one — refView explains why.
//	shallow             newline separated commit hashes
```

- [ ] **Step 2: Document the layout in the architecture page**

Read `docs/architecture/tigris-storer.md` first, and match its existing headings and voice. Add a section covering:

- The two keys and their roles.
- The format: versioned header, sorted `<name>\t<value>\n` text, optionally zstd.
- The read path: one `GetObject` plus one `ListObjectsV2`, memoized per request by `refCache`.
- The merge rule, and the invariant that makes it sound. This is the part a future reader most needs, so state the invariant before the rule.
- The write path: one conditional `PutObject` as the commit point, retry on 412.
- `ErrRefContention` against `storage.ErrReferenceHasChanged`, and why they are separate.
- The fold, the PUT-before-DELETE order, and what a crash between them leaves.
- `PackRefs` and `CountLooseRefs` and their new meanings.
- Peeled tags as a known gap, with the reserved third field.
- **CAUTION: this design needs a Single-region or Multi-region bucket.** A Global or Dual-region bucket reads eventually, so a compare-and-swap can evaluate against a stale read. Link <https://www.tigrisdata.com/docs/concepts/consistency/>.

Write it with the `simple-english` skill: sentences under 25 words, conditions before commands, no `should`, `may`, `might`, or `could`.

- [ ] **Step 3: Document the atomicity change**

In `docs/architecture/transports.md`, add to the protocol-points section: a push's ref updates are now all-or-nothing when the storer offers `refUpdater`. On failure every command in the push reports the same error. `report-status` permits this, and it matches `git push --atomic`.

- [ ] **Step 4: Point `AGENTS.md` at the new files**

`AGENTS.md` keeps a "Where the code lives" table. `internal/storage/tigris` already has a row, so no new row is needed. In the architecture table, `tigris-storer.md`'s "Read it before you change..." cell currently reads "Object layout, packs, the pack cache, or the upload path." Change it to "Object layout, refs, packs, the pack cache, or the upload path."

While in the file, correct one stale fact found during planning. `AGENTS.md` states `Module path: tangled.org/xeiaso.net/objgit`. `go.mod` says `github.com/tigrisdata/objgit`, and every import in the tree uses that. Change the line to:

```markdown
Module path: `github.com/tigrisdata/objgit`. Go 1.26.
```

- [ ] **Step 5: Record how to flip the default**

Append to `docs/architecture/tigris-storer.md`, in the section from Step 2:

```markdown
### Turning packed-ref writes on

`WithPackedRefs` is off by default. Reading packed refs is not gated, so
every binary that carries this code can already read the format.

Flip the default only when every node runs a binary that can read packed
refs. Two steps, in one commit:

1. In `New` (`internal/storage/tigris/tigris.go`), change the `packedRefs`
   field's initial value to `true`.
2. In `internal/storage/tigris/refcommit_test.go`, delete
   `TestPackedWritesAreGated` and add its opposite: a default storer writes
   `packed-refs` and not a loose key.

CAUTION: Do not flip the default and change the format in one release. A
rollback then has no binary that can read what the window wrote.
```

- [ ] **Step 6: Confirm the docs build and the suite is green**

Run: `go build ./... && go test ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add docs/architecture/tigris-storer.md docs/architecture/transports.md internal/storage/tigris/tigris.go AGENTS.md
git commit -m "docs: describe the packed-refs layout and the atomicity change

Records the two keys, the merge rule and the invariant it rests on, the
compare-and-swap write path, the one-shot fold, and the bucket-type
constraint conditional writes carry. Also writes down how to flip
WithPackedRefs on once the fleet can read the format.

Signed-off-by: Xe Iaso <xe@tigrisdata.com>"
```

---

## Verification checklist

Run after Task 8. Every line must hold.

- [ ] `go build ./...` succeeds.
- [ ] `go test ./...` passes, with `git` on PATH so the protocol tests do not skip.
- [ ] `go test -race ./internal/storage/tigris/ ./cmd/objgitd/` passes. `TestConcurrentCommitsAllLand` and the end-to-end push test are the two that need it.
- [ ] `go vet ./...` is clean.
- [ ] `OBJGIT_TIGRIS_LIVE_BUCKET=<bucket> go test -run 'TestLiveBucket' ./internal/storage/tigris/ -v` passes against a real single-region or multi-region bucket.
- [ ] `TestRefViewCostsTwoCalls` and `TestUpdateReferencesCostsOnePut` both pass. These two are the change.
- [ ] `grep -rn 'packedRefs: *true' internal/storage/tigris/tigris.go` finds nothing. The default must still be off.
- [ ] `grep -rn '"error"' internal/storage/tigris/ internal/metrics/` finds no `slog` key. The key is `"err"`.
- [ ] A manual push of many tags against a live bucket shows one `PutObject` in the `objgit_s3_requests_total` metric per push, and `objgit_ref_cas_retries_total` stays at 0 with one client pushing.
