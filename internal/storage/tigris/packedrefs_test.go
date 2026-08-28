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
