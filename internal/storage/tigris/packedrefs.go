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
// object. A v1 reader refuses such a line rather than misreading it: the cut in
// decodePackedRefs splits on the first tab only, so the extra field lands
// inside the value and decodeRefValue rejects it. That is the right direction
// to fail, and it is why adding the field is a version bump and not a new key.
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
