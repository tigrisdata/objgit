package tigris

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Payload codecs recorded per object in a .cue record's flags byte. A v1 cue
// predates codecs entirely, so parseCue reports every v1 record as codecRaw.
const (
	codecRaw  uint8 = 0
	codecZstd uint8 = 1
)

// The compression policy, measured against a real source repository — see
// docs/superpowers/specs/2026-08-27-cue-compression-design.md for the numbers
// behind each of these.
const (
	// compressionFloor is the object size below which nothing is compressed,
	// with no probe and no buffering at all. It sits on the measured knee: a
	// 2 KiB floor recovers 58.3% of a source repository's object bytes, where
	// 64 KiB recovers only 8.1%, and dropping to 512 B buys just 1.9 points
	// more. Below 512 B compression is actively pointless — objects there
	// average 1.07x and fewer than half of them come out smaller at all,
	// because git trees are mostly incompressible hash bytes.
	compressionFloor = 2 << 10 // 2 KiB

	// inMemoryCap is the largest object decided *exactly*, by compressing it
	// and keeping whichever form is smaller. Nearly every object in a
	// repository lands under this, so the common path never mispredicts and
	// never needs the rewind in packSegment.add.
	inMemoryCap = 1 << 20 // 1 MiB

	// probeWindow is how much of an object larger than inMemoryCap gets
	// compressed to decide the whole object's fate. The point is to avoid
	// compressing 500 MiB of video to discover it is incompressible.
	probeWindow = 64 << 10 // 64 KiB

	// minGainBytes is the smallest saving worth storing compressed. A zstd
	// frame header plus content checksum runs 13-17 bytes, so this keeps a
	// trivial win from buying a codec branch on every future read. There is
	// deliberately no percentage margin: decode costs single-digit
	// microseconds against a 10-50ms ranged GET, so decode cost is not a
	// reason to refuse a real saving.
	minGainBytes = 64

	// probeMinGainRatio is the reciprocal of the shrinkage a head window must
	// show for the whole object to be worth compressing: 1/32 is a little over
	// 3%. Low on purpose — the probe only needs to separate "this content
	// compresses" from "this is already compressed", and zstd leaves
	// incompressible data essentially the same size, so the two are far apart.
	probeMinGainRatio = 32

	// decodeHintCap bounds the capacity hint used when decompressing a payload.
	// The real decoded size comes from a .cue record, which is data read back
	// out of the bucket, so it is a hint and not a promise.
	decodeHintCap = 8 << 20 // 8 MiB
)

// Decode bounds. zstd defaults to 64 GiB, which is no bound at all: a few
// kilobytes of corrupt or hostile input could exhaust memory long before any
// length check ran. Both values are far above legitimate use, and exist only
// to turn an out-of-memory kill into an error.
//
// Variables rather than constants so a test can lower them; treat them as
// constants everywhere else.
var (
	// cueMaxDecoded bounds a .cue record block. Only maxPackBytes (128 MiB of
	// payload) bounds a container, and nothing bounds its object count, so the
	// block scales with how small the objects in it are: at 90 bytes per record
	// this admits a container whose objects average 45 bytes apiece. No real
	// repository is anywhere near that, and every honest container is orders of
	// magnitude under the bound.
	cueMaxDecoded uint64 = 256 << 20 // 256 MiB

	// payloadMaxDecoded bounds one object's payload. Generous, because a git
	// object legitimately can be large, and decodeBody already buffers the
	// whole thing. Its job is to cap *amplification*: a small stored payload
	// must not be able to claim gigabytes.
	payloadMaxDecoded uint64 = 1 << 30 // 1 GiB
)

// One encoder and two decoders for the whole process. EncodeAll and DecodeAll
// are both safe for concurrent use, and constructing either per call is the
// expensive mistake here — a zstd.Decoder allocates window buffers per
// instance. Cue blocks and payloads get separate decoders only because their
// size bounds differ by orders of magnitude.
var (
	zstdEnc     *zstd.Encoder
	zstdEncOnce sync.Once
	cueDec      *zstd.Decoder
	cueDecOnce  sync.Once
	payloadDec  *zstd.Decoder
	payloadOnce sync.Once
)

// encoderConcurrency caps how many EncodeAll calls the encoder() singleton
// serves at once. zstd.NewWriter defaults WithEncoderConcurrency to GOMAXPROCS,
// and every internal encoder state allocates its own match-history buffer (tens
// of megabytes, never released), so the default puts a permanent floor under
// retained heap that scales with core count and not with real demand. The value
// that matters is "expected simultaneous pushes", not "core count"; 4 matches
// deltaScanWorkers in spirit — a bound picked for resource reasons. Revisit it
// alongside the push cap that actually bounds demand.
//
// A package-level variable with a setter, not a Storer option: the encoder is a
// process-wide singleton, so SetEncoderConcurrency must be called from main
// before the first encode. Reads are not synchronized because the setter runs
// once at startup, before any EncodeAll.
var encoderConcurrency = 4

// SetEncoderConcurrency sets how many EncodeAll calls the encoder() singleton
// serves at once. Call it once from main before the first encode; values below
// 1 are clamped to 1.
func SetEncoderConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	encoderConcurrency = n
}

func encoder() *zstd.Encoder {
	zstdEncOnce.Do(func() {
		// Errors here are impossible with a nil writer and valid options; the
		// option list is a compile-time constant.
		zstdEnc, _ = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(encoderConcurrency),
		)
	})
	return zstdEnc
}

// streamEncPool holds *zstd.Encoder values dedicated to copyCompressed's
// streaming path (packwriter.go), separate from the encoder() singleton
// above: that singleton serves concurrent EncodeAll calls and must never be
// Reset out from under them, while a streaming copy owns its encoder for the
// duration of one object and can safely Reset it at a new io.Writer for the
// next. Pooling avoids paying for a fresh match-history buffer — tens of
// megabytes, sized off the encoder's window — on every large object.
var streamEncPool = sync.Pool{
	New: func() any {
		// Same error-impossibility reasoning as encoder() above.
		//
		// WithEncoderConcurrency(1) because a streaming copy owns its encoder for
		// the duration of one object, as the comment above states: the extra
		// states a default GOMAXPROCS encoder would allocate can never be used
		// concurrently by a single owner, so they are pure retained-heap floor.
		zw, _ := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1),
		)
		return zw
	},
}

func cueDecoder() *zstd.Decoder {
	cueDecOnce.Do(func() {
		cueDec, _ = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(cueMaxDecoded))
	})
	return cueDec
}

func payloadDecoder() *zstd.Decoder {
	payloadOnce.Do(func() {
		payloadDec, _ = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(payloadMaxDecoded))
	})
	return payloadDec
}

// resetCueDecoder rebuilds the cue decoder so a changed cueMaxDecoded takes
// effect. Test-only: the bound is fixed in production.
func resetCueDecoder() {
	cueDec = nil
	cueDecOnce = sync.Once{}
}

// codecName labels a codec for the payload observer, and thus for the metric
// behind it.
func codecName(codec uint8) string {
	if codec == codecZstd {
		return "zstd"
	}
	return "raw"
}

// worthStoring reports whether a compressed form beats the raw form by enough
// to be worth a codec branch on every future read. The final say for a .cue's
// record block, for an object decided exactly, and for an object decided by
// probe once its real stored size is known.
func worthStoring(compressed, raw int) bool {
	return compressed+minGainBytes <= raw
}

// probeWins judges a head window, and deliberately tests a *ratio* where
// worthStoring tests absolute bytes. A probe is an estimate of how the rest of
// the object will behave, so an absolute byte floor makes no sense against it —
// applied to a small window it can be unsatisfiable no matter how well the
// content compresses. The absolute rule still holds for the object as a whole:
// writeProbed re-checks with worthStoring and rewinds if the estimate did not
// pay off.
func probeWins(compressed, head int) bool {
	if head == 0 {
		return false
	}
	return compressed <= head-head/probeMinGainRatio
}

// compressBlock returns the zstd form of b when worthStoring says so, and
// reports whether it chose to compress.
func compressBlock(b []byte) ([]byte, bool) {
	if len(b) == 0 {
		return b, false
	}
	out := encoder().EncodeAll(b, nil)
	if !worthStoring(len(out), len(b)) {
		return b, false
	}
	return out, true
}
