package tigris

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
)

// cachePayload returns the pack id (hex sha256, as packwriter.go names them)
// for body plus a fetch function that serves it, along with a counter of how
// many times that fetch actually ran.
func cachePayload(body []byte) (id string, fetch func(io.Writer) error, calls *atomic.Int64) {
	sum := sha256.Sum256(body)
	calls = &atomic.Int64{}
	return hex.EncodeToString(sum[:]), func(w io.Writer) error {
		calls.Add(1)
		_, err := w.Write(body)
		return err
	}, calls
}

func newTestPackCache(t *testing.T, maxBytes int64) *PackCache {
	t.Helper()
	c, err := NewPackCache(t.TempDir(), maxBytes)
	if err != nil {
		t.Fatalf("NewPackCache: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Cleanup(); err != nil {
			t.Errorf("Cleanup: %v", err)
		}
	})
	return c
}

// readAll drains f from the start and closes it.
func readAll(t *testing.T, f *os.File) []byte {
	t.Helper()
	defer f.Close()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return b
}

// countCacheFiles reports how many files sit in the cache directory. A failed
// download must leave none: neither a staged temp file nor a partial pack.
func countCacheFiles(t *testing.T, c *PackCache) int {
	t.Helper()
	ents, err := os.ReadDir(c.dir)
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	return len(ents)
}

// TestPackCacheDownloadsOnce is the whole point of the cache: a second request
// for a pack already on disk must not go back to the network.
func TestPackCacheDownloadsOnce(t *testing.T) {
	t.Parallel()

	body := bytes.Repeat([]byte("pack payload\n"), 64)
	id, fetch, calls := cachePayload(body)
	c := newTestPackCache(t, 0)

	for i := 0; i < 3; i++ {
		f, err := c.Get(id, fetch)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if got := readAll(t, f); !bytes.Equal(got, body) {
			t.Fatalf("Get %d returned %d bytes, want %d", i, len(got), len(body))
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fetch ran %d times, want 1", got)
	}
}

// TestPackCacheVerifiesChecksum pins the guarantee that makes sharing one
// cache across repositories safe: a body whose sha256 disagrees with the id it
// was filed under is refused, leaves nothing behind, and is retried rather
// than remembered as a failure.
func TestPackCacheVerifiesChecksum(t *testing.T) {
	t.Parallel()

	c := newTestPackCache(t, 0)
	realID, _, _ := cachePayload([]byte("the bytes this id names"))

	var calls atomic.Int64
	liar := func(w io.Writer) error {
		calls.Add(1)
		_, err := w.Write([]byte("entirely different bytes"))
		return err
	}

	for i := 0; i < 2; i++ {
		f, err := c.Get(realID, liar)
		if err == nil {
			f.Close()
			t.Fatalf("Get %d accepted a body that does not hash to its id", i)
		}
		if !strings.Contains(err.Error(), "failed checksum") {
			t.Fatalf("Get %d error = %v, want a checksum complaint", i, err)
		}
	}
	// Retried, not cached as a failure: a transient bad read stays recoverable.
	if got := calls.Load(); got != 2 {
		t.Errorf("fetch ran %d times, want 2 (a failure must not be cached)", got)
	}
	if n := countCacheFiles(t, c); n != 0 {
		t.Errorf("%d files left in the cache directory after a failed download, want 0", n)
	}
}

// TestPackCacheFetchErrorIsNotCached covers the same retry posture for a fetch
// that fails outright rather than returning wrong bytes.
func TestPackCacheFetchErrorIsNotCached(t *testing.T) {
	t.Parallel()

	c := newTestPackCache(t, 0)
	body := []byte("eventually available")
	id, good, _ := cachePayload(body)

	boom := fmt.Errorf("network is down")
	if _, err := c.Get(id, func(io.Writer) error { return boom }); err != boom {
		t.Fatalf("Get error = %v, want %v", err, boom)
	}

	f, err := c.Get(id, good)
	if err != nil {
		t.Fatalf("Get after a failure: %v", err)
	}
	if got := readAll(t, f); !bytes.Equal(got, body) {
		t.Error("retry after a failed fetch returned the wrong bytes")
	}
}

// TestPackCacheEvictsLRU verifies the budget is enforced least-recently-used:
// re-reading a pack protects it, and the untouched one is what gets dropped.
func TestPackCacheEvictsLRU(t *testing.T) {
	t.Parallel()

	const size = 512
	bodyA := bytes.Repeat([]byte("a"), size)
	bodyB := bytes.Repeat([]byte("b"), size)
	bodyC := bytes.Repeat([]byte("c"), size)

	idA, fetchA, callsA := cachePayload(bodyA)
	idB, fetchB, callsB := cachePayload(bodyB)
	idC, fetchC, callsC := cachePayload(bodyC)

	c := newTestPackCache(t, 2*size) // room for exactly two packs

	get := func(id string, fetch func(io.Writer) error) {
		t.Helper()
		f, err := c.Get(id, fetch)
		if err != nil {
			t.Fatalf("Get %s: %v", id[:8], err)
		}
		f.Close()
	}

	get(idA, fetchA)
	get(idB, fetchB)
	get(idA, fetchA) // touch A so B becomes the least recently used
	get(idC, fetchC) // over budget: evicts B

	if got := callsA.Load(); got != 1 {
		t.Errorf("pack A fetched %d times, want 1 (it was touched most recently)", got)
	}
	if got := callsC.Load(); got != 1 {
		t.Errorf("pack C fetched %d times, want 1", got)
	}

	get(idA, fetchA)
	if got := callsA.Load(); got != 1 {
		t.Errorf("pack A fetched %d times after eviction of B, want 1 — A should have survived", got)
	}

	get(idB, fetchB)
	if got := callsB.Load(); got != 2 {
		t.Errorf("pack B fetched %d times, want 2 — B was the least recently used and should have been evicted", got)
	}
}

// TestPackCacheEvictionSpareOpenReaders pins the reason no reference counting
// appears in packcache.go: eviction unlinks, and a descriptor handed out
// earlier keeps serving reads afterwards. A clone in flight must never break
// because another request pushed the cache over its budget.
func TestPackCacheEvictionSpareOpenReaders(t *testing.T) {
	t.Parallel()

	const size = 256
	bodyA := bytes.Repeat([]byte("a"), size)
	bodyB := bytes.Repeat([]byte("b"), size)
	idA, fetchA, _ := cachePayload(bodyA)
	idB, fetchB, _ := cachePayload(bodyB)

	c := newTestPackCache(t, size) // room for exactly one pack

	held, err := c.Get(idA, fetchA)
	if err != nil {
		t.Fatalf("Get A: %v", err)
	}
	defer held.Close()

	f, err := c.Get(idB, fetchB) // evicts A out from under the open handle
	if err != nil {
		t.Fatalf("Get B: %v", err)
	}
	f.Close()

	if _, err := os.Stat(filepath.Join(c.dir, idA+binSuffix)); !os.IsNotExist(err) {
		t.Errorf("pack A still on disk after eviction (stat err = %v)", err)
	}
	if got := readAll(t, held); !bytes.Equal(got, bodyA) {
		t.Error("the descriptor held across an eviction stopped returning pack A's bytes")
	}
}

// TestPackCacheConcurrentGetDownloadsOnce covers the singleflight: several
// clones of the same repository arriving together must produce one download,
// not one each.
func TestPackCacheConcurrentGetDownloadsOnce(t *testing.T) {
	t.Parallel()

	body := bytes.Repeat([]byte("concurrent\n"), 128)
	id, fetch, calls := cachePayload(body)
	c := newTestPackCache(t, 0)

	const readers = 8
	var wg sync.WaitGroup
	errs := make([]error, readers)
	bodies := make([][]byte, readers)
	start := make(chan struct{})

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			f, err := c.Get(id, fetch)
			if err != nil {
				errs[i] = err
				return
			}
			defer f.Close()
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				errs[i] = err
				return
			}
			bodies[i], errs[i] = io.ReadAll(f)
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("reader %d: %v", i, err)
			continue
		}
		if !bytes.Equal(bodies[i], body) {
			t.Errorf("reader %d got %d bytes, want %d", i, len(bodies[i]), len(body))
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("%d concurrent readers caused %d downloads, want 1", readers, got)
	}
}

func TestPackCacheCleanupRemovesTheDirectory(t *testing.T) {
	t.Parallel()

	c, err := NewPackCache(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewPackCache: %v", err)
	}
	body := []byte("something to leave behind")
	id, fetch, _ := cachePayload(body)
	f, err := c.Get(id, fetch)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if err := c.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if _, err := os.Stat(c.dir); !os.IsNotExist(err) {
		t.Errorf("cache directory survived Cleanup (stat err = %v)", err)
	}
	// Descriptors handed out before shutdown keep working, so an in-flight
	// request is never truncated by the cleanup.
	if got := readAll(t, f); !bytes.Equal(got, body) {
		t.Error("a descriptor opened before Cleanup stopped returning the pack's bytes")
	}

	if err := (*PackCache)(nil).Cleanup(); err != nil {
		t.Errorf("Cleanup on a nil cache: %v", err)
	}
}

// TestPackCacheSharedAcrossStorers is the end-to-end payoff: a second Storer
// over the same bucket — a second clone, in production — crosses the bulk
// threshold and downloads nothing, because the first one's copy is still on
// disk. Without a cache the same sequence costs one full .bin GET each.
func TestPackCacheSharedAcrossStorers(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	fx := buildPackFixture(t, 40, false)
	blobs := blobHashes(fx)

	// readAndSettle does one packed read through s — which is all it takes to
	// start the container's download — and waits for that download to land.
	readAndSettle := func(s *Storer, id string) {
		t.Helper()
		obj, err := s.EncodedObject(plumbing.AnyObject, blobs[0])
		if err != nil {
			t.Fatalf("read %s: %v", blobs[0], err)
		}
		if want := fx.byHash[blobs[0]]; obj.Size() != want.size {
			t.Fatalf("read %s: size %d, want %d", blobs[0], obj.Size(), want.size)
		}
		waitPackFetch(t, s, id)
	}

	f := newFakeS3(t)
	cache := newTestPackCache(t, 0)
	writer := newTestStorer(t, f, WithPackCache(cache))
	writePack(t, writer, fx)
	if err := writer.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	id, _ := onlyPack(t, writer, f)

	// First reader: cold cache, so it pays for the download.
	readAndSettle(newTestStorer(t, f, WithPackCache(cache)), id)
	if got := f.nfullBinGets(); got != 1 {
		t.Fatalf("first reader caused %d whole-pack GETs, want 1", got)
	}

	// Second reader: separate Storer, separate pack index, same process-wide
	// cache. It must serve the bulk tier off local disk.
	readAndSettle(newTestStorer(t, f, WithPackCache(cache)), id)
	if got := f.nfullBinGets(); got != 1 {
		t.Errorf("second reader caused %d whole-pack GETs in total, want 1 — the cache did not hold", got)
	}

	// Control: with no cache, the same second read pays again.
	f2 := newFakeS3(t)
	uncachedWriter := newTestStorer(t, f2)
	writePack(t, uncachedWriter, fx)
	if err := uncachedWriter.up.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	readAndSettle(newTestStorer(t, f2), id)
	readAndSettle(newTestStorer(t, f2), id)
	if got := f2.nfullBinGets(); got != 2 {
		t.Errorf("without a cache two readers caused %d whole-pack GETs, want 2", got)
	}
}
