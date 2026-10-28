package tigris

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
)

// visibilityClient separates successful uploads from read visibility. It is
// deliberately not the default fake: supported buckets must preserve ordering.
type visibilityClient struct {
	s3API
	hide atomic.Bool
	mode string
}

func (c *visibilityClient) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	out, err := c.s3API.ListObjectsV2(ctx, in, opts...)
	if err == nil && c.hide.Load() && c.mode == "list" {
		entries := out.Contents[:0]
		for _, entry := range out.Contents {
			if !strings.HasSuffix(sv(entry.Key), cueSuffix) {
				entries = append(entries, entry)
			}
		}
		out.Contents = entries
	}
	return out, err
}

func (c *visibilityClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if c.hide.Load() && strings.HasSuffix(sv(in.Key), "."+c.mode) {
		return nil, missingKeyErr()
	}
	return c.s3API.GetObject(ctx, in, opts...)
}

func TestPackVisibilityBoundaries(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	fx := buildPackFixture(t, 3, false)
	for _, tt := range []struct{ name string }{{"list"}, {"cue"}, {"bin"}} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3(t)
			writer := packedTestStorer(t, f)
			writePack(t, writer, fx)
			ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), fx.hashes[0])
			if err := writer.SetReference(ref); err != nil {
				t.Fatal(err)
			}
			client := &visibilityClient{s3API: f, mode: tt.name}
			client.hide.Store(true)
			newReader := func() *Storer {
				t.Helper()
				s, err := New(context.Background(), "test-bucket", withClient(client), WithPackedRefs(true))
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			read := func(s *Storer) error {
				obj, err := s.EncodedObject(plumbing.AnyObject, ref.Hash())
				if err != nil {
					return err
				}
				r, err := obj.Reader()
				if err != nil {
					return err
				}
				defer r.Close()
				_, err = io.Copy(io.Discard, r)
				return err
			}
			reader := newReader()
			if got, err := reader.Reference(ref.Name()); err != nil || got.Hash() != ref.Hash() {
				t.Fatalf("ref not visible: %v", err)
			}
			if err := read(reader); err == nil {
				t.Fatal("hidden pack unexpectedly readable")
			}
			client.hide.Store(false)
			if tt.name == "list" {
				// A successful incomplete listing is cached, unlike a failed cue GET.
				if err := read(reader); !errors.Is(err, plumbing.ErrObjectNotFound) {
					t.Fatalf("cached incomplete index: %v", err)
				}
			}
			if err := read(newReader()); err != nil {
				t.Fatalf("fresh reader after visibility restored: %v", err)
			}
		})
	}
}

// A reader that built its index before another writer published a ref must
// recover without a new Storer. Misses within the cooldown share a refresh.
func TestPackLookupRefreshOnMiss(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	fx := buildPackFixture(t, 3, false)
	for _, tt := range []struct{ name string }{{"object"}, {"has"}, {"size"}} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3(t)
			obs, snapshot := countingObserver()
			reader := newTestStorer(t, f, obs)
			if err := reader.ensurePacksBuilt(); err != nil {
				t.Fatal(err)
			}
			writer := newTestStorer(t, f)
			writePack(t, writer, fx)
			if err := writer.up.flush(); err != nil {
				t.Fatal(err)
			}
			h := fx.hashes[0]
			switch tt.name {
			case "object":
				if _, err := reader.EncodedObject(plumbing.AnyObject, h); err != nil {
					t.Fatal(err)
				}
			case "has":
				if err := reader.HasEncodedObject(h); err != nil {
					t.Fatal(err)
				}
			case "size":
				if n, err := reader.EncodedObjectSize(h); err != nil || n != fx.byHash[h].size {
					t.Fatalf("size=%d err=%v", n, err)
				}
			}
			if n := snapshot()["ListObjectsV2"]; n != 2 {
				t.Fatalf("list calls=%d, want 2", n)
			}
			ghost := hashForBody(reader.of, plumbing.BlobObject, "missing object")
			for range 3 {
				if err := reader.HasEncodedObject(ghost); !errors.Is(err, plumbing.ErrObjectNotFound) {
					t.Fatal(err)
				}
			}
			if n := snapshot()["ListObjectsV2"]; n != 2 {
				t.Fatalf("repeated misses added listings: %d", n)
			}
		})
	}
}

func TestPackRefreshCoalescesAndPreservesTables(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	f := newFakeS3(t)
	old := buildPackFixture(t, 2, false)
	writer := newTestStorer(t, f)
	writePack(t, writer, old)
	if err := writer.up.flush(); err != nil {
		t.Fatal(err)
	}
	obs, snapshot := countingObserver()
	reader := newTestStorer(t, f, obs)
	if err := reader.ensurePacksBuilt(); err != nil {
		t.Fatal(err)
	}
	before := reader.packs.snapshot()
	localHash := hashForBody(reader.of, plumbing.BlobObject, "local staged object")
	reader.packs.register("local-staged", []cueRecord{{hash: localHash, typ: plumbing.BlobObject, raw: 19, stored: 19}}, "unused-local-path")
	added := buildPackFixture(t, 4, false)
	writePack(t, writer, added)
	if err := writer.up.flush(); err != nil {
		t.Fatal(err)
	}
	var target plumbing.Hash
	for _, h := range added.hashes {
		if _, _, found := before.lookup(h.Bytes()); !found {
			target = h
			break
		}
	}
	if target.IsZero() {
		t.Fatal("fixture added no new objects")
	}
	errs := make(chan error, 12)
	for range 12 {
		go func() { errs <- reader.HasEncodedObject(target) }()
	}
	for range 12 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if n := snapshot()["ListObjectsV2"]; n != 2 {
		t.Fatalf("list calls=%d want 2", n)
	}
	if err := reader.HasEncodedObject(localHash); err != nil {
		t.Fatalf("lost local registration: %v", err)
	}
	for _, h := range old.hashes {
		if err := reader.HasEncodedObject(h); err != nil {
			t.Fatalf("lost old object: %v", err)
		}
	}
	if _, _, found := before.lookup(target.Bytes()); found {
		t.Fatal("refresh mutated old snapshot")
	}
}

// A refresh runs only after loose storage has already said 404. A refresh that
// then fails must not turn that absence into a storage error: go-git's
// revision walk tolerates only ErrObjectNotFound for a `have` the server lacks.
// Cancellation is the one exception, and a loose-storage error never reaches
// the refresh at all.
func TestPackRefreshErrors(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	for _, tt := range []struct {
		name     string
		want     error
		listings int // ListObjectsV2 calls, including the cold build
	}{
		{"permission", storageErr, 1},
		{"list failure", plumbing.ErrObjectNotFound, 2},
		{"cancelled", context.Canceled, 1},
		{"corrupt cue", plumbing.ErrObjectNotFound, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3(t)
			obs, snapshot := countingObserver()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader, err := New(ctx, "test-bucket", withClient(f), obs)
			if err != nil {
				t.Fatal(err)
			}
			if err := reader.ensurePacksBuilt(); err != nil {
				t.Fatal(err)
			}
			switch tt.name {
			case "permission":
				f.getErr = storageErr
			case "list failure":
				f.listErr = storageErr
			case "cancelled":
				cancel()
			case "corrupt cue":
				f.objs["packs/corrupt.cue"] = fakeObject{body: []byte("invalid cue")}
			}
			h := hashForBody(reader.of, plumbing.BlobObject, "absent")
			_, err = reader.EncodedObject(plumbing.AnyObject, h)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error=%v want=%v", err, tt.want)
			}
			if n := snapshot()["ListObjectsV2"]; n != tt.listings {
				t.Fatalf("list calls=%d, want %d", n, tt.listings)
			}
		})
	}
}

type blockedPackList struct {
	s3API
	entered chan struct{}
	release chan struct{}
}

func (c *blockedPackList) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	close(c.entered)
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return c.s3API.ListObjectsV2(ctx, in, opts...)
}

func TestPackRefreshDoesNotBlockKnownObjects(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	f := newFakeS3(t)
	fx := buildPackFixture(t, 2, false)
	writer := newTestStorer(t, f)
	writePack(t, writer, fx)
	if err := writer.up.flush(); err != nil {
		t.Fatal(err)
	}
	reader := newTestStorer(t, f)
	if err := reader.ensurePacksBuilt(); err != nil {
		t.Fatal(err)
	}
	gate := &blockedPackList{s3API: f, entered: make(chan struct{}), release: make(chan struct{})}
	reader.client = gate
	ghost := hashForBody(reader.of, plumbing.BlobObject, "absent")
	missing := make(chan error, 1)
	known := make(chan error, 1)
	go func() { missing <- reader.HasEncodedObject(ghost) }()
	defer func() {
		close(gate.release)
		select {
		case <-missing:
		case <-time.After(5 * time.Second):
			t.Error("refresh stuck")
		}
	}()
	waitPublication(t, gate.entered)
	go func() { known <- reader.HasEncodedObject(fx.hashes[0]) }()
	select {
	case err := <-known:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Error("known object blocked behind pack refresh")
	}
}

// fakeClock replaces a packIndex's clock, so a test moves through cooldowns
// without sleeping. Advance only between calls on the same goroutine, or after
// every goroutine that could read it has finished.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func installFakeClock(s *Storer) *fakeClock {
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s.packs.now = c.now
	return c
}

// syntheticPack builds one raw-codec blob pack without a git client. The
// caller decides when its two keys land in the fake bucket.
type syntheticPack struct {
	id   string
	hash plumbing.Hash
	recs []cueRecord
	bin  string
	cue  string
}

func makeSyntheticPack(s *Storer, body string) syntheticPack {
	sum := sha256.Sum256([]byte(body))
	recs := []cueRecord{{
		hash:   hashForBody(s.of, plumbing.BlobObject, body),
		typ:    plumbing.BlobObject,
		codec:  codecRaw,
		stored: int64(len(body)),
		raw:    int64(len(body)),
	}}
	return syntheticPack{
		id:   hex.EncodeToString(sum[:]),
		hash: recs[0].hash,
		recs: recs,
		bin:  body,
		cue:  string(encodeCue(s.oh.Size(), recs)),
	}
}

func (p syntheticPack) put(s *Storer, f *fakeS3) {
	f.put(s.prefix+packPrefix+p.id+binSuffix, p.bin, nil)
	f.put(s.prefix+packPrefix+p.id+cueSuffix, p.cue, nil)
}

// A missing probe or a transient error must not poison a long-lived reader.
func TestPackRefreshRecoversAfterCooldown(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	fixtures := []packFixture{buildPackFixture(t, 2, false), buildPackFixture(t, 5, false)}
	for _, tt := range []struct {
		name string
		fail bool
	}{{"missing probe", false}, {"listing failure", true}} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3(t)
			obs, counts := countingObserver()
			reader := newTestStorer(t, f, obs)
			clock := installFakeClock(reader)
			if err := reader.ensurePacksBuilt(); err != nil {
				t.Fatal(err)
			}
			ghost := hashForBody(reader.of, plumbing.BlobObject, "absent")
			if tt.fail {
				f.listErr = errors.New("temporary listing failure")
			}
			for range 12 {
				if err := reader.HasEncodedObject(ghost); !errors.Is(err, plumbing.ErrObjectNotFound) {
					t.Fatalf("initial miss: %v", err)
				}
			}
			if n := counts()["ListObjectsV2"]; n != 2 {
				t.Fatalf("miss burst made %d listings, want 2", n)
			}
			f.listErr = nil
			writer := newTestStorer(t, f)
			for round, fx := range fixtures {
				before := reader.packs.snapshot()
				writePack(t, writer, fx)
				if err := writer.up.flush(); err != nil {
					t.Fatal(err)
				}
				var target plumbing.Hash
				for _, h := range fx.hashes {
					if _, _, found := before.lookup(h.Bytes()); !found {
						target = h
						break
					}
				}
				if target.IsZero() {
					t.Fatal("fixture has no new objects")
				}
				// The first cooldown, and the one after a refresh that found a
				// pack, are both the base length.
				clock.advance(time.Second)
				errs := make(chan error, 12)
				for i := range 12 {
					go func() {
						var err error
						switch i % 3 {
						case 0:
							err = reader.HasEncodedObject(target)
						case 1:
							_, err = reader.EncodedObjectSize(target)
						case 2:
							_, err = reader.EncodedObject(plumbing.AnyObject, target)
						}
						errs <- err
					}()
				}
				for range 12 {
					if err := <-errs; err != nil {
						t.Fatalf("publication %d: %v", round+1, err)
					}
				}
				if n := counts()["ListObjectsV2"]; n != round+3 {
					t.Fatalf("publication %d: %d listings, want %d", round+1, n, round+3)
				}
			}
		})
	}
}

// A slow listing must not let queued misses start another attempt immediately.
func TestPackRefreshCooldownStartsAfterCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeS3(t)
		obs, counts := countingObserver()
		reader := newTestStorer(t, f, obs)
		if err := reader.ensurePacksBuilt(); err != nil {
			t.Fatal(err)
		}
		gate := &blockedPackList{s3API: f, entered: make(chan struct{}), release: make(chan struct{})}
		reader.client = gate
		errs := make(chan error, 12)
		for range 12 {
			go func() { errs <- reader.refreshPacks() }()
		}
		<-gate.entered
		synctest.Wait()
		time.Sleep(2 * time.Second)
		close(gate.release)
		for range 12 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		reader.client = f
		if err := reader.refreshPacks(); err != nil {
			t.Fatal(err)
		}
		if n := counts()["ListObjectsV2"]; n != 2 {
			t.Fatalf("slow attempt caused %d listings, want 2", n)
		}
		time.Sleep(time.Second)
		if err := reader.refreshPacks(); err != nil {
			t.Fatal(err)
		}
		if n := counts()["ListObjectsV2"]; n != 3 {
			t.Fatalf("after cooldown: %d listings, want 3", n)
		}
	})
}

// The cooldown adapts to what a refresh finds. An attempt that adds nothing,
// or fails, doubles the next cooldown up to the cap. An attempt that adds a
// pack resets it to the base. A reader is never told to stop refreshing: a
// fresh reader fetching a just-pushed ref is the case this path exists for.
func TestPackRefreshCooldownAdapts(t *testing.T) {
	for _, tt := range []struct {
		name string
		fail bool
	}{{"empty listings", false}, {"listing failures", true}} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3(t)
			obs, counts := countingObserver()
			reader := newTestStorer(t, f, obs)
			clock := installFakeClock(reader)
			if err := reader.ensurePacksBuilt(); err != nil {
				t.Fatal(err)
			}
			if tt.fail {
				f.listErr = errors.New("temporary listing failure")
			}
			listings := 1 // the cold build
			refresh := func() {
				t.Helper()
				err := reader.refreshPacks()
				if (err != nil) != tt.fail {
					t.Fatalf("refresh error=%v, want failure=%v", err, tt.fail)
				}
			}
			for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second} {
				refresh()
				listings++
				if got := reader.packs.refreshWait; got != want {
					t.Fatalf("attempt %d: cooldown=%v, want %v", i+1, got, want)
				}
				if n := counts()["ListObjectsV2"]; n != listings {
					t.Fatalf("attempt %d: %d listings, want %d", i+1, n, listings)
				}
				// A miss just inside the cooldown lists nothing.
				clock.advance(want - time.Millisecond)
				if err := reader.refreshPacks(); err != nil {
					t.Fatalf("attempt %d: cooldown returned %v", i+1, err)
				}
				if n := counts()["ListObjectsV2"]; n != listings {
					t.Fatalf("attempt %d: miss inside the cooldown listed (%d listings)", i+1, n)
				}
				clock.advance(time.Millisecond)
			}

			f.listErr = nil
			pack := makeSyntheticPack(reader, "published while the reader backed off")
			pack.put(reader, f)
			if err := reader.refreshPacks(); err != nil {
				t.Fatal(err)
			}
			if got := reader.packs.refreshWait; got != time.Second {
				t.Fatalf("a productive refresh left the cooldown at %v, want %v", got, time.Second)
			}
			if err := reader.HasEncodedObject(pack.hash); err != nil {
				t.Fatalf("new pack not visible: %v", err)
			}
		})
	}
}

// blockedCueGet holds the first .cue GetObject open until release closes.
type blockedCueGet struct {
	s3API
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockedCueGet) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if strings.HasSuffix(sv(in.Key), cueSuffix) {
		c.once.Do(func() { close(c.entered) })
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.s3API.GetObject(ctx, in, opts...)
}

// tablesHolding counts the tables that carry records of pack num.
func tablesHolding(p *packIndex, num uint32) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, t := range p.tables {
		if slices.ContainsFunc(t.recs, func(r tableRec) bool { return r.pack == num }) {
			n++
		}
	}
	return n
}

// A pack this storer registers while a refresh is in flight must not be loaded
// a second time from its own freshly landed .cue. The write can race the
// listing, or the cue fetch that follows it.
func TestPackRefreshSkipsPacksRegisteredMeanwhile(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	fx := buildPackFixture(t, 3, false)
	for _, tt := range []struct {
		name string
		// published is true when another writer lands the same pack before
		// the refresh lists, so the listing sees it and the race is on the
		// cue fetch instead of the listing.
		published bool
	}{{"during the listing", false}, {"during the cue fetch", true}} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3(t)
			reader := newTestStorer(t, f)
			if err := reader.ensurePacksBuilt(); err != nil {
				t.Fatal(err)
			}
			var entered, release chan struct{}
			if tt.published {
				other := newTestStorer(t, f)
				writePack(t, other, fx)
				if err := other.up.flush(); err != nil {
					t.Fatal(err)
				}
				gate := &blockedCueGet{s3API: f, entered: make(chan struct{}), release: make(chan struct{})}
				entered, release = gate.entered, gate.release
				reader.client = gate
			} else {
				gate := &blockedPackList{s3API: f, entered: make(chan struct{}), release: make(chan struct{})}
				entered, release = gate.entered, gate.release
				reader.client = gate
			}

			ghost := hashForBody(reader.of, plumbing.BlobObject, "absent")
			missing := make(chan error, 1)
			go func() { missing <- reader.HasEncodedObject(ghost) }()
			waitPublication(t, entered)

			// The normal write path: register, then upload the bin and cue.
			writePack(t, reader, fx)
			if err := reader.up.flush(); err != nil {
				t.Fatal(err)
			}
			close(release)
			select {
			case err := <-missing:
				if !errors.Is(err, plumbing.ErrObjectNotFound) {
					t.Fatalf("miss: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("refresh stuck")
			}

			f.mu.Lock()
			var ids []string
			for k := range f.objs {
				if strings.HasSuffix(k, cueSuffix) {
					ids = append(ids, strings.TrimSuffix(strings.TrimPrefix(k, packPrefix), cueSuffix))
				}
			}
			f.mu.Unlock()
			if len(ids) != 1 {
				t.Fatalf("bucket holds %d packs, want 1 (same content, same id)", len(ids))
			}
			reader.packs.mu.Lock()
			num, ok := reader.packs.idNum[ids[0]]
			reader.packs.mu.Unlock()
			if !ok {
				t.Fatalf("pack %s is not numbered", ids[0])
			}
			if n := tablesHolding(reader.packs, num); n != 1 {
				t.Fatalf("pack %s is in %d tables, want 1", ids[0], n)
			}
			for _, h := range fx.hashes {
				if err := reader.HasEncodedObject(h); err != nil {
					t.Fatalf("own pack not visible: %v", err)
				}
			}
		})
	}
}

// deregister runs when an upload reports failure, but the bin and cue can be
// durable regardless: the PUT succeeded server-side and only the reply was
// lost. A later refresh on the same storer must then load the pack from the
// bucket like any other, including when its table was shared with other packs
// and deregister could only mark it dead.
func TestPackRefreshReloadsDeregisteredPack(t *testing.T) {
	for _, tt := range []struct {
		name   string
		shared bool
	}{{"own table", false}, {"shared table", true}} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3(t)
			reader := newTestStorer(t, f)
			clock := installFakeClock(reader)
			if err := reader.ensurePacksBuilt(); err != nil {
				t.Fatal(err)
			}
			pack := makeSyntheticPack(reader, "durable despite a failed upload reply")
			reader.packs.register(pack.id, pack.recs, "unused-local-path")
			if tt.shared {
				other := makeSyntheticPack(reader, "a neighbour in the same table")
				reader.packs.register(other.id, other.recs, "unused-local-path")
				reader.packs.mu.Lock()
				reader.packs.tables = []*packTable{mergeTables(reader.packs.hashSize, reader.packs.tables, nil)}
				reader.packs.mu.Unlock()
			}
			reader.packs.deregister(pack.id)
			if err := reader.HasEncodedObject(pack.hash); !errors.Is(err, plumbing.ErrObjectNotFound) {
				t.Fatalf("deregistered pack still visible: %v", err)
			}

			pack.put(reader, f)
			clock.advance(reader.packs.refreshWait)
			if err := reader.HasEncodedObject(pack.hash); err != nil {
				t.Fatalf("durable pack not reloaded: %v", err)
			}
			obj, err := reader.EncodedObject(plumbing.BlobObject, pack.hash)
			if err != nil {
				t.Fatal(err)
			}
			r, err := obj.Reader()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			body, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != pack.bin {
				t.Fatalf("body=%q want %q", body, pack.bin)
			}
		})
	}
}

// Model a storage call that cannot stop promptly on context cancellation.
type uncancelablePackList struct{ *blockedPackList }

func (c *uncancelablePackList) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return c.blockedPackList.ListObjectsV2(context.WithoutCancel(ctx), in, opts...)
}

// A canceled storer reports the cancellation from refreshPacks whether the
// cancel lands during a cooldown or while a caller waits on another caller's
// slow listing.
func TestPackRefreshCancellation(t *testing.T) {
	for _, tt := range []struct {
		name string
		// waiting is true when the cancel lands while a second caller waits
		// on a listing that itself ignores cancellation.
		waiting bool
	}{{"during the cooldown", false}, {"while waiting on a slow listing", true}} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeS3(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reader, err := New(ctx, "test-bucket", withClient(f))
				if err != nil {
					t.Fatal(err)
				}
				if err := reader.ensurePacksBuilt(); err != nil {
					t.Fatal(err)
				}

				if !tt.waiting {
					if err := reader.refreshPacks(); err != nil {
						t.Fatal(err)
					}
					cancel()
					if err := reader.refreshPacks(); !errors.Is(err, context.Canceled) {
						t.Fatalf("cooldown masked cancellation: %v", err)
					}
					return
				}

				gate := &blockedPackList{s3API: f, entered: make(chan struct{}), release: make(chan struct{})}
				reader.client = &uncancelablePackList{gate}
				leader := make(chan error, 1)
				waiter := make(chan error, 1)
				go func() { leader <- reader.refreshPacks() }()
				<-gate.entered
				go func() { waiter <- reader.refreshPacks() }()
				synctest.Wait()
				cancel()
				synctest.Wait()
				select {
				case err := <-waiter:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("waiter error=%v", err)
					}
				default:
					t.Error("canceled waiter remained blocked on storage")
				}
				close(gate.release)
				<-leader
			})
		})
	}
}
