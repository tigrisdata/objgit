package tigris

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
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

func TestPackRefreshErrors(t *testing.T) {
	for _, tt := range []struct{ name string }{{"permission"}, {"list failure"}, {"cancelled"}, {"corrupt cue"}} {
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
			want := errors.New("storage unavailable")
			switch tt.name {
			case "permission":
				f.getErr = want
			case "list failure":
				f.listErr = want
			case "cancelled":
				cancel()
				want = context.Canceled
			case "corrupt cue":
				f.objs["packs/corrupt.cue"] = fakeObject{body: []byte("invalid cue")}
				want = errBadCue
			}
			h := hashForBody(reader.of, plumbing.BlobObject, "absent")
			_, err = reader.EncodedObject(plumbing.AnyObject, h)
			if !errors.Is(err, want) {
				t.Fatalf("error=%v want=%v", err, want)
			}
			if errors.Is(err, plumbing.ErrObjectNotFound) {
				t.Fatal("storage error became object-not-found")
			}
			if tt.name == "permission" && snapshot()["ListObjectsV2"] != 1 {
				t.Fatal("permission error triggered refresh")
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
			if err := reader.ensurePacksBuilt(); err != nil {
				t.Fatal(err)
			}
			ghost := hashForBody(reader.of, plumbing.BlobObject, "absent")
			want := plumbing.ErrObjectNotFound
			if tt.fail {
				want = errors.New("temporary listing failure")
				f.listErr = want
			}
			for range 12 {
				if err := reader.HasEncodedObject(ghost); !errors.Is(err, want) {
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
				time.Sleep(time.Second)
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

func TestPackRefreshCancellationDuringCooldown(t *testing.T) {
	f := newFakeS3(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := New(ctx, "test-bucket", withClient(f))
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.refreshPacks(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := reader.refreshPacks(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached result masked cancellation: %v", err)
	}
}

// Model a storage call that cannot stop promptly on context cancellation.
type uncancelablePackList struct{ *blockedPackList }

func (c *uncancelablePackList) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return c.blockedPackList.ListObjectsV2(context.WithoutCancel(ctx), in, opts...)
}

func TestPackRefreshWaiterCancellation(t *testing.T) {
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
}
