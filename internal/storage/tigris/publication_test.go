package tigris

import (
	"errors"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
)

// A failed upload cannot be retried: done removes its staging files. Every
// later ref mutation on that writer must keep returning the upload failure.
func TestPackUploadFailureRemainsFatal(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	for _, tt := range []struct {
		name, suffix string
		packed       bool
	}{
		{"loose bin", binSuffix, false}, {"loose cue", cueSuffix, false},
		{"packed bin", binSuffix, true}, {"packed cue", cueSuffix, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fx := buildPackFixture(t, 3, false)
			f := newFakeS3(t)
			uploadErr := errors.New("injected upload failure")
			f.beforePut = func(key string) error {
				if strings.HasSuffix(key, tt.suffix) {
					return uploadErr
				}
				return nil
			}
			s := newTestStorer(t, f, WithPackedRefs(tt.packed))
			writePack(t, s, fx)
			for _, branch := range []string{"main", "other"} {
				ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName(branch), fx.hashes[0])
				if err := s.SetReference(ref); !errors.Is(err, uploadErr) {
					t.Errorf("SetReference(%s) = %v, want upload error", branch, err)
				}
				reader := newTestStorer(t, f, WithPackedRefs(tt.packed))
				if _, err := reader.Reference(ref.Name()); !errors.Is(err, plumbing.ErrReferenceNotFound) {
					t.Errorf("failed upload published %s: %v", branch, err)
				}
			}
		})
	}
}

func TestPackPublicationOrder(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	for _, tt := range []struct {
		name   string
		packed bool
		cap    int64
	}{
		{"loose single", false, maxPackBytes}, {"packed single", true, maxPackBytes},
		{"loose multiple", false, 1}, {"packed multiple", true, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fx := buildPackFixture(t, 3, false)
			f := newFakeS3(t)
			bins, cues := make(chan struct{}), make(chan struct{})
			binStarted, cueStarted := make(chan struct{}, 100), make(chan struct{}, 100)
			var releaseBins, releaseCues sync.Once
			unblockBins := func() { releaseBins.Do(func() { close(bins) }) }
			unblockCues := func() { releaseCues.Do(func() { close(cues) }) }
			defer unblockBins()
			defer unblockCues()
			var binCount, cueCount atomic.Int32
			f.beforePut = func(key string) error {
				switch {
				case strings.HasSuffix(key, binSuffix):
					binCount.Add(1)
					binStarted <- struct{}{}
					<-bins
				case strings.HasSuffix(key, cueSuffix):
					f.mu.Lock()
					_, exists := f.objs[strings.TrimSuffix(key, cueSuffix)+binSuffix]
					f.mu.Unlock()
					if !exists {
						t.Error("cue PUT started before its bin completed")
					}
					cueCount.Add(1)
					cueStarted <- struct{}{}
					<-cues
				}
				return nil
			}
			s := newTestStorer(t, f, WithPackedRefs(tt.packed), withMaxPackBytes(tt.cap))
			writePack(t, s, fx)
			ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), fx.hashes[0])
			done := make(chan error, 1)
			go func() { done <- s.SetReference(ref) }()
			waitPublication(t, binStarted)
			assertPending := func() {
				t.Helper()
				select {
				case err := <-done:
					t.Fatalf("ref update finished before uploads: %v", err)
				default:
				}
				reader := newTestStorer(t, f, WithPackedRefs(tt.packed))
				if _, err := reader.Reference(ref.Name()); !errors.Is(err, plumbing.ErrReferenceNotFound) {
					t.Fatalf("ref visible while upload blocked: %v", err)
				}
			}
			assertPending()
			if cueCount.Load() != 0 {
				t.Fatal("cue started while bins blocked")
			}
			unblockBins()
			waitPublication(t, cueStarted)
			assertPending()
			unblockCues()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("ref update did not finish")
			}
			if binCount.Load() != cueCount.Load() {
				t.Fatalf("bins=%d cues=%d", binCount.Load(), cueCount.Load())
			}
			if tt.cap == 1 && binCount.Load() < 2 {
				t.Fatal("fixture did not split containers")
			}
			reader := newTestStorer(t, f, WithPackedRefs(tt.packed))
			for _, h := range fx.hashes {
				obj, err := reader.EncodedObject(plumbing.AnyObject, h)
				if err != nil {
					t.Fatal(err)
				}
				r, err := obj.Reader()
				if err != nil {
					t.Fatal(err)
				}
				hasher := plumbing.NewHasher(reader.of, obj.Type(), obj.Size())
				_, err = io.Copy(hasher, r)
				r.Close()
				if err != nil {
					t.Fatal(err)
				}
				if hasher.Sum() != h {
					t.Fatalf("cold read hash differs for %s", h)
				}
			}
		})
	}
}

func waitPublication(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("upload did not reach gate")
	}
}
