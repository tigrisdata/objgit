package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/go-git/go-git/v6/utils/ioutil"
)

// gatedRefStorer models the ref-publication contract. The Tigris tests exercise
// the actual bin/cue PUT barriers; this test checks receive-pack's callback.
type gatedRefStorer struct {
	storage.Storer
	entered chan struct{}
	release chan struct{}
	err     error
}

func (s *gatedRefStorer) UpdateReferences(sets []*plumbing.Reference, removes []plumbing.ReferenceName) error {
	close(s.entered)
	<-s.release
	if s.err != nil {
		return s.err
	}
	for _, ref := range sets {
		if err := s.Storer.SetReference(ref); err != nil {
			return err
		}
	}
	for _, name := range removes {
		if err := s.Storer.RemoveReference(name); err != nil {
			return err
		}
	}
	return nil
}

func TestReceivePackWaitsForPublication(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	work := seedRepo(t)
	tip, ok := plumbing.FromHex(strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD")))
	if !ok {
		t.Fatal("invalid fixture tip")
	}
	pack := runGit(t, work, "pack-objects", "--stdout", "--all")
	for _, tt := range []struct {
		name           string
		fail, deletion bool
	}{
		{name: "success"}, {name: "failed upload", fail: true}, {name: "deletion", deletion: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs syncBuffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			defer slog.SetDefault(prev)
			s := &gatedRefStorer{Storer: memory.NewStorage(), entered: make(chan struct{}), release: make(chan struct{})}
			uploadErr := errors.New("pack upload failed")
			if tt.fail {
				s.err = uploadErr
			}
			zero := plumbing.ZeroHash
			cmd := &packp.Command{Name: plumbing.NewBranchReferenceName("main"), Old: zero, New: tip}
			if tt.deletion {
				if err := s.Storer.SetReference(plumbing.NewHashReference(cmd.Name, tip)); err != nil {
					t.Fatal(err)
				}
				cmd.Old, cmd.New = tip, zero
			}
			req := &packp.UpdateRequests{Commands: []*packp.Command{cmd}}
			req.Capabilities.Set(capability.ReportStatus)
			var input, output bytes.Buffer
			if err := req.Encode(&input); err != nil {
				t.Fatal(err)
			}
			if !tt.deletion {
				input.WriteString(pack)
			}
			called := make(chan struct{}, 1)
			done := make(chan error, 1)
			finished := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(s.release) }) }
			go func() {
				defer close(finished)
				done <- receivePackStreaming(context.Background(), s, io.NopCloser(&input), ioutil.WriteNopCloser(&output), &transport.ReceivePackRequest{StatelessRPC: true}, nil, func(_ io.Writer, _ []refUpdate, _ time.Time) { called <- struct{}{} })
			}()
			// Always release the gate before waiting for the request during cleanup.
			defer func() {
				release()
				select {
				case <-finished:
				case <-time.After(10 * time.Second):
					t.Error("receive-pack did not exit")
				}
			}()
			select {
			case <-s.entered:
			case err := <-done:
				t.Fatalf("receive-pack exited before publication: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("publication not reached")
			}
			select {
			case <-called:
				t.Fatal("callback ran before publication")
			default:
			}
			release()
			select {
			case err := <-done:
				if tt.fail {
					if !errors.Is(err, uploadErr) {
						t.Errorf("receive-pack error = %v, want upload failure", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("receive-pack did not finish")
			}
			select {
			case <-called:
				if tt.fail {
					t.Error("callback ran after upload failure")
				}
			default:
				if !tt.fail {
					t.Error("callback missing after publication")
				}
			}
			if tt.fail && !strings.Contains(output.String(), uploadErr.Error()) {
				t.Errorf("missing failure status: %q", output.String())
			}
		})
	}
}
