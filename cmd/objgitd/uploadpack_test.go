package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/go-git/go-git/v6/utils/ioutil"
)

// pkt frames one pkt-line payload.
func pkt(payload string) string {
	return fmt.Sprintf("%04x%s", len(payload)+4, payload)
}

// seedCommitChain stores n commits on an empty tree, each the parent of the
// next, and returns them oldest first.
func seedCommitChain(t *testing.T, st *memory.Storage, n int) []plumbing.Hash {
	t.Helper()
	to := st.NewEncodedObject()
	if err := (&object.Tree{}).Encode(to); err != nil {
		t.Fatal(err)
	}
	th, err := st.SetEncodedObject(to)
	if err != nil {
		t.Fatal(err)
	}
	var chain []plumbing.Hash
	for i := range n {
		sig := object.Signature{Name: "T", Email: "t@example.com", When: time.Unix(int64(i), 0)}
		c := &object.Commit{Author: sig, Committer: sig, Message: fmt.Sprint(i), TreeHash: th}
		if i > 0 {
			c.ParentHashes = []plumbing.Hash{chain[i-1]}
		}
		co := st.NewEncodedObject()
		if err := c.Encode(co); err != nil {
			t.Fatal(err)
		}
		ch, err := st.SetEncodedObject(co)
		if err != nil {
			t.Fatal(err)
		}
		chain = append(chain, ch)
	}
	return chain
}

func TestUploadPackHTTP(t *testing.T) {
	st := memory.NewStorage()
	chain := seedCommitChain(t, st, 3)
	tip := chain[2].String()
	shallowTip := pkt("shallow "+tip+"\n") + "0000"
	const nak = "0008NAK\n"

	for _, tt := range []struct {
		name      string
		stateless bool
		input     string
		want      string // the whole response, or its prefix when pack is set
		pack      bool   // a packfile follows want
		wantErr   string
	}{
		{
			name:      "shallow discovery round",
			stateless: true,
			input:     pkt("want "+tip+" multi_ack_detailed side-band-64k\n") + pkt("deepen 1\n") + "0000",
			want:      shallowTip,
		},
		{
			name:      "shallow discovery round without multi_ack",
			stateless: true,
			input:     pkt("want "+tip+"\n") + pkt("deepen 1\n") + "0000",
			want:      shallowTip,
		},
		{
			name:      "shallow with done in one round",
			stateless: true,
			input:     pkt("want "+tip+"\n") + pkt("deepen 1\n") + "0000" + pkt("done\n"),
			want:      shallowTip + nak + "PACK",
			pack:      true,
		},
		{
			name:      "shallow with haves and done in one round",
			stateless: true,
			input:     pkt("want "+tip+"\n") + pkt("deepen 1\n") + "0000" + pkt("have "+chain[0].String()+"\n") + pkt("done\n"),
			want:      shallowTip + pkt("ACK "+chain[0].String()+"\n") + "PACK",
			pack:      true,
		},
		{
			name:      "ordinary request",
			stateless: true,
			input:     pkt("want "+tip+"\n") + "0000" + pkt("done\n"),
			want:      nak + "PACK",
			pack:      true,
		},
		{
			name:      "deepen-since is unsupported",
			stateless: true,
			input:     pkt("want "+tip+"\n") + pkt("deepen-since 1234567890\n") + "0000",
			wantErr:   "unsupported depth",
		},
		{
			name:      "deepen-not is unsupported",
			stateless: true,
			input:     pkt("want "+tip+"\n") + pkt("deepen-not main\n") + "0000",
			wantErr:   "unsupported depth",
		},
		{
			name:      "flush only",
			stateless: true,
			input:     "0000",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := uploadPackHTTP(context.Background(), st,
				io.NopCloser(strings.NewReader(tt.input)),
				ioutil.WriteNopCloser(&out),
				&transport.UploadPackRequest{StatelessRPC: tt.stateless})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err=%v want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("err=%v\nresponse=%q", err, out.String())
			}
			got := out.String()
			if tt.pack {
				if !strings.HasPrefix(got, tt.want) {
					t.Fatalf("response=%q\nwant prefix %q", got, tt.want)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("response=%q\nwant %q", got, tt.want)
			}
		})
	}
}
