package main

import (
	"strings"
	"testing"
)

func TestParseProcKV(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		input   string
		want    map[string]uint64
		wantErr bool
	}{
		{
			name: "status excerpt",
			input: "Name:\tobjgitd\n" +
				"State:\tS (sleeping)\n" +
				"VmRSS:\t   28160 kB\n" +
				"VmHWM:\t   31744 kB\n",
			want: map[string]uint64{"VmRSS": 28160 * 1024, "VmHWM": 31744 * 1024},
		},
		{
			name: "smaps_rollup excerpt",
			input: "55d0c0000000-7ffd0c0f1000 ---p 00000000 00:00 0                          [rollup]\n" +
				"Rss:                7168 kB\n" +
				"Pss:                4096 kB\n" +
				"Private_Dirty:      2048 kB\n",
			want: map[string]uint64{"Rss": 7168 * 1024, "Pss": 4096 * 1024, "Private_Dirty": 2048 * 1024},
		},
		{
			name:  "no kB lines",
			input: "Name:\tobjgitd\nThreads:\t12\n",
			want:  map[string]uint64{},
		},
		{
			name:    "unparseable count",
			input:   "VmRSS:\t   twelve kB\n",
			wantErr: true,
		},
		{
			name:  "empty",
			input: "",
			want:  map[string]uint64{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseProcKV(strings.NewReader(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Error("wanted an error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(got) != len(tt.want) {
				t.Logf("want: %v", tt.want)
				t.Logf("got:  %v", got)
				t.Fatalf("got %d keys, want %d", len(got), len(tt.want))
			}
			for k, want := range tt.want {
				if got[k] != want {
					t.Logf("want: %d", want)
					t.Logf("got:  %d", got[k])
					t.Errorf("%s is wrong", k)
				}
			}
		})
	}
}
