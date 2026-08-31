package main

import (
	"strings"
	"testing"
)

func TestParseMetrics(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		input   string
		want    goMetrics
		wantErr bool
	}{
		{
			name: "the metrics the harness records",
			input: "# HELP go_goroutines Number of goroutines that currently exist.\n" +
				"# TYPE go_goroutines gauge\n" +
				"go_goroutines 42\n" +
				"go_memstats_heap_inuse_bytes 1.2345678e+07\n" +
				"go_memstats_heap_sys_bytes 3.3554432e+07\n" +
				"go_memstats_next_gc_bytes 4.194304e+06\n" +
				"process_resident_memory_bytes 2.8835840e+07\n",
			want: goMetrics{
				HeapInuse:  12345678,
				HeapSys:    33554432,
				NextGC:     4194304,
				Goroutines: 42,
				ProcessRSS: 28835840,
			},
		},
		{
			name: "labelled series are skipped",
			input: "objgit_pushes_total{repo=\"a/b\"} 99\n" +
				"go_goroutines 7\n",
			want: goMetrics{Goroutines: 7},
		},
		{
			name:  "a metric name that happens to prefix a wanted one",
			input: "go_goroutines_extra 5\n",
			want:  goMetrics{},
		},
		{
			name:    "non-numeric value",
			input:   "go_goroutines NaNsense\n",
			wantErr: true,
		},
		{
			name:  "comments and blanks only",
			input: "# HELP nothing\n\n",
			want:  goMetrics{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseMetrics(strings.NewReader(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Error("wanted an error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tt.want {
				t.Logf("want: %+v", tt.want)
				t.Logf("got:  %+v", got)
				t.Error("parsed the wrong metrics")
			}
		})
	}
}
