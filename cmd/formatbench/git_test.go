package main

import "testing"

// scale states an expected byte count the way git prints it, as a decimal
// times a power-of-two unit. A plain constant expression will not do: the
// products here are not whole numbers, and Go refuses to convert a
// non-integral untyped constant to int64.
func scale(n float64, unit int64) int64 { return int64(n * float64(unit)) }

func TestParseReceivedBytes(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		out  string
		want int64
	}{
		{
			name: "mebibytes",
			out:  "Receiving objects: 100% (59459/59459), 80.86 MiB | 4.20 MiB/s, done.",
			want: scale(80.86, 1<<20),
		},
		{
			name: "kibibytes",
			out:  "Receiving objects: 100% (318/318), 200.50 KiB | 1.10 MiB/s, done.",
			want: scale(200.50, 1<<10),
		},
		{
			name: "a small pack is reported in plain bytes",
			out:  "Receiving objects: 100% (3/3), 244 bytes | 244.00 KiB/s, done.",
			want: 244,
		},
		{
			name: "partial progress lines are ignored, only the 100% line counts",
			out: "Receiving objects:  47% (28000/59459), 40.00 MiB\r" +
				"Receiving objects: 100% (59459/59459), 80.86 MiB | 4.20 MiB/s, done.",
			want: scale(80.86, 1<<20),
		},
		{
			name: "no receive line at all",
			out:  "warning: You appear to have cloned an empty repository.",
			want: 0,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := parseReceivedBytes(tt.out); got != tt.want {
				t.Logf("want: %d", tt.want)
				t.Logf("got:  %d", got)
				t.Error("wire byte count is wrong")
			}
		})
	}
}

func TestOpResultOK(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		in   opResult
		want bool
	}{
		{"finished cleanly", opResult{}, true},
		{"hit its time limit", opResult{DNF: true}, false},
		{"failed", opResult{Err: "connection refused"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.in.ok(); got != tt.want {
				t.Errorf("want %v, got %v", tt.want, got)
			}
		})
	}
}
