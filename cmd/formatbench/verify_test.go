package main

import (
	"strings"
	"testing"
)

func TestDiffRefs(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  map[string]string
		dst  map[string]string
		want []string
	}{
		{
			name: "identical",
			src:  map[string]string{"refs/heads/main": "aaa"},
			dst:  map[string]string{"refs/heads/main": "aaa"},
		},
		{
			name: "the clone lost a ref",
			src:  map[string]string{"refs/heads/main": "aaa", "refs/tags/v1": "bbb"},
			dst:  map[string]string{"refs/heads/main": "aaa"},
			want: []string{"clone is missing ref refs/tags/v1"},
		},
		{
			name: "the clone gained a ref",
			src:  map[string]string{"refs/heads/main": "aaa"},
			dst:  map[string]string{"refs/heads/main": "aaa", "refs/heads/stray": "ccc"},
			want: []string{"clone has an extra ref refs/heads/stray"},
		},
		{
			name: "a ref points somewhere else",
			src:  map[string]string{"refs/heads/main": "aaa"},
			dst:  map[string]string{"refs/heads/main": "zzz"},
			want: []string{"ref refs/heads/main is aaa in the mirror and zzz in the clone"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := diffRefs(tt.src, tt.dst)
			if len(got) != len(tt.want) {
				t.Fatalf("want %v, got %v", tt.want, got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("want %q, got %q", tt.want[i], got[i])
				}
			}
		})
	}
}

func TestVerifyResultFailAccumulates(t *testing.T) {
	t.Parallel()

	got := verifyResult{Ran: true, OK: true}.
		fail("first problem").
		fail("second problem with %d", 2)

	if got.OK {
		t.Error("a failed check left OK set")
	}
	if len(got.Problems) != 2 {
		t.Fatalf("want 2 problems, got %v", got.Problems)
	}
	if !strings.Contains(got.Problems[1], "second problem with 2") {
		t.Errorf("format arguments were dropped: %q", got.Problems[1])
	}
}
