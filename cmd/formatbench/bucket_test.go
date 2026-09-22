package main

import (
	"slices"
	"testing"
)

// The two builds disagree about the ".git" suffix, so a measurement has to look
// under both spellings or the older one silently reports an empty bucket.
func TestKeyPrefixes(t *testing.T) {
	t.Parallel()

	want := []string{"formatbench/before-objgit-abc/", "formatbench/before-objgit-abc.git/"}

	for _, tt := range []struct {
		name string
		in   string
	}{
		{"bare prefix", "formatbench/before-objgit-abc"},
		{"already carries the suffix", "formatbench/before-objgit-abc.git"},
		{"trailing slash", "formatbench/before-objgit-abc/"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := keyPrefixes(tt.in)
			if !slices.Equal(got, want) {
				t.Logf("want: %v", want)
				t.Logf("got:  %v", got)
				t.Error("prefix set is wrong")
			}
		})
	}
}
