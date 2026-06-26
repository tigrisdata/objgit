package repofs

import (
	"context"
	"errors"
	"testing"

	"github.com/go-git/go-billy/v6/memfs"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    RepoRef
		wantErr error
	}{
		{
			name:  "org and repo",
			input: "acme/widgets",
			want:  RepoRef{OrgID: "acme", Name: "widgets"},
		},
		{
			name:  "strips .git suffix",
			input: "acme/widgets.git",
			want:  RepoRef{OrgID: "acme", Name: "widgets"},
		},
		{
			name:  "leading slash",
			input: "/acme/widgets.git",
			want:  RepoRef{OrgID: "acme", Name: "widgets"},
		},
		{
			name:  "trailing slash",
			input: "acme/widgets/",
			want:  RepoRef{OrgID: "acme", Name: "widgets"},
		},
		{
			name:    "single segment",
			input:   "widgets.git",
			wantErr: ErrInvalidPath,
		},
		{
			name:    "three segments",
			input:   "acme/team/widgets.git",
			wantErr: ErrInvalidPath,
		},
		{
			name:    "empty",
			input:   "",
			wantErr: ErrInvalidPath,
		},
		{
			name:    "empty org",
			input:   "/widgets.git",
			wantErr: ErrInvalidPath,
		},
		{
			name:    "name is only .git",
			input:   "acme/.git",
			wantErr: ErrInvalidPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Parse(%q) error = %v, want %v", tt.input, err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if got != tt.want {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestRepoRefPath(t *testing.T) {
	got := RepoRef{OrgID: "acme", Name: "widgets"}.Path()
	if want := "acme/widgets"; got != want {
		t.Errorf("Path() = %q, want %q", got, want)
	}
}

// TestBucketResolverChroots verifies the default resolver roots the returned
// filesystem at ref.Path(): a file written through it lands under "orgID/name".
func TestBucketResolverChroots(t *testing.T) {
	base := memfs.New()
	r := BucketResolver{Base: base}

	fs, err := r.Resolve(context.Background(), RepoRef{OrgID: "acme", Name: "widgets"}, Credential{}, false)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if _, err := fs.Create("HEAD"); err != nil {
		t.Fatalf("Create through resolved fs: %v", err)
	}
	if _, err := base.Stat("acme/widgets/HEAD"); err != nil {
		t.Errorf("expected file at acme/widgets/HEAD on base fs: %v", err)
	}
}
