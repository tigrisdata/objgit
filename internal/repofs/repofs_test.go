package repofs

import (
	"context"
	_ "crypto/sha1" // registers SHA-1 for plumbing.FromObjectFormat, used by storage/memory
	"errors"
	"sync"
	"testing"

	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"
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

// fakeBase is a Base that records the prefix each Scoped call received and
// hands back an independent in-memory storer per prefix.
type fakeBase struct {
	mu     sync.Mutex
	calls  []string
	scoped map[string]storage.Storer
}

func (f *fakeBase) Scoped(prefix string) storage.Storer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, prefix)
	if f.scoped == nil {
		f.scoped = map[string]storage.Storer{}
	}
	st, ok := f.scoped[prefix]
	if !ok {
		st = memory.NewStorage()
		f.scoped[prefix] = st
	}
	return st
}

// TestBucketResolverScopesToRefPath verifies the default resolver scopes the
// base to ref.Path(), ignoring the credential and the create flag.
func TestBucketResolverScopesToRefPath(t *testing.T) {
	base := &fakeBase{}
	r := BucketResolver{Base: base}

	st, err := r.Resolve(context.Background(), RepoRef{OrgID: "acme", Name: "widgets"}, Credential{}, false)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(base.calls) != 1 || base.calls[0] != "acme/widgets" {
		t.Fatalf("Scoped calls = %v, want [%q]", base.calls, "acme/widgets")
	}

	// Resolving the same ref again must return the same underlying storer
	// (data persists the way a real bucket's key-prefix scoping does).
	again, err := r.Resolve(context.Background(), RepoRef{OrgID: "acme", Name: "widgets"}, Credential{}, true)
	if err != nil {
		t.Fatalf("Resolve (create): %v", err)
	}
	if st != again {
		t.Error("expected the same storer instance for the same ref across calls")
	}
}
