package tigrisfs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/tigrisdata/objgit/internal/repofs"
	"github.com/tigrisdata/objgit/internal/s3fs"
)

// fakeClient satisfies the tigrisfs client interface: the embedded s3fs.S3Client
// supplies the (unused) object methods, and the bucket methods are observable.
type fakeClient struct {
	s3fs.S3Client // nil; object methods are never called in these tests
	mu            sync.Mutex
	createCalls   int
	headCalls     int
	exists        bool
}

func (f *fakeClient) CreateBucket(_ context.Context, _ *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	f.exists = true
	return &s3.CreateBucketOutput{}, nil
}

func (f *fakeClient) HeadBucket(_ context.Context, _ *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.headCalls++
	if f.exists {
		return &s3.HeadBucketOutput{}, nil
	}
	return nil, &types.NotFound{}
}

func TestBucketName(t *testing.T) {
	tests := []struct {
		name string
		ref  repofs.RepoRef
	}{
		{"simple", repofs.RepoRef{OrgID: "acme", Name: "widgets"}},
		{"other org", repofs.RepoRef{OrgID: "globex", Name: "widgets"}},
		{"other repo", repofs.RepoRef{OrgID: "acme", Name: "gadgets"}},
	}

	seen := map[string]string{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bucketName(tt.ref)

			if !strings.HasPrefix(got, "objgit-") {
				t.Errorf("bucketName(%v) = %q, want objgit- prefix", tt.ref, got)
			}
			if len(got) < 3 || len(got) > 63 {
				t.Errorf("bucketName(%v) length = %d, want within [3,63]", tt.ref, len(got))
			}
			for _, c := range got {
				if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
					t.Errorf("bucketName(%v) = %q has invalid bucket char %q", tt.ref, got, c)
				}
			}
			// Deterministic and collision-free across distinct refs.
			if again := bucketName(tt.ref); again != got {
				t.Errorf("bucketName not deterministic: %q != %q", got, again)
			}
			if other, ok := seen[got]; ok {
				t.Errorf("bucketName collision: %v and %s both map to %q", tt.ref, other, got)
			}
			seen[got] = tt.ref.OrgID + "/" + tt.ref.Name
		})
	}
}

func TestResolveRequiresCredential(t *testing.T) {
	r := New()
	for _, cred := range []repofs.Credential{
		{},
		{Username: "key"},
		{Password: "secret"},
	} {
		_, err := r.Resolve(context.Background(), repofs.RepoRef{OrgID: "acme", Name: "widgets"}, cred, true)
		if !errors.Is(err, repofs.ErrUnauthenticated) {
			t.Errorf("Resolve(cred=%+v) err = %v, want repofs.ErrUnauthenticated", cred, err)
		}
	}
}

func TestResolveCreateGating(t *testing.T) {
	cred := repofs.Credential{Username: "key", Password: "secret"}
	ref := repofs.RepoRef{OrgID: "acme", Name: "widgets"}

	t.Run("push creates bucket", func(t *testing.T) {
		fake := &fakeClient{}
		r := newWithClient(fake)

		if _, err := r.Resolve(context.Background(), ref, cred, true); err != nil {
			t.Fatalf("Resolve(create=true): %v", err)
		}
		if fake.createCalls != 1 {
			t.Errorf("CreateBucket calls = %d, want 1", fake.createCalls)
		}
		if fake.headCalls != 0 {
			t.Errorf("HeadBucket calls = %d, want 0 on the write path", fake.headCalls)
		}
	})

	t.Run("read of missing bucket is not-found", func(t *testing.T) {
		fake := &fakeClient{}
		r := newWithClient(fake)

		_, err := r.Resolve(context.Background(), ref, cred, false)
		if !errors.Is(err, transport.ErrRepositoryNotFound) {
			t.Fatalf("Resolve(create=false) err = %v, want ErrRepositoryNotFound", err)
		}
		if fake.headCalls != 1 {
			t.Errorf("HeadBucket calls = %d, want 1", fake.headCalls)
		}
		if fake.createCalls != 0 {
			t.Errorf("CreateBucket calls = %d, want 0 on the read path", fake.createCalls)
		}
	})

	t.Run("read of existing bucket succeeds and client is cached", func(t *testing.T) {
		fake := &fakeClient{}
		var builds int
		r := New()
		r.newClient = func(context.Context, repofs.Credential) (client, error) {
			builds++
			return fake, nil
		}

		// Push to create the bucket, then read it back.
		if _, err := r.Resolve(context.Background(), ref, cred, true); err != nil {
			t.Fatalf("Resolve(create=true): %v", err)
		}
		if _, err := r.Resolve(context.Background(), ref, cred, false); err != nil {
			t.Fatalf("Resolve(create=false) after create: %v", err)
		}
		if builds != 1 {
			t.Errorf("newClient builds = %d, want 1 (client cached per keypair)", builds)
		}
	})
}

// newWithClient returns a Resolver whose newClient always yields the given fake.
func newWithClient(c client) *Resolver {
	r := New()
	r.newClient = func(context.Context, repofs.Credential) (client, error) { return c, nil }
	return r
}
