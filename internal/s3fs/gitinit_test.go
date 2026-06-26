package s3fs

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
)

// byteStub is an in-memory s3Client that actually stores object bytes, so we can
// verify a write/read round-trip (unlike stubClient, which tracks only sizes).
type byteStub struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func newByteStub() *byteStub { return &byteStub{objs: map[string][]byte{}} }

func (s *byteStub) get(k string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objs[k]
	return b, ok
}

func (s *byteStub) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if b, ok := s.get(aws.ToString(in.Key)); ok {
		return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(b))), LastModified: aws.Time(time.Unix(0, 0))}, nil
	}
	return nil, &smithy.GenericAPIError{Code: "NotFound"}
}

func (s *byteStub) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	b, ok := s.get(aws.ToString(in.Key))
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(b)),
		ContentLength: aws.Int64(int64(len(b))),
		LastModified:  aws.Time(time.Unix(0, 0)),
	}, nil
}

func (s *byteStub) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	var buf []byte
	if in.Body != nil {
		b, err := io.ReadAll(in.Body)
		if err != nil {
			return nil, err
		}
		buf = b
	}
	s.mu.Lock()
	s.objs[aws.ToString(in.Key)] = buf
	s.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}

func (s *byteStub) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(in.Prefix)
	delim := aws.ToString(in.Delimiter)
	s.mu.Lock()
	ks := make([]string, 0, len(s.objs))
	for k := range s.objs {
		ks = append(ks, k)
	}
	s.mu.Unlock()
	sort.Strings(ks)

	seen := map[string]bool{}
	out := &s3.ListObjectsV2Output{}
	for _, k := range ks {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k[len(prefix):]
		if delim != "" {
			if i := strings.Index(rest, delim); i >= 0 {
				cp := prefix + rest[:i+1]
				if !seen[cp] {
					seen[cp] = true
					out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(cp)})
				}
				continue
			}
		}
		out.Contents = append(out.Contents, types.Object{Key: aws.String(k), Size: aws.Int64(0)})
	}
	return out, nil
}

func (s *byteStub) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	s.mu.Lock()
	delete(s.objs, aws.ToString(in.Key))
	s.mu.Unlock()
	return &s3.DeleteObjectOutput{}, nil
}

func (s *byteStub) RenameObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	src := strings.TrimPrefix(aws.ToString(in.CopySource), aws.ToString(in.Bucket)+"/")
	if b, ok := s.get(src); ok {
		s.mu.Lock()
		s.objs[aws.ToString(in.Key)] = b
		delete(s.objs, src)
		s.mu.Unlock()
	}
	return &s3.CopyObjectOutput{}, nil
}

func (s *byteStub) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	panic("unexpected multipart upload in init test")
}
func (s *byteStub) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	panic("unexpected multipart upload in init test")
}
func (s *byteStub) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	panic("unexpected multipart upload in init test")
}

// TestInitAtBucketRootAdvertisesRefs reproduces the per-keypair (root="") flow:
// init a bare repo directly at the bucket root, then read HEAD and iterate all
// references the way the receive-pack advertisement does.
//
// Iterating references is the part that regressed: git.Init's MkdirAll creates
// refs/heads and refs/tags directory markers, and if those land as zero-byte
// objects without a trailing slash they list as empty *files*, so go-git reads
// them as references and fails with "ref file is empty". The marker must be a
// directory (trailing-slash key) instead.
func TestInitAtBucketRootAdvertisesRefs(t *testing.T) {
	stub := newByteStub()
	fsys, err := NewS3FS(stub, "repo-bucket")
	if err != nil {
		t.Fatalf("NewS3FS: %v", err)
	}

	st := filesystem.NewStorage(fsys, cache.NewObjectLRUDefault())
	if _, err := git.Init(st, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main"))); err != nil {
		t.Fatalf("git.Init: %v", err)
	}

	head, ok := stub.get("HEAD")
	t.Logf("HEAD object present=%v bytes=%q", ok, string(head))

	ref, err := st.Reference(plumbing.HEAD)
	if err != nil {
		t.Fatalf("read HEAD reference: %v", err)
	}
	if got, want := ref.Target().String(), "refs/heads/main"; got != want {
		t.Errorf("HEAD target = %q, want %q", got, want)
	}

	// The advertisement walks every reference; the directory markers must not be
	// read as empty ref files.
	it, err := st.IterReferences()
	if err != nil {
		t.Fatalf("IterReferences: %v", err)
	}
	defer it.Close()
	if err := it.ForEach(func(*plumbing.Reference) error { return nil }); err != nil {
		t.Fatalf("iterating references (advertisement) failed: %v", err)
	}
}

// TestMkdirAllCreatesDirectoryMarker asserts a created directory lists as a
// directory entry, not a regular file — the property go-git's ref walker needs.
func TestMkdirAllCreatesDirectoryMarker(t *testing.T) {
	stub := newByteStub()
	fsys, err := NewS3FS(stub, "repo-bucket")
	if err != nil {
		t.Fatalf("NewS3FS: %v", err)
	}
	if err := fsys.MkdirAll("refs/heads", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	entries, err := fsys.ReadDir("refs")
	if err != nil {
		t.Fatalf("ReadDir(refs): %v", err)
	}
	var found bool
	for _, e := range entries {
		if e.Name() == "heads" {
			found = true
			if !e.IsDir() {
				t.Errorf(`ReadDir(refs): "heads" is a file, want a directory`)
			}
		}
	}
	if !found {
		t.Errorf(`ReadDir(refs) did not list "heads"; got %v`, entries)
	}
}
