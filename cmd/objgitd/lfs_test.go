package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/lfs"
	"github.com/tigrisdata/objgit/internal/repofs"
	tstorage "github.com/tigrisdata/storage-go"
)

const (
	testOID  = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	testRepo = "acme/test"
)

// memBucket is an in-memory stand-in for the Tigris bucket. The handler tests
// need a bucket but CI has no credentials, so every LFS request lands here.
type memBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	meta    map[string]map[string]string
	etags   map[string]string
	seq     int
}

func newMemBucket() *memBucket {
	return &memBucket{
		objects: map[string][]byte{},
		meta:    map[string]map[string]string{},
		etags:   map[string]string{},
	}
}

func (m *memBucket) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := aws.ToString(in.Key)
	body, ok := m.objects[key]
	if !ok {
		return nil, &types.NotFound{}
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(body))),
		Metadata:      m.meta[key],
		ETag:          aws.String(m.etags[key]),
	}, nil
}

func (m *memBucket) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := aws.ToString(in.Key)
	body, ok := m.objects[key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: aws.Int64(int64(len(body))),
		ETag:          aws.String(m.etags[key]),
	}, nil
}

func (m *memBucket) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := aws.ToString(in.Key)
	_, exists := m.objects[key]
	if aws.ToString(in.IfNoneMatch) == "*" && exists {
		return nil, errPreconditionFailed
	}
	if want := aws.ToString(in.IfMatch); want != "" && m.etags[key] != want {
		return nil, errPreconditionFailed
	}
	var body []byte
	if in.Body != nil {
		body, _ = io.ReadAll(in.Body)
	}
	m.seq++
	m.objects[key] = body
	m.meta[key] = in.Metadata
	m.etags[key] = string(rune('a'+m.seq%26)) + "-etag"
	return &s3.PutObjectOutput{}, nil
}

// RenameObject models the Tigris extension: the source key is gone afterwards.
func (m *memBucket) RenameObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, src, _ := strings.Cut(aws.ToString(in.CopySource), "/")
	body, ok := m.objects[src]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	key := aws.ToString(in.Key)
	m.seq++
	m.objects[key] = body
	m.meta[key] = m.meta[src]
	m.etags[key] = string(rune('a'+m.seq%26)) + "-etag"
	delete(m.objects, src)
	delete(m.meta, src)
	delete(m.etags, src)
	return &s3.CopyObjectOutput{}, nil
}

func (m *memBucket) DeleteObject(_ context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := aws.ToString(in.Key)
	delete(m.objects, key)
	delete(m.meta, key)
	delete(m.etags, key)
	return &s3.DeleteObjectOutput{}, nil
}

// put seeds an object directly, bypassing conditional writes.
func (m *memBucket) put(key string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	m.objects[key] = body
	m.etags[key] = string(rune('a'+m.seq%26)) + "-etag"
}

// errPreconditionFailed carries the error code a refused conditional write
// reports, which internal/lfs matches to decide whether to retry.
var errPreconditionFailed = &smithy.GenericAPIError{
	Code:    "PreconditionFailed",
	Message: "At least one of the pre-conditions you specified did not hold",
}

// staticLFSTTL is the per-request URL lifetime for a test: a fixed value, with
// no credential to inspect. The daemon builds this from the live credential
// instead (lfsURLTTLFunc).
func staticLFSTTL(d time.Duration) func(context.Context) time.Duration {
	return func(context.Context) time.Duration { return d }
}

// newLFSServer starts an httptest server whose daemon has LFS enabled over an
// in-memory bucket.
func newLFSServer(t *testing.T, allowPush, allowLocks bool) (*httptest.Server, *lfs.Store, *memBucket) {
	t.Helper()

	bucket := newMemBucket()
	client := s3.New(s3.Options{
		Region:       "auto",
		BaseEndpoint: aws.String("https://t3.storage.dev"),
		Credentials: credentials.NewStaticCredentialsProvider(
			"AKIAEXAMPLEEXAMPLE", "secretsecretsecret", ""),
	})
	store := lfs.NewStore(bucket, lfs.NewPresigner(client, "test-bucket"), "test-bucket")

	d := &daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: newMemBase()},
		authz:    auth.AllowAnonymous{AllowWrite: allowPush},
		lfs: &lfsService{
			store:      store,
			ttl:        staticLFSTTL(15 * time.Minute),
			maxSize:    1 << 30,
			maxBatch:   100,
			allowLocks: allowLocks,
		},
	}
	ts := httptest.NewServer(d.httpHandler())
	t.Cleanup(ts.Close)
	return ts, store, bucket
}

// postLFS sends an LFS request and returns the status and decoded body.
func postLFS(t *testing.T, ts *httptest.Server, path string, body any) (int, map[string]any) {
	t.Helper()
	return requestLFS(t, ts, http.MethodPost, path, body)
}

func requestLFS(t *testing.T, ts *httptest.Server, method, path string, body any) (int, map[string]any) {
	t.Helper()

	var r io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encoding request: %v", err)
		}
		r = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, ts.URL+path, r)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Accept", lfs.MediaType)
	if body != nil {
		req.Header.Set("Content-Type", lfs.MediaType)
	}

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}
	out := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decoding response %q: %v", raw, err)
		}
	}
	// Every LFS answer has to be recognizable as one.
	if ct := resp.Header.Get("Content-Type"); ct != lfs.MediaType {
		t.Errorf("Content-Type = %q, want %q", ct, lfs.MediaType)
	}
	return resp.StatusCode, out
}

func TestLFSBatchDownload(t *testing.T) {
	ts, store, bucket := newLFSServer(t, true, true)

	// Seed membership through the real path: a verified upload.
	seedObject(t, store, bucket)

	status, body := postLFS(t, ts, "/acme/test.git/info/lfs/objects/batch", map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": testOID, "size": 4}},
	})

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %v)", status, body)
	}
	objects, _ := body["objects"].([]any)
	if len(objects) != 1 {
		t.Fatalf("got %d objects, want 1", len(objects))
	}
	obj, _ := objects[0].(map[string]any)
	actions, ok := obj["actions"].(map[string]any)
	if !ok {
		t.Fatalf("object has no actions: %v", obj)
	}
	if _, ok := actions["download"]; !ok {
		t.Errorf("actions = %v, want a download entry", actions)
	}
}

// TestLFSBatchReportsAMissingObjectInsideA200 pins the protocol rule that a
// per-object failure is not a request failure.
func TestLFSBatchReportsAMissingObjectInsideA200(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, true)

	status, body := postLFS(t, ts, "/acme/test.git/info/lfs/objects/batch", map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": testOID, "size": 4}},
	})

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	objects, _ := body["objects"].([]any)
	obj, _ := objects[0].(map[string]any)
	objErr, ok := obj["error"].(map[string]any)
	if !ok {
		t.Fatalf("object has no error: %v", obj)
	}
	if code, _ := objErr["code"].(float64); int(code) != http.StatusNotFound {
		t.Errorf("object error code = %v, want 404", objErr["code"])
	}
}

func TestLFSBatchUploadDeniedWithoutPush(t *testing.T) {
	ts, _, _ := newLFSServer(t, false, true)

	status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/objects/batch", map[string]any{
		"operation": "upload",
		"objects":   []map[string]any{{"oid": testOID, "size": 4}},
	})

	// A denied write is 403. The git path answers 401 here, but an LFS client
	// reads 401 as "retry with credentials" and loops.
	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", status)
	}
}

func TestLFSBatchDownloadAllowedWithoutPush(t *testing.T) {
	ts, _, _ := newLFSServer(t, false, true)

	status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/objects/batch", map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": testOID, "size": 4}},
	})

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
}

func TestLFSBatchRejects(t *testing.T) {
	for _, tt := range []struct {
		name string
		body map[string]any
		want int
	}{
		{
			name: "malformed oid",
			body: map[string]any{
				"operation": "download",
				"objects":   []map[string]any{{"oid": "../../etc/passwd", "size": 4}},
			},
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "unknown operation",
			body: map[string]any{
				"operation": "sideways",
				"objects":   []map[string]any{{"oid": testOID, "size": 4}},
			},
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "unsupported transfer",
			body: map[string]any{
				"operation": "download",
				"transfers": []string{"tus"},
				"objects":   []map[string]any{{"oid": testOID, "size": 4}},
			},
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "object over the size cap",
			body: map[string]any{
				"operation": "upload",
				"objects":   []map[string]any{{"oid": testOID, "size": 1<<30 + 1}},
			},
			want: http.StatusUnprocessableEntity,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ts, _, _ := newLFSServer(t, true, true)
			status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/objects/batch", tt.body)
			if status != tt.want {
				t.Fatalf("status = %d, want %d", status, tt.want)
			}
		})
	}
}

func TestLFSBatchRejectsAnOversizedBatch(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, true)

	objects := make([]map[string]any, 101)
	for i := range objects {
		objects[i] = map[string]any{"oid": testOID, "size": 4}
	}

	status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/objects/batch", map[string]any{
		"operation": "download",
		"objects":   objects,
	})

	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", status)
	}
}

func TestLFSBatchRejectsAMalformedBody(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, true)

	req, err := http.NewRequest(http.MethodPost,
		ts.URL+"/acme/test.git/info/lfs/objects/batch", strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Accept", lfs.MediaType)

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("posting: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
}

func TestLFSRejectsAnUnacceptableClient(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, true)

	req, err := http.NewRequest(http.MethodPost,
		ts.URL+"/acme/test.git/info/lfs/objects/batch", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Accept", "text/plain")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("posting: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotAcceptable {
		t.Fatalf("status = %d, want 406", resp.StatusCode)
	}
}

// TestLFSRoutesAreAbsentWhenDisabled is what -allow-lfs off has to look like
// from outside: a plain 404, which both git-lfs and the locking spec read as
// "this server has no LFS".
func TestLFSRoutesAreAbsentWhenDisabled(t *testing.T) {
	d := &daemon{
		sysFS:    memfs.New(),
		resolver: repofs.BucketResolver{Base: newMemBase()},
		authz:    auth.AllowAnonymous{AllowWrite: true},
	}
	ts := httptest.NewServer(d.httpHandler())
	t.Cleanup(ts.Close)

	for _, path := range []string{
		"/acme/test.git/info/lfs/objects/batch",
		"/acme/test.git/info/lfs/objects/verify",
		"/acme/test.git/info/lfs/locks",
		"/acme/test.git/info/lfs/locks/verify",
		"/acme/test.git/info/lfs/locks/abc/unlock",
	} {
		t.Run(path, func(t *testing.T) {
			resp, err := ts.Client().Post(ts.URL+path, lfs.MediaType, strings.NewReader("{}"))
			if err != nil {
				t.Fatalf("posting: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("status = %d, want 404", resp.StatusCode)
			}
		})
	}
}

func TestLFSVerify(t *testing.T) {
	for _, tt := range []struct {
		name string
		seed func(*testing.T, *memBucket)
		size int
		want int
	}{
		{
			name: "object is stored at the declared size",
			seed: func(_ *testing.T, b *memBucket) { b.put(lfs.StagingKey(testRepo, testOID), []byte("test")) },
			size: 4,
			want: http.StatusOK,
		},
		{
			name: "nothing was staged",
			seed: func(*testing.T, *memBucket) {},
			size: 4,
			want: http.StatusNotFound,
		},
		{
			name: "object is stored at another size",
			seed: func(_ *testing.T, b *memBucket) { b.put(lfs.StagingKey(testRepo, testOID), []byte("longer")) },
			size: 4,
			want: http.StatusUnprocessableEntity,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ts, _, bucket := newLFSServer(t, true, true)
			tt.seed(t, bucket)

			status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/objects/verify", map[string]any{
				"oid": testOID, "size": tt.size,
			})
			if status != tt.want {
				t.Fatalf("status = %d, want %d", status, tt.want)
			}
		})
	}
}

// TestLFSVerifyGrantsMembership is the join between upload and download: until
// verify runs, the repository does not hold the object and a download 404s.
func TestLFSVerifyGrantsMembership(t *testing.T) {
	ts, _, bucket := newLFSServer(t, true, true)
	// Stand in for the client's presigned upload, which lands in this
	// repository's own staging area.
	bucket.put(lfs.StagingKey(testRepo, testOID), []byte("test"))

	batch := map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": testOID, "size": 4}},
	}

	_, before := postLFS(t, ts, "/acme/test.git/info/lfs/objects/batch", batch)
	if !objectHasError(before) {
		t.Fatal("an unverified object must not be downloadable")
	}

	status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/objects/verify", map[string]any{
		"oid": testOID, "size": 4,
	})
	if status != http.StatusOK {
		t.Fatalf("verify status = %d, want 200", status)
	}

	_, after := postLFS(t, ts, "/acme/test.git/info/lfs/objects/batch", batch)
	if objectHasError(after) {
		t.Fatal("a verified object must be downloadable")
	}
}

func objectHasError(body map[string]any) bool {
	objects, _ := body["objects"].([]any)
	if len(objects) == 0 {
		return true
	}
	obj, _ := objects[0].(map[string]any)
	_, has := obj["error"]
	return has
}

func TestLFSLocks(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, true)

	status, body := postLFS(t, ts, "/acme/test.git/info/lfs/locks", map[string]any{
		"path": "big.bin",
	})
	if status != http.StatusCreated {
		t.Fatalf("create status = %d, want 201 (body %v)", status, body)
	}
	lock, _ := body["lock"].(map[string]any)
	id, _ := lock["id"].(string)
	if id == "" {
		t.Fatalf("created lock has no id: %v", body)
	}

	// A second lock on the same path is a conflict that names the holder.
	status, body = postLFS(t, ts, "/acme/test.git/info/lfs/locks", map[string]any{
		"path": "big.bin",
	})
	if status != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", status)
	}
	if held, _ := body["lock"].(map[string]any); held["id"] != id {
		t.Errorf("conflict names lock %v, want %q", held["id"], id)
	}

	status, body = requestLFS(t, ts, http.MethodGet, "/acme/test.git/info/lfs/locks", nil)
	if status != http.StatusOK {
		t.Fatalf("list status = %d, want 200", status)
	}
	if locks, _ := body["locks"].([]any); len(locks) != 1 {
		t.Fatalf("listed %d locks, want 1", len(locks))
	}

	status, _ = postLFS(t, ts, "/acme/test.git/info/lfs/locks/"+id+"/unlock", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("unlock status = %d, want 200", status)
	}

	_, body = requestLFS(t, ts, http.MethodGet, "/acme/test.git/info/lfs/locks", nil)
	if locks, _ := body["locks"].([]any); len(locks) != 0 {
		t.Fatalf("listed %d locks after unlock, want 0", len(locks))
	}
}

// TestLFSLocksVerifySplitsOwnership is the call git-lfs makes on every push. An
// anonymous deployment has one owner, so every lock is "ours" and no push is
// ever blocked.
func TestLFSLocksVerifySplitsOwnership(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, true)

	if status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/locks", map[string]any{
		"path": "big.bin",
	}); status != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", status)
	}

	status, body := postLFS(t, ts, "/acme/test.git/info/lfs/locks/verify", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("verify status = %d, want 200", status)
	}

	ours, _ := body["ours"].([]any)
	theirs, _ := body["theirs"].([]any)
	if len(ours) != 1 {
		t.Errorf("ours = %d locks, want 1", len(ours))
	}
	if len(theirs) != 0 {
		t.Errorf("theirs = %d locks, want 0; a same-owner lock must never block a push", len(theirs))
	}
}

func TestLFSLocksAreAbsentWhenDisabled(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, false)

	for _, path := range []string{
		"/acme/test.git/info/lfs/locks",
		"/acme/test.git/info/lfs/locks/verify",
		"/acme/test.git/info/lfs/locks/abc/unlock",
	} {
		resp, err := ts.Client().Post(ts.URL+path, lfs.MediaType, strings.NewReader("{}"))
		if err != nil {
			t.Fatalf("posting %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404", path, resp.StatusCode)
		}
	}
}

func TestLFSLockCreateDeniedWithoutPush(t *testing.T) {
	ts, _, _ := newLFSServer(t, false, true)

	status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/locks", map[string]any{"path": "big.bin"})
	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", status)
	}
}

// seedObject stores a blob and verifies it, which is the only way a repository
// comes to hold an object.
func seedObject(t *testing.T, store *lfs.Store, bucket *memBucket) {
	t.Helper()
	bucket.put(lfs.StagingKey(testRepo, testOID), []byte("test"))

	status, err := store.Verify(context.Background(), testRepo, testOID, 4)
	if err != nil {
		t.Fatalf("seeding object: %v", err)
	}
	if status != lfs.VerifyOK {
		t.Fatalf("seeding object: verify status %v, want ok", status)
	}
}

// TestLFSVerifyCannotStealAnotherRepositorysObject is the end-to-end form of
// the read-oracle attack. An oid and its size sit side by side in every pointer
// file, so knowing both must not be enough to join a repository to an object.
func TestLFSVerifyCannotStealAnotherRepositorysObject(t *testing.T) {
	ts, store, bucket := newLFSServer(t, true, true)

	// A victim repository uploads and verifies an object properly.
	bucket.put(lfs.StagingKey("victim/private", testOID), []byte("test"))
	if _, err := store.Verify(context.Background(), "victim/private", testOID, 4); err != nil {
		t.Fatalf("seeding the victim: %v", err)
	}

	// The attacker uploads nothing and claims the object in its own repository.
	status, _ := postLFS(t, ts, "/attacker/scratch.git/info/lfs/objects/verify", map[string]any{
		"oid": testOID, "size": 4,
	})
	if status != http.StatusNotFound {
		t.Fatalf("verify status = %d, want 404; the attacker uploaded nothing", status)
	}

	// And still cannot download it.
	_, body := postLFS(t, ts, "/attacker/scratch.git/info/lfs/objects/batch", map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": testOID, "size": 4}},
	})
	if !objectHasError(body) {
		t.Fatal("the attacker obtained a download URL for another repository's object")
	}
}

// TestLFSBatchCapsTheRequestBody keeps an unauthenticated caller from making
// the daemon buffer an arbitrary body. The batch body is decoded before
// authorization, because the operation inside it decides which access is
// needed, so the cap has to come first.
func TestLFSBatchCapsTheRequestBody(t *testing.T) {
	ts, _, _ := newLFSServer(t, false, true)

	// Far larger than maxBodyBytes for the configured batch cap.
	huge := `{"operation":"download","objects":[` +
		strings.Repeat(`{"oid":"`+testOID+`","size":1},`, 20000) +
		`{"oid":"` + testOID + `","size":1}]}`

	req, err := http.NewRequest(http.MethodPost,
		ts.URL+"/acme/test.git/info/lfs/objects/batch", strings.NewReader(huge))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Accept", lfs.MediaType)
	req.Header.Set("Content-Type", lfs.MediaType)

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("posting: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Fatal("an oversized body was accepted")
	}
}

// postRawLFS sends a body verbatim. The shared helpers encode a Go value, which
// cannot express a body that is deliberately too large to decode.
func postRawLFS(t *testing.T, ts *httptest.Server, path, body string) int {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Accept", lfs.MediaType)
	req.Header.Set("Content-Type", lfs.MediaType)

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// TestLFSHandlersCapTheRequestBody covers every LFS route that reads a body.
// The batch route capped its own from the start; the rest read theirs whole,
// which let an anonymous caller hand the daemon as much JSON as it liked.
func TestLFSHandlersCapTheRequestBody(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, true)

	// Just past the cap for the batch size newLFSServer configures. Kept small
	// on purpose: a body large enough to fill the socket buffer would race the
	// server's early response against the client still writing.
	svc := &lfsService{maxBatch: 100}
	filler := strings.Repeat("a", int(svc.maxBodyBytes())+4096)

	for _, tt := range []struct {
		name string
		path string
		body string
	}{
		{
			name: "batch",
			path: "/acme/test.git/info/lfs/objects/batch",
			body: `{"operation":"download","padding":"` + filler + `","objects":[]}`,
		},
		{
			name: "verify",
			path: "/acme/test.git/info/lfs/objects/verify",
			body: `{"oid":"` + testOID + `","size":4,"padding":"` + filler + `"}`,
		},
		{
			name: "lock create",
			path: "/acme/test.git/info/lfs/locks",
			body: `{"path":"` + filler + `"}`,
		},
		{
			name: "lock verify",
			path: "/acme/test.git/info/lfs/locks/verify",
			body: `{"cursor":"` + filler + `"}`,
		},
		{
			name: "unlock",
			path: "/acme/test.git/info/lfs/locks/deadbeef/unlock",
			body: `{"force":true,"padding":"` + filler + `"}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if status := postRawLFS(t, ts, tt.path, tt.body); status != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, want 413", status)
			}
		})
	}
}

// TestLFSLockRejectsAnOversizedPath keeps an unbounded string out of the lock
// document. The path is stored verbatim and the document is read whole on every
// push, so one accepted giant path would slow every later push to the
// repository for good.
func TestLFSLockRejectsAnOversizedPath(t *testing.T) {
	ts, _, _ := newLFSServer(t, true, true)

	status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/locks", map[string]any{
		"path": strings.Repeat("a", maxLockPathBytes+1),
	})
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", status)
	}

	// And nothing was written.
	_, body := requestLFS(t, ts, http.MethodGet, "/acme/test.git/info/lfs/locks", nil)
	if locks, _ := body["locks"].([]any); len(locks) != 0 {
		t.Fatalf("listed %d locks, want 0", len(locks))
	}
}

// TestLFSLockRejectsPastTheRepositoryCap bounds how many locks one repository
// can accumulate, for the same reason: every push reads the whole document.
func TestLFSLockRejectsPastTheRepositoryCap(t *testing.T) {
	ts, _, bucket := newLFSServer(t, true, true)
	seedLocks(t, bucket, maxLocksPerRepo)

	status, _ := postLFS(t, ts, "/acme/test.git/info/lfs/locks", map[string]any{
		"path": "one-too-many.bin",
	})
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", status)
	}

	// A path that is already locked still answers with the conflict that names
	// its holder, rather than the cap.
	status, body := postLFS(t, ts, "/acme/test.git/info/lfs/locks", map[string]any{
		"path": "seeded/0.bin",
	})
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %v)", status, body)
	}
}

// TestLFSURLTTLFollowsTheCredential pins that the presigned-URL lifetime is
// answered per request and not frozen at startup. A daemon that happens to
// start while a temporary credential is nearly expired would otherwise sign
// every URL for its whole life against the one-minute floor, long after the SDK
// replaced that credential.
func TestLFSURLTTLFollowsTheCredential(t *testing.T) {
	const want = 15 * time.Minute

	expires := time.Now().Add(30 * time.Second)
	client := &tstorage.Client{Client: s3.New(s3.Options{
		Region: "auto",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "AKIAEXAMPLEEXAMPLE",
				SecretAccessKey: "secretsecretsecret",
				CanExpire:       true,
				Expires:         expires,
			}, nil
		}),
	})}

	ttl := lfsURLTTLFunc(client, want)
	if got := ttl(t.Context()); got != time.Minute {
		t.Fatalf("ttl on a nearly expired credential = %s, want the 1m floor", got)
	}

	// The SDK refreshes the credential. The next request has to see that.
	expires = time.Now().Add(time.Hour)
	if got := ttl(t.Context()); got != want {
		t.Fatalf("ttl after the credential was refreshed = %s, want %s", got, want)
	}
}

// seedLocks writes a lock document holding n locks straight into the bucket.
// Creating them one request at a time would re-encode a growing document a
// thousand times over.
func seedLocks(t *testing.T, bucket *memBucket, n int) {
	t.Helper()

	doc := struct {
		Locks []lfs.Lock `json:"locks"`
	}{Locks: make([]lfs.Lock, n)}
	for i := range doc.Locks {
		doc.Locks[i] = lfs.Lock{
			ID:       fmt.Sprintf("%032x", i),
			Path:     fmt.Sprintf("seeded/%d.bin", i),
			LockedAt: time.Now().UTC(),
			Owner:    lfs.Owner{Name: lfs.AnonymousOwner},
		}
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("encoding seeded lock document: %v", err)
	}
	bucket.put(lfs.LocksKey(testRepo), raw)
}
