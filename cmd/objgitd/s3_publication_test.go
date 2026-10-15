package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-billy/v6/memfs"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/repofs"
	"github.com/tigrisdata/objgit/internal/storage/tigris"
)

// publicationS3 is an HTTP S3 fixture, so these tests exercise the real SDK,
// Tigris uploader, reference transaction, transports, and hook dispatcher.
type publicationS3 struct {
	mu                     sync.Mutex
	objects                map[string][]byte
	etags                  map[string]string
	seq                    int
	binStarted, cueStarted chan struct{}
	bins, cues             chan struct{}
	binOnce, cueOnce       sync.Once
	fail                   string
}

func (s *publicationS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	if r.Method == http.MethodPut && (strings.HasSuffix(key, ".bin") || strings.HasSuffix(key, ".cue")) {
		if strings.HasSuffix(key, ".bin") {
			s.binOnce.Do(func() { close(s.binStarted) })
			select {
			case <-s.bins:
			case <-r.Context().Done():
				return
			}
		} else {
			s.cueOnce.Do(func() { close(s.cueStarted) })
			select {
			case <-s.cues:
			case <-r.Context().Done():
				return
			}
		}
		if s.fail != "" && strings.HasSuffix(key, s.fail) {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, "<Error><Code>AccessDenied</Code><Message>injected upload failure</Message></Error>")
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		type entry struct {
			Key  string
			Size int
		}
		out := struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			IsTruncated bool
			Contents    []entry
		}{}
		keys := make([]string, 0, len(s.objects))
		for k := range s.objects {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if strings.HasPrefix(k, r.URL.Query().Get("prefix")) {
				out.Contents = append(out.Contents, entry{k, len(s.objects[k])})
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(out)
		return
	}
	body, exists := s.objects[key]
	switch r.Method {
	case http.MethodPut:
		if (r.Header.Get("If-None-Match") == "*" && exists) || (r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != s.etags[key]) {
			w.WriteHeader(412)
			fmt.Fprint(w, "<Error><Code>PreconditionFailed</Code></Error>")
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.seq++
		s.objects[key] = data
		s.etags[key] = fmt.Sprintf("\"%d\"", s.seq)
		w.Header().Set("ETag", s.etags[key])
	case http.MethodGet, http.MethodHead:
		if !exists {
			w.WriteHeader(404)
			fmt.Fprint(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		w.Header().Set("ETag", s.etags[key])
		if rng := r.Header.Get("Range"); rng != "" {
			var start, end int
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= len(body) {
				w.WriteHeader(416)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
			body = body[start : end+1]
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(206)
		} else {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		}
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	case http.MethodDelete:
		delete(s.objects, key)
		delete(s.etags, key)
		w.WriteHeader(204)
	default:
		http.Error(w, "unexpected S3 operation", 400)
	}
}

type publicationLog struct {
	syncBuffer
	waiting chan struct{}
	once    sync.Once
}

func (l *publicationLog) Write(p []byte) (int, error) {
	n, err := l.syncBuffer.Write(p)
	if bytes.Contains(p, []byte("waiting for packfiles to finish pushing")) {
		l.once.Do(func() { close(l.waiting) })
	}
	return n, err
}

func TestTigrisPushPublication(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	for _, tt := range []struct{ name, transport, fail string }{
		{"http success", "http", ""}, {"http bin failure", "http", ".bin"}, {"http cue failure", "http", ".cue"},
		{"ssh success", "ssh", ""}, {"ssh bin failure", "ssh", ".bin"}, {"ssh cue failure", "ssh", ".cue"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.transport == "ssh" {
				checkSSHBinaries(t)
			}
			backend := &publicationS3{objects: map[string][]byte{}, etags: map[string]string{}, binStarted: make(chan struct{}), cueStarted: make(chan struct{}), bins: make(chan struct{}), cues: make(chan struct{}), fail: tt.fail}
			var binRelease, cueRelease sync.Once
			releaseBins := func() { binRelease.Do(func() { close(backend.bins) }) }
			releaseCues := func() { cueRelease.Do(func() { close(backend.cues) }) }
			// Release blocked requests before closing the HTTP fixture, even on failure.
			endpoint := httptest.NewServer(backend)
			defer endpoint.Close()
			defer releaseBins()
			defer releaseCues()
			client := s3.New(s3.Options{Region: "auto", BaseEndpoint: aws.String(endpoint.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, RetryMaxAttempts: 1})
			base, err := tigris.New(context.Background(), "bucket", tigris.WithS3Client(client))
			if err != nil {
				t.Fatal(err)
			}
			logs := &publicationLog{waiting: make(chan struct{})}
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
			defer slog.SetDefault(prev)
			d := &daemon{sysFS: memfs.New(), resolver: repofs.BucketResolver{Base: tigrisBase{s: base}}, authz: auth.AllowAnonymous{AllowWrite: true}, allowHooks: true, hookTimeout: 10 * time.Second}
			var remote string
			var gitEnv []string
			if tt.transport == "http" {
				server := httptest.NewServer(d.httpHandler())
				defer server.Close()
				remote = server.URL + "/test/publication.git"
			} else {
				server, err := newSSHServer(d, "")
				if err != nil {
					t.Fatal(err)
				}
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				go server.Serve(ln)
				defer server.Close()
				remote = "ssh://git@" + ln.Addr().String() + "/test/publication.git"
				gitEnv = gitSSHEnv(t)
			}
			defer releaseBins()
			defer releaseCues()
			git := func(dir string, args ...string) (string, error) {
				if tt.transport == "ssh" {
					return gitWithEnv(dir, gitEnv, args...)
				}
				return tryGit(dir, args...)
			}
			work := seedRepo(t)
			writeFile(t, filepath.Join(work, ".objgit", "hooks", "receive-pack"), "echo publication-hook-ran\n")
			writeFile(t, filepath.Join(work, "payload"), strings.Repeat("pack content\n", 1000))
			runGit(t, work, "add", ".")
			runGit(t, work, "commit", "-m", "hook fixture")
			type result struct {
				out string
				err error
			}
			done := make(chan result, 1)
			go func() { out, err := git(work, "push", remote, "main"); done <- result{out, err} }()
			wait := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case res := <-done:
					t.Fatalf("push ended before gate: %v\n%s", res.err, res.out)
				case <-time.After(15 * time.Second):
					t.Fatal("upload did not reach gate")
				}
			}
			wait(backend.binStarted)
			wait(logs.waiting)
			assertNoHook := func() {
				t.Helper()
				if strings.Contains(logs.String(), "hook: running") {
					t.Fatal("hook ran before uploads completed")
				}
			}
			assertNoHook()
			select {
			case <-backend.cueStarted:
				t.Fatal("cue started before bin completed")
			default:
			}
			releaseBins()
			if tt.fail != ".bin" {
				wait(backend.cueStarted)
				assertNoHook()
			}
			releaseCues()
			var res result
			select {
			case res = <-done:
			case <-time.After(15 * time.Second):
				t.Fatal("push did not finish")
			}
			if tt.fail != "" {
				if res.err == nil {
					t.Fatalf("failed upload accepted: %s", res.out)
				}
				assertNoHook()
				out, err := git(work, "ls-remote", remote, "refs/heads/main")
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(out, "\trefs/heads/main") {
					t.Fatalf("failed upload published main: %s", out)
				}
				return
			}
			if res.err != nil {
				t.Fatalf("push: %v\n%s", res.err, res.out)
			}
			if !strings.Contains(res.out, "publication-hook-ran") {
				t.Fatalf("hook output missing: %s", res.out)
			}
			assertPackWaitBeforeHook(t, logs.String())
			clone := filepath.Join(t.TempDir(), "clone")
			if out, err := git(work, "clone", "--depth=1", "--no-checkout", remote, clone); err != nil {
				t.Fatalf("cold clone: %v\n%s\nlogs:\n%s", err, out, logs.String())
			}
			if got, want := runGit(t, clone, "rev-parse", "HEAD"), runGit(t, work, "rev-parse", "HEAD"); got != want {
				t.Fatalf("cloned tip=%s want=%s", got, want)
			}
			runGit(t, clone, "fsck", "--full")
			writeFile(t, filepath.Join(work, "second.txt"), "second push\n")
			runGit(t, work, "add", "second.txt")
			runGit(t, work, "commit", "-m", "second push")
			if out, err := git(work, "push", remote, "main"); err != nil {
				t.Fatalf("second push: %v\n%s", err, out)
			}
			if out, err := git(clone, "fetch", "--depth=1", "origin", "main"); err != nil {
				t.Fatalf("shallow fetch: %v\n%s", err, out)
			}
			if got, want := runGit(t, clone, "rev-parse", "FETCH_HEAD"), runGit(t, work, "rev-parse", "HEAD"); got != want {
				t.Fatalf("fetched tip=%s want=%s", got, want)
			}
			runGit(t, clone, "fsck", "--full")
		})
	}
}
