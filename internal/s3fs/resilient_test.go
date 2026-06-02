package s3fs

import (
	"context"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// optRecorder is an s3Client that records the per-call option functions it
// receives for each method, so a test can confirm Harden injects its hardened
// HTTP client on every S3 round-trip. It implements every s3Client method
// directly (see below), so it needs no embedded delegate.
type optRecorder struct {
	last []func(*s3.Options)
}

func (r *optRecorder) HeadObject(_ context.Context, _ *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	r.last = opts
	return &s3.HeadObjectOutput{}, nil
}

func (r *optRecorder) GetObject(_ context.Context, _ *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	r.last = opts
	return &s3.GetObjectOutput{}, nil
}

func (r *optRecorder) PutObject(_ context.Context, _ *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	r.last = opts
	return &s3.PutObjectOutput{}, nil
}

func (r *optRecorder) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	r.last = opts
	return &s3.ListObjectsV2Output{}, nil
}

func (r *optRecorder) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	r.last = opts
	return &s3.DeleteObjectOutput{}, nil
}

func (r *optRecorder) RenameObject(_ context.Context, _ *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	r.last = opts
	return &s3.CopyObjectOutput{}, nil
}

func (r *optRecorder) CreateMultipartUpload(_ context.Context, _ *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	r.last = opts
	return &s3.CreateMultipartUploadOutput{}, nil
}

func (r *optRecorder) UploadPart(_ context.Context, _ *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	r.last = opts
	return &s3.UploadPartOutput{}, nil
}

func (r *optRecorder) CompleteMultipartUpload(_ context.Context, _ *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	r.last = opts
	return &s3.CompleteMultipartUploadOutput{}, nil
}

// TestHardenInjectsHardenedHTTPClient verifies that every method on a Harden-ed
// client carries the hardened HTTP client (bounded ResponseHeaderTimeout and a
// reduced IdleConnTimeout) into its per-call options. Without this, S3 requests
// run on the AWS default transport, whose stale keep-alive connections hang
// forever — the clone-hang root cause this fix addresses.
func TestHardenInjectsHardenedHTTPClient(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		invoke func(c s3Client) error
	}{
		{"HeadObject", func(c s3Client) error { _, err := c.HeadObject(ctx, &s3.HeadObjectInput{}); return err }},
		{"GetObject", func(c s3Client) error { _, err := c.GetObject(ctx, &s3.GetObjectInput{}); return err }},
		{"PutObject", func(c s3Client) error { _, err := c.PutObject(ctx, &s3.PutObjectInput{}); return err }},
		{"ListObjectsV2", func(c s3Client) error { _, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{}); return err }},
		{"DeleteObject", func(c s3Client) error { _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{}); return err }},
		{"RenameObject", func(c s3Client) error { _, err := c.RenameObject(ctx, &s3.CopyObjectInput{}); return err }},
		{"CreateMultipartUpload", func(c s3Client) error {
			_, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{})
			return err
		}},
		{"UploadPart", func(c s3Client) error { _, err := c.UploadPart(ctx, &s3.UploadPartInput{}); return err }},
		{"CompleteMultipartUpload", func(c s3Client) error {
			_, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{})
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &optRecorder{}
			if err := tt.invoke(Harden(rec)); err != nil {
				t.Fatalf("%s: %v", tt.name, err)
			}
			if len(rec.last) == 0 {
				t.Fatalf("%s: no per-call options injected; stale connections would hang", tt.name)
			}

			// Apply the recorded options the way the SDK would and inspect the
			// resulting HTTP client's transport.
			var o s3.Options
			for _, fn := range rec.last {
				fn(&o)
			}
			if o.HTTPClient == nil {
				t.Fatalf("%s: HTTPClient not set on options", tt.name)
			}
			bc, ok := o.HTTPClient.(*awshttp.BuildableClient)
			if !ok {
				t.Fatalf("%s: HTTPClient is %T, want *awshttp.BuildableClient", tt.name, o.HTTPClient)
			}
			tr := bc.GetTransport()
			if tr.ResponseHeaderTimeout != hardenedResponseHeaderTimeout {
				t.Errorf("%s: ResponseHeaderTimeout = %v, want %v", tt.name, tr.ResponseHeaderTimeout, hardenedResponseHeaderTimeout)
			}
			if tr.IdleConnTimeout != hardenedIdleConnTimeout {
				t.Errorf("%s: IdleConnTimeout = %v, want %v", tt.name, tr.IdleConnTimeout, hardenedIdleConnTimeout)
			}
		})
	}
}

// TestHardenExplicitOptionWins confirms an explicit per-call option still
// overrides the injected default (later options win), so callers retain control.
func TestHardenExplicitOptionWins(t *testing.T) {
	rec := &optRecorder{}
	override := awshttp.NewBuildableClient()
	_, err := Harden(rec).GetObject(context.Background(), &s3.GetObjectInput{},
		func(o *s3.Options) { o.HTTPClient = override })
	if err != nil {
		t.Fatal(err)
	}

	var o s3.Options
	for _, fn := range rec.last {
		fn(&o)
	}
	if got, _ := o.HTTPClient.(*awshttp.BuildableClient); got != override {
		t.Errorf("explicit HTTPClient did not win: got %p, want %p", got, override)
	}
}
