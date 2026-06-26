package s3fs

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Stale keep-alive connections are the failure this guards against. The S3
// client storage-go builds uses the AWS SDK default transport: IdleConnTimeout
// 90s and — unlike storage-go's own bundle client — no ResponseHeaderTimeout.
// Tigris (or an intermediary/NAT) silently drops idle connections well before
// 90s, so a pooled connection can be dead while the client still believes it is
// usable. A request written to such a connection is never answered; with no
// response-header deadline and the call's context.TODO() carrying no timeout,
// the read blocks forever.
//
// The lazy reader (see file.go) made this acute: where the old eager reader did
// one GetObject per pack file in a burst on fresh connections, the lazy reader
// issues many small GetObjects strung out across go-git's processing — long
// enough for pooled connections to go stale before reuse. A single clone of a
// real repository reliably wedges.
//
// hardenedTimeouts bound those two windows: prune idle connections before
// Tigris does (so they are never reused stale), and fail a stalled reused
// connection fast (so the SDK retryer retries the idempotent request on a fresh
// connection instead of hanging). ResponseHeaderTimeout bounds only the wait
// for response headers, not body streaming, so large pack reads are unaffected.
const (
	hardenedIdleConnTimeout       = 30 * time.Second
	hardenedResponseHeaderTimeout = 30 * time.Second
)

// newHardenedHTTPClient returns a single AWS HTTP client whose transport prunes
// idle connections early and times out the wait for response headers. It must
// be shared across all calls so its connection pool is reused; a per-call client
// would defeat pooling.
func newHardenedHTTPClient() *awshttp.BuildableClient {
	return awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.IdleConnTimeout = hardenedIdleConnTimeout
		t.ResponseHeaderTimeout = hardenedResponseHeaderTimeout
	})
}

// resilientClient wraps an s3Client, injecting a shared hardened HTTP client
// into every request via a per-call option. The embedded s3Client supplies any
// method not overridden below; all of them are, so the option reaches every S3
// round-trip the filesystem makes.
type resilientClient struct {
	s3Client
	opt func(*s3.Options)
}

// Harden wraps a Tigris/S3 client so every request it makes carries an HTTP
// client that fails fast on stale keep-alive connections rather than hanging
// forever. Pass the result to NewS3FS and NewListingCache. See the package
// constants above for the rationale.
//
// It also opts the request/response checksum workflow back to "when required".
// aws-sdk-go-v2 (s3 >= v1.73) defaults to "when supported", which adds a CRC32
// trailing checksum and sends the body with Content-Encoding: aws-chunked. Some
// S3-compatible endpoints mishandle that framing and store an empty or corrupt
// object even though PutObject returns 200 — which surfaces here as go-git
// reading a just-written ref back as zero bytes ("ref file is empty"). Forcing
// the legacy behavior sends a plain body that every S3 implementation accepts.
func Harden(c s3Client) s3Client {
	hc := newHardenedHTTPClient()
	return resilientClient{
		s3Client: c,
		opt: func(o *s3.Options) {
			o.HTTPClient = hc
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		},
	}
}

// withOpt prepends the hardening option so an explicit per-call option still
// takes precedence (later options win in the SDK's option application).
func (c resilientClient) withOpt(opts []func(*s3.Options)) []func(*s3.Options) {
	return append([]func(*s3.Options){c.opt}, opts...)
}

func (c resilientClient) HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return c.s3Client.HeadObject(ctx, in, c.withOpt(opts)...)
}

func (c resilientClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return c.s3Client.GetObject(ctx, in, c.withOpt(opts)...)
}

func (c resilientClient) PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return c.s3Client.PutObject(ctx, in, c.withOpt(opts)...)
}

func (c resilientClient) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return c.s3Client.ListObjectsV2(ctx, in, c.withOpt(opts)...)
}

func (c resilientClient) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return c.s3Client.DeleteObject(ctx, in, c.withOpt(opts)...)
}

func (c resilientClient) RenameObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return c.s3Client.RenameObject(ctx, in, c.withOpt(opts)...)
}

func (c resilientClient) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return c.s3Client.CreateMultipartUpload(ctx, in, c.withOpt(opts)...)
}

func (c resilientClient) UploadPart(ctx context.Context, in *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return c.s3Client.UploadPart(ctx, in, c.withOpt(opts)...)
}

func (c resilientClient) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return c.s3Client.CompleteMultipartUpload(ctx, in, c.withOpt(opts)...)
}
