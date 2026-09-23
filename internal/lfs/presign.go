package lfs

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Presigner mints the URLs a git-lfs client transfers against. Every LFS byte
// moves over one of these, so the daemon carries none of them.
//
// Uploads and downloads are signed by different clients on purpose. See
// NewPresigner.
type Presigner struct {
	// putClient signs uploads with header hoisting off, so the checksum stays
	// a signed header.
	putClient *s3.PresignClient
	// getClient signs downloads with the stock settings, so the URL works on
	// its own with no extra request headers.
	getClient *s3.PresignClient
	bucket    string
}

// noHoistPresigner signs a presigned request without moving headers into the
// query string.
//
// By default the SigV4 presigner hoists any "X-Amz-*" header that is not on its
// required-signed list into the query string (see AllowedQueryHoisting in
// aws/signer/internal/v4/headers.go). "X-Amz-Checksum-Sha256" is not on that
// list, so the stock presigner would strip it out of the signature and leave
// SignedHeader empty. Git LFS sends only the headers the batch response names,
// so a hoisted checksum is a checksum nobody sends and nothing enforces.
//
// Disabling hoisting affects nothing else here. The parameters that make a URL
// presigned (X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature and the rest)
// are built by the signer itself, not hoisted from headers.
type noHoistPresigner struct{ signer *v4.Signer }

func (p noHoistPresigner) PresignHTTP(
	ctx context.Context, creds aws.Credentials, r *http.Request,
	payloadHash string, service string, region string, signingTime time.Time,
	optFns ...func(*v4.SignerOptions),
) (string, http.Header, error) {
	optFns = append(optFns, func(o *v4.SignerOptions) { o.DisableHeaderHoisting = true })
	return p.signer.PresignHTTP(ctx, creds, r, payloadHash, service, region, signingTime, optFns...)
}

// NewPresigner builds a Presigner over an S3 client.
//
// Pass the concrete client, not a hardened wrapper: presigning makes no network
// call, so the hardened transport buys nothing, and s3.NewPresignClient needs
// the concrete type. In cmd/objgitd that is the *s3.Client embedded in the
// Tigris client.
//
// Uploads and downloads get different signers. Turning off header hoisting is
// what keeps an upload's checksum inside the signature, but it applies to every
// "X-Amz-*" header, and a GetObject carries "x-amz-checksum-mode". Signing that
// one would oblige every download client to send it back, and a plain fetch of
// the URL would fail with SignatureDoesNotMatch. A download needs nothing
// signed beyond the host, so it uses the stock signer and the URL stands alone.
func NewPresigner(c *s3.Client, bucket string) *Presigner {
	return &Presigner{
		putClient: s3.NewPresignClient(c, func(o *s3.PresignOptions) {
			o.Presigner = noHoistPresigner{signer: v4.NewSigner()}
		}),
		getClient: s3.NewPresignClient(c),
		bucket:    bucket,
	}
}

// PresignGet returns a URL the client reads an object from, valid for ttl.
func (p *Presigner) PresignGet(ctx context.Context, key string, ttl time.Duration) (*v4.PresignedHTTPRequest, error) {
	return p.getClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(ttl))
}

// PresignPut returns a URL the client writes an object to, valid for ttl.
//
// The request carries the object's own SHA-256 as ChecksumSHA256, so the bucket
// rejects a body that does not hash to the key it is being written to. The
// caller must copy the returned SignedHeader into the batch response, because
// the signature covers those headers and the client has to send them back.
//
// The declared size is deliberately absent: Content-Length is not signed on a
// presigned PUT, so it cannot be enforced here. Size is checked afterwards, by
// Verify.
func (p *Presigner) PresignPut(ctx context.Context, key, oid string, ttl time.Duration) (*v4.PresignedHTTPRequest, error) {
	if err := ValidateOID(oid); err != nil {
		return nil, err
	}
	raw, err := hex.DecodeString(oid)
	if err != nil {
		return nil, fmt.Errorf("decoding oid %q: %w", oid, err)
	}
	return p.putClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(p.bucket),
		Key:            aws.String(key),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(raw)),
	}, s3.WithPresignExpires(ttl))
}

// TransferHeader renders a presigned request's signed headers as the "header"
// map of a batch action. Host is dropped because the client sets it from the
// URL, and sending it twice breaks the request.
func TransferHeader(req *v4.PresignedHTTPRequest) map[string]string {
	if req == nil {
		return nil
	}
	out := make(map[string]string, len(req.SignedHeader))
	for k := range req.SignedHeader {
		if http.CanonicalHeaderKey(k) == "Host" {
			continue
		}
		out[k] = req.SignedHeader.Get(k)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
