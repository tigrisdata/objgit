// Package awssig holds the algorithm-independent internals for SigV4
// request signing: canonicalization, payload-hash resolution, and
// the constants that AWS defines.
package awssig

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
)

const (
	// AmzTimeFormat is the X-Amz-Date timestamp layout in package time format.
	AmzTimeFormat = "20060102T150405Z"
	// AmzDateOnly is the credential-scope date layout.
	AmzDateOnly = "20060102"
	// Terminator is the fixed string that ends every credential scope.
	Terminator = "aws4_request"
	// UnsignedPayload is the payload-hash sentinel for unsigned bodies. This is
	// used with streaming RPCs.
	UnsignedPayload = "UNSIGNED-PAYLOAD"
	// StreamingPayload is the payload-hash sentinel for streaming bodies. This is
	// used with AWS style streaming RPCs in a way that isn't supported here.
	StreamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
)

// Error sentinels for table-driven tests.
var (
	ErrBodyHashMismatch     = errors.New("awssig: body hash does not match x-amz-content-sha256")
	ErrBodyTooLarge         = errors.New("awssig: request body exceeds signature verification limit")
	ErrStreamingUnsupported = errors.New("awssig: " + StreamingPayload + " is not supported")
)

// HMACSHA256 computes HMAC-SHA256(key, data).
func HMACSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
