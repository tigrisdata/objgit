package sigv4

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/tigrisdata/objgit/web/middleware/authctx"
	"github.com/tigrisdata/objgit/web/middleware/internal/awssig"
)

const (
	algorithm = "AWS4-HMAC-SHA256"
)

// Errors returned by Verify. Callers typically map ErrUnauthorized and ErrUnknownKey
// to error code 403 and the rest to error code 400.
var (
	ErrMissingAuth          = errors.New("sigv4: missing or malformed Authorization header")
	ErrMissingSignedHost    = errors.New("sigv4: host must appear in signed header list")
	ErrUnknownKey           = errors.New("sigv4: unknown access key ID")
	ErrTemporalSkew         = errors.New("sigv4: request time is outside allowed temporal skew window")
	ErrScopeMismatch        = errors.New("sigv4: credential scope does not match")
	ErrUnauthorized         = errors.New("sigv4: signature mismatch from what was calculated")
	ErrNotConfigured        = errors.New("[unexpected] sigv4: Verifier is not configured correctly: need Lookup or KeyLookup to be set")
	ErrMaxBodyNotConfigured = errors.New("[unexpected] sigv4: Verifier is not configured correctly: need MaxBodySize to be set")
)

// Verifier validates SigV4-signed requests for a single service in a single region.
type Verifier struct {
	// Region and Service are mapped to the credential scope in incoming requests.
	// This configures the region and service for this middleware and MUST match
	// what clients are sending.
	Region, Service string

	// Lookup looks up the secret access key for a given access key ID. Return
	// ErrUnknownKey for unknown keys.
	//
	// This is intended for local development and debugging only.
	Lookup Lookuper

	// KeyLookup resolves a credential scope to its derived signing key. When set
	// it takes precedence over Lookup, and the Verifier never sees the raw secret.
	// Exactly one of Lookup or KeyLookup MUST be set.
	KeyLookup SigningKeyLookuper

	// MaxTemporalSkew bounds how far the request's X-Amz-Date may be from now.
	//
	// If not set, this defaults to 15 minutes when zero.
	MaxTemporalSkew time.Duration

	// MaxBodySize caps how many bytes of the body will be buffered to verify the
	// payload hash. This MUST be set to a value above zero. Requests that exceed
	// it are rejected with awssig.ErrBodyTooLarge.
	MaxBodySize int64

	// Now overrides time resolution logic for tests.
	//
	// Defaults to time.Now.
	Now func() time.Time
}

// Middleware wraps the given HTTP handler with SigV4 request validation.
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	ew := connect.NewErrorWriter()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keyID, err := v.Verify(r)
		if err != nil {
			slog.DebugContext(r.Context(), "cannot serve request", "err", err, "method", r.Method, "path", r.URL.Path)
			_ = ew.Write(w, r, ConnectError(r.Context(), err))
			return
		}
		next.ServeHTTP(w, r.WithContext(authctx.WithKeyID(r.Context(), keyID)))
	})
}

// Verify checks a single request. On success it returns the access key ID of
// the caller. The request body is buffered and reset so downstream handlers
// can read it normally.
func (v *Verifier) Verify(r *http.Request) (string, error) {
	if v.Lookup == nil && v.KeyLookup == nil {
		return "", ErrNotConfigured
	}

	if v.MaxBodySize == 0 {
		return "", ErrMaxBodyNotConfigured
	}

	sr, err := parseAuthHeader(r.Header.Get("Authorization"))
	if err != nil {
		return "", err
	}

	payloadHash, err := awssig.ResolvePayloadHash(r, v.MaxBodySize)
	if err != nil {
		return "", err
	}

	return v.verify(r, sr, payloadHash)
}

// verify is the shared core run after the Authorization header is parsed and the
// canonical payload hash is known. It pins the credential scope, requires a
// signed host header, enforces the clock-skew window, resolves the signing
// secret, and compares the recomputed signature in constant time. It never
// touches r.Body.
func (v *Verifier) verify(r *http.Request, sr *signedRequest, payloadHash string) (string, error) {
	now := time.Now
	if v.Now != nil {
		now = v.Now
	}
	skew := v.MaxTemporalSkew
	if skew == 0 {
		skew = 15 * time.Minute
	}

	// Pin the scope. Without this, a signature valid for some other
	// region/service would be accepted here.
	if sr.scope.region != v.Region || sr.scope.service != v.Service {
		return "", ErrScopeMismatch
	}

	// AWS always signs the host header; requiring it binds the signature to
	// the actual host so a captured request cannot be replayed against a
	// different vhost or port.
	if !slices.Contains(sr.signedHeaders, "host") {
		return "", ErrMissingSignedHost
	}

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		// Some clients sign the standard Date header instead; convert its
		// RFC 7231 IMF-fixdate form into the AWS timestamp format.
		if d := r.Header.Get("Date"); d != "" {
			dt, derr := http.ParseTime(d)
			if derr != nil {
				return "", fmt.Errorf("%w: bad Date", ErrMissingAuth)
			}
			amzDate = dt.UTC().Format(awssig.AmzTimeFormat)
		}
	}
	t, err := time.Parse(awssig.AmzTimeFormat, amzDate)
	if err != nil {
		return "", fmt.Errorf("%w: bad X-Amz-Date", ErrMissingAuth)
	}
	if t.Format(awssig.AmzDateOnly) != sr.scope.date {
		return "", ErrScopeMismatch
	}
	if d := now().Sub(t); d > skew || d < -skew {
		return "", ErrTemporalSkew
	}

	var key []byte
	if v.KeyLookup != nil {
		key, err = v.KeyLookup.LookupSigningKey(r.Context(), sr.accessKeyID, sr.scope.date, sr.scope.region, sr.scope.service)
	} else {
		var secret string
		secret, err = v.Lookup.Lookup(sr.accessKeyID)
		if err == nil {
			key = DeriveSigningKey(secret, sr.scope.date, sr.scope.region, sr.scope.service)
		}
	}
	if err != nil {
		return "", err
	}

	canonReq := v.canonicalRequest(r, sr, payloadHash)
	scopeStr := strings.Join([]string{sr.scope.date, sr.scope.region, sr.scope.service, awssig.Terminator}, "/")
	hashed := sha256.Sum256([]byte(canonReq))
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		scopeStr,
		hex.EncodeToString(hashed[:]),
	}, "\n")

	want := hex.EncodeToString(awssig.HMACSHA256(key, []byte(stringToSign)))

	if subtle.ConstantTimeCompare([]byte(want), []byte(sr.signature)) != 1 {
		return "", ErrUnauthorized
	}
	return sr.accessKeyID, nil
}
