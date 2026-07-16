package sigv4

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/tigrisdata/objgit/web/middleware/internal/awssig"
)

type credentialScope struct {
	date    string
	region  string
	service string
}

func (v *Verifier) canonicalRequest(r *http.Request, sr *signedRequest, payloadHash string) string {
	headers := append([]string(nil), sr.signedHeaders...)
	sort.Strings(headers)
	return awssig.BuildCanonicalRequest(r, headers, payloadHash, false)
}

type signedRequest struct {
	accessKeyID   string
	scope         credentialScope
	signedHeaders []string
	signature     string
}

func parseAuthHeader(h string) (*signedRequest, error) {
	if !strings.HasPrefix(h, algorithm) {
		return nil, ErrMissingAuth
	}
	rest := strings.TrimSpace(strings.TrimPrefix(h, algorithm))

	var cred, signed, sig string
	for _, part := range strings.Split(rest, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return nil, ErrMissingAuth
		}
		switch kv[0] {
		case "Credential":
			cred = kv[1]
		case "SignedHeaders":
			signed = kv[1]
		case "Signature":
			sig = kv[1]
		default:
			return nil, ErrMissingAuth
		}
	}
	if cred == "" || signed == "" || sig == "" {
		return nil, ErrMissingAuth
	}

	cp := strings.Split(cred, "/")
	if len(cp) != 5 || cp[4] != awssig.Terminator {
		return nil, fmt.Errorf("%w: bad credential scope", ErrMissingAuth)
	}
	return &signedRequest{
		accessKeyID:   cp[0],
		scope:         credentialScope{date: cp[1], region: cp[2], service: cp[3]},
		signedHeaders: strings.Split(signed, ";"),
		signature:     sig,
	}, nil
}

// Credential is the parsed Credential= component of a SigV4 Authorization
// header: the access key id plus the literal, unnormalized scope strings.
type Credential struct {
	AccessKeyID string
	Date        string
	Region      string
	Service     string
}

// ParseCredential extracts the credential scope from an AWS4-HMAC-SHA256
// Authorization header value. It returns ErrMissingAuth for anything
// malformed, matching Verify.
func ParseCredential(authorization string) (*Credential, error) {
	sr, err := parseAuthHeader(authorization)
	if err != nil {
		return nil, err
	}
	return &Credential{
		AccessKeyID: sr.accessKeyID,
		Date:        sr.scope.date,
		Region:      sr.scope.region,
		Service:     sr.scope.service,
	}, nil
}

// DeriveSigningKey computes the SigV4 derived signing key for a credential
// scope: HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service),
// "aws4_request"). The derived key can only validate requests whose scope
// matches (date, region, service) exactly, so exposure is bounded to one UTC
// day and one service; it never reveals the secret.
func DeriveSigningKey(secret, date, region, service string) []byte {
	kDate := awssig.HMACSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := awssig.HMACSHA256(kDate, []byte(region))
	kService := awssig.HMACSHA256(kRegion, []byte(service))
	return awssig.HMACSHA256(kService, []byte(awssig.Terminator))
}
