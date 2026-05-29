package auth

import (
	"context"

	gossh "golang.org/x/crypto/ssh"
)

// Operation is the access a request needs. Transports map the git service:
// upload-pack/upload-archive → Read, receive-pack → Write.
type Operation int

const (
	Read Operation = iota
	Write
)

// Credential is what the client presented. Exactly one concrete type per
// scheme; a transport constructs the variant it can produce, or Anonymous.
type Credential interface{ isCredential() }

// Anonymous is "no credential presented" (git://, or HTTP/SSH with none).
type Anonymous struct{}

// PublicKey is an SSH public key. Uses x/crypto/ssh's type (gliderlabs/ssh
// keys satisfy it) so this package stays free of the SSH server library.
type PublicKey struct{ Key gossh.PublicKey }

// BasicAuth is an HTTP Basic credential. Unvalidated — the Authorizer owns the
// user store.
type BasicAuth struct{ Username, Password string }

func (Anonymous) isCredential() {}
func (PublicKey) isCredential() {}
func (BasicAuth) isCredential() {}

// Request is a transport-neutral authorization request.
type Request struct {
	Repo      string
	Operation Operation
	Cred      Credential
	Transport string // "git", "ssh", "http" — for policy/logging
}

// Decision is the outcome. Unauthenticated is the seam that lets HTTP issue a
// 401 challenge; SSH and git:// treat it as Deny.
type Decision int

const (
	Deny Decision = iota
	Allow
	Unauthenticated
)

// Authorizer decides whether a request may proceed. This is the seam a real
// authn/authz layer plugs into later.
type Authorizer interface {
	Authorize(ctx context.Context, req Request) Decision
}

// AllowAnonymous is the permissive default: read for everyone, write only when
// AllowWrite is set. "Dangerously allow everything the server is configured to
// allow" — never more open than the -allow-push gate. It ignores the credential
// entirely and never returns Unauthenticated.
type AllowAnonymous struct{ AllowWrite bool }

func (a AllowAnonymous) Authorize(_ context.Context, req Request) Decision {
	if req.Operation == Write && !a.AllowWrite {
		return Deny
	}
	return Allow
}
