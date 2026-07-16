// Package authctx holds the canonical context keys for the verified caller,
// shared by the sigv4 and sigv4a middleware families (and their iamsts
// sub-packages) so identity stored by either family is readable through
// either — and directly through this package, which new code should prefer.
package authctx

import (
	"context"
)

type keyIDKey struct{}

// WithKeyID stores the verified access key id in ctx.
func WithKeyID(ctx context.Context, keyID string) context.Context {
	return context.WithValue(ctx, keyIDKey{}, keyID)
}

// KeyID returns the verified access key id stored in ctx, if any.
func KeyID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(keyIDKey{}).(string)
	return id, ok
}
