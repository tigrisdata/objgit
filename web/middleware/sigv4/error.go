package sigv4

import (
	"context"
	"errors"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/tigrisdata/objgit/web/middleware/internal/awssig"
)

// ConnectError maps a verification error to a *connect.Error, mirroring the
// buckets in TwirpError: request-describing sentinels keep their message; key
// and signature failures collapse to one opaque message; the rest are internal.
func ConnectError(ctx context.Context, err error) *connect.Error {
	switch {
	case errors.Is(err, ErrMissingAuth):
		return connect.NewError(connect.CodeUnauthenticated,
			errors.New("no authentication header present"))
	case errors.Is(err, ErrScopeMismatch), errors.Is(err, ErrTemporalSkew),
		errors.Is(err, awssig.ErrBodyTooLarge), errors.Is(err, awssig.ErrStreamingUnsupported),
		errors.Is(err, ErrMissingSignedHost):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, ErrUnknownKey), errors.Is(err, ErrUnauthorized),
		errors.Is(err, awssig.ErrBodyHashMismatch):
		return connect.NewError(connect.CodePermissionDenied,
			errors.New("invalid authentication header"))
	default:
		slog.ErrorContext(ctx, "sigv4 verification failed unexpectedly", "err", err)
		return connect.NewError(connect.CodeInternal, errors.New("internal error"))
	}
}
