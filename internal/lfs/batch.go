package lfs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
)

// The two batch operations. Any other value is a malformed request, never a
// default: guessing "download" for an unrecognized operation would answer a
// write request with read access.
const (
	OperationDownload = "download"
	OperationUpload   = "upload"
)

// TransferBasic is the only transfer adapter this server implements. "basic" is
// a plain GET and PUT against the action's href, which is exactly what a
// presigned URL is.
const TransferBasic = "basic"

// Action names in a batch response.
const (
	ActionDownload = "download"
	ActionUpload   = "upload"
	ActionVerify   = "verify"
)

// hashAlgoSHA256 is the only hash Git LFS defines. An absent value means the
// same thing.
const hashAlgoSHA256 = "sha256"

// defaultBatchConcurrency bounds the marker reads one batch makes. A batch
// carries up to a hundred objects by default, and each one costs a HEAD.
const defaultBatchConcurrency = 16

// Whole-request failures. These become an HTTP status, unlike a per-object
// error, which travels inside a 200.
var (
	// ErrInvalidRequest is a malformed batch body.
	ErrInvalidRequest = errors.New("lfs: malformed batch request")
	// ErrTooManyObjects is a batch larger than the configured cap.
	ErrTooManyObjects = errors.New("lfs: too many objects in one batch")
	// ErrObjectTooLarge is an object larger than the configured cap.
	ErrObjectTooLarge = errors.New("lfs: object is larger than the limit")
	// ErrNoSupportedTransfer means the client offered no transfer this server
	// implements.
	ErrNoSupportedTransfer = errors.New("lfs: no supported transfer adapter offered")
)

// Pointer is one object in a batch request: its ID and its declared length.
type Pointer struct {
	OID  string `json:"oid"`
	Size int64  `json:"size"`
}

// Ref names the branch the client is working against. It is advisory, and this
// server does not use it.
type Ref struct {
	Name string `json:"name"`
}

// BatchRequest is the body of POST .../info/lfs/objects/batch.
type BatchRequest struct {
	Operation string    `json:"operation"`
	Transfers []string  `json:"transfers,omitempty"`
	Ref       *Ref      `json:"ref,omitempty"`
	Objects   []Pointer `json:"objects"`
	HashAlgo  string    `json:"hash_algo,omitempty"`
}

// Action tells the client how to transfer one object.
type Action struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header,omitempty"`
	// ExpiresIn is how many seconds the href stays valid. Without it a client
	// cannot tell a stale URL from a denied one.
	ExpiresIn int `json:"expires_in,omitempty"`
}

// ObjectError is a per-object failure. It travels inside a 200 response.
type ObjectError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ObjectResult is one object's outcome in a batch response.
type ObjectResult struct {
	OID  string `json:"oid"`
	Size int64  `json:"size"`
	// Authenticated tells git-lfs the action href needs no further credentials.
	// Without it the client runs credential discovery against the storage host
	// and can attach an Authorization header, which a presigned request
	// rejects outright.
	Authenticated bool               `json:"authenticated,omitempty"`
	Actions       map[string]*Action `json:"actions,omitempty"`
	Error         *ObjectError       `json:"error,omitempty"`
}

// BatchResponse is the body returned by the batch endpoint.
type BatchResponse struct {
	Transfer string         `json:"transfer"`
	Objects  []ObjectResult `json:"objects"`
	HashAlgo string         `json:"hash_algo,omitempty"`
}

// BatchOptions is the per-request configuration the caller supplies.
type BatchOptions struct {
	// Repo is the repository path, "orgID/name".
	Repo string
	// VerifyURL is the absolute URL of this repository's verify endpoint.
	VerifyURL string
	// TTL is how long a minted transfer URL stays valid.
	TTL time.Duration
	// MaxSize is the largest object accepted, in bytes.
	MaxSize int64
	// MaxBatch is the most objects accepted in one request.
	MaxBatch int
	// Concurrency bounds the marker reads. Zero picks a default.
	Concurrency int
}

// Batch answers a Git LFS batch request.
//
// Every decision reads the per-repository marker, never the shared blob, so a
// caller who knows an oid cannot use one repository to reach another's data.
// The cost is that a repository which does not yet hold an object uploads it
// again even when the bytes are already in the bucket.
//
// A returned error is a whole-request failure. A per-object failure is not an
// error: it rides in the response as ObjectResult.Error, and the request still
// succeeds.
func (s *Store) Batch(ctx context.Context, req *BatchRequest, opt BatchOptions) (*BatchResponse, error) {
	if err := validateBatch(req, opt); err != nil {
		return nil, err
	}

	results := make([]ObjectResult, len(req.Objects))
	g, gctx := errgroup.WithContext(ctx)
	limit := opt.Concurrency
	if limit <= 0 {
		limit = defaultBatchConcurrency
	}
	g.SetLimit(limit)

	for i, p := range req.Objects {
		// Results are indexed, not appended, so the response keeps the request
		// order whatever order the reads finish in.
		g.Go(func() error {
			res, err := s.resolveObject(gctx, p, req.Operation, opt)
			if err != nil {
				return err
			}
			results[i] = res
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &BatchResponse{
		Transfer: TransferBasic,
		Objects:  results,
		HashAlgo: hashAlgoSHA256,
	}, nil
}

// validateBatch checks everything that makes the whole request unanswerable.
func validateBatch(req *BatchRequest, opt BatchOptions) error {
	if req == nil {
		return fmt.Errorf("%w: empty body", ErrInvalidRequest)
	}
	switch req.Operation {
	case OperationDownload, OperationUpload:
	case "":
		return fmt.Errorf("%w: missing operation", ErrInvalidRequest)
	default:
		return fmt.Errorf("%w: unknown operation %q", ErrInvalidRequest, req.Operation)
	}
	if req.HashAlgo != "" && req.HashAlgo != hashAlgoSHA256 {
		return fmt.Errorf("%w: unsupported hash algorithm %q", ErrInvalidRequest, req.HashAlgo)
	}
	// An empty list means the client accepts the default, which is "basic".
	if len(req.Transfers) > 0 && !slices.Contains(req.Transfers, TransferBasic) {
		return fmt.Errorf("%w: client offered %v", ErrNoSupportedTransfer, req.Transfers)
	}
	if opt.MaxBatch > 0 && len(req.Objects) > opt.MaxBatch {
		return fmt.Errorf("%w: %d objects, limit is %d", ErrTooManyObjects, len(req.Objects), opt.MaxBatch)
	}
	for _, p := range req.Objects {
		if err := ValidateOID(p.OID); err != nil {
			return err
		}
		if p.Size < 0 {
			return fmt.Errorf("%w: negative size for %s", ErrInvalidRequest, p.OID)
		}
		if opt.MaxSize > 0 && p.Size > opt.MaxSize {
			return fmt.Errorf("%w: %s is %d bytes, limit is %d", ErrObjectTooLarge, p.OID, p.Size, opt.MaxSize)
		}
	}
	return nil
}

// resolveObject decides what one object's entry in the response says.
func (s *Store) resolveObject(ctx context.Context, p Pointer, operation string, opt BatchOptions) (ObjectResult, error) {
	res := ObjectResult{OID: p.OID, Size: p.Size, Authenticated: true}

	size, held, err := s.Marker(ctx, opt.Repo, p.OID)
	if err != nil {
		return ObjectResult{}, err
	}

	if operation == OperationDownload {
		if !held {
			res.Error = &ObjectError{Code: 404, Message: "object does not exist"}
			return res, nil
		}
		req, err := s.pre.PresignGet(ctx, BlobKey(p.OID), opt.TTL)
		if err != nil {
			return ObjectResult{}, fmt.Errorf("presigning download for %s: %w", p.OID, err)
		}
		res.Actions = map[string]*Action{
			ActionDownload: {
				Href:      req.URL,
				Header:    TransferHeader(req),
				ExpiresIn: int(opt.TTL.Seconds()),
			},
		}
		return res, nil
	}

	if held {
		if size != p.Size {
			res.Error = &ObjectError{
				Code:    422,
				Message: fmt.Sprintf("object is stored with size %d, not %d", size, p.Size),
			}
			return res, nil
		}
		// Already held. Omitting actions entirely is how a batch response says
		// "nothing to do"; git-lfs skips the transfer.
		return res, nil
	}

	// The upload goes to this repository's staging key, not the shared blob.
	// Verify promotes it. See the package doc for why membership needs that
	// indirection.
	req, err := s.pre.PresignPut(ctx, StagingKey(opt.Repo, p.OID), p.OID, opt.TTL)
	if err != nil {
		return ObjectResult{}, fmt.Errorf("presigning upload for %s: %w", p.OID, err)
	}
	res.Actions = map[string]*Action{
		ActionUpload: {
			Href:      req.URL,
			Header:    TransferHeader(req),
			ExpiresIn: int(opt.TTL.Seconds()),
		},
		// The daemon never sees the upload, so verify is the only step that can
		// check the bytes and record that this repository holds them.
		ActionVerify: {
			Href:      opt.VerifyURL,
			Header:    map[string]string{"Content-Type": MediaType},
			ExpiresIn: int(opt.TTL.Seconds()),
		},
	}
	return res, nil
}
