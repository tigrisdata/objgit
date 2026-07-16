package awssig

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func ResolvePayloadHash(r *http.Request, maxSize int64) (string, error) {
	declared := r.Header.Get("X-Amz-Content-Sha256")

	if strings.HasPrefix(strings.ToUpper(declared), "STREAMING-") {
		return "", ErrStreamingUnsupported
	}

	body, err := readAndLimitBody(r, maxSize)
	if err != nil {
		return "", err
	}

	// The client opted out of signing the payload. The size cap above still
	// applies, but there is no body hash to compare against; the canonical
	// request uses the literal sentinel the client signed.
	if declared == UnsignedPayload {
		return UnsignedPayload, nil
	}

	sum := sha256.Sum256(body)
	computed := hex.EncodeToString(sum[:])

	if declared == "" {
		return computed, nil
	}

	if subtle.ConstantTimeCompare([]byte(strings.ToLower(declared)), []byte(computed)) != 1 {
		return "", ErrBodyHashMismatch
	}

	// Use the verified lowercase hash in the canonical request. Some clients send
	// uppercase for some reason.
	return computed, nil
}

func readAndLimitBody(r *http.Request, maxSize int64) ([]byte, error) {
	var rdr io.Reader = r.Body
	rdr = io.LimitReader(rdr, maxSize)

	if r.ContentLength > maxSize {
		return nil, ErrBodyTooLarge
	}

	body, err := io.ReadAll(rdr)
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil {
		switch {
		case errors.Is(err, io.EOF):
			return nil, errors.Join(ErrBodyTooLarge, io.EOF)
		default:
			return nil, fmt.Errorf("can't read body: %w", err)
		}
	}

	return body, nil
}
