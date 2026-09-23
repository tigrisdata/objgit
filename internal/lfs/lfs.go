// Package lfs implements the Git LFS server protocol over a Tigris bucket.
//
// The batch endpoint answers with a presigned URL, and the client transfers
// to and from the bucket directly. EROFS snapshot builds also read LFS blobs
// from the bucket after checking repository membership. See
// docs/architecture/lfs.md.
//
// # Layout in the bucket
//
//	lfs/blobs/<oid>                    the bytes, stored once for the whole bucket
//	<orgID>/<repo>/lfs/staging/<oid>   an upload in flight, before it is verified
//	<orgID>/<repo>/lfs/objects/<oid>   a zero-byte marker: this repository has it
//	<orgID>/<repo>/lfs/locks.json      every lock held in this repository
//
// Bytes are global, so two repositories that hold the same file store it once.
// Membership is per repository, and every batch decision reads the marker,
// never the blob. Without that split, a caller who knows an oid could ask any
// repository for a presigned URL and read a blob belonging to a repository it
// cannot otherwise see. LFS oids are not secret: they sit in pointer files,
// forks, and CI logs.
//
// # Uploads land in the repository, not in the shared key
//
// A client uploads to its own staging key. Verify checks those bytes, copies
// them to the shared key, and only then writes the marker.
//
// The indirection is what makes membership mean something. If a client uploaded
// straight to the shared key, verify could only check that the bytes exist and
// have the right length, which a caller who merely knows an oid and a size can
// already say. An oid and its size sit next to each other in every pointer
// file. Staging makes the client prove it holds the content: to get a marker it
// must first write the bytes, and Tigris checks them against the oid as it
// does.
//
// Staging also keeps a failed upload from touching anything shared. A rejected
// object is deleted from the caller's own staging key, never from the blob
// other repositories are reading.
//
// The marker is also the only reference count a future collector can use. A
// blob whose last marker is gone is unreachable.
//
// This package imports no transport and no Prometheus client. It takes observer
// functions, and cmd/objgitd wires internal/metrics into them.
package lfs

import (
	"errors"
	"fmt"
)

// ErrInvalidOID is returned for an object ID that is not exactly 64 lowercase
// hexadecimal characters.
var ErrInvalidOID = errors.New("lfs: object id must be 64 lowercase hex characters")

// oidLen is the length of a hex-encoded SHA-256 digest. Git LFS uses SHA-256
// for every object, and hash_algo has no other legal value.
const oidLen = 64

const (
	// blobPrefix holds object bytes, shared by every repository.
	blobPrefix = "lfs/blobs/"
	// markerSuffix holds one repository's membership markers, under its own
	// "orgID/name" prefix.
	markerSuffix = "/lfs/objects/"
	// stagingSuffix holds one repository's uploads until they are verified.
	stagingSuffix = "/lfs/staging/"
	// locksSuffix holds one repository's lock document.
	locksSuffix = "/lfs/locks.json"
)

// ValidateOID reports whether oid is a well-formed Git LFS object ID.
//
// The oid becomes part of a bucket key, so this is the boundary that keeps
// caller-supplied text out of the key space. It accepts only 64 lowercase
// hexadecimal characters, which admits no separator, no dot, no escape, and no
// control byte. Uppercase is rejected rather than folded: git-lfs computes
// lowercase, so accepting uppercase would store the same bytes under a second
// key and break deduplication.
func ValidateOID(oid string) error {
	if len(oid) != oidLen {
		return fmt.Errorf("%w: got %d characters", ErrInvalidOID, len(oid))
	}
	for i := range len(oid) {
		c := oid[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return fmt.Errorf("%w: illegal character %q at offset %d", ErrInvalidOID, c, i)
	}
	return nil
}

// BlobKey is the bucket key holding the bytes for oid. It carries no repository
// prefix: the bytes are shared by the whole bucket.
//
// Callers must validate oid first. The key is only safe because ValidateOID
// admits nothing but lowercase hex.
func BlobKey(oid string) string { return blobPrefix + oid }

// MarkerKey is the bucket key proving that the repository at repo holds oid.
// repo is a repofs.RepoRef.Path(), an "orgID/name" pair.
//
// Callers must validate oid first.
func MarkerKey(repo, oid string) string { return repo + markerSuffix + oid }

// StagingKey is the bucket key a client uploads oid to, before it is verified.
// It is scoped to one repository, so writing it proves the caller holds the
// content rather than merely knowing its name.
//
// Callers must validate oid first.
func StagingKey(repo, oid string) string { return repo + stagingSuffix + oid }

// LocksKey is the bucket key holding the lock document for the repository at
// repo.
func LocksKey(repo string) string { return repo + locksSuffix }
