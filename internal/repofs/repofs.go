// Package repofs maps a transport repository path to the billy.Filesystem that
// holds it. It is the seam a real backend implements to route an org to its own
// bucket/credentials; the default implementation chroots one bucket filesystem.
//
// Like internal/auth, this package is transport-neutral: it imports only the
// standard library and go-billy, never a concrete transport.
package repofs

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/go-git/go-billy/v6"
)

// ErrInvalidPath is returned by Parse when a repository path is not of the form
// {orgID}/{repoName}.
var ErrInvalidPath = errors.New("repository path must be of the form {orgID}/{repoName}")

// ErrUnauthenticated is returned by a Resolver when the request lacks the
// credential it needs to resolve a repository. Transports surface it as an
// authentication challenge (HTTP 401) rather than a 404 or 500.
var ErrUnauthenticated = errors.New("repofs: authentication required")

// RepoRef identifies a repository. OrgID is an opaque reference a later API call
// will validate; for now it is accepted as-is. Name has any trailing ".git"
// stripped, so org/repo.git and org/repo denote the same repository.
type RepoRef struct {
	OrgID string
	Name  string
}

// Path is the canonical storage and identity path, "orgID/name".
func (r RepoRef) Path() string { return path.Join(r.OrgID, r.Name) }

// Parse converts a raw transport path into a RepoRef. It trims surrounding
// slashes, requires exactly two non-empty segments, and strips a trailing
// ".git" from the name. OrgID is not otherwise validated.
func Parse(raw string) (RepoRef, error) {
	parts := strings.Split(strings.Trim(raw, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return RepoRef{}, ErrInvalidPath
	}
	name := strings.TrimSuffix(parts[1], ".git")
	if name == "" {
		return RepoRef{}, ErrInvalidPath
	}
	return RepoRef{OrgID: parts[0], Name: name}, nil
}

// Credential carries the HTTP Basic-auth username and password a caller
// presented (the zero value means none was presented). It is unvalidated; a
// Resolver decides what, if anything, to do with it.
type Credential struct {
	Username string
	Password string
}

// Resolver maps a RepoRef — plus the caller's credential — to the
// billy.Filesystem rooted at that repository. The returned filesystem is the
// repository root: go-git's storage layer is built directly on top of it.
//
// create distinguishes the write path from the read path: it is true on a push
// (loadOrInit), allowing a backend to provision storage (e.g. create a bucket)
// on demand, and false on a read (load), where a missing repository must surface
// as transport.ErrRepositoryNotFound rather than being created.
type Resolver interface {
	Resolve(ctx context.Context, ref RepoRef, cred Credential, create bool) (billy.Filesystem, error)
}

// BucketResolver is the default Resolver: it chroots a single base filesystem
// (the whole bucket) to ref.Path(), ignoring the credential. This preserves the
// original single-bucket behavior.
type BucketResolver struct {
	Base billy.Filesystem
}

// Resolve chroots the base filesystem to the repository's "orgID/name" path.
// Chroot is creation-free, so create is ignored.
func (b BucketResolver) Resolve(_ context.Context, ref RepoRef, _ Credential, _ bool) (billy.Filesystem, error) {
	return b.Base.Chroot(ref.Path())
}
