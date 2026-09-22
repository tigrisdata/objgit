package lfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// lockCASRetries bounds the compare-and-swap loop. Contention on one
// repository's lock document is rare, so a handful of attempts is plenty, and a
// bound stops a permanently refused write from spinning.
const lockCASRetries = 8

// AnonymousOwner is the lock owner recorded when a caller presents no
// credential.
//
// With no user store, every anonymous caller is this one owner, so every lock
// looks like the caller's own and a push is never blocked. Locking only tells
// users apart once callers send a Basic username. See docs/usage/lfs.md.
const AnonymousOwner = "anonymous"

// Lock failures.
var (
	// ErrLockNotFound is an unlock of an ID nobody holds.
	ErrLockNotFound = errors.New("lfs: no such lock")
	// ErrLockHeldByAnother is an unlock of someone else's lock without force.
	ErrLockHeldByAnother = errors.New("lfs: lock is held by another user")
	// ErrLockContention is a compare-and-swap that kept losing.
	ErrLockContention = errors.New("lfs: lock document is too contended")
)

// Owner names who holds a lock.
type Owner struct {
	Name string `json:"name"`
}

// Lock is one held path.
type Lock struct {
	ID       string    `json:"id"`
	Path     string    `json:"path"`
	LockedAt time.Time `json:"locked_at"`
	Owner    Owner     `json:"owner"`
}

// lockDoc is the whole lock state of one repository, stored as one object.
//
// One document rather than one object per lock: a lock list has to be
// consistent, and a single object gives that through one conditional write.
// Listing many small objects would also cost a request per page.
type lockDoc struct {
	Locks []Lock `json:"locks"`
}

// LockFilter narrows a lock listing.
type LockFilter struct {
	Path   string
	ID     string
	Cursor string
	Limit  int
}

// newLockID mints an opaque lock ID.
//
// The ID travels back to the server as a path segment on unlock, so it is
// generated here and never taken from the client. Lowercase hex keeps it safe
// in a URL.
func newLockID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generating lock id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// CreateLock locks path for owner.
//
// The bool reports whether a lock was created. When it is false the returned
// Lock is the one already holding the path, which the caller renders as a 409
// so the client can name the holder.
func (s *Store) CreateLock(ctx context.Context, repo, path, owner string) (Lock, bool, error) {
	if path == "" {
		return Lock{}, false, fmt.Errorf("%w: empty lock path", ErrInvalidRequest)
	}
	if owner == "" {
		owner = AnonymousOwner
	}

	var held Lock
	var created bool
	err := s.mutateLocks(ctx, repo, func(doc *lockDoc) error {
		// Reset per attempt: a retry re-reads the document, so a decision made
		// against the previous read must not leak forward.
		held, created = Lock{}, false

		if i := slices.IndexFunc(doc.Locks, func(l Lock) bool { return l.Path == path }); i >= 0 {
			held = doc.Locks[i]
			return errNoChange
		}

		id, err := newLockID()
		if err != nil {
			return err
		}
		held = Lock{
			ID:       id,
			Path:     path,
			LockedAt: time.Now().UTC(),
			Owner:    Owner{Name: owner},
		}
		created = true
		doc.Locks = append(doc.Locks, held)
		return nil
	})
	if err != nil {
		return Lock{}, false, err
	}
	return held, created, nil
}

// DeleteLock releases a lock.
//
// Without force, only the owner may release it. force lets anyone break a lock,
// which is what `git lfs unlock --force` is for.
func (s *Store) DeleteLock(ctx context.Context, repo, id, owner string, force bool) (Lock, error) {
	if owner == "" {
		owner = AnonymousOwner
	}

	var removed Lock
	err := s.mutateLocks(ctx, repo, func(doc *lockDoc) error {
		removed = Lock{}

		i := slices.IndexFunc(doc.Locks, func(l Lock) bool { return l.ID == id })
		if i < 0 {
			return ErrLockNotFound
		}
		// EqualFold, matching SplitLocks. A username that round-trips through a
		// credential helper can come back in a different case, and a user told
		// a lock is theirs must be able to release it.
		if !force && !strings.EqualFold(doc.Locks[i].Owner.Name, owner) {
			return fmt.Errorf("%w: %s holds it", ErrLockHeldByAnother, doc.Locks[i].Owner.Name)
		}
		removed = doc.Locks[i]
		doc.Locks = slices.Delete(doc.Locks, i, i+1)
		return nil
	})
	if err != nil {
		return Lock{}, err
	}
	return removed, nil
}

// ListLocks returns the locks held in repo, narrowed by filter. The second
// return is the next cursor, empty when the listing is complete.
func (s *Store) ListLocks(ctx context.Context, repo string, filter LockFilter) ([]Lock, string, error) {
	doc, _, err := s.readLocks(ctx, repo)
	if err != nil {
		return nil, "", err
	}

	out := make([]Lock, 0, len(doc.Locks))
	for _, l := range doc.Locks {
		if filter.Path != "" && l.Path != filter.Path {
			continue
		}
		if filter.ID != "" && l.ID != filter.ID {
			continue
		}
		out = append(out, l)
	}

	// The cursor is an index into the filtered list. The document is small and
	// read whole, so there is nothing to gain from an opaque token.
	start := 0
	if filter.Cursor != "" {
		// strconv, not fmt.Sscanf: Sscanf reads "3junk" as 3 and reports no
		// error, so a mangled cursor would silently skip entries.
		n, err := strconv.Atoi(filter.Cursor)
		if err != nil || n < 0 {
			return nil, "", fmt.Errorf("%w: bad cursor %q", ErrInvalidRequest, filter.Cursor)
		}
		start = n
	}
	if start >= len(out) {
		return nil, "", nil
	}
	out = out[start:]

	next := ""
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
		next = strconv.Itoa(start + filter.Limit)
	}
	return out, next, nil
}

// errNoChange tells mutateLocks the callback decided against writing. It never
// reaches the caller.
var errNoChange = errors.New("lfs: lock document unchanged")

// mutateLocks applies fn to the repository's lock document under a
// compare-and-swap.
//
// This is the same conditional-write loop the ref cache uses: read the document
// with its ETag, apply the change, then write with If-Match (or If-None-Match
// for a document that does not exist yet). A refused write means another
// client committed first, so the loop re-reads and re-applies rather than
// overwriting that client's lock.
func (s *Store) mutateLocks(ctx context.Context, repo string, fn func(*lockDoc) error) error {
	for range lockCASRetries {
		doc, etag, err := s.readLocks(ctx, repo)
		if err != nil {
			return err
		}

		if err := fn(doc); err != nil {
			if errors.Is(err, errNoChange) {
				return nil
			}
			return err
		}

		body, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("encoding lock document: %w", err)
		}

		in := &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(LocksKey(repo)),
			Body:        bytes.NewReader(body),
			ContentType: aws.String("application/json"),
		}
		if etag == "" {
			in.IfNoneMatch = aws.String("*")
		} else {
			in.IfMatch = aws.String(etag)
		}

		_, err = s.api.PutObject(ctx, in)
		if err == nil {
			return nil
		}
		if !isPreconditionFailed(err) {
			return fmt.Errorf("writing lock document for %s: %w", repo, err)
		}
		// Lost the race. Read the winner's document and try again.
	}
	return fmt.Errorf("%w: gave up after %d attempts", ErrLockContention, lockCASRetries)
}

// readLocks loads a repository's lock document and the ETag to write against.
// A repository that has never been locked reads as an empty document with an
// empty ETag, which mutateLocks turns into an If-None-Match create.
func (s *Store) readLocks(ctx context.Context, repo string) (*lockDoc, string, error) {
	out, err := s.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(LocksKey(repo)),
	})
	if err != nil {
		if isNotFound(err) {
			return &lockDoc{}, "", nil
		}
		return nil, "", fmt.Errorf("reading lock document for %s: %w", repo, err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("reading lock document body for %s: %w", repo, err)
	}

	doc := &lockDoc{}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, doc); err != nil {
			return nil, "", fmt.Errorf("decoding lock document for %s: %w", repo, err)
		}
	}
	return doc, aws.ToString(out.ETag), nil
}

// SplitLocks divides locks into the caller's own and everyone else's, which is
// the shape `POST .../locks/verify` answers with.
func SplitLocks(locks []Lock, owner string) (ours, theirs []Lock) {
	if owner == "" {
		owner = AnonymousOwner
	}
	for _, l := range locks {
		if strings.EqualFold(l.Owner.Name, owner) {
			ours = append(ours, l)
			continue
		}
		theirs = append(theirs, l)
	}
	return ours, theirs
}
