package tigris

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/tigrisdata/objgit/internal/bundler"
)

// uploadTimeout bounds one bundle's worth of PutObject calls. internal/bundler
// derives its handler context from context.Background() with this as the
// timeout — its ContextDeadline field defaults to zero, which would otherwise
// hand every handler an already-expired context — not from the Storer's own
// ctx: uploads are meant to keep running after the request that queued them
// returns.
const uploadTimeout = 5 * time.Minute

const (
	// uploadHandlerLimit is how many bundles the uploader lets the bundler hand
	// to its handler at the same time.
	//
	// The bundler's own default is 1, which serializes bundles: handleBundle
	// uploads one bundle's jobs concurrently, but nothing in the next bundle
	// starts until every upload in this one lands. Since the bundler cuts a
	// bundle every BundleCountThreshold items (10) — and every single pack
	// container, each far above BundleByteThreshold — a large push spends its
	// wall clock waiting on those barriers rather than on bandwidth.
	//
	// 8 lets that many bundles overlap. Nothing here needs a bundle to finish
	// before the next starts: each job owns its own staging file and its own
	// key, and flush() still waits for all of them.
	uploadHandlerLimit = 8

	// uploadBufferedBytes budgets bytes that are staged locally but not yet
	// uploaded. AddWait blocks the producer once the backlog reaches it, which
	// is what stops a fast push from staging without bound while the network
	// drains slowly.
	//
	// This is a *disk* budget, not a memory one: staged bytes live in temp
	// files and run() streams them to PutObject. The bundler calls the field
	// BufferedByteLimit and defaults it to 1 GiB, which reads as a memory
	// figure and is too tight here — 8 concurrent 128 MiB pack containers
	// (maxPackBytes) fill exactly that much, leaving the walk no room to run
	// ahead of the uploads at all. 4 GiB leaves it room.
	uploadBufferedBytes = 4 << 30
)

// uploadJob is one thing to upload asynchronously — a loose object
// (looseJob) or a pack container (packJob, packwriter.go). Sharing one
// bundler across both kinds means one SetReference-triggered flush() waits
// for everything and surfaces either kind of failure, with no separate
// queues or error slots to keep in sync.
type uploadJob interface {
	bytes() int64 // AddWait size hint
	run(ctx context.Context, s *Storer) error
	done(s *Storer, err error) // always runs after run, success or failure
}

// looseJob is one loose object queued for an asynchronous PutObject. path is
// wherever RawObjectWriter.Close staged it — an os.CreateTemp-generated name,
// deliberately NOT content-addressed: two unrelated pushes (even to
// different repos) can easily produce byte-identical content — a shared
// empty-tree hash, a common .gitignore, an empty blob — and a content-derived
// path would let one push's cleanup delete a file a sibling push's upload
// job is still reading, silently losing that upload. Random names make that
// collision class impossible regardless of what the bytes are.
type looseJob struct {
	hash plumbing.Hash
	key  string
	typ  string
	size int64
	path string
}

func (j looseJob) bytes() int64 { return j.size }

func (j looseJob) run(ctx context.Context, s *Storer) error {
	f, err := os.Open(j.path)
	if err != nil {
		return fmt.Errorf("tigris: reopen staged %s: %w", j.key, err)
	}
	defer f.Close()

	start := time.Now()
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: sp(s.bucket),
		Key:    sp(j.key),
		Body:   f,
		Metadata: map[string]string{
			metaType: j.typ,
			metaSize: strconv.FormatInt(j.size, 10),
		},
	})
	s.observe("PutObject", start, err)
	if err != nil {
		return fmt.Errorf("tigris: upload %s: %w", j.key, err)
	}
	return nil
}

func (j looseJob) done(s *Storer, _ error) {
	s.up.evict(j.hash, j.path)
}

// pendingMeta is what a reader needs to serve a not-yet-uploaded object from
// the local cache overlay: the git object type and size a plain file on disk
// can't carry on its own, plus where its bytes currently live.
type pendingMeta struct {
	typ  string
	size int64
	path string
}

// uploader batches uploads through internal/bundler so RawObjectWriter.Close,
// SetEncodedObject, and PackfileWriter.Close all return as soon as bytes are
// staged locally, instead of blocking on the PutObject round trip. Every
// Storer value — the one New returns and every Scoped descendant — owns its
// own uploader; they are never shared, so one repository's backlog or upload
// failure can never block or poison another's.
//
// Reads (headInfo, EncodedObject) consult pending before ever touching S3: a
// packfile can reference an object it just wrote earlier in the same stream
// (delta-base resolution is the case that matters), and that read must see it
// even though the upload is still in flight or hasn't started.
type uploader struct {
	b *bundler.Bundler[uploadJob]

	mu      sync.Mutex
	err     error // first upload error since the last flush
	pending map[plumbing.Hash]pendingMeta
}

// newUploader builds an uploader whose bundle handler uploads through s.
// s must be the exact *Storer the caller intends to keep using — the handler
// closure is bound to it once, here.
func newUploader(s *Storer) *uploader {
	u := &uploader{pending: make(map[plumbing.Hash]pendingMeta)}
	u.b = bundler.New(u.handleBundle(s))
	u.b.ContextDeadline = uploadTimeout
	u.b.HandlerLimit = uploadHandlerLimit
	u.b.BufferedByteLimit = uploadBufferedBytes
	return u
}

// handleBundle returns the bundler handler bound to s, running every job in
// a bundle concurrently so one slow upload never head-of-line blocks the
// rest of the bundle.
func (u *uploader) handleBundle(s *Storer) func(context.Context, []uploadJob) {
	return func(ctx context.Context, jobs []uploadJob) {
		var wg sync.WaitGroup
		wg.Add(len(jobs))
		for _, job := range jobs {
			go func(job uploadJob) {
				defer wg.Done()
				err := job.run(ctx, s)
				if err != nil {
					u.recordErr(err)
				}
				job.done(s, err)
			}(job)
		}
		wg.Wait()
	}
}

func (u *uploader) recordErr(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.err == nil {
		u.err = err
	}
}

// registerPending makes a staged-but-not-yet-uploaded object visible to reads
// through this uploader (see lookupPending). Call it before enqueue, so a
// read can never race ahead of the write it depends on.
func (u *uploader) registerPending(h plumbing.Hash, typ string, size int64, path string) {
	u.mu.Lock()
	u.pending[h] = pendingMeta{typ: typ, size: size, path: path}
	u.mu.Unlock()
}

// evict drops h's pending-read visibility and removes its staged bytes.
// Idempotent — safe to call once the upload finishes, whether it succeeded
// or failed.
func (u *uploader) evict(h plumbing.Hash, path string) {
	u.mu.Lock()
	delete(u.pending, h)
	u.mu.Unlock()
	os.Remove(path)
}

// lookupPending reports h's type/size/local-path if it is currently staged
// locally and not yet known to have finished uploading. By the time the
// caller opens that path the upload may have already finished and evicted
// it; callers must fall back to the normal S3 read in that case.
func (u *uploader) lookupPending(h plumbing.Hash) (pendingMeta, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	p, ok := u.pending[h]
	return p, ok
}

// enqueue stages job for asynchronous upload. It applies backpressure —
// blocking on local memory/fd pressure once the bundler's BufferedByteLimit
// is reached, never on the network — so Close never waits on a PutObject
// round trip but an unbounded backlog of staged uploads can't accumulate
// either.
func (u *uploader) enqueue(ctx context.Context, job uploadJob) error {
	if err := u.b.AddWait(ctx, job, u.sizeHint(job)); err != nil {
		return fmt.Errorf("tigris: queue upload: %w", err)
	}
	return nil
}

// sizeHint is job's weight for AddWait's BufferedByteLimit accounting, clamped
// to that limit.
//
// The clamp is not a nicety. AddWait acquires the weight from a semaphore whose
// capacity *is* BufferedByteLimit, and x/sync/semaphore parks a weight larger
// than the whole capacity until ctx is done rather than failing it — so an
// unclamped oversized job hangs its push for as long as the request lives. The
// 128 MiB pack cap keeps every ordinary container far under the limit, but one
// case survives it by design: an object larger than the cap spills into a
// container of its own (see packwriter.go), and that container can be larger
// than any limit we pick.
//
// Clamping costs nothing real. The weight is backpressure accounting for bytes
// that sit in a staging file on disk, and run() streams that file rather than
// buffering it, so a job that reports less than it holds overshoots the
// backlog budget instead of breaking anything.
func (u *uploader) sizeHint(job uploadJob) int {
	size := job.bytes()
	if lim := int64(u.b.BufferedByteLimit); lim > 0 && size > lim {
		return int(lim)
	}
	return int(size)
}

// flush waits for every upload queued so far to finish and returns the first
// error any of them hit, clearing it so the next flush starts fresh. Callers
// that hand out references to objects (SetReference) flush first, so a ref
// can never point at an object that failed — or hasn't yet finished —
// uploading.
func (u *uploader) flush() error {
	u.b.Flush()
	u.mu.Lock()
	defer u.mu.Unlock()
	err := u.err
	u.err = nil
	return err
}
