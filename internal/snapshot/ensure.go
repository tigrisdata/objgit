package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/Xe/erofs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// Result statuses.
const (
	StatusBuilt  = "built"
	StatusExists = "exists"
)

// Two EROFS limits that git does not have. The builder checks neither one,
// and the reader refuses a symlink target longer than maxSymlink.
const (
	maxName    = 255
	maxSymlink = 255 * 4
)

// blockSizeBits is the image block size as a power of two: 1<<12 = 4096
// bytes. It is the argument to erofs.WithBlockSize.
const blockSizeBits = 12

// epoch pins every mtime, so one tree always gives identical bytes.
var epoch = time.Unix(0, 0)

// Result describes one Ensure call.
type Result struct {
	Key     string
	Status  string // StatusBuilt or StatusExists
	Files   int    // regular files in the image; 0 when Status is StatusExists
	Bytes   int64  // image size; 0 when Status is StatusExists
	Elapsed time.Duration
}

// Ensure makes sure that the image of tree exists in store. If the image is
// absent, it builds the image from the git objects in objs, in a temp file in
// tmpDir (the OS temp directory when tmpDir is empty), and puts it.
//
// Ensure is idempotent. Two concurrent calls for one tree both build and put
// identical bytes, which is correct, so it takes no lock.
func Ensure(ctx context.Context, objs storer.EncodedObjectStorer, store Store, tree plumbing.Hash, tmpDir string) (Result, error) {
	start := time.Now()
	key := Key(tree)
	res := Result{Key: key}
	done := func() { res.Elapsed = time.Since(start) }

	if err := ctx.Err(); err != nil {
		return res, err
	}
	if _, err := store.StatSnapshot(ctx, key); err == nil {
		res.Status = StatusExists
		done()
		return res, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return res, fmt.Errorf("snapshot: stat %s: %w", key, err)
	}

	root, err := object.GetTree(objs, tree)
	if err != nil {
		return res, fmt.Errorf("snapshot: load tree %s: %w", tree, err)
	}

	// The name is random, never the tree hash: two pushes of one tree can
	// build at the same time.
	f, err := os.CreateTemp(tmpDir, "objgit-snapshot-*.erofs")
	if err != nil {
		return res, fmt.Errorf("snapshot: create temp file: %w", err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()
	// Build's checksum step reads back the whole first block through the
	// same io.ReaderAt it wrote through. *os.File.ReadAt returns io.EOF for
	// a read that runs past the current end of file, which any image
	// smaller than one block does. Preallocating the block up front (as a
	// sparse hole; the trailing zeros are exactly what an unwritten region
	// of the image should read as) avoids that short read.
	if err := f.Truncate(1 << blockSizeBits); err != nil {
		return res, fmt.Errorf("snapshot: preallocate temp file: %w", err)
	}

	b := erofs.NewBuilder(f,
		erofs.WithBlockSize(blockSizeBits),
		erofs.WithEpoch(epoch),
		erofs.WithCompression(erofs.CompressionZstd),
	)
	// Add the root directory explicitly. Build() otherwise creates it
	// itself, but that path mishandles a root whose entries spill past one
	// block (the "large directory" case below), producing an image whose
	// root inode reads back as a non-directory.
	if err := b.AddDir("/", info{"/", fs.ModeDir | 0o755}); err != nil {
		return res, fmt.Errorf("snapshot: add root: %w", err)
	}
	w := &walker{ctx: ctx, objs: objs, b: b}
	if err := w.addTree("/", root); err != nil {
		return res, err
	}
	if err := b.Build(); err != nil {
		return res, fmt.Errorf("snapshot: build %s: %w", key, err)
	}

	sum, size, err := hashFile(f)
	if err != nil {
		return res, err
	}
	meta := map[string]string{
		MetaFormat: strconv.Itoa(FormatVersion),
		MetaSHA256: sum,
		MetaTree:   tree.String(),
		MetaFiles:  strconv.Itoa(w.files),
	}
	if err := store.PutSnapshot(ctx, key, f, size, meta); err != nil {
		return res, fmt.Errorf("snapshot: put %s: %w", key, err)
	}

	res.Status = StatusBuilt
	res.Files = w.files
	res.Bytes = size
	done()
	return res, nil
}

// hashFile returns the hex SHA-256 and size of f, and leaves f at offset 0.
func hashFile(f *os.File) (string, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", 0, fmt.Errorf("snapshot: rewind image: %w", err)
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("snapshot: hash image: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", 0, fmt.Errorf("snapshot: rewind image: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

type walker struct {
	ctx   context.Context
	objs  storer.EncodedObjectStorer
	b     *erofs.Builder
	files int
}

// addTree adds every entry of t under dir, depth first. It walks tree
// entries and not t.Files(), because go-git's FileIter skips gitlinks.
func (w *walker) addTree(dir string, t *object.Tree) error {
	for _, e := range t.Entries {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		p := path.Join(dir, e.Name)
		if len(e.Name) > maxName {
			return fmt.Errorf("snapshot: %s: name is %d bytes, EROFS allows %d", p, len(e.Name), maxName)
		}
		switch e.Mode {
		case filemode.Dir:
			sub, err := object.GetTree(w.objs, e.Hash)
			if err != nil {
				return fmt.Errorf("snapshot: %s: load tree: %w", p, err)
			}
			if err := w.b.AddDir(p, info{e.Name, fs.ModeDir | 0o755}); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p, err)
			}
			if err := w.addTree(p, sub); err != nil {
				return err
			}
		case filemode.Submodule:
			// Same as git checkout of a submodule that is not initialized.
			if err := w.b.AddDir(p, info{e.Name, fs.ModeDir | 0o755}); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p, err)
			}
		case filemode.Regular, filemode.Deprecated, filemode.Executable:
			data, err := w.blob(p, e.Hash)
			if err != nil {
				return err
			}
			perm := fs.FileMode(0o644)
			if e.Mode == filemode.Executable {
				perm = 0o755
			}
			if err := w.b.AddFile(p, info{e.Name, perm}, data); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p, err)
			}
			w.files++
		case filemode.Symlink:
			data, err := w.blob(p, e.Hash)
			if err != nil {
				return err
			}
			if len(data) > maxSymlink {
				return fmt.Errorf("snapshot: %s: symlink target is %d bytes, EROFS allows %d", p, len(data), maxSymlink)
			}
			if err := w.b.AddSymlink(p, string(data), info{e.Name, fs.ModeSymlink | 0o777}); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p, err)
			}
		default:
			return fmt.Errorf("snapshot: %s: unknown git mode %o", p, uint32(e.Mode))
		}
	}
	return nil
}

func (w *walker) blob(p string, h plumbing.Hash) ([]byte, error) {
	blob, err := object.GetBlob(w.objs, h)
	if err != nil {
		return nil, fmt.Errorf("snapshot: %s: load blob: %w", p, err)
	}
	r, err := blob.Reader()
	if err != nil {
		return nil, fmt.Errorf("snapshot: %s: read blob: %w", p, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("snapshot: %s: read blob: %w", p, err)
	}
	return data, nil
}

// info is the fs.FileInfo the builder reads the mode and mtime from.
type info struct {
	name string
	mode fs.FileMode
}

func (i info) Name() string       { return i.name }
func (i info) Size() int64        { return 0 }
func (i info) Mode() fs.FileMode  { return i.mode }
func (i info) ModTime() time.Time { return epoch }
func (i info) IsDir() bool        { return i.mode.IsDir() }
func (i info) Sys() any           { return nil }
