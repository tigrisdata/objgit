// tempfile.go implements the interface billy.TempFile

package s3fs

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/go-git/go-billy/v6"
)

// TempFile creates a uniquely named file under dir whose name begins with
// prefix and returns a write handle to it. The bytes live in an in-memory
// buffer registered against the filesystem; a subsequent Open of the same
// path returns a reader over that same buffer (needed by go-git's streaming
// PackWriter, which reads the temp pack back as it is written). The buffer
// is uploaded to S3 only when the caller renames the path to its final
// location; Remove discards it.
func (fs3 *S3FS) TempFile(dir, prefix string) (billy.File, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("s3fs: generating temp file name: %w", err)
	}

	name := fs3.Join(dir, prefix+hex.EncodeToString(b[:]))
	buf := &tempBuffer{}
	fs3.registerTemp(name, buf)
	return &tempWriteFile{buf: buf, name: name}, nil
}
