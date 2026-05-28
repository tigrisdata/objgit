// tempfile.go implements the interface billy.TempFile

package s3fs

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/go-git/go-billy/v6"
)

// TempFile creates a uniquely named, write-only file under dir whose name
// begins with prefix. The object is uploaded to S3 when the returned file is
// closed; until then nothing exists in the bucket. The caller is responsible
// for renaming or removing it.
//
// Note: the returned file is write-only. S3 has no read-while-write temp file,
// so callers that reopen the temp path for reading before Close (e.g. go-git's
// streaming PackWriter) are not supported; use the loose-object path instead.
func (fs3 *S3FS) TempFile(dir, prefix string) (billy.File, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("s3fs: generating temp file name: %w", err)
	}

	name := fs3.Join(dir, prefix+hex.EncodeToString(b[:]))
	return newS3WriteFile(fs3.client, fs3.bucket, fs3.key(name), name)
}
