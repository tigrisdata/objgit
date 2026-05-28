// tempfile.go implements the interface billy.TempFile

package s3fs

import "github.com/go-git/go-billy/v6"

// TempFile creates a new temporary file in the directory dir with a name
// beginning with prefix, opens the file for reading and writing, and
// returns the resulting *os.File. If dir is the empty string, TempFile
// uses the default directory for temporary files (see os.TempDir).
// Multiple programs calling TempFile simultaneously will not choose the
// same file. The caller can use f.Name() to find the pathname of the file.
// It is the caller's responsibility to remove the file when no longer
// needed.
func (fs3 *S3FS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, nil
}
