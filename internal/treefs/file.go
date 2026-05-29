package treefs

import (
	"bytes"
	"io/fs"

	"github.com/go-git/go-billy/v6"
)

// file is a read-only billy.File backed by an in-memory copy of a blob.
type file struct {
	*bytes.Reader
	name string
	info fileInfo
}

func newFile(name string, data []byte, info fileInfo) *file {
	return &file{Reader: bytes.NewReader(data), name: name, info: info}
}

func (f *file) Name() string                       { return f.name }
func (f *file) Stat() (fs.FileInfo, error)         { return f.info, nil }
func (f *file) Close() error                       { return nil }
func (f *file) Write([]byte) (int, error)          { return 0, billy.ErrReadOnly }
func (f *file) WriteAt([]byte, int64) (int, error) { return 0, billy.ErrReadOnly }
func (f *file) Truncate(int64) error               { return billy.ErrReadOnly }
func (f *file) Lock() error                        { return nil }
func (f *file) Unlock() error                      { return nil }

var _ billy.File = (*file)(nil)
