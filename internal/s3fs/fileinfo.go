package s3fs

import (
	"io/fs"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// simpleFileInfo implements os.FileInfo
type simpleFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

func newFileInfo(name string, size int64, modTime time.Time) os.FileInfo {
	return simpleFileInfo{
		name:    name,
		size:    size,
		mode:    0666,
		modTime: modTime,
	}
}

func newDirInfo(name string) os.FileInfo {
	return simpleFileInfo{
		name:    name,
		mode:    fs.ModeDir,
		modTime: time.Now(),
	}
}

func (fi simpleFileInfo) Name() string       { return fi.name }
func (fi simpleFileInfo) Size() int64        { return fi.size }
func (fi simpleFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi simpleFileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi simpleFileInfo) Sys() interface{}   { return nil }
func (fi simpleFileInfo) ModTime() time.Time { return fi.modTime }

type enrichedFileInfo struct {
	s3.HeadObjectOutput
	key  string
	mode fs.FileMode
}

func (tfi enrichedFileInfo) Name() string       { return tfi.key }
func (tfi enrichedFileInfo) Size() int64        { return *tfi.ContentLength }
func (tfi enrichedFileInfo) Mode() fs.FileMode  { return tfi.mode }
func (tfi enrichedFileInfo) ModTime() time.Time { return *tfi.LastModified }
func (tfi enrichedFileInfo) IsDir() bool        { return tfi.mode.IsDir() }
func (tfi enrichedFileInfo) Sys() any           { return &tfi.HeadObjectOutput }
