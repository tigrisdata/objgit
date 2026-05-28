// dir.go implements the interface billy.Dir

package s3fs

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	pathpkg "path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ReadDir reads the directory named by dirname and returns a list of
// directory entries sorted by filename.
func (fs3 *S3FS) ReadDir(dir string) ([]fs.DirEntry, error) {
	key := strings.TrimPrefix(fs3.cleanPath(dir), "/")
	var prefix string
	if key != "" && key != "." {
		prefix = key + "/"
	}

	ctx := context.TODO()

	var ct *string
	var dirs []fs.DirEntry
	var files []fs.DirEntry
	for {
		res, err := fs3.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &fs3.bucket,
			Prefix:            &prefix,
			ContinuationToken: ct,
			Delimiter:         &fs3.separator,
		})
		if err != nil {
			return nil, err
		}

		for _, d := range res.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(d.Prefix), prefix), "/")
			if name == "" {
				continue
			}
			dirs = append(dirs, fs.FileInfoToDirEntry(newDirInfo(name)))
		}

		for _, f := range res.Contents {
			full := aws.ToString(f.Key)
			if full == prefix {
				// zero-byte directory placeholder; skip
				continue
			}
			name := strings.TrimPrefix(full, prefix)
			if name == "" {
				continue
			}
			files = append(files,
				fs.FileInfoToDirEntry(newFileInfo(
					pathpkg.Base(name),
					aws.ToInt64(f.Size),
					aws.ToTime(f.LastModified),
				)),
			)
		}

		if !aws.ToBool(res.IsTruncated) {
			break
		}
		ct = res.NextContinuationToken
	}

	return append(dirs, files...), nil
}

// MkdirAll creates a directory named path, along with any necessary
// parents, and returns nil, or else returns an error. The permission bits
// perm are used for all directories that MkdirAll creates. If path is/
// already a directory, MkdirAll does nothing and returns nil.
func (fs3 *S3FS) MkdirAll(filename string, perm os.FileMode) error {
	_, err := fs3.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: new(fs3.bucket),
		Key:    new(filename),
		Body:   bytes.NewBuffer(nil),
	})

	return err
}
