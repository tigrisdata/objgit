// dir.go implements the interface billy.Dir

package s3fs

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	pathpkg "path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// listChildren lists the immediate children of prefix (a full-canonical key
// prefix: "" for the bucket root, otherwise ending in separator), paginating to
// completion. Sub-prefixes come back as kindDir, objects as kindFile carrying
// size/mtime — dirs first then files, preserving S3's lexicographic order. It
// is a free function so the listing cache's getter, which holds only the raw
// client, can reuse it.
func listChildren(ctx context.Context, client s3Client, bucket, separator, prefix string) ([]childEntry, error) {
	var ct *string
	var dirs, files []childEntry
	for {
		start := time.Now()
		res, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            &prefix,
			ContinuationToken: ct,
			Delimiter:         &separator,
		})
		observeS3("ListObjectsV2", start, err)
		if err != nil {
			return nil, err
		}

		for _, d := range res.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(d.Prefix), prefix), "/")
			if name == "" {
				continue
			}
			dirs = append(dirs, childEntry{Name: name, Kind: kindDir})
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
			files = append(files, childEntry{
				Name:  pathpkg.Base(name),
				Kind:  kindFile,
				Size:  aws.ToInt64(f.Size),
				Mtime: aws.ToTime(f.LastModified).UnixNano(),
			})
		}

		if !aws.ToBool(res.IsTruncated) {
			break
		}
		ct = res.NextContinuationToken
	}

	return append(dirs, files...), nil
}

// listSubtree lists every object under root with a single delimiter-less
// (recursive) paginated ListObjectsV2, returning each object's full key plus
// size/mtime. It stops once more than maxKeys objects have accumulated and
// reports truncated=true; the caller then abandons the subtree and falls back to
// delimited per-folder listing, so an unbounded namespace can't blow up memory.
func listSubtree(ctx context.Context, client s3Client, bucket, root string, maxKeys int) (objs []subtreeObject, truncated bool, err error) {
	var ct *string
	for {
		start := time.Now()
		res, lerr := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            &root,
			ContinuationToken: ct,
			// No Delimiter: a recursive listing of the whole subtree.
		})
		observeS3("ListObjectsV2", start, lerr)
		if lerr != nil {
			return nil, false, lerr
		}

		for _, o := range res.Contents {
			objs = append(objs, subtreeObject{
				Key:   aws.ToString(o.Key),
				Size:  aws.ToInt64(o.Size),
				Mtime: aws.ToTime(o.LastModified).UnixNano(),
			})
		}

		if maxKeys > 0 && len(objs) > maxKeys {
			return objs, true, nil
		}
		if !aws.ToBool(res.IsTruncated) {
			return objs, false, nil
		}
		ct = res.NextContinuationToken
	}
}

// ReadDir reads the directory named by dirname and returns a list of
// directory entries. When a listing cache is attached the listing is served
// through it (and reused by later Stat/Open of siblings).
func (fs3 *S3FS) ReadDir(dir string) ([]fs.DirEntry, error) {
	key := strings.TrimPrefix(fs3.cleanPath(dir), "/")
	var prefix string
	if key != "" && key != "." {
		prefix = key + "/"
	}

	ctx := context.TODO()

	var entries []childEntry
	var err error
	if fs3.cache != nil {
		entries, err = fs3.cache.list(ctx, prefix)
	} else {
		entries, err = listChildren(ctx, fs3.client, fs3.bucket, fs3.separator, prefix)
	}
	if err != nil {
		return nil, err
	}

	out := make([]fs.DirEntry, 0, len(entries))
	for _, e := range entries {
		if e.Kind == kindDir {
			out = append(out, fs.FileInfoToDirEntry(newDirInfo(e.Name)))
			continue
		}
		out = append(out, fs.FileInfoToDirEntry(newFileInfo(e.Name, e.Size, time.Unix(0, e.Mtime))))
	}

	return out, nil
}

// MkdirAll creates a directory named path, along with any necessary
// parents, and returns nil, or else returns an error. The permission bits
// perm are used for all directories that MkdirAll creates. If path is/
// already a directory, MkdirAll does nothing and returns nil.
func (fs3 *S3FS) MkdirAll(filename string, perm os.FileMode) error {
	start := time.Now()
	_, err := fs3.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: new(fs3.bucket),
		Key:    new(filename),
		Body:   bytes.NewBuffer(nil),
	})
	observeS3("PutObject", start, err)
	if err == nil && fs3.cache != nil {
		prefix, _ := splitKey(fs3.key(filename))
		fs3.cache.invalidate(prefix)
	}

	return err
}
