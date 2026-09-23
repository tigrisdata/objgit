package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	tstorage "github.com/tigrisdata/storage-go"
)

// bucketUsage is what one repository costs in the bucket after a push: how many
// keys it left and how many bytes those keys hold. Both eras address a
// repository by the same key prefix, so one listing measures either of them.
// The "before" build reaches that prefix through go-git's loader over
// internal/s3fs; the "after" build reaches it through
// repofs.BucketResolver.Resolve.
type bucketUsage struct {
	Keys  int   `json:"keys"`
	Bytes int64 `json:"bytes"`
}

// bucketClient reads the bucket the daemon under test writes to. It is the
// harness's own client, separate from the daemon's, so its calls never land in
// the daemon's own S3 counters.
type bucketClient struct {
	api    *tstorage.Client
	bucket string
}

func newBucketClient(ctx context.Context, bucket string) (*bucketClient, error) {
	if bucket == "" {
		return nil, fmt.Errorf("formatbench: -bucket is required (BUCKET in .env)")
	}

	api, err := tstorage.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("formatbench: can't create Tigris storage client: %w", err)
	}

	return &bucketClient{api: api, bucket: bucket}, nil
}

// keyPrefixes returns every bucket prefix a repository can live under, because
// the two builds disagree about the ".git" suffix.
//
// The "after" build parses the transport path through repofs.Parse, which
// strips a trailing ".git" (internal/repofs/repofs.go:47), so its keys sit
// under "<org>/<name>/". The "before" build hands the raw URL path to go-git's
// server loader over internal/s3fs, which keeps the suffix, so its keys sit
// under "<org>/<name>.git/". Only one of the two exists for any measurement,
// so walking both and summing is safe and keeps the harness from having to
// know which era it is talking to.
func keyPrefixes(prefix string) []string {
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, ".git")

	return []string{prefix + "/", prefix + ".git/"}
}

// usage walks every prefix a repository can live under and sums what it finds.
func (c *bucketClient) usage(ctx context.Context, prefix string) (bucketUsage, error) {
	var out bucketUsage

	for _, p := range keyPrefixes(prefix) {
		err := c.walk(ctx, p, func(key string, size int64) error {
			out.Keys++
			out.Bytes += size
			return nil
		})
		if err != nil {
			return out, err
		}
	}

	return out, nil
}

// deleteBatch is how many keys one DeleteObjects call carries. S3 caps a single
// request at 1000 keys, so that cap is what the chunking below is for.
const deleteBatch = 1000

// remove deletes every key a repository left behind. It is only reached from
// -cleanup; a benchmark run never deletes anything on its own.
//
// The count it returns is how many keys the bucket confirmed gone, including on
// the error path, where it is the keys deleted before the failure and not the
// whole chunk that failure happened in.
func (c *bucketClient) remove(ctx context.Context, prefix string) (int, error) {
	var keys []string
	for _, p := range keyPrefixes(prefix) {
		if err := c.walk(ctx, p, func(key string, _ int64) error {
			keys = append(keys, key)
			return nil
		}); err != nil {
			return 0, err
		}
	}

	deleted := 0
	for i := 0; i < len(keys); i += deleteBatch {
		end := min(i+deleteBatch, len(keys))

		objects := make([]types.ObjectIdentifier, 0, end-i)
		for _, key := range keys[i:end] {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
		}

		out, err := c.api.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &c.bucket,
			Delete: &types.Delete{Objects: objects},
		})
		if err != nil {
			return deleted, fmt.Errorf("formatbench: can't delete %d keys under %q: %w", len(objects), prefix, err)
		}

		// DeleteObjects answers 200 even when some of the keys failed, so a
		// per-key failure arrives in the body and never as err.
		deleted += len(out.Deleted)
		if len(out.Errors) > 0 {
			bad := out.Errors[0]
			return deleted, fmt.Errorf("formatbench: can't delete %q (%d of %d keys failed): %s: %s",
				aws.ToString(bad.Key), len(out.Errors), len(objects), aws.ToString(bad.Code), aws.ToString(bad.Message))
		}
	}

	return deleted, nil
}

// walk pages one exact prefix and calls fn for every key.
func (c *bucketClient) walk(ctx context.Context, prefix string, fn func(key string, size int64) error) error {
	token := ""
	for {
		in := &s3.ListObjectsV2Input{Bucket: &c.bucket, Prefix: &prefix}
		if token != "" {
			in.ContinuationToken = &token
		}

		page, err := c.api.ListObjectsV2(ctx, in)
		if err != nil {
			return fmt.Errorf("formatbench: can't list %q: %w", prefix, err)
		}

		for _, entry := range page.Contents {
			if entry.Key == nil {
				continue
			}
			var size int64
			if entry.Size != nil {
				size = *entry.Size
			}
			if err := fn(*entry.Key, size); err != nil {
				return err
			}
		}

		if page.IsTruncated == nil || !*page.IsTruncated || page.NextContinuationToken == nil {
			return nil
		}
		token = *page.NextContinuationToken
	}
}
