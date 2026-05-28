# How Unix metadata is stored on Tigris objects

A POSIX filesystem mapped onto S3-compatible object storage needs somewhere to keep
Unix attributes (owner, group, permissions, timestamps, symlink targets) that S3
itself does not natively model. The convention used here is to store them as
**S3 user-defined metadata** — a small set of `x-amz-meta-*` HTTP headers attached
to each object.

This document describes the on-the-wire format and shows how to read and write it
with `aws-sdk-go-v2`.

## The headers

Every value is a string. Integers are decimal-encoded.

| Header | Meaning | Value format |
|---|---|---|
| `x-amz-meta-uid` | Owner user ID | decimal `uint32` |
| `x-amz-meta-gid` | Owner group ID | decimal `uint32` |
| `x-amz-meta-mode` | POSIX file mode (type + permission bits) | decimal `uint32` |
| `x-amz-meta-rdev` | Device number (block/character devices only) | decimal `uint32` |
| `x-amz-meta-mtime` | Modification time | Unix seconds, decimal |
| `x-amz-meta---symlink-target` | Target path of a symlink | URL-percent-escaped string |

The header names are lowercase. Headers are written only when the value differs
from the consumer's default; a missing header means "use the default" rather than
"value is zero." Typical defaults are the mounting user's UID/GID and `0o644` for
files / `0o755` for directories.

The `--symlink-target` header really does have three dashes after `meta-`: the
attribute name is the literal string `--symlink-target`, and the S3 SDK prepends
the standard `x-amz-meta-` prefix.

## Mode encoding

The `mode` value is the standard POSIX `mode_t` integer: the low 9 bits encode
`rwxrwxrwx`, the next 3 encode setuid/setgid/sticky, and the file type lives in
the high bits.

| Type | Octal mask |
|---|---|
| Regular file | `0o100000` |
| Directory | `0o040000` |
| Symbolic link | `0o120000` |
| Block device | `0o060000` |
| Character device | `0o020000` |
| FIFO | `0o010000` |
| Socket | `0o140000` |

So `33188` (decimal) = `0o100644` = regular file with `rw-r--r--`.

Go's `os.FileMode` uses a different layout (type bits at `1<<31` and friends), so
a conversion is required at the boundary.

## Value escaping

HTTP header values must be plain ASCII without control characters, and S3
normalizes whitespace. To keep arbitrary bytes (UTF-8 paths, control characters,
`%`) round-tripping safely, values are percent-encoded before being put in the
header and decoded after reading. This is important for `--symlink-target`,
whose value is an arbitrary filesystem path.

## Writing metadata

The AWS SDK accepts user metadata as a `map[string]string` on
`PutObjectInput.Metadata`. The SDK prepends `x-amz-meta-` and lowercases the
keys, so you supply the bare attribute name.

```go
package unixmeta

import (
	"net/url"
	"os"
	"strconv"
	"time"
)

// PosixMode converts a Go os.FileMode to the POSIX mode_t integer
// stored in x-amz-meta-mode.
func PosixMode(m os.FileMode) uint32 {
	out := uint32(m.Perm()) // low 9 permission bits
	if m&os.ModeSetuid != 0 {
		out |= 0o4000
	}
	if m&os.ModeSetgid != 0 {
		out |= 0o2000
	}
	if m&os.ModeSticky != 0 {
		out |= 0o1000
	}
	switch {
	case m&os.ModeDir != 0:
		out |= 0o040000
	case m&os.ModeSymlink != 0:
		out |= 0o120000
	case m&os.ModeDevice != 0 && m&os.ModeCharDevice != 0:
		out |= 0o020000
	case m&os.ModeDevice != 0:
		out |= 0o060000
	case m&os.ModeNamedPipe != 0:
		out |= 0o010000
	case m&os.ModeSocket != 0:
		out |= 0o140000
	default:
		out |= 0o100000
	}
	return out
}

// Attrs is what a caller wants to record on an object.
type Attrs struct {
	UID, GID      uint32
	Mode          os.FileMode
	Rdev          uint32
	Mtime         time.Time
	SymlinkTarget string // "" if not a symlink
}

// Encode produces the user-metadata map for a PutObject call.
// Pass the result as PutObjectInput.Metadata.
func Encode(a Attrs) map[string]string {
	mode := PosixMode(a.Mode)
	m := map[string]string{
		"uid":   strconv.FormatUint(uint64(a.UID), 10),
		"gid":   strconv.FormatUint(uint64(a.GID), 10),
		"mode":  strconv.FormatUint(uint64(mode), 10),
		"mtime": strconv.FormatInt(a.Mtime.Unix(), 10),
	}
	if a.Mode&(os.ModeDevice|os.ModeCharDevice) != 0 {
		m["rdev"] = strconv.FormatUint(uint64(a.Rdev), 10)
	}
	if a.SymlinkTarget != "" {
		m["--symlink-target"] = url.QueryEscape(a.SymlinkTarget)
	}
	return m
}
```

Used at the call site:

```go
_, err := client.PutObject(ctx, &s3.PutObjectInput{
	Bucket:   aws.String("mybucket"),
	Key:      aws.String("path/to/file"),
	Body:     body,
	Metadata: unixmeta.Encode(unixmeta.Attrs{
		UID:   1000,
		GID:   1000,
		Mode:  0o644,
		Mtime: time.Now(),
	}),
})
```

## Reading metadata

`HeadObject` and `GetObject` both return user metadata in `Metadata
map[string]string`, with the `x-amz-meta-` prefix already stripped and keys
lowercased.

```go
package unixmeta

import (
	"net/url"
	"os"
	"strconv"
	"time"
)

// GoFileMode is the inverse of PosixMode.
func GoFileMode(p uint32) os.FileMode {
	m := os.FileMode(p & 0o777)
	if p&0o4000 != 0 {
		m |= os.ModeSetuid
	}
	if p&0o2000 != 0 {
		m |= os.ModeSetgid
	}
	if p&0o1000 != 0 {
		m |= os.ModeSticky
	}
	switch p & 0o170000 {
	case 0o040000:
		m |= os.ModeDir
	case 0o120000:
		m |= os.ModeSymlink
	case 0o020000:
		m |= os.ModeDevice | os.ModeCharDevice
	case 0o060000:
		m |= os.ModeDevice
	case 0o010000:
		m |= os.ModeNamedPipe
	case 0o140000:
		m |= os.ModeSocket
	case 0o100000:
		// regular file: no extra bits
	}
	return m
}

// Decode merges metadata from a HeadObject / GetObject response into defaults.
// Missing keys leave the corresponding field of `defaults` untouched, which is
// the behavior a POSIX filesystem usually wants: an object with no uid header
// inherits the mount's default uid, not zero.
func Decode(meta map[string]string, defaults Attrs) Attrs {
	out := defaults
	if s, ok := meta["uid"]; ok {
		if v, err := strconv.ParseUint(s, 0, 32); err == nil {
			out.UID = uint32(v)
		}
	}
	if s, ok := meta["gid"]; ok {
		if v, err := strconv.ParseUint(s, 0, 32); err == nil {
			out.GID = uint32(v)
		}
	}
	if s, ok := meta["mode"]; ok {
		if v, err := strconv.ParseUint(s, 0, 32); err == nil {
			out.Mode = GoFileMode(uint32(v))
		}
	}
	if s, ok := meta["rdev"]; ok {
		if v, err := strconv.ParseUint(s, 0, 32); err == nil {
			out.Rdev = uint32(v)
		}
	}
	if s, ok := meta["mtime"]; ok {
		if v, err := strconv.ParseInt(s, 0, 64); err == nil {
			out.Mtime = time.Unix(v, 0)
		}
	}
	if s, ok := meta["--symlink-target"]; ok {
		if dec, err := url.QueryUnescape(s); err == nil {
			out.SymlinkTarget = dec
		}
	}
	return out
}
```

Used at the call site:

```go
head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
	Bucket: aws.String("mybucket"),
	Key:    aws.String("path/to/file"),
})
if err != nil {
	return err
}

attrs := unixmeta.Decode(head.Metadata, unixmeta.Attrs{
	UID:  uint32(os.Getuid()),
	GID:  uint32(os.Getgid()),
	Mode: 0o644,
	// Mtime defaults to the object's S3 LastModified if mtime is missing:
	Mtime: aws.ToTime(head.LastModified),
})
```

## Parsing notes

- `strconv.ParseUint(s, 0, ...)` with base `0` accepts decimal, `0o`-prefixed
  octal, and `0x`-prefixed hex. Writers should emit decimal; readers should be
  liberal.
- Treat malformed values as missing — parse errors fall through to the default
  rather than failing the whole lookup. A single bad header should not make a
  file unreadable.
- The `mode` header carries both the permission bits and the file-type bits.
  When reapplying it to an in-memory inode, mask out the type bits if you only
  want to update permissions (for `chmod`), and mask out the permission bits if
  you only want to update the type (rare — typically set once at creation).

## Directories and zero-byte objects

Directories are zero-byte S3 objects whose key ends in `/`. They carry the same
metadata headers as regular files, with `mode` containing the directory type bit
(`0o040000`). Symlinks are likewise zero-byte objects; the link target lives
entirely in the `--symlink-target` header, not in the object body.
