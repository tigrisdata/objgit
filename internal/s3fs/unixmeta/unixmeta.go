// Package unixmeta encodes and decodes POSIX file attributes (owner, group,
// permissions, timestamps) as S3 user-defined metadata (x-amz-meta-* headers).
//
// S3 does not natively model Unix attributes, so a POSIX filesystem layered on
// top of object storage keeps them in a small set of string-valued metadata
// headers. The on-the-wire format is documented in
// docs/reference/how-tigris-fs-unix-metadata.md.
package unixmeta

import (
	"net/url"
	"os"
	"os/user"
	"strconv"
	"time"
)

// Metadata keys (without the x-amz-meta- prefix the S3 SDK prepends).
const (
	keyUID           = "uid"
	keyGID           = "gid"
	keyMode          = "mode"
	keyRdev          = "rdev"
	keyMtime         = "mtime"
	keySymlinkTarget = "--symlink-target"
)

// POSIX file-type mask and the bits stored in the high part of mode_t.
const (
	modeTypeMask = 0o170000
	modeRegular  = 0o100000
	modeDir      = 0o040000
	modeSymlink  = 0o120000
	modeBlock    = 0o060000
	modeChar     = 0o020000
	modeFIFO     = 0o010000
	modeSocket   = 0o140000
)

// Attrs is the set of POSIX attributes recorded on an object.
type Attrs struct {
	UID, GID      uint32
	Mode          os.FileMode
	Rdev          uint32
	Mtime         time.Time
	SymlinkTarget string // "" if not a symlink
}

// PosixMode converts a Go os.FileMode to the POSIX mode_t integer stored in
// x-amz-meta-mode.
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
		out |= modeDir
	case m&os.ModeSymlink != 0:
		out |= modeSymlink
	case m&os.ModeDevice != 0 && m&os.ModeCharDevice != 0:
		out |= modeChar
	case m&os.ModeDevice != 0:
		out |= modeBlock
	case m&os.ModeNamedPipe != 0:
		out |= modeFIFO
	case m&os.ModeSocket != 0:
		out |= modeSocket
	default:
		out |= modeRegular
	}
	return out
}

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
	switch p & modeTypeMask {
	case modeDir:
		m |= os.ModeDir
	case modeSymlink:
		m |= os.ModeSymlink
	case modeChar:
		m |= os.ModeDevice | os.ModeCharDevice
	case modeBlock:
		m |= os.ModeDevice
	case modeFIFO:
		m |= os.ModeNamedPipe
	case modeSocket:
		m |= os.ModeSocket
	case modeRegular:
		// regular file: no extra bits
	}
	return m
}

// Encode produces the user-metadata map for a PutObject call. Pass the result
// as PutObjectInput.Metadata; the S3 SDK prepends x-amz-meta- and lowercases
// the keys.
func Encode(a Attrs) map[string]string {
	mode := PosixMode(a.Mode)
	m := map[string]string{
		keyUID:   strconv.FormatUint(uint64(a.UID), 10),
		keyGID:   strconv.FormatUint(uint64(a.GID), 10),
		keyMode:  strconv.FormatUint(uint64(mode), 10),
		keyMtime: strconv.FormatInt(a.Mtime.Unix(), 10),
	}
	if a.Mode&(os.ModeDevice|os.ModeCharDevice) != 0 {
		m[keyRdev] = strconv.FormatUint(uint64(a.Rdev), 10)
	}
	if a.SymlinkTarget != "" {
		m[keySymlinkTarget] = url.QueryEscape(a.SymlinkTarget)
	}
	return m
}

// Decode merges metadata from a HeadObject / GetObject response into defaults.
// Missing keys leave the corresponding field of defaults untouched, which is
// the behavior a POSIX filesystem usually wants: an object with no uid header
// inherits the mount's default uid, not zero. Malformed values are treated as
// missing so a single bad header doesn't make a file unreadable.
func Decode(meta map[string]string, defaults Attrs) Attrs {
	out := defaults
	if s, ok := meta[keyUID]; ok {
		if v, err := strconv.ParseUint(s, 0, 32); err == nil {
			out.UID = uint32(v)
		}
	}
	if s, ok := meta[keyGID]; ok {
		if v, err := strconv.ParseUint(s, 0, 32); err == nil {
			out.GID = uint32(v)
		}
	}
	if s, ok := meta[keyMode]; ok {
		if v, err := strconv.ParseUint(s, 0, 32); err == nil {
			out.Mode = GoFileMode(uint32(v))
		}
	}
	if s, ok := meta[keyRdev]; ok {
		if v, err := strconv.ParseUint(s, 0, 32); err == nil {
			out.Rdev = uint32(v)
		}
	}
	if s, ok := meta[keyMtime]; ok {
		if v, err := strconv.ParseInt(s, 0, 64); err == nil {
			out.Mtime = time.Unix(v, 0)
		}
	}
	if s, ok := meta[keySymlinkTarget]; ok {
		if dec, err := url.QueryUnescape(s); err == nil {
			out.SymlinkTarget = dec
		}
	}
	return out
}

// LookupUID resolves a user name to a numeric UID. It first consults the host
// passwd database via os/user; if the name is not found there it is parsed as a
// decimal UID. This lets callers pass either "alice" or "1000" and works in
// containers that lack a matching passwd entry. The package never calls this
// itself — callers decide whether to resolve names.
func LookupUID(name string) (uint32, error) {
	if u, err := user.Lookup(name); err == nil {
		v, perr := strconv.ParseUint(u.Uid, 10, 32)
		if perr != nil {
			return 0, perr
		}
		return uint32(v), nil
	}
	v, err := strconv.ParseUint(name, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

// LookupGID resolves a group name to a numeric GID, with the same name-or-number
// semantics as LookupUID.
func LookupGID(name string) (uint32, error) {
	if g, err := user.LookupGroup(name); err == nil {
		v, perr := strconv.ParseUint(g.Gid, 10, 32)
		if perr != nil {
			return 0, perr
		}
		return uint32(v), nil
	}
	v, err := strconv.ParseUint(name, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}
