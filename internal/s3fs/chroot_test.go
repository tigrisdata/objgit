package s3fs

import "testing"

// TestChrootPreservesSeparator guards against dropping the separator when
// chrooting. An empty separator makes ListObjectsV2 run without a delimiter,
// which flattens ReadDir and breaks directory-structured reads such as git ref
// enumeration under refs/.
func TestChrootPreservesSeparator(t *testing.T) {
	base := &S3FS{bucket: "bucket", separator: DefaultSeparator}

	sub, err := base.Chroot("repo.git")
	if err != nil {
		t.Fatalf("Chroot: %v", err)
	}

	got := sub.(*S3FS).separator
	if got != DefaultSeparator {
		t.Logf("want: %q", DefaultSeparator)
		t.Logf("got:  %q", got)
		t.Error("Chroot dropped the separator")
	}
}
