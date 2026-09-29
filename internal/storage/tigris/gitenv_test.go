package tigris

import (
	"testing"

	"github.com/tigrisdata/objgit/internal/gittest"
)

// TestMain strips the GIT_* variables that a git hook exports, so the real
// git client in these tests can not write to the repository that runs them.
func TestMain(m *testing.M) {
	gittest.Isolate()
	m.Run()
}
