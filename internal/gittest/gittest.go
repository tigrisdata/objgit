// Package gittest keeps the real git client used by tests away from the
// repository that runs the tests.
package gittest

import (
	"os"
	"strings"
)

// Isolate removes every GIT_* variable from the process environment. Then it
// points git at empty global and system config files, and turns off
// credential prompts. Call it from TestMain in each package that runs git.
//
// Git exports GIT_DIR, GIT_INDEX_FILE, and other variables to hooks. The
// husky commit-msg hook runs go test. Without Isolate, a test's `git init` or
// `git config` writes to the repository that runs the hook, and not to the
// test's temp dir. In a worktree, that is the config that all worktrees share.
// It gets core.bare = true, and then every worktree stops working.
func Isolate() {
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "GIT_") {
			os.Unsetenv(name)
		}
	}
	os.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	os.Setenv("GIT_CONFIG_SYSTEM", os.DevNull)
	os.Setenv("GIT_TERMINAL_PROMPT", "0")
}
