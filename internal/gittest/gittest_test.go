package gittest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain isolates this package too. The victim repositories that
// TestIsolate sets up must not see the variables of a real hook.
func TestMain(m *testing.M) {
	Isolate()
	m.Run()
}

// TestIsolate simulates a test binary run from a git hook, where git has
// exported GIT_DIR and friends. After Isolate, a test's own git commands
// must not touch the repository that ran the hook.
func TestIsolate(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}

	for _, tt := range []struct {
		name string
		env  func(victim string) map[string]string
	}{
		{
			name: "commit-msg hook in main checkout",
			env: func(victim string) map[string]string {
				return map[string]string{
					"GIT_DIR":        filepath.Join(victim, ".git"),
					"GIT_INDEX_FILE": filepath.Join(victim, ".git", "index"),
				}
			},
		},
		{
			name: "commit-msg hook in worktree",
			env: func(victim string) map[string]string {
				wt := filepath.Join(filepath.Dir(victim), "wt")
				git(t, victim, "worktree", "add", "-q", wt)
				gitDir := filepath.Join(victim, ".git", "worktrees", "wt")
				return map[string]string{
					"GIT_DIR":        gitDir,
					"GIT_INDEX_FILE": filepath.Join(gitDir, "index"),
				}
			},
		},
		{
			name: "git -c parameters",
			env: func(string) map[string]string {
				return map[string]string{
					"GIT_CONFIG_PARAMETERS": "'user.name'='Leaked'",
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			victim := filepath.Join(t.TempDir(), "victim")
			git(t, "", "init", "-q", victim)
			git(t, victim, "-c", "user.name=a", "-c", "user.email=a@example.com",
				"commit", "-q", "--allow-empty", "-m", "init")
			configPath := filepath.Join(victim, ".git", "config")
			before := readFile(t, configPath)

			// t.Setenv registers a restore for each name that Isolate touches.
			for _, name := range []string{"GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM", "GIT_TERMINAL_PROMPT"} {
				t.Setenv(name, os.Getenv(name))
			}
			for k, v := range tt.env(victim) {
				t.Setenv(k, v)
			}

			Isolate()

			for _, kv := range os.Environ() {
				name, _, _ := strings.Cut(kv, "=")
				switch name {
				case "GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM", "GIT_TERMINAL_PROMPT":
				default:
					if strings.HasPrefix(name, "GIT_") {
						t.Errorf("%s still set after Isolate", name)
					}
				}
			}

			// This is what the protocol tests do in their temp dirs.
			scratch := t.TempDir()
			git(t, scratch, "init", "-q", "--bare", "bare.git")
			git(t, scratch, "init", "-q", "work")
			work := filepath.Join(scratch, "work")
			git(t, work, "config", "user.name", "Test")
			git(t, work, "config", "user.email", "test@example.com")
			git(t, work, "commit", "-q", "--allow-empty", "-m", "test")

			if after := readFile(t, configPath); after != before {
				t.Errorf("victim config changed:\n--- before\n%s\n--- after\n%s", before, after)
			}
			if got := strings.TrimSpace(git(t, work, "log", "-1", "--format=%an")); got != "Test" {
				t.Errorf("commit author = %q, want Test", got)
			}
		})
	}
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
