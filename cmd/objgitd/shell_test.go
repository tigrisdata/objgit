package main

import (
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"mvdan.cc/sh/v3/expand"
)

func TestHookEnv(t *testing.T) {
	u := refUpdate{
		Name: plumbing.NewBranchReferenceName("main"),
		Old:  plumbing.NewHash("1111111111111111111111111111111111111111"),
		New:  plumbing.NewHash("2222222222222222222222222222222222222222"),
	}
	c := hookChanges{
		added:   []byte(`["a.txt"]`),
		changed: []byte(`["b.txt"]`),
		deleted: []byte(`[]`),
	}
	env := expand.ListEnviron(hookEnv("acme/test", "receive-pack", u, c)...)

	for _, tt := range []struct {
		name string
		want string
	}{
		{name: "HOME", want: "/tmp"},
		{name: "PWD", want: "/src"},
		{name: "TMPDIR", want: "/tmp"},
		{name: "KEFKA", want: "1"},
		{name: "OBJGIT_REPO", want: "acme/test"},
		{name: "OBJGIT_SERVICE", want: "receive-pack"},
		{name: "OBJGIT_REF", want: "refs/heads/main"},
		{name: "OBJGIT_BRANCH", want: "main"},
		{name: "OBJGIT_OLD_SHA", want: u.Old.String()},
		{name: "OBJGIT_NEW_SHA", want: u.New.String()},
		{name: "OBJGIT_ADDED_FILES_JSON", want: `["a.txt"]`},
		{name: "OBJGIT_CHANGED_FILES_JSON", want: `["b.txt"]`},
		{name: "OBJGIT_DELETED_FILES_JSON", want: `[]`},
		{name: "OBJGIT_CHANGES_FILE", want: "/tmp/objgit-changes.json"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := env.Get(tt.name).String(); got != tt.want {
				t.Errorf("%s = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

// historyRepo builds an on-disk repository with two commits on main and one on
// a dev branch forked from main's first commit. It returns the work tree and
// the commit hashes by name.
func historyRepo(t *testing.T) (string, map[string]string) {
	t.Helper()
	work := seedRepo(t) // main: "initial"
	hashes := map[string]string{}
	rev := func(name string) string {
		return strings.TrimSpace(runGit(t, work, "rev-parse", name))
	}
	hashes["root"] = rev("HEAD")

	runGit(t, work, "commit", "--allow-empty", "-m", "second")
	hashes["main"] = rev("HEAD")

	runGit(t, work, "checkout", "-b", "dev", hashes["root"])
	runGit(t, work, "commit", "--allow-empty", "-m", "on dev")
	hashes["dev"] = rev("HEAD")

	runGit(t, work, "checkout", "--orphan", "lonely")
	runGit(t, work, "commit", "--allow-empty", "-m", "orphan root")
	hashes["lonely"] = rev("HEAD")

	runGit(t, work, "checkout", "main")
	return work, hashes
}

func TestShellTarget(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	work, hashes := historyRepo(t)

	for _, tt := range []struct {
		name     string
		branch   string
		detach   bool
		wantRef  string
		wantOld  string // hash name in hashes; "" means the zero hash
		wantNew  string
		wantErr  bool
		errMatch string
	}{
		{
			name:    "empty branch uses HEAD's branch",
			wantRef: "refs/heads/main",
			wantOld: "root",
			wantNew: "main",
		},
		{
			name:    "named branch",
			branch:  "dev",
			wantRef: "refs/heads/dev",
			wantOld: "root",
			wantNew: "dev",
		},
		{
			name:    "root commit has a zero old sha",
			branch:  "lonely",
			wantRef: "refs/heads/lonely",
			wantNew: "lonely",
		},
		{
			name:     "missing branch",
			branch:   "nope",
			wantErr:  true,
			errMatch: `branch "nope" not found`,
		},
		{
			name:     "detached HEAD needs a branch",
			detach:   true,
			wantErr:  true,
			errMatch: "HEAD is detached",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo, err := git.PlainOpen(work)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			st := repo.Storer
			if tt.detach {
				head := plumbing.NewHashReference(plumbing.HEAD, plumbing.NewHash(hashes["main"]))
				if err := st.SetReference(head); err != nil {
					t.Fatalf("detach HEAD: %v", err)
				}
				t.Cleanup(func() {
					_ = st.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main")))
				})
			}

			u, tree, err := shellTarget(st, tt.branch)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("shellTarget(%q) = %+v, want error", tt.branch, u)
				}
				if !strings.Contains(err.Error(), tt.errMatch) {
					t.Errorf("error = %q, want it to contain %q", err, tt.errMatch)
				}
				return
			}
			if err != nil {
				t.Fatalf("shellTarget(%q): %v", tt.branch, err)
			}
			if tree == nil {
				t.Fatal("tree is nil")
			}
			if got := u.Name.String(); got != tt.wantRef {
				t.Errorf("ref = %q, want %q", got, tt.wantRef)
			}
			wantOld := plumbing.ZeroHash
			if tt.wantOld != "" {
				wantOld = plumbing.NewHash(hashes[tt.wantOld])
			}
			if u.Old != wantOld {
				t.Errorf("old = %s, want %s", u.Old, wantOld)
			}
			if want := plumbing.NewHash(hashes[tt.wantNew]); u.New != want {
				t.Errorf("new = %s, want %s", u.New, want)
			}
		})
	}
}

// sshShell runs "ssh <addr> sh <args...>" with the given stdin. forcePTY
// passes -tt so ssh allocates a remote PTY even though stdin is a pipe.
func sshShell(t *testing.T, addr string, forcePTY bool, stdin string, args ...string) (string, int) {
	t.Helper()
	host, port, _ := strings.Cut(addr, ":")
	sshArgs := []string{
		"-i", sshClientKey(t),
		"-o", "IdentitiesOnly=yes",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "LogLevel=ERROR",
		"-p", port,
	}
	if forcePTY {
		sshArgs = append(sshArgs, "-tt")
	} else {
		sshArgs = append(sshArgs, "-T")
	}
	sshArgs = append(sshArgs, "git@"+host, "sh")
	sshArgs = append(sshArgs, args...)

	cmd := exec.Command("ssh", sshArgs...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case errors.As(err, &exitErr):
		return string(out), exitErr.ExitCode()
	case err != nil:
		t.Fatalf("ssh: %v\n%s", err, out)
	}
	return string(out), 0
}

// TestSSHShell drives the sh command with a real ssh client against a
// repository pushed over SSH, covering the sandbox, the hook environment, the
// exit status, and each refusal path.
func TestSSHShell(t *testing.T) {
	checkSSHBinaries(t)

	for _, tt := range []struct {
		name       string
		allowPush  bool
		allowHooks bool
		push       bool
		forcePTY   bool
		args       []string
		stdin      string
		wantCode   int
		want       []string
	}{
		{
			name:       "interactive session sees the hook sandbox",
			allowPush:  true,
			allowHooks: true,
			push:       true,
			forcePTY:   true,
			args:       []string{"acme/shell.git"},
			stdin: strings.Join([]string{
				`echo "new=$OBJGIT_NEW_SHA"`,
				`echo "branch=$OBJGIT_BRANCH cwd=$PWD svc=$OBJGIT_SERVICE"`,
				`read old new ref; echo "stdin=$ref"`,
				`cat README.md`,
				`echo "added=$OBJGIT_ADDED_FILES_JSON"`,
				`cat "$OBJGIT_CHANGES_FILE"`,
				`printf 'x%sy\n' tmp > /tmp/x && cat /tmp/x`,
				`if true; then`,
				`echo "cont$((1))"`,
				`fi`,
				`echo nope > /src/y`,
				`echo "alive$((2))"`,
				`exit 3`,
			}, "\n") + "\n",
			wantCode: 3,
			want: []string{
				"objgit hook shell",
				"branch=main cwd=/src svc=receive-pack\r",
				"stdin=refs/heads/main",
				"hello from the shell repo",
				`added=["README.md"]`,
				`{"added":["README.md"],"changed":[],"deleted":[]}`,
				"xtmpy",
				"cont1",
				"read-only filesystem",
				"alive2",
			},
		},
		{
			name:       "named branch",
			allowPush:  true,
			allowHooks: true,
			push:       true,
			forcePTY:   true,
			args:       []string{"acme/shell.git", "main"},
			stdin:      "echo \"ref=$OBJGIT_REF\"\nexit\n",
			want:       []string{"ref=refs/heads/main"},
		},
		{
			name:       "ctrl-c drops a partial statement",
			allowPush:  true,
			allowHooks: true,
			push:       true,
			forcePTY:   true,
			args:       []string{"acme/shell.git"},
			stdin:      "if true; then\n\x03echo \"after$((5))\"\nexit 4\n",
			wantCode:   4,
			want:       []string{"^C", "after5"},
		},
		{
			name:       "syntax error keeps the session",
			allowPush:  true,
			allowHooks: true,
			push:       true,
			forcePTY:   true,
			args:       []string{"acme/shell.git"},
			stdin:      "fi\necho \"ok$((6))\"\nexit\n",
			want:       []string{"ok6"},
		},
		{
			name:       "eof ends the session with the last status",
			allowPush:  true,
			allowHooks: true,
			push:       true,
			forcePTY:   true,
			args:       []string{"acme/shell.git"},
			stdin:      "false\n",
			wantCode:   1,
		},
		{
			name:       "no pty",
			allowPush:  true,
			allowHooks: true,
			push:       true,
			args:       []string{"acme/shell.git"},
			wantCode:   1,
			want:       []string{"sh needs a terminal"},
		},
		{
			name:      "hooks disabled",
			allowPush: true,
			push:      true,
			forcePTY:  true,
			args:      []string{"acme/shell.git"},
			wantCode:  1,
			want:      []string{"-allow-hooks"},
		},
		{
			name:       "write access denied",
			allowHooks: true,
			forcePTY:   true,
			args:       []string{"acme/shell.git"},
			wantCode:   1,
			want:       []string{"access denied"},
		},
		{
			name:       "missing repository",
			allowPush:  true,
			allowHooks: true,
			forcePTY:   true,
			args:       []string{"acme/missing.git"},
			wantCode:   1,
			want:       []string{"not found"},
		},
		{
			name:       "missing branch",
			allowPush:  true,
			allowHooks: true,
			push:       true,
			forcePTY:   true,
			args:       []string{"acme/shell.git", "nope"},
			wantCode:   1,
			want:       []string{`branch "nope" not found`},
		},
		{
			name:       "no repository argument",
			allowPush:  true,
			allowHooks: true,
			forcePTY:   true,
			wantCode:   1,
			want:       []string{"usage: sh <repo> [branch, tag, or commit]"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			addr, _ := startSSHServer(t, tt.allowPush, tt.allowHooks)
			env := gitSSHEnv(t)

			var head string
			if tt.push {
				work := t.TempDir()
				runGit(t, work, "init", "-b", "main")
				runGit(t, work, "config", "user.email", "test@example.com")
				runGit(t, work, "config", "user.name", "Test")
				writeFile(t, filepath.Join(work, "README.md"), "hello from the shell repo\n")
				runGit(t, work, "add", ".")
				runGit(t, work, "commit", "-m", "shell repo")
				head = strings.TrimSpace(runGit(t, work, "rev-parse", "HEAD"))
				if out, err := gitWithEnv(work, env, "push", "ssh://git@"+addr+"/acme/shell.git", "main"); err != nil {
					t.Fatalf("push failed: %v\n%s", err, out)
				}
			}

			out, code := sshShell(t, addr, tt.forcePTY, tt.stdin, tt.args...)
			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d; output:\n%s", code, tt.wantCode, out)
			}
			want := slices.Clone(tt.want)
			if tt.wantCode == 3 {
				want = append(want, fmt.Sprintf("new=%s", head))
			}
			for _, w := range want {
				if !strings.Contains(out, w) {
					t.Errorf("output missing %q; output:\n%s", w, out)
				}
			}
		})
	}
}
