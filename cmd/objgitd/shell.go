package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	ssh "github.com/gliderlabs/ssh"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/tigrisdata/objgit/internal/auth"
	"github.com/tigrisdata/objgit/internal/metrics"
	"github.com/tigrisdata/objgit/internal/repofs"
	"golang.org/x/term"
	"mvdan.cc/sh/v3/interp"
	"mvdan.cc/sh/v3/syntax"
)

const shellUsage = "usage: sh <repo> [branch]"

// errShellInterrupt is what shellLines returns when Ctrl-C is pressed at the
// prompt, so the parser drops the pending input.
var errShellInterrupt = errors.New("interrupt")

// shellTarget resolves the commit an sh session inspects: the tip of branch, or
// of HEAD's branch when branch is empty. The returned update describes that
// commit as if it had just been pushed: Old is its first parent, or the zero
// hash for a root commit.
func shellTarget(st storage.Storer, branch string) (refUpdate, *object.Tree, error) {
	name := plumbing.NewBranchReferenceName(branch)
	if branch == "" {
		head, err := st.Reference(plumbing.HEAD)
		if err != nil {
			return refUpdate{}, nil, fmt.Errorf("reading HEAD: %w", err)
		}
		if head.Type() != plumbing.SymbolicReference || !head.Target().IsBranch() {
			return refUpdate{}, nil, errors.New("HEAD is detached; name a branch: " + shellUsage)
		}
		name = head.Target()
	}

	ref, err := st.Reference(name)
	if errors.Is(err, plumbing.ErrReferenceNotFound) {
		return refUpdate{}, nil, fmt.Errorf("branch %q not found", name.Short())
	}
	if err != nil {
		return refUpdate{}, nil, fmt.Errorf("reading %s: %w", name, err)
	}

	commit, err := object.GetCommit(st, ref.Hash())
	if err != nil {
		return refUpdate{}, nil, fmt.Errorf("loading commit %s: %w", ref.Hash(), err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return refUpdate{}, nil, fmt.Errorf("loading tree of %s: %w", commit.Hash, err)
	}

	u := refUpdate{Name: name, New: commit.Hash}
	if len(commit.ParentHashes) > 0 {
		u.Old = commit.ParentHashes[0]
	}
	return u, tree, nil
}

// handleShell services "sh <repo> [branch]": an interactive kefka shell with
// the sandbox and environment a receive-pack hook gets, filled in from the
// branch's tip commit. It needs -allow-hooks, a PTY, and write access to the
// repository, because only a pusher can trigger a hook.
func (d *daemon) handleShell(s ssh.Session, args []string) {
	fail := func(format string, a ...any) {
		fmt.Fprintf(s.Stderr(), "objgitd: "+format+"\n", a...)
		_ = s.Exit(1)
	}

	if !d.allowHooks {
		fail("sh is disabled; start objgitd with -allow-hooks to enable it")
		return
	}
	if len(args) < 1 || len(args) > 2 {
		fail(shellUsage)
		return
	}
	_, winCh, isPty := s.Pty()
	if !isPty {
		fail("sh needs a terminal; use ssh -t")
		return
	}

	ref, err := repofs.Parse(args[0])
	if err != nil {
		fail("%v", err)
		return
	}
	var branch string
	if len(args) == 2 {
		branch = args[1]
	}

	var cred auth.Credential = auth.Anonymous{}
	if key := s.PublicKey(); key != nil {
		cred = auth.PublicKey{Key: key}
	}

	defer metrics.TrackInFlight("ssh")()
	start := time.Now()

	if d.authorize(s.Context(), auth.Request{
		Repo:      ref.Path(),
		Operation: operationFor(transport.ReceivePackService),
		Cred:      cred,
		Transport: "ssh",
	}) != auth.Allow {
		metrics.ObserveGitOp("ssh", "sh", "denied", start)
		fail("access denied")
		return
	}

	// SSH carries no Basic-auth credential into the filesystem resolver.
	var none repofs.Credential
	st, err := d.load(s.Context(), ref, none)
	if err != nil {
		metrics.ObserveGitOp("ssh", "sh", "error", start)
		fail("repository %q not found", ref.Path())
		return
	}
	u, tree, err := shellTarget(st, branch)
	if err != nil {
		metrics.ObserveGitOp("ssh", "sh", "error", start)
		fail("%v", err)
		return
	}

	log := slog.With("repo", ref.Path(), "ref", u.Name.String(), "sha", u.New.String(), "remote", s.RemoteAddr().String())
	log.Info("serving ssh shell")

	in := &shellInput{ReadWriter: s}
	t := term.NewTerminal(in, "$ ")
	// gliderlabs blocks the session's request loop until each window change is
	// received, so drain the channel for the whole session. It closes with the
	// session.
	go func() {
		for win := range winCh {
			// A client without a local terminal (ssh -tt < file) reports 0x0;
			// keep x/term's 80x24 default rather than wrap at every column.
			if win.Width > 0 && win.Height > 0 {
				_ = t.SetSize(win.Width, win.Height)
			}
		}
	}()

	sh, err := newHookShell(tree, hookEnv(ref.Path(), receivePackHook, u), nil, t, t)
	if err != nil {
		metrics.ObserveGitOp("ssh", "sh", "error", start)
		log.Error("ssh shell: build shell", "err", err)
		fail("cannot start shell")
		return
	}

	fmt.Fprintf(t, "objgit hook shell: %s %s @ %s\n", ref.Path(), u.Name, u.New)
	fmt.Fprintln(t, "/src is read-only and /tmp is writable. Type exit or press Ctrl-D to leave.")

	status := d.runShell(s.Context(), t, in, sh, hookStdin(u))
	metrics.ObserveGitOp("ssh", "sh", "ok", start)
	log.Info("ssh shell finished", "exit", status)
	_ = s.Exit(status)
}

// runShell reads statements from t and runs each one in sh until exit, Ctrl-D,
// or a closed session. Each statement gets a fresh copy of the hook's stdin and
// its own -hook-timeout. It returns the shell's exit status.
func (d *daemon) runShell(ctx context.Context, t *term.Terminal, in *shellInput, sh *interp.Runner, stdin string) int {
	lines := &shellLines{t: t, in: in}
	status := 0
	for {
		// A new parser per round drops any partial statement after Ctrl-C or a
		// syntax error.
		lines.reset()
		t.SetPrompt("$ ")
		parser := syntax.NewParser(syntax.Variant(syntax.LangBash))
		for stmts, err := range parser.InteractiveSeq(lines) {
			if err != nil {
				// The terminal already echoed ^C for an interrupt.
				if !lines.interrupted {
					fmt.Fprintln(t, err)
				}
				break
			}
			if parser.Incomplete() {
				t.SetPrompt("> ")
				continue
			}
			for _, stmt := range stmts {
				var exited bool
				status, exited = d.runShellStmt(ctx, t, sh, stmt, stdin)
				if exited {
					return status
				}
			}
			t.SetPrompt("$ ")
		}
		if lines.eof {
			return status
		}
	}
}

// runShellStmt runs one statement and returns its exit status, and whether it
// ran exit. Errors other than a plain exit status, such as a write into /src,
// are printed to t. A script run of the hook stops at such an error; the
// interactive session reports it and carries on, as bash does.
func (d *daemon) runShellStmt(ctx context.Context, t *term.Terminal, sh *interp.Runner, stmt *syntax.Stmt, stdin string) (int, bool) {
	in, err := stdinPipe(stdin)
	if err != nil {
		fmt.Fprintln(t, "objgitd:", err)
		return 1, false
	}
	defer in.Close()
	if err := interp.StdIO(in, t, t)(sh); err != nil {
		fmt.Fprintln(t, "objgitd:", err)
		return 1, false
	}

	ctx, cancel := context.WithTimeout(ctx, d.hookTimeout)
	defer cancel()
	err = sh.Run(ctx, stmt)

	var exit interp.ExitStatus
	switch {
	case err == nil:
		return 0, sh.Exited()
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		fmt.Fprintf(t, "objgitd: command timed out after %s\n", d.hookTimeout)
		return 1, false
	case errors.As(err, &exit):
		return int(exit), sh.Exited()
	default:
		fmt.Fprintln(t, err)
		return 1, false
	}
}

// stdinPipe returns a pipe that reads s and then EOF. s is one hook stdin line,
// which fits in the pipe buffer, so the write cannot block.
func stdinPipe(s string) (*os.File, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	_, err = w.WriteString(s)
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

// ctrlC is the byte a terminal sends for Ctrl-C.
const ctrlC = 0x03

// shellInput is the session as the terminal reads it. x/term reports Ctrl-C as
// io.EOF, the same as Ctrl-D, and it drops both the partial line and the rest
// of the read. shellInput therefore rewrites each Ctrl-C to "^C" and Enter
// before x/term sees it: the terminal echoes ^C as bash does, ends the line,
// and keeps the input that follows. shellLines then discards that line.
type shellInput struct {
	io.ReadWriter
	pending int // Ctrl-C lines that shellLines has not discarded yet

	buf []byte // rewritten input that did not fit in the caller's buffer
	err error  // read error held back until buf drains
}

func (in *shellInput) Read(p []byte) (int, error) {
	if len(in.buf) == 0 {
		if in.err != nil {
			return 0, in.err
		}
		n, err := in.ReadWriter.Read(p)
		count := bytes.Count(p[:n], []byte{ctrlC})
		if count == 0 {
			return n, err
		}
		in.pending += count
		in.buf = bytes.ReplaceAll(p[:n], []byte{ctrlC}, []byte("^C\r"))
		in.err = err
	}
	n := copy(p, in.buf)
	in.buf = in.buf[n:]
	return n, nil
}

// shellLines feeds the parser one terminal line per Read, so line editing and
// history come from x/term.
type shellLines struct {
	t   *term.Terminal
	in  *shellInput
	buf []byte

	eof         bool // the session ended or Ctrl-D was pressed
	interrupted bool // the parser saw errShellInterrupt
}

func (l *shellLines) Read(p []byte) (int, error) {
	if len(l.buf) == 0 {
		line, err := l.t.ReadLine()
		if err != nil {
			l.eof = true
			return 0, err
		}
		if l.in.pending > 0 && strings.Contains(line, "^C") {
			l.in.pending--
			l.interrupted = true
			return 0, errShellInterrupt
		}
		l.buf = append([]byte(line), '\n')
	}
	n := copy(p, l.buf)
	l.buf = l.buf[n:]
	return n, nil
}

// reset drops buffered input and the interrupt mark before a new parser starts.
func (l *shellLines) reset() {
	l.buf = nil
	l.interrupted = false
}
