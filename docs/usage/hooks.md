# Push hooks

`objgitd` can run a script after a successful push. The script lives **inside the
repository** at `.objgit/hooks/receive-pack` and runs in a restricted, in-process
shell — not a real operating-system shell. This document describes how to enable
hooks, what a hook can and cannot do, and how to write one.

## Enabling hooks

Hooks are off by default. Start the server with `-allow-hooks`:

```text
./objgitd -bucket $BUCKET -allow-push -allow-hooks
```

| Flag            | Env            | Default | Meaning                                                  |
| --------------- | -------------- | ------- | -------------------------------------------------------- |
| `-allow-hooks`  | `ALLOW_HOOKS`  | `false` | Run `.objgit/hooks/receive-pack` after a successful push |
| `-hook-timeout` | `HOOK_TIMEOUT` | `60s`   | Wall-clock limit for a single hook run                   |

`-allow-hooks` is independent of `-allow-push`, but a hook can only fire on a
push, so in practice you want both.

## When a hook runs

- **On push only.** The hook is named after the git service that triggered it,
  and the only service that runs a hook is `receive-pack` (push). Fetches,
  clones, and archives never run hooks.
- **After the push completes.** Refs are already updated and the client has
  already received its response when the hook starts. A hook therefore **cannot
  reject a push** and its output is never shown to the person pushing — it goes
  to the server log only. This is a post-receive hook, not a pre-receive gate.
- **Once per changed branch.** If a push updates or creates several branches,
  the hook runs once for each, with environment variables describing that
  branch (see below). Branch **deletions** are skipped (there is nothing to
  check out).
- **Asynchronously.** The push returns immediately; the hook runs in the
  background. On server shutdown, in-flight hooks are given a short grace period
  to finish.

The script is read from the **commit that was just pushed**, so a hook travels
with the branch — different branches can carry different hooks, and updating a
hook is just another commit.

## The execution environment

Hooks run in [kefka](https://xeiaso.net/blog/2026/dancing-mad-sandboxing/), a
virtual `bash` interpreter. **This is not a container, VM, or OS sandbox.** It is
safe because of what it _cannot_ reach, not because of kernel isolation:

- **No system binaries.** Only kefka's built-in commands exist — roughly the
  POSIX coreutils: `cat`, `ls`, `echo`, `printf`, `head`, `tail`, `cut`, `sort`,
  `uniq`, `wc`, `tr`, `grep`, `sha256sum`, `base64`, `mkdir`, `cp`, `mv`, `rm`,
  `touch`, `date`, `sleep`, `seq`, `expr`, and so on. There is no `git`, no
  package manager, no compiler, no `curl`.
- **No network.**
- **No host filesystem.** The only files a hook can see are the two mounts
  below.

Standard `bash` syntax works: variables, `if`/`for`/`while`, pipes,
redirections, command substitution, `[ ... ]` tests, and `&&`/`||`.

### Filesystem layout

| Path   | Contents                                                        | Writable?          |
| ------ | --------------------------------------------------------------- | ------------------ |
| `/src` | The pushed commit, checked out. The shell starts here (`$PWD`). | **No** — read-only |
| `/tmp` | Empty scratch space. Also `$HOME` and `$TMPDIR`.                | Yes                |

`/src` is a live, lazy view of the git tree: files are fetched from object
storage as they are opened, so nothing is copied to disk up front. Everything
under `/src` is **read-only**. Writing scratch data — including shell
redirections like `echo x > out` — must target `/tmp`.

> **Important:** a redirection into a read-only path _aborts the script_. For
> example `echo hi > /src/note.txt` does not merely fail that one line; it stops
> the hook with a "read-only filesystem" error. Always redirect into `/tmp`.

### Environment variables

Each run gets variables describing the branch that triggered it:

| Variable         | Example           | Notes                                               |
| ---------------- | ----------------- | --------------------------------------------------- |
| `OBJGIT_REPO`    | `/myproject.git`  | Repository path                                     |
| `OBJGIT_SERVICE` | `receive-pack`    | Always `receive-pack`                               |
| `OBJGIT_REF`     | `refs/heads/main` | Full ref name                                       |
| `OBJGIT_BRANCH`  | `main`            | Short branch name                                   |
| `OBJGIT_OLD_SHA` | `0000…0000`       | Previous tip; all zeros when the branch was created |
| `OBJGIT_NEW_SHA` | `f43417…`         | New tip                                             |

For compatibility with scripts written for stock git, the same information is
also fed on **stdin** as a single `<old> <new> <ref>` line.

## Writing a hook

Put the script at `.objgit/hooks/receive-pack` in your repository and commit it.
The executable bit is not required — `objgitd` reads the file's contents, not its
mode.

```bash
#!/usr/bin/env bash
# .objgit/hooks/receive-pack

echo "push to ${OBJGIT_REPO} ${OBJGIT_REF}: ${OBJGIT_OLD_SHA} -> ${OBJGIT_NEW_SHA}"

# /src is the checkout of the new commit; the shell starts there.
echo "top-level contents:"
ls /src

# Read a file out of the push.
if [ -f /src/go.mod ]; then
	module="$(head -n 1 /src/go.mod | cut -d' ' -f2)"
	echo "go module: ${module}"
fi

# Scratch work goes in /tmp.
manifest=/tmp/manifest.txt
echo "ref ${OBJGIT_REF}" > "${manifest}"
echo "sha ${OBJGIT_NEW_SHA}" >> "${manifest}"
cat "${manifest}"

echo "hook done"
```

A copy of this example lives at
[`.objgit/hooks/receive-pack`](../../.objgit/hooks/receive-pack) in this
repository.

## Observing hooks

All hook activity is logged through the server's structured (`slog`) logger:

- `hook: running` — a hook started, with `repo`, `service`, `ref`, and `sha`.
- `hook: finished` — success, with the hook's `exit` code, captured `stdout`,
  and `stderr`.
- `hook: finished with errors` — the hook exited non-zero, failed to parse, hit
  the timeout, or tried to write somewhere read-only. The error is attached
  under the `err` key.
- `hook: no hook file in pushed tree` (debug level) — the push had no
  `.objgit/hooks/receive-pack`, so nothing ran.

Because output is log-only, a hook cannot communicate back to the client that
pushed.

## Limitations

- No writable working tree: `/src` is strictly read-only and `/tmp` is the only
  scratch space.
- No way to reject a push from a hook (it runs after the fact).
- No system tooling, network, or arbitrary executables — only kefka built-ins.
- Output is not relayed to the pusher.
