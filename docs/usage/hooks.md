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
- **After refs are updated.** The hook runs after Git's status report is sent,
  but before the push response closes. Its stdout and stderr stream to the
  pushing client as `remote:` lines when sideband is available. A hook
  **cannot reject a push**. This is a post-receive hook, not a pre-receive gate.
- **Once per changed branch.** If a push updates or creates several branches,
  the hook runs once for each, with environment variables describing that
  branch (see below). Branch **deletions** are skipped (there is nothing to
  check out).
- **Synchronously.** The client waits for the hook to finish or reach
  `-hook-timeout` before the push response closes.

The script is read from the **commit that was just pushed**, so a hook travels
with the branch — different branches can carry different hooks, and updating a
hook is just another commit.

## The execution environment

Hooks run in [kefka](https://xeiaso.net/blog/2026/dancing-mad-sandboxing/), a
virtual `bash` interpreter. **This is not a container, VM, or OS sandbox.** It is
safe because of what it _cannot_ reach, not because of kernel isolation:

- **No system binaries.** Kefka provides Go commands, WASM-backed uutils, and
  the WASM programs `jq`, `python3` (also `python`), `qjs`, and `rg`. Available
  coreutils include `cat`, `ls`, `printf`, `head`, `tail`, `cut`, `sort`,
  `uniq`, `wc`, `tr`, `sha256sum`, `base64`, `base32`, `mkdir`, `cp`, `mv`,
  `rm`, `touch`, `date`, `sleep`, `seq`, and `expr`. There is no `git`,
  package manager, compiler, or `curl`.
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

| Variable                    | Example                    | Notes                                                     |
| --------------------------- | -------------------------- | --------------------------------------------------------- |
| `OBJGIT_REPO`               | `/myproject.git`           | Repository path                                           |
| `OBJGIT_SERVICE`            | `receive-pack`             | Always `receive-pack`                                     |
| `OBJGIT_REF`                | `refs/heads/main`          | Full ref name                                             |
| `OBJGIT_BRANCH`             | `main`                     | Short branch name                                         |
| `OBJGIT_OLD_SHA`            | `0000…0000`                | Previous tip; all zeros when the branch was created       |
| `OBJGIT_NEW_SHA`            | `f43417…`                  | New tip                                                   |
| `OBJGIT_ADDED_FILES_JSON`   | `["new.txt"]`              | Added paths, as a JSON array                              |
| `OBJGIT_CHANGED_FILES_JSON` | `["README.md"]`            | Changed paths, as a JSON array                            |
| `OBJGIT_DELETED_FILES_JSON` | `["old.txt"]`              | Deleted paths, as a JSON array                            |
| `OBJGIT_CHANGES_FILE`       | `/tmp/objgit-changes.json` | File containing all three arrays in a single JSON object |

The file lists describe the **net change of the branch tip** across the push.
If one commit adds a path and a later commit deletes it, that path does not
appear in these lists. A new branch compares its tip against an empty tree.
Renames appear as one added path and one deleted path. Mode-only changes
appear in `changed`. Whitespace and newlines in paths are JSON escaped;
parse the JSON rather than splitting on spaces or lines. A path with invalid
UTF-8 bytes is represented as `/objgit/raw-path/base64url/` followed by the
unpadded base64url encoding of its raw bytes.

`OBJGIT_CHANGES_FILE` lives in the hook's writable `/tmp` filesystem and
contains the same complete lists. For example:

```json
{"added":["new.txt"],"changed":["README.md"],"deleted":["old.txt"]}
```

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

## Exploring the sandbox

You can open the hook sandbox as an interactive shell over SSH. Use it to try
commands before you put them in a hook.

1. Start `objgitd` with `-ssh-bind` and `-allow-hooks`.
2. Push the branch that you want to examine.
3. Connect with a terminal:

   ```text
   ssh -t -p 2222 git@host sh myproject.git
   ```

4. To examine a branch other than the one that `HEAD` points to, give its name:

   ```text
   ssh -t -p 2222 git@host sh myproject.git feature
   ```

The shell is the same as the one that a hook gets:

- `/src` is the tip commit of the branch, read-only, and the shell starts there.
- `/tmp` is scratch space. At the start of each session, it holds only
  `objgit-changes.json`.
- The `OBJGIT_*` variables describe the tip commit as if you pushed it.
  `OBJGIT_OLD_SHA` is its first parent, or all zeros for a root commit.
- The file lists compare the tip commit with that first parent. For a root
  commit, every file is in `OBJGIT_ADDED_FILES_JSON`.
- Each command gets the hook stdin line, so `read old new ref` works.

The shell is different from a hook in these ways:

- A write into `/src` shows an error, but the session continues.
- `-hook-timeout` applies to each command, and not to the session.
- Ctrl-C clears the line at the prompt. It does not stop a command that runs.
  The timeout stops that command.

To leave the shell, type `exit` or press Ctrl-D.

To open the shell, you need write access to the repository. If the server
refuses the session, it shows one of these errors:

| Error                                                          | Cause                                       |
| -------------------------------------------------------------- | ------------------------------------------- |
| `sh is disabled; start objgitd with -allow-hooks to enable it` | The server runs without `-allow-hooks`.     |
| `sh needs a terminal; use ssh -t`                              | The client did not request a PTY.           |
| `access denied`                                                | You do not have write access.               |
| `repository "…" not found`                                     | The repository does not exist.              |
| `branch "…" not found`                                         | The branch does not exist.                  |
| `HEAD is detached; name a branch: …`                           | `HEAD` is not a branch. Give a branch name. |

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

When sideband is unavailable, hook output is captured in the server log.
Hook failure is logged, but it cannot undo the accepted push.

## Limitations

- No writable working tree: `/src` is strictly read-only and `/tmp` is the only
  scratch space.
- No way to reject a push from a hook (it runs after the fact).
- No system tooling, network, or arbitrary executables — only kefka's
  registered commands.
- Hook output reaches the pusher only when the client negotiated sideband.
