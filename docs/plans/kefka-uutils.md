# Update kefka and add uutils to the hook shell

## Context

`objgitd` pins kefka to a May 2026 commit and registers only its Go
coreutils. Upstream `v0.0.8` moves common
coreutils to a WASM-backed uutils registry and exposes WASM programs through
`wasmprog`. Hooks and SSH `sh` share
`newHookShell`, so that function is the registration point for both.

## Changes

1. Pin kefka to `v0.0.8` and update the module sums.
2. Register wasmprog and uutils after coreutils, following kefka's own CLI.
   The uutils registry provides commands such as `cat` and `ls` and adds
   `base32`; wasmprog provides `jo`, `jq`, `python3`, `qjs`, and `rg`.
3. Let `mountfs` open read-only directory handles. WASI needs them to
   resolve paths beneath `/`, including `/src` and its subdirectories. Return
   mount-qualified file names so WASI can stat opened files.
4. Exercise a uutils-only command, `jq`, relative paths after `cd`, and the
   read-only `/src` and writable `/tmp` mounts in the existing push test.
5. Update the hook documentation to describe the available commands.

## Verification

Run `go build ./...`, the hook and SSH protocol tests, then `go test ./...`.

The tag points to commit `e1236bb201d5013afcfd3b71ce7259e2d42c7830`.
The public Go module proxy initially returned 404, but Go's normal direct
fallback fetched the tag. `go mod download` passed with an empty module cache
and the `v0.0.8` checksums in `go.sum`.
