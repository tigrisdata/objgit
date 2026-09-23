# Update kefka and add uutils to the hook shell

## Context

`objgitd` pins kefka to a May 2026 commit and registers only its Go
coreutils. The current upstream `main` commit (August 23, 2026) moves common
coreutils to a WASM-backed uutils registry and exposes WASM programs through
`wasmprog`. Hooks and SSH `sh` share
`newHookShell`, so that function is the registration point for both.

## Changes

1. Pin kefka to the current `main` commit and update the module sums.
2. Register wasmprog and uutils after coreutils, following kefka's own CLI.
   The uutils registry provides commands such as `cat` and `ls` and adds
   `base32`; wasmprog provides `jq`, `python3`, `qjs`, and `rg`.
3. Let `mountfs` open read-only directory handles. WASI needs them to
   resolve paths beneath `/`, including `/src` and its subdirectories.
4. Exercise a uutils-only command, `jq`, relative paths after `cd`, and the
   read-only `/src` and writable `/tmp` mounts in the existing push test.
5. Update the hook documentation to describe the available commands.

## Verification

Run `go build ./...`, the hook and SSH protocol tests, then `go test ./...`.

The upstream August commit is not on the public Go module proxy yet. At the
time of this update, the proxy returned 404, and a direct all-refs fetch from
Tangled failed because its `Xe/uutils` ref could not be fetched. The module
was downloaded from a local clone of upstream `main` with the same commit
hash. A clean builder needs the proxy or the upstream fetch to recover.
