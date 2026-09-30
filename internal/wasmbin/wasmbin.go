// Package wasmbin runs WASI programs from directories on the host as Kefka
// shell commands, so a hook can use a program such as kustomize that the
// daemon binary does not embed.
//
// Load lists the directories once. Each *.wasm file in them becomes a command
// named after the file without the extension. When two directories have the
// same name, the first one wins, as in PATH. A program is read and compiled on
// its first run, not by Load, and every shell shares the compiled form. With a
// cache directory, the compiled form also survives a restart.
//
// The image ships the repository's bin directory, whose .wasm files are Git
// LFS objects. A checkout without LFS hydration has pointer files there, and
// Exec then fails with an error that says so.
package wasmbin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/Xe/kefka/command"
	"github.com/Xe/kefka/command/registry"
	"github.com/Xe/kefka/wasm/billyfs"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	wsys "github.com/tetratelabs/wazero/sys"
	"mvdan.cc/sh/v3/interp"
)

// wasmMagic starts every WebAssembly binary module.
var wasmMagic = []byte("\x00asm")

// Set is the WASI programs found in a list of directories. A nil *Set has no
// programs. Its methods are safe for concurrent use.
type Set struct {
	runtime wazero.Runtime
	cache   wazero.CompilationCache // nil without a cache directory
	modules map[string]*module
}

// Load lists the *.wasm files in dirs and returns them as a Set. It does not
// read the files. Empty entries and directories that do not exist are
// skipped, as in PATH. When cacheDir is not empty, compiled programs are
// also kept there, and Load creates it if necessary.
func Load(ctx context.Context, dirs []string, cacheDir string) (*Set, error) {
	modules := map[string]*module{}
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		entries, err := os.ReadDir(dir)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("wasmbin: %w", err)
		}
		for _, e := range entries {
			name, ok := strings.CutSuffix(e.Name(), ".wasm")
			if !ok || name == "" || modules[name] != nil {
				continue
			}
			p := filepath.Join(dir, e.Name())
			// Stat, not the entry type, so that a symlink to a program counts.
			info, err := os.Stat(p)
			if err != nil {
				return nil, fmt.Errorf("wasmbin: %w", err)
			}
			if !info.Mode().IsRegular() {
				continue
			}
			modules[name] = &module{name: name, path: p}
		}
	}

	// Close a running instance when its context ends, so -hook-timeout can
	// stop a long build.
	config := wazero.NewRuntimeConfig().WithCloseOnContextDone(true)
	var cache wazero.CompilationCache
	if cacheDir != "" {
		var err error
		if cache, err = wazero.NewCompilationCacheWithDir(cacheDir); err != nil {
			return nil, fmt.Errorf("wasmbin: compilation cache: %w", err)
		}
		config = config.WithCompilationCache(cache)
	}
	runtime := wazero.NewRuntimeWithConfig(ctx, config)
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		_ = runtime.Close(ctx)
		if cache != nil {
			_ = cache.Close(ctx)
		}
		return nil, fmt.Errorf("wasmbin: %w", err)
	}
	for _, m := range modules {
		m.runtime = runtime
	}
	return &Set{runtime: runtime, cache: cache, modules: modules}, nil
}

// Names returns the command names in sorted order.
func (s *Set) Names() []string {
	if s == nil {
		return nil
	}
	return slices.Sorted(maps.Keys(s.modules))
}

// Register adds every program in s to reg. A program replaces a command of
// the same name that reg already has.
func (s *Set) Register(reg *registry.Impl) {
	if s == nil {
		return
	}
	for name, m := range s.modules {
		reg.Register(name, m)
	}
}

// Close releases the compiled programs. Commands from s fail after it.
func (s *Set) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	err := s.runtime.Close(ctx)
	if s.cache != nil {
		err = errors.Join(err, s.cache.Close(ctx))
	}
	return err
}

// module is one program in a Set. It is not kefka's generic wasmcommand, which
// neither sets the guest PWD nor stops an instance when its context ends.
type module struct {
	name    string
	path    string
	runtime wazero.Runtime

	// mu makes concurrent first runs wait for one compile. Only a success is
	// kept, so a failed read is tried again on the next run.
	mu       sync.Mutex
	compiled wazero.CompiledModule
}

func (m *module) compile() (wazero.CompiledModule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.compiled != nil {
		return m.compiled, nil
	}
	bin, err := os.ReadFile(m.path)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(bin, wasmMagic) {
		return nil, fmt.Errorf("%s is not a WebAssembly module; it is probably a Git LFS pointer, so run git lfs pull and rebuild", m.path)
	}
	// Not the caller's context: a compile that a hook timeout stopped would
	// only have to start again on the next run.
	compiled, err := m.runtime.CompileModule(context.Background(), bin)
	if err != nil {
		return nil, err
	}
	m.compiled = compiled
	return compiled, nil
}

// Exec runs the program with args against ec.FS mounted at /.
func (m *module) Exec(ctx context.Context, ec *command.ExecContext, args []string) error {
	compiled, err := m.compile()
	if err != nil {
		return fmt.Errorf("%s: %w", m.name, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", m.name, err)
	}

	fsConfig := wazero.NewFSConfig().(sysfs.FSConfig).
		WithSysFSMount(billyfs.New(ec.FS), "/")

	// Go's wasip1 port takes its working directory from PWD, so a relative
	// path such as "." resolves against the shell's directory.
	config := wazero.NewModuleConfig().
		WithStdin(ec.Stdin).
		WithStdout(ec.Stdout).
		WithStderr(ec.Stderr).
		WithArgs(append([]string{m.name}, args...)...).
		WithName("").
		WithEnv("PWD", ec.GuestPWD()).
		WithFSConfig(fsConfig).
		WithSysNanosleep().
		WithSysNanotime().
		WithSysWalltime()
	if ec.Environ != nil {
		for _, name := range []string{"HOME", "TMPDIR"} {
			if v := ec.Environ.Get(name); v.IsSet() {
				config = config.WithEnv(name, v.String())
			}
		}
	}

	mod, err := m.runtime.InstantiateModule(ctx, compiled, config)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%s: %w", m.name, ctxErr)
		}
		if exitErr, ok := errors.AsType[*wsys.ExitError](err); ok {
			if code := exitErr.ExitCode(); code != 0 {
				return interp.ExitStatus(uint8(code))
			}
			return nil
		}
		return err
	}
	return mod.Close(ctx)
}
