// Package kustomize runs an embedded WASI build of kustomize as a Kefka shell
// command, so a hook can render a Kustomize bundle from its /src checkout.
//
// kustomize.wasm is tracked with Git LFS. A checkout without LFS hydration
// embeds the pointer file instead, and Exec then fails with an error that says
// so. TestEmbeddedModule catches that in CI.
//
// Provenance: the module reports version "(devel)", so its source revision is
// unknown. SHA256 records the exact artifact; update it with the module.
package kustomize

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
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

// SHA256 is the hex SHA-256 of the embedded kustomize.wasm.
const SHA256 = "1724a4e906800e5af0c104e816d56213fdf1d2cb6e2cef417c4e25d4baac061b"

//go:embed kustomize.wasm
var wasm []byte

// wasmMagic starts every WebAssembly binary module.
var wasmMagic = []byte("\x00asm")

var embedded = newModule(wasm)

// Register adds the kustomize command to reg.
func Register(reg *registry.Impl) {
	reg.Register("kustomize", Command{})
}

// Command is the kustomize Kefka command. The module compiles on first use,
// which takes seconds, and the compiled form is shared by every shell.
type Command struct{}

// Exec runs kustomize with args against ec.FS mounted at /.
func (Command) Exec(ctx context.Context, ec *command.ExecContext, args []string) error {
	return embedded.exec(ctx, ec, args)
}

// module compiles one WASI program once, in a runtime that closes a running
// instance when its context ends. The generic Kefka adapter does not, so there
// a hook timeout cannot stop a long build.
type module struct {
	bin []byte

	once     sync.Once
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
	err      error
}

func newModule(bin []byte) *module { return &module{bin: bin} }

func (m *module) compile() error {
	m.once.Do(func() {
		if !bytes.HasPrefix(m.bin, wasmMagic) {
			m.err = errors.New("embedded kustomize.wasm is not a WebAssembly module; the build probably embedded a Git LFS pointer, so run git lfs pull and rebuild")
			return
		}
		ctx := context.Background()
		runtime := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCloseOnContextDone(true))
		if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
			_ = runtime.Close(ctx)
			m.err = err
			return
		}
		compiled, err := runtime.CompileModule(ctx, m.bin)
		if err != nil {
			_ = runtime.Close(ctx)
			m.err = err
			return
		}
		m.runtime, m.compiled = runtime, compiled
	})
	return m.err
}

func (m *module) exec(ctx context.Context, ec *command.ExecContext, args []string) error {
	if err := m.compile(); err != nil {
		return fmt.Errorf("kustomize: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("kustomize: %w", err)
	}

	fsConfig := wazero.NewFSConfig().(sysfs.FSConfig).
		WithSysFSMount(billyfs.New(ec.FS), "/")

	// Go's wasip1 port takes its working directory from PWD, so a relative
	// path such as "." resolves against the shell's directory.
	config := wazero.NewModuleConfig().
		WithStdin(ec.Stdin).
		WithStdout(ec.Stdout).
		WithStderr(ec.Stderr).
		WithArgs(append([]string{"kustomize"}, args...)...).
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

	mod, err := m.runtime.InstantiateModule(ctx, m.compiled, config)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("kustomize: %w", ctxErr)
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
