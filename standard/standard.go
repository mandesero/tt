// Package standard lists the modules the official tt is built with and
// which a distribution takes by default ([distribution].standard in
// tt.manifest.toml). The core pins their versions.
//
// The core's own commands, builtin, are not in the list: they are
// core.Builtin, and this package does not import the core, so that the
// core's internal tests may import it. The caller adds builtin itself:
//
//	modules := standard.Modules()
//
//	modules["builtin"] = core.Builtin
//	os.Exit(core.Main(modules))
//
// tt binary build tt reads the modules from the source of this package,
// without building it, and imports the constructors of those a
// distribution takes into the main package it generates, so that an
// excluded module is not compiled in. The body of Modules is therefore one
// return of a map literal whose keys are string literals and whose values
// are <import>.<constructor>: an exported function of a package the file
// imports, under its name or the last element of its path, and not an
// internal package.
package standard

import (
	wasm "github.com/mandesero/tt-wasm"
	"github.com/tarantool/tt/modules/aeon"
	"github.com/tarantool/tt/modules/build"
	"github.com/tarantool/tt/modules/coredump"
	"github.com/tarantool/tt/modules/emmylua"
)

// Modules returns the standard modules by name, a new map on each call:
// the caller may add to it and delete from it. A value is the constructor
// of the module, of either type core.Modules takes: an sdk.Constructor or
// an sdk.ModuleConstructor. build has the build backends of tt: the core
// has none of its own. wasm provides tt wasm, the wasm build backend and
// the check of the .wasm files of a package.
func Modules() map[string]any {
	return map[string]any{
		"aeon":     aeon.New,
		"build":    build.New,
		"coredump": coredump.New,
		"emmylua":  emmylua.New,
		"wasm":     wasm.New,
	}
}
