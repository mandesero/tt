# Run WebAssembly components (experimental)

[tarantool-wasm](https://github.com/tarantool/tarantool-wasm-rs) runs
WebAssembly components inside Tarantool. Lua code uses them through
`require('wasm')`. The standard `wasm` module of tt provides the
`tt wasm` commands and the `wasm` build backend. This guide builds a Python
function and calls it from Lua. For a full application, see
[An HTTP API in a WebAssembly component](../write-your-app/http-api-in-wasm.md).

The runtime requires Tarantool 3.6 or newer on Linux x86-64 or macOS arm64.

## Prepare the project

The default runner uses Docker. Download `builder-images.json` from the
[tt-wasm release](https://github.com/mandesero/tt-wasm/releases/tag/v0.1.0).
In the application directory containing `app.manifest.toml`, select the
released Python builder by digest:

```console
$ image=$(jq -er '.images[] | select(.target == "python") | .reference' builder-images.json)
$ tt wasm prepare --lang python --image "$image"
```

For a development image, build the [tt-wasm module](https://github.com/mandesero/tt-wasm)
pinned in `go.mod` from the root of the tt checkout:

```console
$ go mod download github.com/mandesero/tt-wasm
$ wasm_dir=$(go list -m -f '{{.Dir}}' github.com/mandesero/tt-wasm)
$ docker build --build-context "tt-sdk=$PWD/sdk" --platform linux/amd64 \
    --target python -t tt-wasm-builder:dev "$wasm_dir"
```

Select that image from the application directory:

```console
$ tt wasm prepare --lang python --image tt-wasm-builder:dev
```

`prepare` declares the `wasm` rock dependency, resolves the package lock,
and checks the selected toolchain. The rock index serves builds of the
native module for the runtime platforms. The Docker image has its own
platform, `linux/amd64`, independent of the platform on which the app runs.

## The toolchains

The Docker image provides the Python tools. To run them directly on the host,
put these tools on `PATH`:

| Tool | Version |
| --- | --- |
| `python3` with pip | 3.10 or newer |
| `componentize-py` | 0.25.1 |
| `wasm-tools` | 1.261.0 |
| `wkg` | 0.16.1 |

Then run:

```console
$ tt wasm prepare --lang python --runner local
```

`prepare` checks local tools. Install or update them yourself when a check
fails. Docker and local runners use the same sequence after preparation.

## Add a component

```console
$ tt wasm new calc --lang python
$ tt wasm prepare
$ tt wasm fetch calc
```

`new` puts the component in `components/calc/` and adds it to the manifest.
The second `prepare` records the new component's toolchain profile, runtime
version and SDK digest in `wasm.lock.json`. A Docker profile also pins the
builder image. `fetch` downloads the SDK and any dependencies declared in
`wkg.lock` and `requirements.lock`. Python requirements need exact versions
and hashes of pure-Python wheels.

```toml
[components.calc]
path = "components/calc"

[components.calc.build]
backend = "wasm"
language = "python"
source = "main.py"
wit = "wit"
world = "calc"
profile = "python-ttwasm-0.1.0-r1"
output = "dist/calc.wasm"
```

Commit `app.manifest.lock`, `wasm.lock.json`, the sources and any dependency
lock files. Keep `wasm.local.json` and the generated `.tt-wasm/` and `dist/`
directories out of version control. A locally built image is pinned by its
Docker image ID and must be available in the Docker context on each builder.

## Other languages

Use the same sequence for these language names. Set `--target` to the
Dockerfile stage when building a toolchain image locally. The example
builds the JavaScript calculator.

```console
$ docker build --build-context "tt-sdk=$PWD/sdk" --platform linux/amd64 \
    --target js -t tt-wasm-builder:js "$wasm_dir"
$ tt wasm prepare --lang js --image tt-wasm-builder:js
$ tt wasm new calc --lang js
$ tt wasm prepare
$ tt wasm fetch calc
$ tt wasm bind calc
$ tt package build --locked
```

| `--lang` | Dockerfile stage | Local toolchain | Dependency files |
| --- | --- | --- | --- |
| `python` | `python` | Python 3.10+, pip, componentize-py 0.25.1 | `requirements.lock` |
| `js` | `js` | Node.js 24+, Jco 1.37.0, ComponentizeJS 0.23.0 | `package-lock.json` |
| `go` | `go` | Go 1.27.x, TinyGo 0.43.0, Binaryen 133, wit-bindgen-go 0.7.0 | `go.mod`, `go.sum` |
| `rust` | `rust` | Rust 1.96+, cargo-component 0.21.1, `wasm32-wasip1` | `Cargo.lock` |
| `c`, `cpp` | `c` | WASI SDK 34.0, wit-bindgen 0.62.0 | `wkg.lock` |
| `csharp` | `csharp` | .NET SDK 10.0.402, WASI SDK 29.0, wit-bindgen 0.58.0 | `packages.lock.json` |

All local toolchains also need `wasm-tools` 1.261.0 and `wkg` 0.16.1. For
local C# builds use Linux amd64 and set `WASI_SDK_PATH` to the WASI SDK
root. A project can also lock external WIT packages in `wkg.lock`.

## Change dependencies

`tt wasm tool` runs a command from the component directory in its pinned
Docker image. Run it from the application directory. For example, add a
JavaScript dependency and build with the updated lockfile.

```console
$ tt wasm tool calc -- npm install --ignore-scripts lodash-es@4.17.23
$ tt wasm build calc --fetch
```

`--fetch` prepares the locked dependencies before the build. Commit the
updated manifest and lockfile. Open a shell with `tt wasm tool calc -- sh`.
With a local toolchain, run the package manager in the component directory.

Use `go get` for Go and `cargo add` for Rust. `tool` prepares their SDK on a
clean checkout. With a local toolchain, do this with `tt wasm fetch calc`
and `tt wasm bind calc`. For older Rust components, update `.tt-wasm-sdk-rust`
in `Cargo.toml` to `.tt-wasm/bindings/tarantool-sdk` before generating the SDK.

For C#, use `dotnet package add`. If `.csproj` sets `RestoreLockedMode` to
`true`, pass `--no-restore` and refresh the lockfile yourself. To refresh it,
including after `dotnet package remove`, run
`dotnet restore -p:RestoreLockedMode=false -p:Configuration=Release`.

JavaScript fetch installs production dependencies from the lockfile with
npm lifecycle scripts disabled. For C and C++, build libraries for WASI
Preview 2 with the WASI SDK. Place the headers and archives in the
component and pass them to Clang through the component's build arguments.
Relative compiler and linker paths resolve from the component directory
in both runners.

```toml
[components.calc.build.args]
build = ["-Ivendor/include", "-Lvendor/lib", "-lxxhash"]
```

Docker fetch uses the builder's package managers and writes the cache in
the project. Subsequent Docker builds reuse that cache across containers.
After changing runners, fetch again before building. For example, after
preparing the same component in Docker, switch to the installed local
toolchain with:

```console
$ tt wasm prepare --lang js --runner local
$ tt wasm fetch calc
$ tt wasm bind calc
$ tt package build --locked
```

Use `tt wasm prepare --lang js --runner docker` to switch back to the
previously pinned image. The `--runner` option on `fetch`, `bind`, or
`build` selects a runner for that invocation. Each runner has a separate
dependency cache. Run `tt wasm bind` again for the active runner before
checking its generated bindings.

### Try a Python package in Docker

```console
$ tt wasm tool calc -- pip install python-dateutil
$ tt wasm build calc --dev
```

On first use, `tool` seeds the component's virtual environment from
`requirements.lock`. Subsequent `tool` and `build --dev` calls reuse the
environment, so you can run `pip install --upgrade` and `pip uninstall`.
`--fetch` refreshes only the locked build cache. Use pure-Python wheels
compatible with the guest Python version.
If a package copied from the locked cache provides a console command, run
`pip install --force-reinstall` for that package in the environment to
create its entry point.

To share a dependency or include it in a package, record exact versions and
wheel hashes for it and its transitive dependencies in `requirements.lock`.
Commit the lockfile and run `tt wasm build calc --fetch` for a build from it.
`tt package build` and `tt package pack` use the locked dependencies.

## Write it

The generated `components/calc/wit/app.wit` declares the export:

```wit
package example:calc@0.1.0;

world calc {
    include tarantool:tarantool/guest@0.2.0;
    export add: func(a: s32, b: s32) -> s32;
}
```

The generated `components/calc/main.py` implements it:

```python
import wit_world


class WitWorld(wit_world.WitWorld):
    def add(self, a: int, b: int) -> int:
        return a + b
```

Use `tt wasm bind calc` to generate bindings in
`components/calc/.tt-wasm/bindings/` for editing and inspection. A build
generates the bindings it needs in a temporary directory. After editing WIT,
run `tt wasm bind calc` and update the implementation to match. If you
changed a WIT dependency or its lockfile, run `tt wasm fetch calc` first.

`tt wasm clean calc` removes generated bindings and build outputs. You can
then generate fresh bindings and build again with the fetched dependencies.

## Build it and call it from Lua

```console
$ tt package build --locked
```

Use `tt wasm build calc` to build just this component. On success the
backend writes `components/calc/dist/calc.wasm` and places it under the
package namespace in `.rocks/share/tarantool/myapp/dist/calc.wasm` for a
package named `myapp`. A failed compilation or validation preserves the
previous component.

From the application directory:

```console
$ tt run -e "local wasm = require('wasm') \
    local calc = wasm.load('.rocks/share/tarantool/myapp/dist/calc.wasm') \
    print(wasm.call(calc, 'add', 20, 22)) os.exit()"
42
```

Tarantool uses `;` and `?` in Lua module search paths. Use an application
path without those characters if you want `require('wasm')` to find the
installed runtime through the standard search path.

`wasm.bind(calc)` gives Lua functions for the exports of the component.
Use `wasm.drop(calc)` to release a component you no longer need.

Docker runs `bind` and compilation without network access. `build --fetch`
can use the network during its fetch step. For an offline package build or
pack, fetch the component with `tt wasm fetch` and mirror the runtime rock.
Use the version from `app.manifest.lock` and the source
registry from the `wasm` dependency in `app.manifest.toml`. For the default
registry and runtime version in this example:

```console
$ registry=https://raw.githubusercontent.com/tarantool-contrib/rocks-repo/index/
$ tt package download wasm@0.1.0-1 --registry "$registry" --dir ./vendor-rocks
$ tt wasm prepare --rock-server ./vendor-rocks
$ tt package pack --locked --registry ./vendor-rocks
```

`prepare --rock-server` changes the runtime's explicit registry while keeping
its version constraint. Download the other rock dependencies too when the
application has them. See
[A mirror for offline builds](../concepts/rock-servers.md#a-mirror-for-offline-builds).

## Check the project

```console
$ tt wasm status
$ tt wasm doctor --component calc
$ tt wasm bind calc --check
```

`status` reports the declared components, toolchain profiles, dependencies
and bindings. `doctor` also checks the selected toolchain. `bind --check`
detects missing, changed or outdated bindings.

## Pack it

The default archive carries the component and its dependencies, including
the runtime rock for the target platform. The example uses Linux x86-64.

```console
$ tt package pack --locked
$ tt package install _build/pack/myapp-1.0.0-linux-amd64.tt --scope user --yes
```

If you use `--without-deps`, installation gets the appropriate runtime rock
from the rock server and checks it against the archive lock.

## Share the rock

`tt wasm rock` creates a rock for each platform of a known runtime release
and writes the files needed to serve the directory as a rock server:

```console
$ tt wasm rock --version 0.1.0 --out dist/rocks
```

Use `tt wasm prepare --rock-server URL` to declare the runtime rock from
another server. To use a local runtime build:

```console
$ tt wasm prepare --from target/release/libtarantool_wasm_rs.so
```

The local build is packed into `.tarantool/registry` for the host platform.
See the [release mirror format](../reference/environment.md#webassembly-components)
to supply both runtime and SDK assets from a mirror.

### Test a runtime and SDK from the same checkout

For SDK CI, pack the checkout's wheel and resolved WIT in the same `.tar.gz`
layout as a released SDK. Under the archive's top-level directory, Python
uses `packages/python/twr_python_sdk-*.whl` and `wit/python/deps/`. Build the
native runtime from the same checkout, then prepare the test project with
both artifacts. Use the builder image selected earlier in this guide.

```console
$ tt wasm prepare --lang python --image "$image" \
    --from ./artifacts/wasm.so --sdk-from ./artifacts/sdk.tar.gz
$ tt wasm new calc --lang python
$ tt wasm prepare
$ tt wasm build calc --fetch
```

`--lang python` selects the archive for Python components. `prepare` imports
the archive into the local Wasm cache and pins its SHA-256 in `wasm.lock.json`.
The second `prepare` reuses that selection for the new component. On another
machine, pass the same archive to `prepare --sdk-from` to restore the cached
copy. To use an archive with a new digest, pass `--update` as well.

## Commands and options

| Command | Purpose |
| --- | --- |
| `tt wasm prepare` | Declare the runtime dependency and prepare toolchain profiles |
| `tt wasm new NAME --lang python` | Scaffold and declare a Python component |
| `tt wasm fetch [NAME]` | Fetch pinned SDK and language dependencies |
| `tt wasm bind [NAME] [--check]` | Generate or check language bindings |
| `tt wasm build NAME` | Build one component |
| `tt wasm status` | Report project state |
| `tt wasm doctor [--component NAME] [--json]` | Check the project and toolchain |
| `tt wasm clean [NAME]` | Remove generated outputs |
| `tt wasm rock --out DIR` | Pack the runtime rocks for a rock server |

`new --dir` sets the component directory. `new --world` sets the name of
the WIT world. `new --template` accepts `call` or `http` and displays a
corresponding Lua example. Use `prepare --update` to accept a changed
toolchain pin. Use `tt wasm build NAME -- --help` for the underlying
build tool's help.
