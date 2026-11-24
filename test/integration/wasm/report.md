# tt and tt-wasm end-to-end test report

## v0.1.0 release verification

The [tt-wasm v0.1.0 release](https://github.com/mandesero/tt-wasm/releases/tag/v0.1.0)
contains six public GHCR indexes. The [release workflow](https://github.com/mandesero/tt-wasm/actions/runs/45492998680)
passed all 11 native smoke tests and scans before promoting the indexes.
The C, Go, JavaScript, Python, and Rust images support Linux amd64 and
arm64. C and C++ share an image. C# supports Linux amd64.

The release uses companion source `aaf9234b06ff` and SDK source
`e336165d7524`. The six registry indexes were verified anonymously
against the image and platform digests in the public
[`builder-images.json`](https://github.com/mandesero/tt-wasm/releases/download/v0.1.0/builder-images.json).
The regression binary was built with `GOWORK=off` and the `v0.1.0`
pin in `go.mod`. The affected Go packages and regenerated CLI help
snapshot passed.

All 23 integration cases passed on Linux x86-64 in two selections.

| Selection | Result |
| --- | --- |
| Local toolchains and CLI regressions | 16 passed in 4m 15s |
| Docker toolchains, package archives, and installed runtime calls | 7 passed in 7m 04s |

Docker used an empty credential configuration and a host PATH without the
guest toolchains. Every project lock recorded its released index digest.
The runtime and SDK used the same verified release bytes as the
development matrix below. The additional Docker package also built,
packed, installed, and ran all seven languages together. Its first
attempt encountered a GHCR connection reset during image pull and a retry
completed with the same digest.

## Test environment

The test host was Linux x86-64. The baseline was `tt` at `f042c194` with
`tt-wasm` pinned at `f23e74d`. The companion fixes were developed against
`tt-wasm` at `3b598bf`. Candidate runs used a Go workspace containing both
checkouts. The test binary was stamped `3.0.0` so the generated package's
`>=3.0.0` runtime requirement could be resolved during pack.

Runtime calls used Tarantool 3.9 development builds. The published 0.1.0
Linux x86-64 native module and guest SDK matched the SHA-256 pins in
`tt-wasm`. The clean final runs used a local mirror of those assets after
intermittent connection resets from the release and rock servers. Language
packages continued to come from their actual registries. The test Docker
engine was 24.0.2. The C# and JavaScript fix runs used cached builder images
with a replacement `tt-wasm-builder` binary from the companion checkout.

## Core workflows

The automated matrix is described in [Wasm integration tests](README.md).
Each real-toolchain case asserts behavior in Tarantool after initial build,
warm rebuild, a source edit, a WIT signature edit, and a clean rebuild. A
failed WIT or source build must preserve both the component's output and the
package's installed copy. The Docker case also packs, installs, and calls
the revised component in a fresh project.

The expanded run passed 22 of 23 cases. The Docker Python case hit a
connection reset while downloading the SDK's msgpack source. Its targeted
rerun passed with the buffered-download retry fix. Both Rust target cases
also passed again after the editor SDK change. The core registration and
help tests passed against the pinned dependency. The SDK fetch tests and
companion driver tests also passed.

| Language | Local initialize, fetch, bind, build, rebuild, run | Docker initialize, fetch, bind, build, rebuild, run, pack and install |
| --- | --- | --- |
| C | Pass | Pass |
| C++ | Pass | Pass |
| C# | Pass | Pass |
| Go | Pass | Pass |
| JavaScript | Pass. Typed TypeScript entry point also runs. | Pass. Typed TypeScript entry point also runs. |
| Python | Pass | Pass |
| Rust | Pass | Pass |

Run a package from elsewhere with `tt package -C "$project" build --locked`.
Use a subshell for the `tt wasm` command group:

```sh
(cd "$project" && tt wasm doctor --component calc)
(cd "$project/components/calc" && tt wasm build calc)
```

An additional Docker package contained one component from every language.
All seven returned 42 before packing and after installation. The package
was built from outside its root, which included spaces, commas, brackets,
and a dollar sign. The C and C++ scoreboard workflows exercised lists,
records, added and removed exports, and missing symbols during linking.
The Python notes workflow used `tt instance start` to launch an HTTP
server in a Wasm component and verified create, list, read, delete, 404,
and 405 responses against a Tarantool space.
The JavaScript and Python runner-switch cases replaced the SDK world
include and root export with a `tarantool:tarantool/log@0.2.0` import and an
`example:calc/readings@0.1.0` export. A Lua caller passed a record with a
list, received 26, and observed the expected guest log entry in each
runner.

## Issues and remedies

| Baseline result | Root cause and change | Verification |
| --- | --- | --- |
| SIGTERM during bind left a running tool and an abandoned work directory | Mounted module hooks ran without a signal-aware context. The host now cancels the context supplied to those hooks. | The integration test fails against the baseline and passes against the fixed host. A cancelled bind keeps previous bindings and supports retry. |
| Cancellation of a compiler that launched another process hung while its child held the output pipe | The local command context cancelled only its immediate process. The driver now isolates Unix tooling in a process group and terminates the group on cancellation. | The regression starts a child process that holds the pipe. The fixed driver returns promptly. A real Docker build also stopped, kept the last component, and built successfully on retry. |
| Jco reported only that a WIT package failed to parse | Jco's componentization error omitted the WIT source diagnostic. JavaScript bind and build now validate staged WIT with the same parser used by fetch. | Invalid WIT identifies the missing type before invoking Jco. The edited component still packs, installs and runs. |
| Repeating the same C# local Release build produced different bytes | NativeAOT put the randomized work path in DWARF. The Release driver removes DWARF from the finished component and retains symbol names. | Two builds in distinct work directories produce identical bytes. The resulting component still runs in Tarantool. |
| C# local build could not locate its emitted component in a bracketed project path | The glob treated the complete path as a pattern. The driver now resolves the pattern relative to the publish directory. | The baseline reports zero artifacts in a bracketed path. The fixed build finds its artifact and runs. |
| Python and JavaScript local fetch could miss their SDK packages in bracketed paths | SDK discovery now applies its glob within the package directory. | Both archive types have regression coverage. The Python component fetches, builds, and runs under a bracketed path. |
| JavaScript local build failed when the work path contained whitespace | ComponentizeJS passes an unquoted temporary initializer path to Wizer. JavaScript and C# now share the build-path alias logic. | The alias test verifies staging and cleanup. The fixed local CLI fetches, builds and runs a component under a path containing spaces and brackets. |
| Cargo could not edit or lock dependencies of a newly scaffolded Rust component | The SDK dependency was staged inside the builder, leaving the declared local path missing in the source tree. Rust bindings now include the local SDK. The driver accepts both current and legacy scaffold paths. | The Rust builder tests cover the staged SDK paths. The generated SDK is available under `.tt-wasm/bindings/tarantool-sdk`. |
| C and C++ could not use component-relative include and library arguments | Clang ran from a disposable work directory. The compiler now runs from the component directory, with generated bindings and its output still in staging. | A relative header lookup failed in both baseline modes. The container test compiles and links a separate WASI archive using relative paths. |
| Standard core checks rejected the Wasm command surface and the backend owner | The checked-in help and backend expectations described the earlier integration. | The core tests now verify the current help, command groups, and that the `wasm` module owns the `wasm` backend. |
| Cold package or SDK downloads intermittently failed | The host and some containers received connection resets from upstream servers. Checksummed downloads now retry resets and truncated bodies. Runtime rock downloads also have a mirror option. | A truncated response is retried and only the complete checksum-verified bytes enter the cache. The matrix mirror used the published runtime and SDK bytes. |
| Archive build failed with the first Tarantool installation on PATH | Its shared files included a rock manifest unreadable by the test user. Using the readable Tarantool installation allowed the same archive flow to complete. | Both the individual archive tests and the mixed package use the readable installation. |

## Runner behavior and negative cases

- `tt wasm prepare --lang c --runner local` with a PATH containing only base
  system tools reports the missing versions and executables. A missing
  Docker development tag reports `No such image`. Either error permits a
  retry with the correct runner or image. Runtime declaration can succeed
  before the toolchain check reports an error.
- `tt wasm build calc` before fetch exits with a missing or stale dependency
  message and the `tt wasm fetch` recovery command.
- `tt wasm bind calc --check` returns an error after a WIT edit.
  Regenerating bindings updates the record. Source-only edits rebuild
  against WIT in the temporary build tree.
- Changing the dependency lock or switching the runner selects a different
  project cache. After fetch, a fresh Docker container builds offline
  against the cached dependencies. The runner identity is also part of the
  binding record. Generate the bindings again when changing runners.
- Docker performs the build with project sources mounted read-only. Native
  files produced by the build belong to the invoking UID and GID on Linux.
  Local tools run under the invoking user. The package has the same
  component placement in both cases.
- Local and Docker builds use disposable compiler work trees. Warm builds
  reuse fetched SDK and dependency caches and still run the compiler.
- Independent projects ran concurrently, including a C and C++ package with
  two components, and a separate C application. Paths with spaces, commas,
  quotes, brackets, or dollar signs were exercised. Tarantool uses
  semicolons and question marks as Lua search-path syntax. A semicolon in
  the application path reproduced `require('wasm')` failure at runtime
  after a successful build.

## Repeat dependency and interruption checks

Start in a scaffolded component with `tt wasm fetch` and `tt wasm bind`
already run. Execute the package manager in the component directory. For a
Docker-only setup, use the existing builder image with the package manager
as entrypoint and the project mounted read-write for lock authoring. Run
the subsequent build through `tt`.

```sh
cd components/calc
docker run --rm --user "$(id -u):$(id -g)" --env HOME=/tmp \
  --mount "type=bind,source=$PWD,target=/guest" --workdir /guest \
  --entrypoint npm tt-wasm-builder:js install --save-exact \
  --package-lock-only --ignore-scripts camelcase-keys@9.1.2
```

| Language | Dependency exercise |
| --- | --- |
| JavaScript | `npm install --save-exact --package-lock-only camelcase-keys@9.1.2`, upgrade to `9.1.3`, then `npm uninstall --package-lock-only camelcase-keys`. The automated test uses the import and verifies the transitive `map-obj` entry and the Lua result. |
| Python | Use `python3 -m pip download --only-binary=:all:` for `python-dateutil` and its dependencies, and `python3 -m pip hash` for each wheel. Record the two exact versions and hashes in `requirements.lock`, import `dateutil.relativedelta`, and fetch and build. |
| C# | `dotnet package add Polly --version 8.6.5 --no-restore`, `dotnet restore -p:RestoreLockedMode=false -p:NuGetAudit=false -p:Configuration=Release`, then fetch and build. `Polly.Core` is transitive. The list export used `ResiliencePipeline.Empty.Execute`. Repeat with 8.6.6 and then `dotnet package remove Polly`. |
| Go | Run `go get github.com/google/uuid@v1.5.0` and `go mod download all` in a component with its generated SDK in place. Use `uuid.NewSHA1(...).Version()` in the guest. Update to v1.6.0 and remove with `go mod edit -droprequire=github.com/google/uuid` and `go mod tidy`. A second component used `github.com/gosimple/slug@v1.15.0` with its transitive `unidecode` dependency. |
| Rust | Run `cargo add serde@=1.0.228 --features derive` and `cargo add serde_json@=1.0.145` in a component with its generated SDK in place. Serde uses a proc macro built for the build host, and the component uses serde_json on the guest target. Change serde_json to 1.0.144 and remove both direct dependencies. The SDK still requires serde_json transitively. |
| C and C++ | Build xxHash 0.8.2 and 0.8.3 or fmt 11.1.4 and 11.2.0 for WASI Preview 2. Stage the headers and static archive under `vendor/` and pass `-Ivendor/include -Lvendor/lib -lxxhash` or `-lfmt` through the manifest. fmt used `-fno-exceptions -include cstdlib` with this WASI SDK. |

After each update, run `tt wasm fetch calc`, `tt wasm bind calc`, and
`tt package build --locked`. A stale lock or cache must give a nonzero exit
and leave the previous artifact intact. Exercise a runner switch by preparing
the other runner and repeating fetch, bind, build, and the same Lua assertion.

C and C++ linked and used both versions of their libraries in both modes.
The fixed C/C++ offline container smoke also linked a separate archive from
the read-only component, and both outputs returned 42. The npm, pip, Go,
and NuGet cases exercised dependency addition, version changes, removal,
and calls using the installed components. Python rejected the native-only
`lxml` distribution during fetch and preserved the previous output. Both
runners rejected an npm file link and a platform-native NumPy wheel. Fetch
and bind worked again after the unsupported dependencies were removed. Rust
compiled the host proc macro and ran the guest in both modes. A project
with the legacy Rust SDK path also regenerated bindings and ran. A Go
component with CGo and custom WIT passed consecutive source and interface
edits, a runner switch, an invalid WIT recovery, and a clean rebuild.
Same-project runner switches completed for all seven languages.

The deterministic interruption regression can be run independently:

```sh
python3 -m pytest test/integration/wasm -k interrupted_bind --tt-cmd /path/to/tt
```

For a real build, send SIGTERM to an in-progress `tt wasm build calc`.
Observe the builder exit, confirm the previous output hash, then run
the same command and call the component through `tt run`.

## Remaining scope

- The runtime matrix covers Linux x86-64. macOS, Linux arm64, remote Docker
  contexts, and additional Tarantool versions require separate runs.
- Package support follows the guest target. Python uses pure-Python wheels,
  JavaScript install scripts are disabled, and native libraries require a
  WASI-compatible build. npm workspace and file links are outside the
  JavaScript dependency profile.
- Byte-for-byte reproducibility is distinct from matching runtime behavior.
  The C# Release regression specifically checks identical bytes after
  relocation of the build work directory. Some Go local, JavaScript, and
  Python cold and warm artifacts differed even with the same inputs.
  ComponentizeJS preserves initialization strings containing randomized
  paths. The Python byte differences were reproduced in both modes.
