# Wasm integration tests

The tests use the public `tt` CLI. The real-toolchain tests use the
Tarantool Wasm 0.1.0 native module and assert Lua calls against packaged
components.

## Run the matrix

Use Linux x86-64, Tarantool 3.6 or newer, Go, and the integration test
dependencies in `test/requirements.txt`. The Docker matrix also requires a
local Docker daemon that can mount the test projects. Use a version-stamped
`tt` binary for the package archive tests. The active Tarantool installation
and its share directory must be readable by the test user.

```sh
python3 -m pytest test/integration/wasm -m 'not docker' --tt-cmd /path/to/tt
PYTEST_ADDOPTS=--tt-cmd=/path/to/tt test/integration/wasm/run_linux.sh
```

With the local toolchains and Docker images configured below, run both
groups together with `python3 -m pytest test/integration/wasm --tt-cmd /path/to/tt`.

To test changes in both repositories before a dependency update, create
a temporary Go workspace from the `tt` root with `tt-wasm` checked out
alongside it.

```sh
tt_root=$PWD
wasm_root=$(cd ../tt-wasm && pwd)
workspace=$(mktemp -d)
(
  cd "$workspace"
  go work init "$tt_root" "$wasm_root" "$tt_root/sdk" \
    "$tt_root/sdk/build" "$tt_root/sdk/connect" "$tt_root/sdk/fetch"
)
export GOWORK="$workspace/go.work"
go build -ldflags '-X github.com/tarantool/tt/v3/cli/version.gitTag=v3.0.0' \
  -o "$workspace/tt" ./cli
PYTEST_ADDOPTS="--tt-cmd=$workspace/tt" test/integration/wasm/run_linux.sh
```

The Docker script resolves the builder source from the same Go workspace
unless a prebuilt language image is supplied.

Set `TT_WASM_TEST_<LANG>_IMAGE` to a released digest or prebuilt local
image for a Docker case.
The supported names are `C`, `CPP`, `CSHARP`, `GO`, `JS`, `PYTHON`, and
`RUST`. C and C++ use the same builder image. The shell script also accepts
language names to run a subset.

To run against the release, download `builder-images.json` from the
[tt-wasm release](https://github.com/mandesero/tt-wasm/releases/tag/v0.1.0)
and set the image references in Bash. Use a `tt` binary built with
`GOWORK=off` so it uses the dependency pinned in `go.mod`.

```bash
for target in c csharp go js python rust; do
  reference=$(jq -er --arg target "$target" \
    '.images[] | select(.target == $target) | .reference' builder-images.json)
  export "TT_WASM_TEST_${target^^}_IMAGE=$reference"
done
export TT_WASM_TEST_CPP_IMAGE=$TT_WASM_TEST_C_IMAGE
python3 -m pytest test/integration/wasm --tt-cmd /path/to/tt
```

For local cases, provide one JSON file per selected language with the
environment variables required by that toolchain. An empty object uses the
environment of pytest. Use separate `PATH` values for C and C# because
they require different `wit-bindgen` versions.

```sh
printf '{}\n' > /tmp/python-toolchain.json
TT_WASM_TEST_PYTHON_LOCAL_ENV=/tmp/python-toolchain.json \
  python3 -m pytest test/integration/wasm -k 'local_component_development[python]' \
    --tt-cmd /path/to/tt
```

To supply a local rock mirror to the real-toolchain tests, run
`tt wasm rock --out /absolute/path/to/rocks` and set
`TT_WASM_TEST_RUNTIME_ROCK_SERVER=/absolute/path/to/rocks`. The pinned guest
SDK still comes from the release or from the host download cache. Project
caches begin empty under pytest's temporary directory. Set `XDG_CACHE_HOME`
to a fresh directory to exercise cold host downloads as well. Keep raw
test logs private if the registries in use return signed download URLs.

## What the automated workflow checks

| Scenario | Local cases | Docker cases |
| --- | --- | --- |
| Initialize package and component, resolve runtime, fetch locked dependencies | All seven languages | All seven languages |
| Generate bindings and verify the current binding record | All seven | All seven |
| Cold build, warm rebuild, source edit, WIT signature edit | All seven | All seven |
| Invalid WIT and source errors preserve both previously built files | All seven | All seven |
| Clean, regenerate, rebuild, and verify both locks stay intact | All seven | All seven |
| Assert results 42, 62, and 48 from the installed artifact through Tarantool | All seven | All seven |
| Add, update, and remove an npm package and inspect its transitive lock entries | JavaScript | JavaScript |
| Pack, install in a fresh project, run the revised component | Covered by the Docker package cases | All seven |

The default suite also covers the offline mirror flow, a deliberately failed
build, and interruption while publishing bindings. The interrupted bind
must terminate its builder, remove its staging directory, and leave the
previous bindings current. A second bind must succeed.
