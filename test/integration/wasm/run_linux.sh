#!/usr/bin/env bash
# Build the pinned Wasm module's toolchain images and check pack, install and a Lua call.
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: test/integration/wasm/run_linux.sh [python js go rust c cpp csharp]

With no languages, run the full matrix on Linux x86-64. The offline mirror
test runs with every selection. Requires Docker, Tarantool 3.6 or newer on PATH,
Python with test/requirements.txt installed, and Go to fetch the Wasm
module. Requires Mage to build tt when no existing binary is supplied.

Set PYTEST_ADDOPTS=--tt-cmd=/path/to/tt to test an existing binary.
Set TT_TEST_TARANTOOL to select a version or executable through Mage.
Set TT_WASM_TEST_<LANG>_IMAGE to use a prebuilt local image for a language.
EOF
}

case ${1:-} in
    -h|--help) usage; exit 0 ;;
esac

if [[ $# == 0 ]]; then
    set -- python js go rust c cpp csharp
fi

for language in "$@"; do
    case "$language" in
        python|js|go|rust|c|cpp|csharp) ;;
        *) echo "Unknown Wasm language: $language" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
    echo "The Wasm package matrix requires a Linux x86-64 host." >&2
    exit 2
fi

cd "$(dirname -- "$0")/../../.."

built_images=()
wasm_module_dir=
test_file=test/integration/wasm/test_wasm.py
offline_test=test_prepared_python_builds_and_packs_offline
test_name=test_component_calls_from_installed_package
cases=("$test_file::$offline_test")
filters=("$offline_test")

for language in "$@"; do
    image_var=TT_WASM_TEST_$(tr '[:lower:]' '[:upper:]' <<<"$language")_IMAGE
    image=${!image_var:-}
    if [[ -z $image ]]; then
        stage=$language
        if [[ $language == cpp ]]; then
            stage=c
        fi

        image=tt-wasm-builder:e2e-$stage
        if [[ " ${built_images[*]-} " != *" $image "* ]]; then
            if [[ -z $wasm_module_dir ]]; then
                go mod download github.com/mandesero/tt-wasm
                wasm_module_dir=$(go list -m -f "{{.Dir}}" github.com/mandesero/tt-wasm)
            fi

            docker build --build-context "tt-sdk=$PWD/sdk" \
                --platform linux/amd64 --target "$stage" -t "$image" "$wasm_module_dir"
            built_images+=("$image")
        fi
    fi

    export "$image_var=$image"
    cases+=("$test_file::${test_name}[$language]")
    filters+=("${test_name}[$language]")
done

if [[ -n ${TT_TEST_TARANTOOL:-} ]]; then
    # Let Mage activate the requested Tarantool as in the other integration runs.
    printf -v selected '%s or ' "${filters[@]}"
    export PYTEST_ADDOPTS="${PYTEST_ADDOPTS:-} -k '${selected% or }'"
    exec mage integration:full
fi

exec python3 -m pytest "${cases[@]}"
