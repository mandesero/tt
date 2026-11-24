"""tt wasm against a release of its own, offline.

TT_WASM_RELEASES replaces the releases tt knows with a table whose assets a
loopback server serves: the native module is a few bytes nothing loads, which is
enough to follow it from the download, through the rock, into the rocks tree a
build lays out. The Python builder flow uses a stand-in for Docker, and a
real image exercises the runtime when supplied explicitly.
"""

import functools
import hashlib
import http.server
import io
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import threading
import tomllib
from pathlib import Path

import pytest

from utils import LoopbackHTTPServer, sha256_of, write_rock_index

COMPONENT = b"\x00asm\x0d\x00\x01\x00"
IMAGE = f"sha256:{'a' * 64}"
MODULE = b"\x7fELF not a module: tt packs and installs it, and nothing loads it\n"

# The builds of the release, as goos, goarch, rock arch and module: the two
# tt knows and linux/arm64, which tarantool-wasm has no build for, so that the
# tests run on an arm64 runner too.
PLATFORMS = [
    ("linux", "amd64", "linux-x86_64", "wasm.so"),
    ("linux", "arm64", "linux-aarch64", "wasm.so"),
    ("darwin", "arm64", "macosx-aarch64", "wasm.dylib"),
]


def host_platform() -> tuple[str, str, str, str]:
    """The build of the host, or a skip on a host the release has none for."""
    system, machine = platform.system(), platform.machine().lower()
    if system == "Linux" and machine in ("x86_64", "amd64"):
        return PLATFORMS[0]
    if system == "Linux" and machine in ("aarch64", "arm64"):
        return PLATFORMS[1]
    if system == "Darwin" and machine in ("arm64", "aarch64"):
        return PLATFORMS[2]
    pytest.skip(f"tarantool-wasm has no build for {system} {machine}")


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


def serve(handler) -> tuple[LoopbackHTTPServer, str]:
    server = LoopbackHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


@pytest.fixture()
def asset_server(tmp_path):
    """A loopback server and the directory its URL serves."""
    root = tmp_path / "assets"
    root.mkdir()
    server, url = serve(functools.partial(QuietHandler, directory=str(root)))
    yield root, url, server
    server.shutdown()
    server.server_close()


@pytest.fixture()
def assets(asset_server):
    root, url, _ = asset_server
    return root, url


def write_release(path: Path, url: str, assets: Path, sha256: str | None = None) -> None:
    """Write a release table of 0.1.0 whose builds the assets server serves."""
    version = assets / "v0.1.0"
    version.mkdir(parents=True, exist_ok=True)
    builds = []
    for goos, goarch, arch, module in PLATFORMS:
        asset = f"tarantool-wasm-0.1.0-{arch}-{module}"
        (version / asset).write_bytes(MODULE)
        builds.append(
            {
                "goos": goos,
                "goarch": goarch,
                "asset": asset,
                "sha256": sha256 or hashlib.sha256(MODULE).hexdigest(),
                "rock_arch": arch,
                "module": module,
            },
        )
    path.write_text(
        json.dumps(
            {
                "0.1.0": {
                    "version": "0.1.0",
                    "rock_revision": 1,
                    "sdk_asset": "tarantool-wasm-0.1.0-sdk.tar.gz",
                    "sdk_md5": "0" * 32,
                    "base_url": url,
                    "platforms": builds,
                },
            },
        ),
    )


@pytest.fixture()
def env(tmp_path, assets):
    """The environment of tt: the fake release, no registry but an empty one,
    and a cache of the test's own."""
    root, url = assets
    table = tmp_path / "releases.json"
    write_release(table, url, root)
    empty = tmp_path / "empty-registry"
    empty.mkdir()
    (empty / "manifest").write_text("commands = {}\nmodules = {}\nrepository = {}\n")
    return {
        **os.environ,
        "TT_WASM_RELEASES": str(table),
        "TT_REGISTRIES": str(empty),
        "XDG_CACHE_HOME": str(tmp_path / "cache"),
    }


def tt(tt_cmd, cwd: Path, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(tt_cmd), *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.fixture()
def project(tt_cmd, tmp_path, env) -> Path:
    """An empty package."""
    root = tmp_path / "app"
    root.mkdir()
    result = tt(tt_cmd, root, "new", env=env)
    assert result.returncode == 0, result.stderr
    return root


def installed_module(project: Path) -> bytes:
    module = host_platform()[3]
    return (project / ".rocks" / "lib" / "tarantool" / module).read_bytes()


def locked_wasm(project: Path) -> dict:
    with (project / "app.manifest.lock").open("rb") as handle:
        lock = tomllib.load(handle)
    return next(
        dep for dep in lock["lock"]["products"]["default"]["dependencies"] if dep["name"] == "wasm"
    )


def test_prepare_packs_a_local_build_and_a_build_installs_it(tt_cmd, tmp_path, project, env):
    arch = host_platform()[2]
    module = tmp_path / "libtarantool_wasm_rs.so"
    module.write_bytes(MODULE)

    result = tt(tt_cmd, project, "wasm", "prepare", "--from", module, env=env)

    assert result.returncode == 0, result.stderr
    assert f"prepared for tarantool-wasm 0.1.0 ({arch})" in result.stdout
    assert (project / ".tarantool" / "registry" / f"wasm-0.1.0-1.{arch}.rock").is_file()
    assert (
        'wasm = { version = "0.1.0", registry = ".tarantool/registry" }'
        in (project / "app.manifest.toml").read_text()
    )
    lock = (project / "app.manifest.lock").read_text()
    assert "name = 'wasm'" in lock
    assert "version = '0.1.0-1'" in lock

    build = tt(tt_cmd, project, "package", "build", env=env)

    assert build.returncode == 0, build.stderr
    assert installed_module(project) == MODULE

    status = tt(tt_cmd, project, "wasm", "status", env=env)
    assert status.returncode == 0, status.stdout + status.stderr
    assert "wasm:       0.1.0 from .tarantool/registry" in status.stdout
    assert "ready" in status.stdout


def test_rock_refuses_a_tampered_asset(tt_cmd, tmp_path, env, assets):
    root, url = assets
    write_release(Path(env["TT_WASM_RELEASES"]), url, root, sha256="0" * 64)
    out = tmp_path / "rocks"

    result = tt(tt_cmd, tmp_path, "wasm", "rock", "--out", str(out), env=env)

    assert result.returncode != 0
    assert "checksum mismatch" in result.stderr
    assert not list(out.glob("*.rock"))


def test_prepare_from_refuses_an_unknown_release(tt_cmd, tmp_path, project, env):
    """A local build is packed with the rockspec of a release tt knows; a rock
    server is asked for any version (see test_prepare_declares_the_rock_of_a_rock_index)."""
    module = tmp_path / "libtarantool_wasm_rs.so"
    module.write_bytes(MODULE)
    before = (project / "app.manifest.toml").read_text()

    result = tt(
        tt_cmd,
        project,
        "wasm",
        "prepare",
        "--from",
        module,
        "--version",
        "9.9.9",
        env=env,
    )

    assert result.returncode != 0
    assert "unknown release: tarantool-wasm 9.9.9; known: 0.1.0" in result.stderr
    assert (project / "app.manifest.toml").read_text() == before


def test_prepare_refuses_a_local_build_and_a_rock_server(tt_cmd, tmp_path, project, env):
    result = tt(
        tt_cmd,
        project,
        "wasm",
        "prepare",
        "--from",
        tmp_path / "module.so",
        "--rock-server",
        "https://rocks.example.test/",
        env=env,
    )

    assert result.returncode != 0
    assert "[from rock-server] were all set" in result.stderr


def test_status_of_an_unprepared_project_fails(tt_cmd, project, env):
    result = tt(tt_cmd, project, "wasm", "status", env=env)

    assert result.returncode != 0
    assert "the package declares no wasm runtime" in result.stderr


class Redirect(http.server.BaseHTTPRequestHandler):
    """Sends every request on to target, as GitHub does a release asset."""

    target = ""

    def do_GET(self):
        self.send_response(302)
        self.send_header("Location", self.target + self.path)
        self.end_headers()

    def log_message(self, format, *args):
        pass


def test_rocks_of_a_release_serve_every_machine(tt_cmd, tmp_path, project, env):
    """tt wasm rock packs the rock of every build; a project declares the wasm
    rock from where they are served, through a redirect, and builds."""
    rocks = tmp_path / "server" / "v0.1.0"

    packed = tt(tt_cmd, tmp_path, "wasm", "rock", "--out", str(rocks), env=env)

    assert packed.returncode == 0, packed.stderr
    # The manifests luarocks-admin make_manifest writes, one per Lua version
    # and one for all, and the rock index, which tt reads first.
    assert sorted(path.name for path in rocks.iterdir()) == [
        "manifest",
        "manifest-5.1",
        "manifest-5.2",
        "manifest-5.3",
        "manifest-5.4",
        "tt-rocks-index.json",
        "wasm-0.1.0-1.linux-aarch64.rock",
        "wasm-0.1.0-1.linux-x86_64.rock",
        "wasm-0.1.0-1.macosx-aarch64.rock",
    ]

    server, url = serve(functools.partial(QuietHandler, directory=str(rocks.parent)))
    redirect = type("ToServer", (Redirect,), {"target": url})
    front, front_url = serve(redirect)
    try:
        registry = f"{front_url}/v0.1.0/"
        result = tt(
            tt_cmd,
            project,
            "wasm",
            "prepare",
            "--rock-server",
            registry,
            env=env,
        )
        assert result.returncode == 0, result.stderr
        assert f"rock:       from {registry}" in result.stdout
        assert not (project / ".tarantool" / "registry").exists()

        build = tt(tt_cmd, project, "package", "build", env=env)
        assert build.returncode == 0, build.stderr
        assert installed_module(project) == MODULE

        # The server publishes the index tt wasm rock wrote: the lock pins
        # the rock of every platform.
        entry = locked_wasm(project)
        assert entry["artifacts"] == {
            rock.name.removeprefix("wasm-0.1.0-1.").removesuffix(".rock"): "sha256:"
            + sha256_of(rock)
            for rock in rocks.glob("*.rock")
        }
    finally:
        front.shutdown()
        server.shutdown()


def test_prepare_declares_the_rock_of_a_rock_index(tt_cmd, tmp_path, project, env):
    """tt wasm prepare --rock-server takes a server that publishes nothing but
    a tt-rocks-index.json, as rocks-repo does by default: nothing is packed in
    the project, the lock pins the rock of every platform and the rockspec,
    and a build installs the rock of the host, checked against the lock."""
    arch = host_platform()[2]
    packed = tmp_path / "packed"

    result = tt(tt_cmd, tmp_path, "wasm", "rock", "--out", str(packed), env=env)
    assert result.returncode == 0, result.stderr

    root = tmp_path / "index"
    digests = write_rock_index(root, packed)
    server, url = serve(functools.partial(QuietHandler, directory=str(root)))
    try:
        registry = f"{url}/"
        result = tt(
            tt_cmd,
            project,
            "wasm",
            "prepare",
            "--rock-server",
            registry,
            env=env,
        )
        assert result.returncode == 0, result.stderr
        assert f"rock:       from {registry}" in result.stdout
        assert not (project / ".tarantool" / "registry").exists()

        entry = locked_wasm(project)
        assert entry["name"] == "wasm"
        assert entry["version"] == "0.1.0-1"
        assert entry["artifacts"] == {
            rock_arch: f"sha256:{digest}" for rock_arch, digest in digests["0.1.0-1"].items()
        }
        assert sorted(entry["artifacts"]) == [
            "linux-aarch64",
            "linux-x86_64",
            "macosx-aarch64",
            "rockspec",
        ]

        build = tt(tt_cmd, project, "package", "build", env=env)
        assert build.returncode == 0, build.stderr
        assert installed_module(project) == MODULE

        status = tt(tt_cmd, project, "wasm", "status", env=env)
        assert status.returncode == 0, status.stderr
        assert f"wasm:       0.1.0 from {registry}" in status.stdout

        # A rock the server swaps for another is not installed.
        rock = root / "releases" / "wasm-0.1.0-1" / f"wasm-0.1.0-1.{arch}.rock"
        rock.write_bytes(rock.read_bytes() + b"tampered")
        shutil.rmtree(project / ".rocks")

        build = tt(tt_cmd, project, "package", "build", env=env)
        assert build.returncode != 0
        assert "checksum mismatch" in build.stderr
        assert f"wasm-0.1.0-1.{arch}.rock" in build.stderr
    finally:
        server.shutdown()


def run(tt_cmd, project: Path, *args: str, env: dict | None = None) -> str:
    result = tt(tt_cmd, project, *args, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def prepare_component(
    tt_cmd,
    project: Path,
    language: str,
    image: str,
    *runtime: str,
    env: dict | None = None,
):
    run(
        tt_cmd,
        project,
        "wasm",
        "prepare",
        "--version",
        "0.1.0",
        "--lang",
        language,
        "--runner",
        "docker",
        "--platform",
        "linux/amd64",
        "--image",
        image,
        *runtime,
        env=env,
    )
    run(tt_cmd, project, "wasm", "new", "calc", "--lang", language, env=env)
    run(tt_cmd, project, "wasm", "prepare", env=env)


def add_sdk(releases: Path, assets: Path):
    """Give the mock release the SDK layout expected by the Python driver."""
    table = json.loads(releases.read_text())
    release = table["0.1.0"]
    root = "tarantool-wasm-0.1.0-sdk"
    sdk = assets / "v0.1.0" / release["sdk_asset"]
    with tarfile.open(sdk, "w:gz") as archive:
        for path in (
            "packages/python/twr_python_sdk-0.1.0-py3-none-any.whl",
            "wit/python/deps/tarantool/package.wit",
        ):
            data = b"resolved by the fake builder\n"
            info = tarfile.TarInfo(f"{root}/{path}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    release["sdk_sha256"] = hashlib.sha256(sdk.read_bytes()).hexdigest()
    releases.write_text(json.dumps(table))


def installed_component(project: Path, tree: Path | None = None) -> Path:
    with (project / "app.manifest.toml").open("rb") as manifest:
        name = tomllib.load(manifest)["package"]["name"]
    scope = project if tree is None else tree
    return scope / ".rocks" / "share" / "tarantool" / name / "dist" / "calc.wasm"


def test_prepared_python_builds_and_packs_offline(tt_cmd, tmp_path, project, env, asset_server):
    """Bind and pack work with cached SDK and a mirror of the runtime."""
    arch = host_platform()[2]
    root, url, server = asset_server
    registry = f"{url}/rocks/"
    rock = f"wasm-0.1.0-1.{arch}.rock"
    mirror = ".tarantool/registry"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    fake_docker = Path(__file__).with_name("fake_docker.py").read_text()
    docker.write_text(f"#!{sys.executable}\n{fake_docker}")
    docker.chmod(0o755)
    trace = tmp_path / "docker.log"
    env = {**env, "PATH": f"{bin_dir}:{env['PATH']}", "TT_WASM_TEST_DOCKER_LOG": str(trace)}

    add_sdk(Path(env["TT_WASM_RELEASES"]), root)
    run(tt_cmd, project, "wasm", "rock", "--out", str(root / "rocks"), env=env)
    prepare_component(tt_cmd, project, "python", IMAGE, "--rock-server", registry, env=env)
    not_fetched = tt(tt_cmd, project, "wasm", "bind", "calc", env=env)
    assert not_fetched.returncode != 0
    assert "run tt wasm fetch" in not_fetched.stderr
    run(tt_cmd, project, "wasm", "fetch", "calc", env=env)
    old_pin = locked_wasm(project)
    run(
        tt_cmd,
        project,
        "package",
        "download",
        "wasm@0.1.0-1",
        "--registry",
        registry,
        "--dir",
        mirror,
        env=env,
    )
    assert sha256_of(project / mirror / rock) == sha256_of(root / "rocks" / rock)
    server.shutdown()
    server.server_close()

    run(tt_cmd, project, "wasm", "prepare", "--rock-server", mirror, env=env)
    with (project / "app.manifest.toml").open("rb") as handle:
        manifest = tomllib.load(handle)
    assert manifest["dependencies"]["wasm"] == {"version": "0.1.0", "registry": mirror}
    new_pin = locked_wasm(project)
    assert new_pin["version"] == old_pin["version"] == "0.1.0-1"
    assert new_pin["artifacts"][arch] == old_pin["artifacts"][arch]

    locks = [project / "app.manifest.lock", project / "wasm.lock.json"]
    before = [path.read_bytes() for path in locks]
    run(tt_cmd, project, "wasm", "bind", "calc", env=env)
    run(tt_cmd, project, "wasm", "bind", "calc", "--check", env=env)
    component = project / "components" / "calc"
    binding = component / ".tt-wasm" / "bindings" / "wit_world" / "__init__.py"
    binding.write_text("# Changed after generation\n")
    stale = tt(tt_cmd, project, "wasm", "bind", "calc", "--check", env=env)
    assert stale.returncode != 0
    assert binding.read_text() == "# Changed after generation\n"

    run(tt_cmd, project, "wasm", "build", "calc", env=env)
    output = component / "dist" / "calc.wasm"
    assert output.read_bytes() == COMPONENT
    run(tt_cmd, project, "package", "build", "--locked", "--rebuild", env=env)
    assert output.read_bytes() == COMPONENT
    assert installed_component(project).read_bytes() == COMPONENT

    run(tt_cmd, project, "package", "pack", "--locked", "--rebuild", "--without-deps", env=env)
    assert len(list((project / "_build" / "pack").glob("*.tt"))) == 1
    assert trace.read_text().splitlines() == ["fetch", "bind", "build", "build", "build"]
    assert [path.read_bytes() for path in locks] == before

    failed = tt(
        tt_cmd,
        project,
        "package",
        "build",
        "--locked",
        "--rebuild",
        env={**env, "TT_WASM_TEST_FAIL_BUILD": "1"},
    )
    assert failed.returncode != 0
    assert "test componentization failure" in failed.stderr
    assert output.read_bytes() == installed_component(project).read_bytes() == COMPONENT


def component_builder_image(language: str):
    image_var = f"TT_WASM_TEST_{language.upper()}_IMAGE"
    image = os.environ.get(image_var)
    return pytest.param(
        language,
        image,
        id=language,
        marks=pytest.mark.skipif(
            not image,
            reason=f"set {image_var} to a locally built {language} builder image",
        ),
    )


@pytest.mark.docker
@pytest.mark.parametrize(
    ("language", "image"),
    [
        component_builder_image(language)
        for language in ("python", "js", "go", "rust", "c", "cpp", "csharp")
    ],
)
def test_component_calls_from_installed_package(tt_cmd, tmp_path, language, image):
    """The default component from a real builder image survives pack and install."""
    os_name, arch, *_ = host_platform()
    if (os_name, arch) == ("linux", "arm64"):
        pytest.skip("tarantool-wasm 0.1.0 has no Linux arm64 build")
    project = tmp_path / "app"
    project.mkdir()
    run(tt_cmd, project, "new")
    prepare_component(tt_cmd, project, language, image)
    run(tt_cmd, project, "wasm", "fetch", "calc")
    run(tt_cmd, project, "wasm", "bind", "calc")
    run(tt_cmd, project, "wasm", "bind", "calc", "--check")
    run(tt_cmd, project, "package", "build", "--locked")
    assert installed_component(project).read_bytes().startswith(COMPONENT)
    run(tt_cmd, project, "package", "pack", "--locked", "--rebuild")

    (archive,) = (project / "_build" / "pack").glob("*.tt")
    installed = tmp_path / "installed"
    installed.mkdir()
    run(tt_cmd, installed, "new")
    run(tt_cmd, installed, "package", "install", str(archive))
    component = installed_component(project, installed)
    command = (
        "local wasm = require('wasm'); "
        f"local calc = wasm.load({json.dumps(str(component))}); "
        "assert(wasm.call(calc, 'add', 20, 22) == 42); os.exit()"
    )
    run(tt_cmd, installed, "run", "-e", command)
