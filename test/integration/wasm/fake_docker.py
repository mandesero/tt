"""Stand-in for the Wasm builder in the tt integration test."""

import json
import os
import signal
import sys
from pathlib import Path

args = sys.argv[1:]
if args[:2] == ["image", "inspect"]:
    print("sha256:" + "a" * 64)
    sys.exit(0)

assert args[0] == "run", args
assert "--pull=never" in args, args
if args[-2:] == ["check", "python"]:
    sys.exit(0)

request = json.load(sys.stdin)
operation = request["operation"]
offline = "--network" in args and args[args.index("--network") + 1] == "none"
assert offline == (operation != "fetch"), args


def host_path(target):
    for index, arg in enumerate(args):
        if arg != "--mount":
            continue
        options = dict(
            option.split("=", 1) for option in args[index + 1].split(",") if "=" in option
        )
        if options["target"] == target:
            return Path(options["source"])
    raise ValueError(f"no mount for {target}")


with Path(os.environ["TT_WASM_TEST_DOCKER_LOG"]).open("a") as log:
    print(operation, file=log)

work = host_path(request["work"])
cache = host_path(request["cache"])
if operation == "bind" and (ready := os.environ.get("TT_WASM_TEST_WAIT_FOR_SIGNAL")):
    Path(ready).write_text(str(work))
    signal.pause()
    sys.exit("expected SIGTERM")

if operation == "fetch":
    assert (cache / "sdk").is_dir()
    for path in ("wit", "python-sdk", "site-packages"):
        (cache / "dependencies" / path).mkdir(parents=True)
else:
    assert (cache / "fetched").is_file()
    if operation == "bind":
        bindings = work / "bindings" / "wit_world"
        bindings.mkdir(parents=True)
        (bindings / "__init__.py").write_text("# generated bindings\n")
    elif operation == "build":
        output = work / "component.wasm"
        if os.environ.get("TT_WASM_TEST_FAIL_BUILD"):
            output.write_bytes(b"unfinished component")
            sys.exit("test componentization failure")
        output.write_bytes(b"\x00asm\x0d\x00\x01\x00")
    else:
        raise ValueError(f"unexpected operation: {operation}")
