"""A stand-in for `rclone` in tests: records its argv and copies between local directories.

Run as `python fake_rclone.py <command> [flags] <src> <dst>`; the `rclone` (POSIX) or
`rclone.cmd` (Windows) wrapper that the `fake_rclone` fixture puts on PATH does that. Each call
appends `{"argv": [...]}` as a JSON line to `$FAKE_RCLONE_LOG`. `$FAKE_RCLONE_FAIL` makes every
call exit 1. Supported: `copy [--immutable] SRC DST` (never deletes; with `--immutable` a
differing destination file is an error, as in rclone) and `sync SRC DST`.
"""

import json
import os
import shutil
import sys
from pathlib import Path


def _copy(src: Path, dst: Path, immutable: bool) -> int:
    code = 0
    for path in sorted(src.rglob("*")):
        if not path.is_file():
            continue
        target = dst / path.relative_to(src)
        if target.is_file():
            if target.read_bytes() == path.read_bytes():
                continue
            if immutable:
                print(f"ERROR : {target}: source and destination differ", file=sys.stderr)
                code = 1
                continue
            target.chmod(0o644)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
    return code


def main(argv: list[str]) -> int:
    with Path(os.environ["FAKE_RCLONE_LOG"]).open("a", encoding="utf-8") as log:
        log.write(json.dumps({"argv": argv}) + "\n")
    if os.environ.get("FAKE_RCLONE_FAIL"):
        print("ERROR : fake failure", file=sys.stderr)
        return 1
    flags = {a for a in argv if a.startswith("--")}
    command, src_arg, dst_arg = (a for a in argv if not a.startswith("--"))
    src, dst = Path(src_arg), Path(dst_arg)
    if not src.is_dir():
        print(f"ERROR : directory not found: {src}", file=sys.stderr)
        return 3
    dst.mkdir(parents=True, exist_ok=True)
    if command == "copy":
        return _copy(src, dst, "--immutable" in flags)
    if command == "sync":
        code = _copy(src, dst, immutable=False)
        for path in sorted(dst.rglob("*"), reverse=True):
            if not (src / path.relative_to(dst)).exists():
                path.unlink() if path.is_file() else path.rmdir()
        return code
    print(f"fake rclone: unsupported command {command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
