"""A stand-in for `rclone` in tests: records its argv and copies between local directories.

Run as `python fake_rclone.py <command> [flags] <src> <dst>`; the `rclone` (POSIX) or
`rclone.cmd` (Windows) wrapper that the `fake_rclone` fixture puts on PATH does that. Each call
appends `{"argv": [...]}` as a JSON line to `$FAKE_RCLONE_LOG`. `$FAKE_RCLONE_FAIL` makes every
call exit 1; `$FAKE_RCLONE_DURATION_EXCEEDED` makes a `copy` given `--max-duration` copy nothing
and exit 10, as rclone does when the limit stops it. Supported: `copy [--immutable]
[--exclude GLOB] SRC DST` (never deletes; with `--immutable` a differing destination file is an
error, as in rclone) and `sync [--backup-dir DIR] SRC DST` (replaced and deleted files move to
DIR). Other flags that take a value (timeouts, retries) are accepted and ignored.
"""

import fnmatch
import json
import os
import shutil
import sys
from pathlib import Path

VALUE_FLAGS = frozenset(
    {"--exclude", "--backup-dir", "--contimeout", "--timeout", "--retries",
     "--low-level-retries", "--max-duration"}
)  # fmt: skip
EXIT_DURATION_EXCEEDED = 10


def _keep(path: Path, root: Path, backup_dir: Path | None) -> None:
    """Move a file about to be replaced or deleted into `backup_dir` (rclone --backup-dir)."""
    if backup_dir is not None:
        target = backup_dir / path.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(path, target)


def _copy(
    src: Path,
    dst: Path,
    immutable: bool,
    excludes: list[str] = [],  # noqa: B006
    backup_dir: Path | None = None,
) -> int:
    code = 0
    for path in sorted(src.rglob("*")):
        if not path.is_file() or any(fnmatch.fnmatch(path.name, p) for p in excludes):
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
            _keep(target, dst, backup_dir)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
    return code


def main(argv: list[str]) -> int:
    with Path(os.environ["FAKE_RCLONE_LOG"]).open("a", encoding="utf-8") as log:
        log.write(json.dumps({"argv": argv}) + "\n")
    if os.environ.get("FAKE_RCLONE_FAIL"):
        print("ERROR : fake failure", file=sys.stderr)
        return 1
    flags, positional = set(), []
    values: dict[str, list[str]] = {}
    args = iter(argv)
    for arg in args:
        if arg in VALUE_FLAGS:
            values.setdefault(arg, []).append(next(args))
        elif arg.startswith("--"):
            flags.add(arg)
        else:
            positional.append(arg)
    command, src_arg, dst_arg = positional
    excludes = values.get("--exclude", [])
    backup_dir = Path(values["--backup-dir"][0]) if "--backup-dir" in values else None
    limited = command == "copy" and "--max-duration" in values
    if limited and os.environ.get("FAKE_RCLONE_DURATION_EXCEEDED"):
        print("ERROR : max transfer duration reached", file=sys.stderr)
        return EXIT_DURATION_EXCEEDED
    src, dst = Path(src_arg), Path(dst_arg)
    if not src.is_dir():
        print(f"ERROR : directory not found: {src}", file=sys.stderr)
        return 3
    dst.mkdir(parents=True, exist_ok=True)
    if command == "copy":
        return _copy(src, dst, "--immutable" in flags, excludes)
    if command == "sync":
        code = _copy(src, dst, immutable=False, backup_dir=backup_dir)
        for path in sorted(dst.rglob("*"), reverse=True):
            if not (src / path.relative_to(dst)).exists():
                if path.is_file():
                    _keep(path, dst, backup_dir)
                    path.unlink(missing_ok=True)
                else:
                    path.rmdir()
        return code
    print(f"fake rclone: unsupported command {command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
