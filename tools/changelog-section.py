"""Print one version's section of CHANGELOG.md, word for word, for the GitHub Release notes.

Usage: python tools/changelog-section.py VERSION [--changelog PATH] [--is-pre-release]

The section is the text under the `## VERSION` heading (which may end in ` - <date>`), up to the
next `## ` heading, without the blank lines around it. Headings inside fenced code blocks don't
count. A missing or empty section exits 1 naming the version, so the release stops before
anything is published. `--is-pre-release` prints `true` or `false` instead.
"""

import argparse
import re
import sys
from pathlib import Path

CHANGELOG = Path(__file__).resolve().parent.parent / "CHANGELOG.md"
PRE_RELEASE = re.compile(r"(a|b|rc|\.dev)\d+$")
FENCE = re.compile(r"^\s*(```|~~~)")


class MissingSection(ValueError):  # noqa: N818 - name set by the tests, written first
    """CHANGELOG.md has no section, or an empty one, for the version."""


def is_pre_release(version: str) -> bool:
    return PRE_RELEASE.search(version.removeprefix("v")) is not None


def section(text: str, version: str) -> str:
    version = version.removeprefix("v")
    heading = re.compile(rf"^## {re.escape(version)}(?: - .*)?$")
    lines = text.splitlines()
    start: int | None = None
    end = len(lines)
    in_fence = False
    for number, line in enumerate(lines):
        if FENCE.match(line):
            in_fence = not in_fence
        if in_fence:
            continue
        if start is None:
            if heading.match(line.rstrip()):
                start = number + 1
        elif line.startswith("## "):
            end = number
            break
    if start is None:
        raise MissingSection(f"CHANGELOG.md has no '## {version}' section")
    notes = "\n".join(lines[start:end]).strip("\n")
    if not notes.strip():
        raise MissingSection(f"CHANGELOG.md's '## {version}' section is empty")
    return notes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("version", help="the version or tag, for example 0.3.0 or v0.3.0rc1")
    parser.add_argument("--changelog", type=Path, default=CHANGELOG, help="default: CHANGELOG.md")
    parser.add_argument("--is-pre-release", action="store_true", help="print true or false")
    args = parser.parse_args(argv)
    if args.is_pre_release:
        print("true" if is_pre_release(args.version) else "false")
        return 0
    try:
        print(section(args.changelog.read_text(encoding="utf-8"), args.version))
    except MissingSection as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
