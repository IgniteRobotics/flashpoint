#!/usr/bin/env python3
"""Pin the README to one release before it's built into the package (design D-g).

    python tools/pin-readme-media.py VERSION [--readme README.md]

The README is also the PyPI page, and a release's page must not change after it ships. So, in
place:
- docs-site images under `/latest/` move to the release's `/X.Y/` (pre-releases too: their tag
  deploys `X.Y`);
- links to this repository's `main` branch (`blob/main/`, `tree/main/`, and raw files) move to
  the release tag `vVERSION`.

Nothing else changes; the release build's link check (tools/check-release-links.py) runs after
this and still refuses anything left moving or from another docs version.
"""

import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SITE_LATEST_MEDIA = "https://igniterobotics.github.io/flashpoint/latest/"
VERSION = re.compile(r"^v?(\d+)\.(\d+)\.\d+(?:(?:a|b|rc)\d+|\.dev\d+)?$")
MARKDOWN_IMAGE = re.compile(r"(!\[[^\]]*\]\(\s*<?)(" + re.escape(SITE_LATEST_MEDIA) + r")")
HTML_IMAGE = re.compile(
    r"(<img\b[^>]*?\ssrc\s*=\s*[\"'])(" + re.escape(SITE_LATEST_MEDIA) + r")", re.IGNORECASE
)
REPO_MAIN = re.compile(
    r"(https://github\.com/IgniteRobotics/flashpoint/(?:blob|tree)/"
    r"|https://raw\.githubusercontent\.com/IgniteRobotics/flashpoint/)main(?=/)"
)


def pin(text: str, version: str) -> str:
    match = VERSION.match(version)
    if match is None:
        raise ValueError(f"'{version}' is not a release version (X.Y.Z, X.Y.ZrcN, X.Y.Z.devN)")
    site_version = f"https://igniterobotics.github.io/flashpoint/{match[1]}.{match[2]}/"
    tag = "v" + version.removeprefix("v")
    text = MARKDOWN_IMAGE.sub(lambda m: m[1] + site_version, text)
    text = HTML_IMAGE.sub(lambda m: m[1] + site_version, text)
    return REPO_MAIN.sub(lambda m: m[1] + tag, text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("version", help="the version being released, e.g. 0.3.0 or 0.3.0rc1")
    parser.add_argument("--readme", type=Path, default=REPO / "README.md")
    args = parser.parse_args(argv)
    try:
        pinned = pin(args.readme.read_text(encoding="utf-8"), args.version)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    args.readme.write_text(pinned, encoding="utf-8")
    print(f"pinned {args.readme} to {args.version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
