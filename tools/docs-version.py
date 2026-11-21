#!/usr/bin/env python3
"""Which docs version a release tag deploys, and whether it moves `latest` (design D-c).

    python tools/docs-version.py TAG|dev [--versions versions.json]

Prints `version=`, `latest=`, `banner=` and `title=` lines for `$GITHUB_OUTPUT`. A final tag
`vX.Y.Z` deploys `X.Y` and moves `latest` when `X.Y` is the highest final version published.
A pre-release tag (`vX.Y.ZaN`, `bN`, `rcN`) deploys `X.Y` with the pre-release banner, titled
"X.Y (pre-release)" so a later run knows it isn't final, and never moves `latest`. `dev`
(built from `development`) carries the unreleased banner, and holds `latest` until the first
final release is published (design D-b).
`--versions` is mike's `versions.json` from `gh-pages` (missing means nothing is published).
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)((?:a|b|rc)\d+)?$")
MINOR = re.compile(r"^(\d+)\.(\d+)$")
PRE_RELEASE = "pre-release"
UNRELEASED = "unreleased"


@dataclass(frozen=True)
class Deploy:
    version: str
    latest: bool
    banner: str
    title: str


def _key(version: str) -> tuple[int, int]:
    major, minor = version.split(".")
    return int(major), int(minor)


def _finals(published: list[dict[str, object]]) -> list[str]:
    """The published versions that came from a final release tag."""
    return [
        str(entry["version"])
        for entry in published
        if MINOR.match(str(entry.get("version", "")))
        and PRE_RELEASE not in str(entry.get("title", ""))
    ]


def for_tag(tag: str, published: list[dict[str, object]]) -> Deploy:
    finals = _finals(published)
    if tag == "dev":
        return Deploy("dev", not finals, UNRELEASED, "dev")
    match = TAG.match(tag)
    if match is None:
        raise ValueError(f"'{tag}' is not a release tag (vX.Y.Z, or vX.Y.Z with aN, bN or rcN)")
    version = f"{match[1]}.{match[2]}"
    if match[4]:
        return Deploy(version, False, PRE_RELEASE, f"{version} ({PRE_RELEASE})")
    highest = max(finals, key=_key, default=None)
    return Deploy(version, highest is None or _key(version) >= _key(highest), "", version)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tag")
    parser.add_argument("--versions", type=Path, help="mike's versions.json from gh-pages")
    args = parser.parse_args(argv)
    published = []
    if args.versions is not None and args.versions.is_file():
        published = json.loads(args.versions.read_text(encoding="utf-8"))
    try:
        deploy = for_tag(args.tag, published)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    print(f"version={deploy.version}")
    print(f"latest={'true' if deploy.latest else 'false'}")
    print(f"banner={deploy.banner}")
    print(f"title={deploy.title}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
