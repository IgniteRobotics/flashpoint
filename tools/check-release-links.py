"""Refuse a release whose package page would show moving or wrong-version documentation links.

Usage: python tools/check-release-links.py --version VERSION FILE...

FILE is a built wheel (its METADATA: the project URLs, and the README as PyPI shows it) or a
Markdown file. Each problem is printed as `<file>:<line>: <reason>` and the exit status is 1:
- a link to a branch of this repository that moves (`blob/main`, `blob/development`,
  `tree/main`, `tree/development`, or a raw.githubusercontent.com file on those branches);
- an image given by a relative path (PyPI can't show it);
- an image from the documentation site under another version's path than VERSION's major.minor
  (`latest` and `dev` included).
"""

import argparse
import re
import sys
import zipfile
from pathlib import Path

SITE = "https://igniterobotics.github.io/flashpoint/"
MOVING = re.compile(
    r"https?://(?:github\.com/IgniteRobotics/flashpoint/(?:blob|tree)"
    r"|raw\.githubusercontent\.com/IgniteRobotics/flashpoint)/(?:main|development)(?=[/#?)\s\"'>]|$)",
    re.IGNORECASE,
)
MARKDOWN_IMAGE = re.compile(r"!\[[^\]]*\]\(\s*<?(?P<url>[^)\s>]+)")
HTML_IMAGE = re.compile(r"<img\b[^>]*?\ssrc\s*=\s*[\"'](?P<url>[^\"']+)[\"']", re.IGNORECASE)
SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")
MINOR = re.compile(r"^v?(\d+)\.(\d+)")


def docs_path(version: str) -> str:
    match = MINOR.match(version)
    if not match:
        raise ValueError(f"not a version: {version}")
    return f"{match.group(1)}.{match.group(2)}"


def problems(text: str, version: str, source: str) -> list[str]:
    minor = docs_path(version)
    found: list[str] = []
    for number, line in enumerate(text.splitlines(), 1):
        found += [
            f"{source}:{number}: link to a moving branch ({match.group(0)}); use a tag or the docs"
            for match in MOVING.finditer(line)
        ]
        images = [
            m.group("url") for m in (*MARKDOWN_IMAGE.finditer(line), *HTML_IMAGE.finditer(line))
        ]
        for url in images:
            if not SCHEME.match(url) and not url.startswith("//"):
                found.append(
                    f"{source}:{number}: image by relative path ({url}); PyPI can't show it"
                )
            elif url.startswith(SITE) and url[len(SITE) :].split("/", 1)[0] != minor:
                found.append(
                    f"{source}:{number}: image from another docs version ({url}); use /{minor}/"
                )
    return found


def wheel_problems(wheel: Path, version: str) -> list[str]:
    with zipfile.ZipFile(wheel) as archive:
        (name,) = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
        metadata = archive.read(name).decode("utf-8")
    # The headers, a blank line, then the README as the package index shows it.
    headers, _, readme = metadata.partition("\n\n")
    return [
        *problems(headers, version, f"{wheel}!METADATA"),
        *problems(readme, version, f"{wheel}!README"),
    ]


def check_file(path: Path, version: str) -> list[str]:
    if path.suffix == ".whl":
        return wheel_problems(path, version)
    return problems(path.read_text(encoding="utf-8"), version, str(path))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--version", required=True, help="the version being built, e.g. 0.3.0")
    parser.add_argument("files", nargs="+", type=Path, help="built wheels or Markdown files")
    args = parser.parse_args(argv)
    found = [problem for path in args.files for problem in check_file(path, args.version)]
    for problem in found:
        print(problem, file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
