#!/usr/bin/env python3
"""Build the Quickstart's sample bundle, flashpoint-sample-1.zip (mvp-documentation design D-i).

    python tools/build-sample-bundle.py [--out flashpoint-sample-1.zip]

It holds corpus-v1's E10 match (its wpilog and four hoots, never the truncated copy) under
`logs/`; a `config/` with what that robot needs (an `acquire.toml` naming team 6829, so the folder
counts as a configuration; Ignite's `seasons/2026.toml`, whose `robot` root the robot file uses;
and `robots/2026-comp.toml`); a `README.txt`; and `SHA256SUMS`.
The corpus comes from `$FLASHPOINT_CORPUS_DIR/corpus-v1` (run `python tools/fetch-corpus.py`
first); each file is checked against the corpus manifest. Entries carry a fixed timestamp, so
the same inputs give the same zip, byte for byte, and the SHA-256 in the Quickstart stays true.
"""

import argparse
import hashlib
import os
import sys
import tomllib
import zipfile
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
MANIFEST = REPO / "tests" / "corpus" / "manifest.toml"
ROBOT = REPO / "config" / "robots" / "2026-comp.toml"
SEASON = REPO / "config" / "seasons" / "2026.toml"
GROUP = "2026-gacmp-e10"
STAMP = (2026, 4, 11, 0, 0, 0)  # the match's day: a fixed time keeps the zip reproducible
README = """\
Flashpoint sample logs (flashpoint-sample-1)

One full match from FRC team 6829 Ignite Robotics' 2026 season: playoff match E10 at the
Georgia district championship (GACMP), 2026-04-11. It's here so you can try Flashpoint before
you have logs of your own.

  logs/                     the match's WPILib data log (.wpilog) and its CTRE hoot logs
  config/                   that robot's settings: its robot file (which motor is which) and
                            the 2026 season file its robot code needs
  SHA256SUMS                checksums of every file in this bundle

Two of the hoot files (the 19-29-15 ones) were cut short when the robot was powered off at the
event, so `flashpoint ingest` warns that they are incomplete. That's expected for this sample.

How to use it: the Quickstart's "Try it with sample logs" section, on the Flashpoint docs site
https://igniterobotics.github.io/flashpoint/latest/quickstart/
It uses this bundle's config/ and its own lake, so your configuration and lake are untouched.
"""


def sample_entries(manifest: Path = MANIFEST) -> list[dict[str, Any]]:
    """The corpus manifest entries that go in the bundle."""
    files: list[dict[str, Any]] = tomllib.loads(manifest.read_text(encoding="utf-8"))["file"]
    return [entry for entry in files if entry["group"] == GROUP]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


ACQUIRE = """\
# The sample's configuration: team 6829 (Ignite Robotics), whose robot these logs are from.
# The Quickstart passes --lake, so nothing here points at your own lake.
team = 6829
"""


def build(logs: list[Path], robot: Path, out: Path, season: Path = SEASON) -> str:
    """Write the bundle and return its SHA-256. ValueError for a truncated log."""
    cut = [path.name for path in logs if "truncated" in path.name]
    if cut:
        raise ValueError(f"a truncated corpus file never goes in the sample: {', '.join(cut)}")
    members = [(f"logs/{path.name}", path.read_bytes()) for path in sorted(logs)]
    members.append(("config/acquire.toml", ACQUIRE.encode()))
    members.append((f"config/seasons/{season.name}", season.read_bytes()))
    members.append((f"config/robots/{robot.name}", robot.read_bytes()))
    members.append(("README.txt", README.encode()))
    sums = "".join(f"{_sha256(data)}  {name}\n" for name, data in sorted(members))
    members.append(("SHA256SUMS", sums.encode()))
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w") as bundle:
        for name, data in members:
            info = zipfile.ZipInfo(name, date_time=STAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            bundle.writestr(info, data, compresslevel=9)
    return _sha256(out.read_bytes())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("flashpoint-sample-1.zip"))
    args = parser.parse_args(argv)
    default = Path.home() / ".cache" / "flashpoint" / "corpus"
    corpus = Path(os.environ.get("FLASHPOINT_CORPUS_DIR", default)) / "corpus-v1"
    logs = []
    for entry in sample_entries():
        path = corpus / entry["name"]
        if not path.is_file() or _sha256(path.read_bytes()) != entry["sha256"]:
            print(
                f"{path} is missing or changed: run python tools/fetch-corpus.py", file=sys.stderr
            )
            return 2
        logs.append(path)
    digest = build(logs, ROBOT, args.out)
    print(f"{args.out}: {args.out.stat().st_size / 1e6:.1f} MB, sha256 {digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
