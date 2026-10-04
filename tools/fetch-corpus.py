"""Fetch and verify the golden log corpus described in tests/corpus/manifest.toml.

Usage:
    python tools/fetch-corpus.py               # download missing files from the release
    python tools/fetch-corpus.py --from DIR    # seed from a local directory instead

Files land in $FLASHPOINT_CORPUS_DIR/<version>/ (default ~/.cache/flashpoint/corpus/<version>/).
Existing files with a matching hash are left alone, so re-runs are cheap.
Zero-byte entries are created locally (GitHub releases reject empty assets).
"""

import argparse
import hashlib
import os
import shutil
import sys
import tomllib
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "tests" / "corpus" / "manifest.toml"
DEFAULT_CORPUS_DIR = Path.home() / ".cache" / "flashpoint" / "corpus"
CHUNK_SIZE = 1 << 20


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(CHUNK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def is_valid(path: Path, entry: dict[str, Any]) -> bool:
    return (
        path.is_file()
        and path.stat().st_size == entry["size"]
        and sha256_of(path) == entry["sha256"]
    )


def fetch_entry(entry: dict[str, Any], dest: Path, release_url: str, source: Path | None) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    if entry["size"] == 0:
        tmp.write_bytes(b"")
    elif source is not None:
        shutil.copyfile(source / entry["name"], tmp)
    else:
        url = f"{release_url}/{entry['name']}"
        with urllib.request.urlopen(url) as response, tmp.open("wb") as out:
            shutil.copyfileobj(response, out, CHUNK_SIZE)
    if not is_valid(tmp, entry):
        tmp.unlink(missing_ok=True)
        raise ValueError(f"checksum or size mismatch for {entry['name']}")
    tmp.replace(dest)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--from", dest="source", type=Path, help="seed from a local directory")
    args = parser.parse_args(argv)

    with MANIFEST_PATH.open("rb") as f:
        manifest = tomllib.load(f)
    root = Path(os.environ.get("FLASHPOINT_CORPUS_DIR", DEFAULT_CORPUS_DIR))
    target = root / manifest["version"]
    target.mkdir(parents=True, exist_ok=True)

    failures = 0
    for entry in manifest["file"]:
        dest = target / entry["name"]
        if is_valid(dest, entry):
            print(f"ok      {entry['name']}")
            continue
        try:
            fetch_entry(entry, dest, manifest["release_url"], args.source)
            print(f"fetched {entry['name']}")
        except (OSError, ValueError) as exc:
            failures += 1
            print(f"FAILED  {entry['name']}: {exc}", file=sys.stderr)

    print(f"corpus {manifest['version']} at {target}: {failures} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
