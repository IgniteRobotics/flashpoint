"""Fetch and verify the golden log corpus described in tests/corpus/manifest.toml.

Usage:
    python tools/fetch-corpus.py               # download missing files from the release
    python tools/fetch-corpus.py --from DIR    # seed from a local directory instead

Files land in $FLASHPOINT_CORPUS_DIR/<version>/ (default ~/.cache/flashpoint/corpus/<version>/).
Existing files with a matching hash are left alone, so re-runs are cheap.
Zero-byte entries are created locally (GitHub releases reject empty assets).
Downloads go through `gh release download` when the GitHub CLI is installed (log in with
`gh auth login`, or set GH_TOKEN); that also works while the repository is private. Without gh,
a plain HTTPS download is tried, which needs the release to be public.
"""

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tomllib
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "tests" / "corpus" / "manifest.toml"
DEFAULT_CORPUS_DIR = Path.home() / ".cache" / "flashpoint" / "corpus"
CHUNK_SIZE = 1 << 20
GITHUB_RELEASE_URL = re.compile(r"https://github\.com/([^/]+/[^/]+)/releases/download/([^/]+)/?")


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


def github_release(release_url: str) -> tuple[str, str] | None:
    """Return (owner/repo, tag) for a GitHub release download URL, else None."""
    match = GITHUB_RELEASE_URL.fullmatch(release_url)
    return (match[1], match[2]) if match else None


def download(name: str, release_url: str, out: Path) -> None:
    gh = shutil.which("gh")
    release = github_release(release_url)
    if gh is not None and release is not None:
        repo, tag = release
        cmd = [gh, "release", "download", tag, "--repo", repo, "--pattern", name]
        try:
            subprocess.run([*cmd, "--output", str(out), "--clobber"], check=True)
        except subprocess.CalledProcessError as exc:
            raise OSError(f"gh release download failed (exit {exc.returncode})") from exc
        return
    with urllib.request.urlopen(f"{release_url}/{name}") as response, out.open("wb") as f:
        shutil.copyfileobj(response, f, CHUNK_SIZE)


def fetch_entry(entry: dict[str, Any], dest: Path, release_url: str, source: Path | None) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    if entry["size"] == 0:
        tmp.write_bytes(b"")
    elif source is not None:
        shutil.copyfile(source / entry["name"], tmp)
    else:
        try:
            download(entry["name"], release_url, tmp)
        except OSError:
            tmp.unlink(missing_ok=True)
            raise
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
