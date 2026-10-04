"""Immutable, content-addressed copies of every ingested file."""

import hashlib
import shutil
from pathlib import Path

from flashpoint.lake.paths import LakePaths


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def raw_path(lake: LakePaths, sha256: str, suffix: str) -> Path:
    return lake.raw / sha256[:2] / f"{sha256}{suffix}"


def store(lake: LakePaths, source: Path, sha256: str, suffix: str) -> Path:
    """Copy source into raw storage keyed by its hash. Never overwrites an existing copy."""
    target = raw_path(lake, sha256, suffix)
    if target.is_file():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".part")
    shutil.copyfile(source, tmp)
    if file_sha256(tmp) != sha256:
        tmp.unlink(missing_ok=True)
        raise ValueError(f"hash mismatch copying {source} into raw storage")
    tmp.chmod(0o444)
    tmp.replace(target)
    return target
