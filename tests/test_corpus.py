import hashlib
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.corpus

EXPECTED_GROUPS = {
    "2026-gacmp-q7",
    "2026-gacmp-e10",
    "2026-gacmp-p2",
    "2026-gadal-q11-empty",
    "2026-gacmp-e10-truncated",
    "2025-gadal-q30",
    "2025-gacmp-q19",
    "2025-nofms",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def test_manifest_covers_every_group(corpus_manifest: dict[str, Any]) -> None:
    groups = {entry["group"] for entry in corpus_manifest["file"]}
    assert groups == EXPECTED_GROUPS


def test_every_file_present_with_matching_size_and_hash(
    corpus_manifest: dict[str, Any], corpus_dir: Path
) -> None:
    for entry in corpus_manifest["file"]:
        path = corpus_dir / entry["name"]
        assert path.is_file(), f"missing {entry['name']}; run tools/fetch-corpus.py"
        assert path.stat().st_size == entry["size"], entry["name"]
        assert _sha256(path) == entry["sha256"], entry["name"]


def test_wpilog_files_have_wpilog_header(corpus_manifest: dict[str, Any], corpus_dir: Path) -> None:
    for entry in corpus_manifest["file"]:
        if entry["kind"] == "wpilog":
            with (corpus_dir / entry["name"]).open("rb") as f:
                assert f.read(6) == b"WPILOG", entry["name"]
