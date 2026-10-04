import os
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

MANIFEST_PATH = Path(__file__).parent / "corpus" / "manifest.toml"
DEFAULT_CORPUS_DIR = Path.home() / ".cache" / "flashpoint" / "corpus"


@pytest.fixture(scope="session")
def corpus_manifest() -> dict[str, Any]:
    with MANIFEST_PATH.open("rb") as f:
        return tomllib.load(f)


@pytest.fixture(scope="session")
def corpus_dir(corpus_manifest: dict[str, Any]) -> Path:
    root = Path(os.environ.get("FLASHPOINT_CORPUS_DIR", DEFAULT_CORPUS_DIR))
    return root / str(corpus_manifest["version"])


@pytest.fixture(scope="session")
def corpus_group(corpus_manifest: dict[str, Any], corpus_dir: Path) -> Callable[[str], list[Path]]:
    """Return the files of a corpus group, failing clearly if the corpus isn't fetched."""

    def resolve(group: str) -> list[Path]:
        entries = [e for e in corpus_manifest["file"] if e["group"] == group]
        if not entries:
            raise KeyError(f"unknown corpus group {group!r}")
        paths = [corpus_dir / e["name"] for e in entries]
        missing = [p.name for p in paths if not p.is_file()]
        if missing:
            pytest.fail(f"corpus not fetched ({missing[0]} missing); run tools/fetch-corpus.py")
        return paths

    return resolve
