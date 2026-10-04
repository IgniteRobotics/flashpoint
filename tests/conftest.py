import os
import tomllib
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
