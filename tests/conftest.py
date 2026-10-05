import os
import threading
import tomllib
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

MANIFEST_PATH = Path(__file__).parent / "corpus" / "manifest.toml"
DEFAULT_CORPUS_DIR = Path.home() / ".cache" / "flashpoint" / "corpus"
THREAD_GRACE_S = 2.0


@pytest.fixture(autouse=True)
def no_leaked_threads() -> Iterator[None]:
    """Fail a test that leaves a non-daemon thread running: pytest would wait on it at exit
    forever, where the per-test timeout no longer applies."""
    before = set(threading.enumerate())
    yield
    leaked = [t for t in threading.enumerate() if t not in before and not t.daemon]
    for thread in leaked:
        thread.join(THREAD_GRACE_S)
    alive = [t.name for t in leaked if t.is_alive()]
    assert not alive, f"test left non-daemon threads running: {alive}"


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


@pytest.fixture(scope="session")
def corpus_lake(corpus_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> Any:
    """A lake with the whole golden corpus ingested (shared by derived-layer corpus tests)."""
    from flashpoint import config
    from flashpoint.ingest import Ingestor
    from flashpoint.lake.paths import LakePaths
    from flashpoint.readers import hoot

    lake = LakePaths(tmp_path_factory.mktemp("corpus-lake") / "lake")
    ingestor = Ingestor(lake, hoot.default_registry(config.cache_root()))
    try:
        ingestor.ingest([corpus_dir])
    finally:
        ingestor.close()
    return lake


@pytest.fixture(scope="session")
def derived_corpus_lake(corpus_lake: Any) -> Any:
    """The corpus lake with silver and gold derived (shared by report and view corpus tests)."""
    from flashpoint import config
    from flashpoint.semantics.derive import Deriver

    deriver = Deriver(corpus_lake, config.config_root())
    try:
        deriver.run()
    finally:
        deriver.close()
    return corpus_lake
