"""How often does owlet fail on healthy corpus hoots? (#17; perf job, non-blocking.)

Runs each conversion several times with no retry and prints every failure's exit code and
last stderr line. Asserts only what the retry relies on: no two runs in a row both fail.
"""

import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.readers import hoot

pytestmark = [pytest.mark.corpus, pytest.mark.perf]

RUNS = 10
HOOTS = [
    ("2026-gacmp-q7", "_rio_"),
    ("2026-gacmp-q7", "_6E9415C3394C485320202050101C18FF_"),
]


@pytest.mark.parametrize(("group", "part"), HOOTS)
def test_owlet_failure_rate(
    group: str, part: str, corpus_group: Callable[[str], list[Path]], tmp_path: Path
) -> None:
    path = next(p for p in corpus_group(group) if p.suffix == ".hoot" and part in p.name)
    registry = hoot.default_registry(config.cache_root())
    owlet = registry.binary_for(hoot.read_header(path).compliancy)
    selected = hoot.select_signals(hoot.scan_signals(owlet, path), "health") or []
    results: list[tuple[int, str]] = []
    for i in range(RUNS):
        out = tmp_path / f"run{i}.wpilog"
        args = [str(owlet), str(path), str(out), "-f", "wpilog", "-s", ",".join(selected)]
        done = subprocess.run(args, capture_output=True, text=True, timeout=hoot.OWLET_TIMEOUT_S)  # noqa: S603
        last = (done.stderr or done.stdout).strip().splitlines()[-1:] or ["no output"]
        results.append((done.returncode, last[0]))
        out.unlink(missing_ok=True)
    failures = [r for r in results if r[0] != 0]
    print(f"\nowlet {path.name}: {len(failures)}/{RUNS} runs failed")  # noqa: T201
    for code, line in failures:
        print(f"  exit {code}: {line}")  # noqa: T201
    both = [i for i in range(RUNS - 1) if results[i][0] != 0 and results[i + 1][0] != 0]
    assert not both, f"two runs in a row failed at {both}: one retry would not have saved them"
