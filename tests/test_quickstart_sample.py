"""The Quickstart's "Try it with sample logs" commands work as written (spec project-documentation,
"Quickstart with sample logs"; mvp-documentation tasks 6.4 and 6.5).

The commands are taken from the page itself: the download becomes a copy of a bundle built from
the corpus, and the blocking `flashpoint serve` is replaced by reading Replay's index."""

import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = [
    pytest.mark.corpus,
    pytest.mark.skipif(sys.platform == "win32", reason="runs the page's sh commands"),
]

REPO = Path(__file__).resolve().parents[1]
QUICKSTART = REPO / "docs" / "guide" / "quickstart.md"
BUNDLE_TOOL = REPO / "tools" / "build-sample-bundle.py"
MATCH = "2026gacmp_qm7"
NOTE = "declared signal(s) not written"


def _bundle_tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("build_sample_bundle", BUNDLE_TOOL)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _try_it_section() -> str:
    text = QUICKSTART.read_text(encoding="utf-8")
    return text.split("\n## Try it with sample logs\n", 1)[1].split("\n## ", 1)[0]


def _sh_commands() -> list[str]:
    """The section's first `sh` block, as command lines."""
    block = re.search(r"```sh\n(.*?)\n\s*```", _try_it_section(), re.DOTALL)
    assert block, "the Try it section has no sh block"
    return [line.strip() for line in block[1].splitlines() if line.strip()]


@pytest.fixture(scope="module")
def bundle(corpus_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    tool = _bundle_tool()
    logs = [corpus_dir / entry["name"] for entry in tool.sample_entries()]
    missing = [path.name for path in logs if not path.is_file()]
    if missing:
        pytest.skip(f"corpus not fetched ({missing[0]}): run python tools/fetch-corpus.py")
    out = tmp_path_factory.mktemp("download") / "flashpoint-sample-1.zip"
    tool.build(logs, tool.ROBOT, out)
    return out


def _environment(home: Path) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("FLASHPOINT_")}
    env["HOME"] = str(home)
    env["PATH"] = os.pathsep.join([str(Path(sys.executable).parent), env.get("PATH", "")])
    # The owlet converter is cached outside the temp home, so the test needs no download.
    cache = os.environ.get("FLASHPOINT_CACHE", str(Path.home() / ".cache" / "flashpoint"))
    env["FLASHPOINT_CACHE"] = cache
    return env


def _run(lines: list[str], cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-e", "-c", "\n".join(lines)],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )


def test_sample_logs_reach_replay(bundle: Path, tmp_path: Path) -> None:
    commands = _sh_commands()
    download = [c for c in commands if "flashpoint-sample-1.zip" in c and c.startswith("curl ")]
    assert len(download) == 1, "the block downloads the bundle once, with curl"
    assert any("flashpoint serve" in c for c in commands)
    lines = [f"cp '{bundle}' ." if c in download else c for c in commands]
    lines = [c for c in lines if "flashpoint serve" not in c]
    home, work = tmp_path / "home", tmp_path / "work"
    home.mkdir()
    work.mkdir()

    done = _run(lines, work, _environment(home))
    output = done.stdout + done.stderr
    assert done.returncode == 0, output

    # Replay lists the sample match, with motor data.
    index_text = (
        work / "flashpoint-sample" / "lake" / "report" / "data" / "matches.js"
    ).read_text()
    index = json.loads(index_text[index_text.index("(") + 1 : index_text.rindex(")")])
    statuses = {m["key"]: m["status"] for m in index["matches"]}
    assert statuses.get(MATCH) == "ok", statuses
    match_text = (
        work / "flashpoint-sample" / "lake" / "report" / "data" / f"{MATCH}.js"
    ).read_text()
    match = json.loads(match_text[match_text.index("(") + 1 : match_text.rindex(")")])
    assert match["series"], "no motor tracks"

    # A full match (spec "Sample is a full match"): phases framed, no file warnings, and
    # detection finds something to triage in the tutorial.
    assert match["framed"], "no autonomous/teleop phases"
    assert "WARNING" not in output, output
    assert re.search(r"detected [1-9]\d* anomal", output), output
    assert f"`{NOTE}`" in _try_it_section()
    assert NOTE in output, output

    # The reader's own configuration and default lake are untouched.
    assert not (home / ".config" / "flashpoint").exists()
    assert not (home / "flashpoint-lake").exists()


def test_a_cut_short_download_fails_the_checksum(bundle: Path, tmp_path: Path) -> None:
    check = [c for c in _sh_commands() if "shasum" in c]
    assert len(check) == 1, "one SHA-256 check line"
    (tmp_path / "flashpoint-sample-1.zip").write_bytes(bundle.read_bytes()[:1_000_000])
    done = _run(check, tmp_path, _environment(tmp_path))
    assert done.returncode != 0
    assert "FAILED" in done.stdout + done.stderr
