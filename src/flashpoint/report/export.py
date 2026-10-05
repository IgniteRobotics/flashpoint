"""`flashpoint report --static OUT`: Replay as a folder that opens from file:// (no server).

OUT/index.html, OUT/static/ (the shell and Replay, without views that need the query API),
OUT/data/ (the built match data), and OUT/raw/ (each match's raw logs, hard-linked when on the
same filesystem, copied otherwise; left out with --no-raw).
"""

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import raw_path
from flashpoint.report.paths import data_dir

STATIC_DIR = Path(__file__).resolve().parents[1] / "web" / "static"
MARKER = ".flashpoint-export"
NEEDS_API = "data-needs-api"  # index.html marks the <script> of each view that needs the API


class ExportError(Exception):
    pass


@dataclass(frozen=True)
class ExportResult:
    matches: int
    raw_files: int
    raw_linked: int


def _payload(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    return json.loads(text[text.index("(") + 1 : text.rindex(")")])


def _prepare(out: Path) -> None:
    if out.exists() and not out.is_dir():
        raise ExportError(f"{out} exists and is not a folder")
    if out.is_dir() and any(out.iterdir()) and not (out / MARKER).is_file():
        raise ExportError(f"{out} is not empty and is not a previous Flashpoint export")
    for name in ("static", "data", "raw", "index.html"):
        target = out / name
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()
    out.mkdir(parents=True, exist_ok=True)
    (out / MARKER).write_text("A static Flashpoint Replay export; `flashpoint report --static`"
                              " replaces this folder's contents.\n")  # fmt: skip


def _static(out: Path, static_dir: Path, include_raw: bool) -> None:
    excluded: set[str] = set()
    lines = []
    for line in (static_dir / "index.html").read_text(encoding="utf-8").splitlines():
        if NEEDS_API in line:
            src = line.split('src="', 1)[1].split('"', 1)[0]
            excluded.add(src.removeprefix("static/"))
            continue
        lines.append(line)
    (out / "index.html").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def ignore(directory: str, names: list[str]) -> set[str]:
        relative = Path(directory).relative_to(static_dir)
        return {n for n in names if (relative / n).as_posix() in excluded or n == "__pycache__"}

    shutil.copytree(static_dir, out / "static", ignore=ignore)
    mode = {"static": True, "raw": include_raw}
    (out / "static" / "mode.js").write_text(
        f"window.FP_MODE = {json.dumps(mode)};\n", encoding="utf-8"
    )


def _link_or_copy(source: Path, target: Path) -> bool:
    """Hard link when possible (same filesystem); returns True if linked."""
    try:
        os.link(source, target)
        return True
    except OSError:
        shutil.copyfile(source, target)
        return False


def export_static(
    lake: LakePaths, out: Path, include_raw: bool = True, static_dir: Path = STATIC_DIR
) -> ExportResult:
    built = data_dir(lake)
    if not (built / "matches.js").is_file():
        raise ExportError(f"no Replay data in {lake.root}; run `flashpoint report` first")
    _prepare(out)
    _static(out, static_dir, include_raw)
    shutil.copytree(built, out / "data", ignore=shutil.ignore_patterns(".*"))
    index = _payload(built / "matches.js")
    raw_files = linked = 0
    if include_raw:
        (out / "raw").mkdir()
        for entry in index["matches"]:
            for source in _payload(built / Path(entry["file"]).name).get("sources", []):
                target = out / "raw" / source["download"]
                origin = raw_path(lake, source["sha256"], source["suffix"])
                if target.exists() or not origin.is_file():
                    continue
                linked += _link_or_copy(origin, target)
                raw_files += 1
    return ExportResult(len(index["matches"]), raw_files, linked)
