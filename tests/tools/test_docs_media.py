"""Docs media placeholders (mvp-documentation design D-f; spec project-documentation,
"Generated screenshots and animations")."""

import importlib.util
import struct
import zlib
from pathlib import Path
from types import ModuleType

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "docs-media.py"
FIXTURE = REPO / "tests" / "fixtures" / "docs-media" / "manifest.toml"


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_media", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_placeholders_match_the_manifest_file_names(tmp_path: Path) -> None:
    tool = _tool()
    tool.placeholders(tmp_path, FIXTURE)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["flow.gif", "flow.webm", "one.png"]
    assert tool.file_names(tool.load_manifest(FIXTURE)) == ["one.png", "flow.webm", "flow.gif"]


def test_placeholders_for_the_real_manifest(tmp_path: Path) -> None:
    tool = _tool()
    written = tool.placeholders(tmp_path)
    assert [p.name for p in written] == tool.file_names(tool.load_manifest())
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(p.name for p in written)


def test_placeholders_are_valid_files(tmp_path: Path) -> None:
    tool = _tool()
    tool.placeholders(tmp_path, FIXTURE)
    image = (tmp_path / "one.png").read_bytes()
    assert image.startswith(b"\x89PNG\r\n\x1a\n")
    length, kind = struct.unpack(">I4s", image[8:16])
    header = image[16 : 16 + length]
    assert kind == b"IHDR" and struct.unpack(">II", header[:8]) == (1, 1)
    assert image[16 + length : 20 + length] == struct.pack(">I", zlib.crc32(kind + header))
    assert (tmp_path / "flow.gif").read_bytes().startswith(b"GIF89a")
    video = (tmp_path / "flow.webm").read_bytes()
    assert video.startswith(bytes.fromhex("1a45dfa3")) and b"webm" in video and b"V_VP8" in video
    assert len(video) < 1024


def test_every_manifest_entry_has_alt_text_and_a_unique_id() -> None:
    shots = _tool().load_manifest()
    assert [s["id"] for s in shots if not s.get("alt", "").strip()] == []
    ids = [s["id"] for s in shots]
    assert len(ids) == len(set(ids))


def test_check_site_names_missing_media(tmp_path: Path) -> None:
    tool = _tool()
    (tmp_path / "assets" / "media").mkdir(parents=True)
    (tmp_path / "assets" / "media" / "here.png").write_bytes(b"x")
    (tmp_path / "views").mkdir()
    (tmp_path / "views" / "index.html").write_text(
        '<img alt="a" src="../assets/media/here.png">'
        '<img src="../assets/media/gone.png" alt="b">'
        '<video controls><source src="../assets/media/flow.webm" type="video/webm"></video>'
        '<img src="https://example.com/x.png"><img src="data:image/png;base64,AA==">',
        encoding="utf-8",
    )
    assert tool.missing_media(tmp_path) == [
        "views/index.html: missing ../assets/media/gone.png",
        "views/index.html: missing ../assets/media/flow.webm",
    ]
    assert tool.main(["check-site", str(tmp_path)]) == 1
    (tmp_path / "views" / "index.html").write_text('<img src="../assets/media/here.png">', "utf-8")
    assert tool.main(["check-site", str(tmp_path)]) == 0
