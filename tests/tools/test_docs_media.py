"""Docs media placeholders (mvp-documentation design D-f; spec project-documentation,
"Generated screenshots and animations")."""

import importlib.util
import struct
import threading
import time
import zlib
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

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


# --- the manifest covers every view, and every entry can be captured (task 8.2) -------------


def test_fixture_bad_manifest_entries_are_named() -> None:
    tool = _tool()
    shots = tool.load_manifest(FIXTURE.parent / "bad-manifest.toml")
    assert tool.manifest_problems(shots, FIXTURE.parent / "views") == [
        "spin: kind 'movie' isn't one of ['animation', 'screenshot']",
        "spin: no alt text",
        "spin: steps 'twirl' isn't a step the script defines",
        "spin: needs a route (#view=...) and an expect selector",
        "view gauge: no screenshot entry with id 'gauge'",
    ]


def test_manifest_covers_every_view_and_every_entry_can_be_captured() -> None:
    tool = _tool()
    problems = tool.manifest_problems(tool.load_manifest())
    assert problems == [], "\n".join(problems)


# --- capture: corpus check and the sample lake's files (task 8.3) ---------------------------


def test_capture_without_the_corpus_exits_2_and_writes_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    tool = _tool()
    out, work = tmp_path / "media", tmp_path / "work"
    code = tool.main(
        ["capture", "--out", str(out), "--corpus-dir", str(tmp_path), "--work", str(work)]
    )
    err = capsys.readouterr().err
    first = tool.sample_entries(tool.corpus_manifest())[0]["name"]
    assert code == 2
    assert f"{tmp_path / first}" in err
    assert "python tools/fetch-corpus.py" in err
    assert not out.exists() and not work.exists()


def test_sample_lake_files_leave_out_the_damaged_copies() -> None:
    tool = _tool()
    names = [e["name"] for e in tool.sample_entries(tool.corpus_manifest())]
    assert names == [
        "FRC_20260409_215113_GACMP_Q7.wpilog",
        "GACMP_Q7_6E9415C3394C485320202050101C18FF_2026-04-09_21-51-06.hoot",
        "GACMP_Q7_rio_2026-04-09_21-51-06.hoot",
        "FRC_20260411_192626_GACMP_E10.wpilog",
        "GACMP_E10_6E9415C3394C485320202050101C18FF_2026-04-11_19-26-19.hoot",
        "GACMP_E10_6E9415C3394C485320202050101C18FF_2026-04-11_19-29-15.hoot",
        "GACMP_E10_rio_2026-04-11_19-26-19.hoot",
        "GACMP_E10_rio_2026-04-11_19-29-15.hoot",
        "FRC_20260409_171208_GACMP_P2.wpilog",
        "FRC_20250307_213719_GADAL_Q30.wpilog",
        "GACMP_Q19_D4F5AE23394C4853202020500E3418FF_2025-04-04_09-53-33.hoot",
        "GACMP_Q19_rio_2025-04-04_09-53-33.hoot",
    ]
    assert not [n for n in names if "truncated" in n or n.startswith("GADAL_Q11")]


# --- capture: one screenshot against a synthetic lake (task 8.4, browser marker) ------------


@pytest.fixture(scope="module")
def synthetic_site(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    from flashpoint.views.queries import HistoryQueries
    from flashpoint.web.server import FlashpointServer
    from tests.web.anomaly_lake import build_filter_lake

    lake, _ = build_filter_lake(tmp_path_factory.mktemp("media-lake"), replay=True)
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    server.queries.close()


REPLAY_SHOT = {
    "id": "replay",
    "kind": "screenshot",
    "route": "#view=replay",
    "steps": "none",
    "expect": "nav.tabs a[data-view='replay'][aria-current='page']",
}


@pytest.mark.browser
def test_capture_writes_one_screenshot_within_budget(
    browser: Any, synthetic_site: str, tmp_path: Path
) -> None:
    tool = _tool()
    page, watch = tool.open_page(browser)
    try:
        started = time.monotonic()
        shot = tool.shoot(page, watch, synthetic_site, REPLAY_SHOT, tmp_path)
        elapsed = time.monotonic() - started
        assert page.viewport_size == {"width": 1440, "height": 900}
        assert page.evaluate("window.devicePixelRatio") == 2
        assert page.evaluate("new Date().toISOString()").startswith("2026-04-12T12:00:00")
    finally:
        page.context.close()
    data = shot.read_bytes()
    assert shot.name == "replay.png"
    assert data.startswith(b"\x89PNG\r\n\x1a\n")
    assert struct.unpack(">II", data[16:24]) == (2880, 1800)  # 1440x900 at scale 2
    assert len(data) <= tool.SCREENSHOT_BUDGET_KB * 1024
    assert elapsed < 30


@pytest.mark.browser
def test_capture_fails_a_shot_with_a_console_error(
    browser: Any, synthetic_site: str, tmp_path: Path
) -> None:
    tool = _tool()
    page, watch = tool.open_page(browser)
    page.add_init_script("console.error('docs-media test: boom')")
    try:
        with pytest.raises(tool.ShotError, match=r"replay: console error: docs-media test: boom"):
            tool.shoot(page, watch, synthetic_site, REPLAY_SHOT, tmp_path)
    finally:
        page.context.close()
    assert not (tmp_path / "replay.png").exists()


@pytest.mark.browser
def test_capture_blocks_a_request_to_another_host(
    browser: Any, synthetic_site: str, tmp_path: Path
) -> None:
    tool = _tool()
    # The app's own CSP already refuses this; bypass it so the capture's route filter is tested.
    page, watch = tool.open_page(browser, bypass_csp=True)
    page.add_init_script("fetch('https://example.com/beacon').catch(() => {})")
    try:
        with pytest.raises(
            tool.ShotError, match=r"blocked a request to another host: https://example\.com/beacon"
        ):
            tool.shoot(page, watch, synthetic_site, REPLAY_SHOT, tmp_path)
    finally:
        page.context.close()
    assert watch.remote == ["https://example.com/beacon"]


# --- animations: one short flow gives a WebM and a GIF within budget (task 8.5) -------------


@pytest.mark.browser
def test_one_flow_gives_a_webm_and_a_gif_within_budget(
    browser: Any, synthetic_site: str, tmp_path: Path
) -> None:
    tool = _tool()
    flow = {**REPLAY_SHOT, "id": "flow", "kind": "animation"}
    webm, gif = tool.record(browser, synthetic_site, flow, tmp_path / "out", tmp_path / "video")
    assert (webm.name, gif.name) == ("flow.webm", "flow.gif")
    assert webm.read_bytes().startswith(bytes.fromhex("1a45dfa3"))
    assert gif.read_bytes().startswith(b"GIF89a")
    assert struct.unpack("<H", gif.read_bytes()[6:8])[0] == tool.GIF_WIDTH
    assert 0 < tool.duration_s(webm) <= tool.ANIMATION_MAX_S
    assert webm.stat().st_size <= tool.WEBM_BUDGET_KB * 1024
    assert gif.stat().st_size <= tool.GIF_BUDGET_KB * 1024

    joined = tool.reel([webm, webm], tmp_path / "reel.mp4")
    assert joined.stat().st_size > 0
    assert tool.duration_s(joined) == pytest.approx(2 * tool.duration_s(webm), abs=1.0)
