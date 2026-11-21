#!/usr/bin/env python3
"""Docs screenshots and animations from `docs/media-manifest.toml` (mvp-documentation, D-f).

    python tools/docs-media.py placeholders [--out DIR]
    python tools/docs-media.py check-site SITE_DIR
    python tools/docs-media.py capture [--out DIR] [--corpus-dir DIR] [--work DIR]

`placeholders` writes a tiny valid file for every manifest entry (a PNG per screenshot, a WebM
and a GIF per animation) so the docs site builds without the corpus. `check-site` fails when a
built page shows an image or video whose file isn't in the built site: Zensical's strict build
doesn't check media (ADR-0019). `capture` makes the real images: it checks corpus-v1 (exit 2
naming the first missing file), builds a sample lake from five corpus groups with the real CLI,
and shoots each entry. Only `capture` needs Flashpoint and the docs group installed.
"""

import argparse
import importlib.util
import os
import re
import shutil
import struct
import subprocess
import sys
import time
import tomllib
import zlib
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import unquote, urlsplit

REPO = Path(__file__).resolve().parents[1]
MANIFEST = REPO / "docs" / "media-manifest.toml"
MEDIA = REPO / "docs" / "guide" / "assets" / "media"
SUFFIXES = {"screenshot": (".png",), "animation": (".webm", ".gif")}
OUT = REPO / "build" / "docs-media"
CORPUS_MANIFEST = REPO / "tests" / "corpus" / "manifest.toml"
FETCH_CORPUS = REPO / "tools" / "fetch-corpus.py"
# The corpus groups the sample lake is built from (design D-f). The empty hoot
# (2026-gadal-q11-empty) and the 60 % copy of E10's rio hoot (2026-gacmp-e10-truncated) stay
# out: the copy would add a second, partial rio hoot to E10. E10's own power-off hoots give the
# real INCOMPLETE badge.
SAMPLE_GROUPS = (
    "2026-gacmp-q7",
    "2026-gacmp-e10",
    "2026-gacmp-p2",
    "2025-gacmp-q19",
    "2025-gadal-q30",
)
DOCS_THEME = 'team_name = "Ignite Robotics"\nteam_number = 6829\n'
EXIT_NO_CORPUS = 2

# A 1x1 transparent GIF89a.
GIF = bytes.fromhex(
    "474946383961010001008000000000000000ff21f90401000000002c00000000010001000002024401003b"
)


VIEWS = REPO / "src" / "flashpoint" / "web" / "static" / "views"

# The named steps a manifest entry can run before its shot, each a short browser flow. `capture`
# runs them; a manifest entry naming one that isn't here is refused.
STEPS: dict[str, str] = {
    "none": "open the route and wait for `expect`",
    "replay-first-match": "open the first match in Replay's list",
    "replay-hottest-motor": "open the first match and select its hottest motor in the readout",
    "history-pick-motor": "pick the hottest motor's line in History",
    "anomaly-open-first": "open the first anomaly in the queue",
    "anomaly-triage": "open the first anomaly and choose INVESTIGATE",
    "live-can-shake": "wait for the fake robot's shake scenario to show dropouts",
}


def manifest_problems(shots: list[dict[str, str]], views: Path = VIEWS) -> list[str]:
    """Entries that capture can't make, and views with no screenshot, by entry id or view."""
    problems = []
    for shot in shots:
        name = shot.get("id", "(no id)")
        if shot.get("kind") not in SUFFIXES:
            problems.append(f"{name}: kind {shot.get('kind')!r} isn't one of {sorted(SUFFIXES)}")
        if not str(shot.get("alt", "")).strip():
            problems.append(f"{name}: no alt text")
        if shot.get("steps") not in STEPS:
            problems.append(f"{name}: steps {shot.get('steps')!r} isn't a step the script defines")
        if not str(shot.get("route", "")).startswith("#") or not shot.get("expect"):
            problems.append(f"{name}: needs a route (#view=...) and an expect selector")
    screenshots = {s.get("id") for s in shots if s.get("kind") == "screenshot"}
    problems += [
        f"view {view.stem}: no screenshot entry with id {view.stem!r}"
        for view in sorted(views.glob("*.js"))
        if view.stem not in screenshots
    ]
    return problems


def load_manifest(path: Path = MANIFEST) -> list[dict[str, str]]:
    shots: list[dict[str, str]] = tomllib.loads(path.read_text(encoding="utf-8"))["shot"]
    return shots


def file_names(shots: list[dict[str, str]]) -> list[str]:
    """Every file the manifest's entries produce, in manifest order."""
    return [shot["id"] + suffix for shot in shots for suffix in SUFFIXES[shot["kind"]]]


def png() -> bytes:
    """A 1x1 transparent PNG."""

    def chunk(kind: bytes, data: bytes) -> bytes:
        crc = struct.pack(">I", zlib.crc32(kind + data))
        return struct.pack(">I", len(data)) + kind + data + crc

    header = struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)  # 1x1, 8-bit RGBA
    pixels = zlib.compress(b"\x00\x00\x00\x00\x00")  # filter byte + one transparent pixel
    chunks = chunk(b"IHDR", header) + chunk(b"IDAT", pixels) + chunk(b"IEND", b"")
    return b"\x89PNG\r\n\x1a\n" + chunks


# One black 16x16 VP8 frame (made once with `ffmpeg -f lavfi -i color=c=black:s=16x16 -frames:v 1
# -c:v libvpx`); browsers play it, unlike a WebM with no frames.
WEBM = bytes.fromhex(
    "1a45dfa39f4286810142f7810142f2810442f381084282847765626d42878102428581021853806701000000"
    "000001ab114d9b74ba4dbb8b53ab841549a96653ac81a14dbb8b53ab841654ae6b53ac81c64dbb8c53ab8412"
    "54c36753ac8201134dbb8c53ab841c53bb6b53ac820195ec0100000000000059000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000000000000000001549a966a02ad7b1830f42"
    "404d80844c6176665741844c61766644898840440000000000001654ae6bc8ae010000000000003fd7810173"
    "c58800000000000000019c810022b59c83756e648881008685565f56503883810123e3838402625a00e090b0"
    "8110ba81109a810255b08455b981011254c367d07373cd63c08b63c588000000000000000167c89845a38745"
    "4e434f44455244878b4c617663206c696276707867c8a145a3884455524154494f4e44879330303a30303a30"
    "302e303430303030303030001f43b675a8e78100a3a3810000801002009d012a100010000047088585888584"
    "880202000c0d6000feffab50801c53bb6b91bb8fb38100b78af78101f1820168f08103"
)


MEDIA_SRC = re.compile(r"<(?:img|video|source)\b[^>]*?\ssrc=[\"']([^\"']+)[\"']", re.IGNORECASE)


def missing_media(site: Path) -> list[str]:
    """Every local image or video a built page shows whose file isn't in the site."""
    problems = []
    for page in sorted(site.rglob("*.html")):
        for source in MEDIA_SRC.findall(page.read_text(encoding="utf-8")):
            parts = urlsplit(source)
            if parts.scheme or parts.netloc or source.startswith("data:"):
                continue
            base = site if parts.path.startswith("/") else page.parent
            if not (base / unquote(parts.path).lstrip("/")).is_file():
                problems.append(f"{page.relative_to(site).as_posix()}: missing {source}")
    return problems


# --- capture: the corpus and the sample lake (task 8.3) --------------------------------------


def _fetch_corpus() -> ModuleType:
    spec = importlib.util.spec_from_file_location("fetch_corpus", FETCH_CORPUS)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def corpus_manifest(path: Path = CORPUS_MANIFEST) -> dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def default_corpus_dir(manifest: dict[str, Any]) -> Path:
    root = os.environ.get("FLASHPOINT_CORPUS_DIR")
    base = Path(root) if root else Path.home() / ".cache" / "flashpoint" / "corpus"
    return base / str(manifest["version"])


def sample_entries(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """The corpus files the sample lake is built from, in manifest order."""
    return [e for e in manifest["file"] if e["group"] in SAMPLE_GROUPS]


def first_missing(corpus: Path, entries: list[dict[str, Any]]) -> str | None:
    """The first sample file that's absent or doesn't match the corpus manifest."""
    check = _fetch_corpus().is_valid
    return next((e["name"] for e in entries if not check(corpus / e["name"], e)), None)


def build_sample_lake(
    work: Path, corpus: Path, entries: list[dict[str, Any]], theme: str, ok: tuple[int, ...] = (0,)
) -> Path:
    """A lake of the sample matches, built with the real CLI; returns the lake folder.

    The configuration is a copy of the repository's `config/` (Ignite's robots, which these
    logs come from) plus `theme.toml`, so nobody's own configuration is read or written.
    """
    config = work / "config"
    lake = work / "lake"
    shutil.rmtree(work, ignore_errors=True)
    shutil.copytree(REPO / "config", config)
    (config / "theme.toml").write_text(theme, encoding="utf-8")
    env = {**os.environ, "FLASHPOINT_CONFIG": str(config), "FLASHPOINT_LAKE": str(lake)}
    cli = [sys.executable, "-m", "flashpoint"]
    files = [str(corpus / e["name"]) for e in entries]
    for args in (["ingest", "--lake", str(lake), *files], ["report", "--lake", str(lake)]):
        started = time.monotonic()
        done = subprocess.run([*cli, *args], env=env, check=False, capture_output=True, text=True)  # noqa: S603 - our own CLI
        print(
            f"{args[0]}: exit {done.returncode} in {time.monotonic() - started:.0f} s", flush=True
        )
        if done.returncode not in ok:
            sys.stderr.write(done.stdout + done.stderr)
            raise SystemExit(f"sample lake: `flashpoint {args[0]}` failed")
    return lake


# --- capture: screenshots (task 8.4) --------------------------------------------------------

VIEWPORT = {"width": 1440, "height": 900}
SCALE = 2
CLOCK = "2026-04-12T12:00:00Z"
SCREENSHOT_BUDGET_KB = 500
WAIT_MS = 15_000
LIVE_WAIT_MS = 60_000  # the shake scenario takes a few updates to drop a device
DROPOUT = ".live-event[data-event='dropout']"
LIVE_SEED = 6829  # the same dropouts every capture
LIVE_PERIOD_S = 0.5
# Always masked: the header's lake path names a folder on the capturing machine.
ALWAYS_MASK = ("#lake-path",)
MASK_COLOR = "#0A0806"  # the default amber palette's background, so a mask reads as blank


class ShotError(Exception):
    """A shot that can't be taken as specified: named, so the run says which entry failed."""


def _first_match(page: Any) -> None:
    page.locator(".match-btn").filter(has_not_text="NO ALIGNED SAMPLES").first.click()
    page.wait_for_selector(".match-btn[aria-current='page']", timeout=WAIT_MS)


def _open_first_anomaly(page: Any) -> None:
    page.locator("#queue button.anom").first.click()
    page.wait_for_selector("#causes", timeout=WAIT_MS)


def _triage(page: Any) -> None:
    """INVESTIGATE the first anomaly still new: an earlier entry may have triaged the first."""
    page.wait_for_selector("#queue button.anom", timeout=WAIT_MS)
    investigate = page.get_by_role("button", name="INVESTIGATE")
    for item in page.locator("#queue button.anom").all():
        item.click()
        page.wait_for_selector("#causes", timeout=WAIT_MS)
        if investigate.count():
            break
    else:
        raise ShotError("no anomaly in the queue is still new, so none can be triaged")
    investigate.click()
    save = page.locator("#triage-save")
    if save.count():
        save.click()
    page.get_by_role("button", name="MARK RESOLVED").wait_for(timeout=WAIT_MS)


def _nothing(_page: Any) -> None:
    return None


def _hottest_motor(page: Any) -> None:
    _first_match(page)  # with no cursor placed, the readout lists match maxima, hottest first
    page.wait_for_selector("#readout tr", timeout=WAIT_MS)


def _pick_motor(page: Any) -> None:
    page.locator("#rows tr button").first.click()


STEP_FUNCTIONS: dict[str, Any] = {
    "none": _nothing,
    "replay-first-match": _first_match,
    "replay-hottest-motor": _hottest_motor,
    "history-pick-motor": _pick_motor,
    "anomaly-open-first": _open_first_anomaly,
    "anomaly-triage": _triage,
    "live-can-shake": lambda page: page.wait_for_selector(DROPOUT, timeout=LIVE_WAIT_MS),
}


class Watch:
    """Console errors, page errors and requests to anywhere but this laptop, during one shot."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.remote: list[str] = []

    def problems(self, name: str) -> list[str]:
        return [f"{name}: console error: {e}" for e in self.errors] + [
            f"{name}: blocked a request to another host: {url}" for url in self.remote
        ]


def open_page(browser: Any, **context: Any) -> tuple[Any, Watch]:
    """A page with the docs' fixed context (design D-f), and its watch."""
    fixed = {"viewport": VIEWPORT, "device_scale_factor": SCALE, "color_scheme": "dark"}
    ctx = browser.new_context(**{**fixed, **context})
    page = ctx.new_page()
    page.clock.set_fixed_time(CLOCK)
    watch = Watch()

    def route(r: Any) -> None:
        url = urlsplit(r.request.url)
        if url.scheme in ("file", "data") or url.hostname in ("127.0.0.1", "localhost"):
            r.continue_()
        else:
            watch.remote.append(r.request.url)
            r.abort()

    page.on("console", lambda m: watch.errors.append(m.text) if m.type == "error" else None)
    page.on("pageerror", lambda e: watch.errors.append(str(e)))
    page.route("**/*", route)
    return page, watch


def encode_png(raw: bytes, budget_kb: int) -> bytes:
    """Optimise a screenshot; over budget, quantise it to 256 colours; still over, refuse."""
    import io

    from PIL import Image

    image = Image.open(io.BytesIO(raw))
    for candidate in (image, image.convert("RGB").quantize(256)):
        buffer = io.BytesIO()
        candidate.save(buffer, format="PNG", optimize=True)
        if buffer.tell() <= budget_kb * 1024:
            return buffer.getvalue()
    raise ShotError(f"over {budget_kb} KB even at 256 colours ({buffer.tell() // 1024} KB)")


def shoot(page: Any, watch: Watch, base_url: str, shot: dict[str, Any], out: Path) -> Path:
    """Take one screenshot entry: open its route, run its steps, wait for `expect`, save it."""
    name = shot["id"]
    page.goto(f"{base_url}/{shot['route']}")
    try:
        STEP_FUNCTIONS[shot["steps"]](page)
        page.wait_for_selector(shot["expect"], state="visible", timeout=WAIT_MS)
    except Exception as exc:  # Playwright raises its own error types; each names the shot
        raise ShotError(f"{name}: {shot['expect']!r} never showed ({exc})") from exc
    page.wait_for_load_state("networkidle")
    problems = watch.problems(name)
    if problems:
        raise ShotError("; ".join(problems))
    masks = [page.locator(s) for s in (*ALWAYS_MASK, *shot.get("mask", []))]
    raw = page.screenshot(full_page=False, mask=masks, mask_color=MASK_COLOR, animations="disabled")
    try:
        data = encode_png(raw, int(shot.get("budget_kb", SCREENSHOT_BUDGET_KB)))
    except ShotError as exc:
        raise ShotError(f"{name}: {exc}") from exc
    out.mkdir(parents=True, exist_ok=True)
    target = out / f"{name}.png"
    target.write_bytes(data)
    return target


# --- capture: animations and the reel (task 8.5) --------------------------------------------

VIDEO_SIZE = {"width": 1280, "height": 800}
ANIMATION_MAX_S = 20.0
GIF_BUDGET_KB = 3 * 1024
WEBM_BUDGET_KB = 2 * 1024
GIF_FPS = 12
GIF_WIDTH = 960
GIF_SMALLER = (10, 800)  # fps and width for a GIF that's over budget at the defaults
REEL_MAX_S = 60.0
HOLD_MS = 1500  # keep the end state on screen this long before the recording stops
DURATION = re.compile(r"Duration: (\d+):(\d+):(\d+(?:\.\d+)?)")


def ffmpeg() -> str:
    """The ffmpeg that imageio-ffmpeg bundles (docs group), so no system install is needed."""
    import imageio_ffmpeg  # type: ignore[import-untyped]  # no type hints shipped

    return str(imageio_ffmpeg.get_ffmpeg_exe())


def _ffmpeg(*args: str) -> subprocess.CompletedProcess[str]:
    command = [ffmpeg(), "-hide_banner", "-y", *args]
    return subprocess.run(command, check=False, capture_output=True, text=True)  # noqa: S603 - the bundled ffmpeg


def duration_s(video: Path) -> float:
    """A video's length, from ffmpeg's own report of the file."""
    found = DURATION.search(_ffmpeg("-i", str(video)).stderr)
    if found is None:
        raise ShotError(f"{video.name}: ffmpeg reports no duration")
    hours, minutes, seconds = found.groups()
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def to_gif(webm: Path, gif: Path, fps: int = GIF_FPS, width: int = GIF_WIDTH) -> Path:
    """WebM to GIF: two-pass palette, 12 fps, 960 px wide (the README and Chief Delphi copy)."""
    palette = gif.with_suffix(".palette.png")
    scale = f"fps={fps},scale={width}:-1:flags=lanczos"
    steps = (
        ("-i", str(webm), "-vf", f"{scale},palettegen=stats_mode=diff", str(palette)),
        (
            "-i",
            str(webm),
            "-i",
            str(palette),
            "-lavfi",
            f"{scale}[x];[x][1:v]paletteuse=dither=bayer",
            str(gif),
        ),
    )
    try:
        for step in steps:
            done = _ffmpeg(*step)
            if done.returncode != 0:
                raise ShotError(
                    f"{gif.name}: ffmpeg failed: {done.stderr.strip().splitlines()[-1:]}"
                )
    finally:
        palette.unlink(missing_ok=True)
    return gif


def check_animation(name: str, webm: Path, gif: Path) -> None:
    length = duration_s(webm)
    if length > ANIMATION_MAX_S:
        raise ShotError(f"{name}: {length:.1f} s, over {ANIMATION_MAX_S:.0f} s")
    for path, budget in ((webm, WEBM_BUDGET_KB), (gif, GIF_BUDGET_KB)):
        size_kb = path.stat().st_size // 1024
        if size_kb > budget:
            raise ShotError(f"{name}: {path.name} is {size_kb} KB, over {budget} KB")


def record(
    browser: Any, base_url: str, shot: dict[str, Any], out: Path, scratch: Path
) -> list[Path]:
    """Record one animation entry: WebM from Playwright, then a GIF from it."""
    name = shot["id"]
    videos = scratch / name
    page, watch = open_page(
        browser, record_video_dir=str(videos), record_video_size=VIDEO_SIZE, viewport=VIDEO_SIZE
    )
    try:
        page.goto(f"{base_url}/{shot['route']}")
        STEP_FUNCTIONS[shot["steps"]](page)
        page.wait_for_selector(shot["expect"], state="visible", timeout=LIVE_WAIT_MS)
        page.wait_for_timeout(HOLD_MS)
    except Exception as exc:  # Playwright raises its own error types; each names the shot
        page.context.close()
        raise ShotError(f"{name}: {shot['expect']!r} never showed ({exc})") from exc
    video = page.video
    page.context.close()  # finishes the video file
    problems = watch.problems(name)
    if problems:
        raise ShotError("; ".join(problems))
    out.mkdir(parents=True, exist_ok=True)
    webm = out / f"{name}.webm"
    shutil.move(video.path(), webm)
    gif = to_gif(webm, out / f"{name}.gif")
    if gif.stat().st_size > GIF_BUDGET_KB * 1024:  # a busy screen: one smaller try, as PNGs get
        gif = to_gif(webm, gif, *GIF_SMALLER)
    check_animation(name, webm, gif)
    return [webm, gif]


def reel(videos: list[Path], target: Path) -> Path:
    """Join the flow recordings into one video for the announcement (#81), 60 s at most."""
    total = sum(duration_s(v) for v in videos)
    if total > REEL_MAX_S:
        raise ShotError(
            f"{target.name}: the flows add up to {total:.1f} s, over {REEL_MAX_S:.0f} s"
        )
    listing = target.with_suffix(".txt")
    listing.write_text(
        "".join(f"file '{v.resolve().as_posix()}'\n" for v in videos), encoding="utf-8"
    )
    try:
        done = _ffmpeg(
            "-f", "concat", "-safe", "0", "-i", str(listing), "-c:v", "libx264",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(target),
        )  # fmt: skip
    finally:
        listing.unlink(missing_ok=True)
    if done.returncode != 0:
        raise ShotError(f"{target.name}: ffmpeg failed: {done.stderr.strip().splitlines()[-1:]}")
    return target


def take_live(shots: list[dict[str, Any]], config: Path, out: Path, scratch: Path) -> list[str]:
    """Live CAN entries: the fake robot's shake scenario on loopback, read by an in-process
    server, as `serve --robot-host 127.0.0.1` would (design D-f)."""
    import socket
    import threading

    from playwright.sync_api import sync_playwright

    from flashpoint.lake.paths import LakePaths
    from flashpoint.live.fake_robot import run_scenario
    from flashpoint.live.service import LiveCanService
    from flashpoint.live.slots import load_resolver
    from flashpoint.views.queries import HistoryQueries
    from flashpoint.web.server import FlashpointServer

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    stop = threading.Event()
    robot = threading.Thread(
        target=run_scenario, args=("shake", port, LIVE_PERIOD_S, stop, LIVE_SEED), daemon=True
    )
    robot.start()
    live = LiveCanService(["127.0.0.1"], port=port, resolver=load_resolver(config))
    lake = LakePaths(scratch / "live-lake")
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake), live=live)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    failed: list[str] = []
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            for shot in shots:
                try:
                    made = _take(browser, base, shot, out, scratch)
                    print("made " + ", ".join(p.name for p in made), flush=True)
                except ShotError as exc:
                    failed.append(str(exc))
            browser.close()
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
        server.queries.close()
        robot.join(timeout=5)
    return failed


def _take(browser: Any, base: str, shot: dict[str, Any], out: Path, scratch: Path) -> list[Path]:
    if shot["kind"] == "animation":
        return record(browser, base, shot, out, scratch)
    page, watch = open_page(browser)
    try:
        return [shoot(page, watch, base, shot, out)]
    finally:
        page.context.close()


def capture(out: Path, corpus: Path | None, work: Path) -> int:
    manifest = corpus_manifest()
    corpus = corpus or default_corpus_dir(manifest)
    entries = sample_entries(manifest)
    missing = first_missing(corpus, entries)
    if missing is not None:
        print(
            f"corpus file missing or damaged: {corpus / missing}\n"
            "fetch the corpus first: python tools/fetch-corpus.py",
            file=sys.stderr,
        )
        return EXIT_NO_CORPUS
    lake = build_sample_lake(work / "sample", corpus, entries, DOCS_THEME)
    empty = [e for e in manifest["file"] if e["group"] == "2026-gadal-q11-empty"]
    quarantine = build_sample_lake(
        work / "quarantine", corpus, empty, DOCS_THEME, ok=(0, 1)
    )  # 1: quarantined
    # Triage is permanent (the ledger only appends), so a second triage entry gets its own copy.
    triage = work / "triage" / "lake"
    shutil.rmtree(triage.parent, ignore_errors=True)
    shutil.copytree(lake, triage)
    lakes = {"sample": lake, "quarantine": quarantine, "triage": triage}
    shots = load_manifest()
    failed = take_media(lakes, [s for s in shots if s["view"] != "live"], out, work / "video")
    failed += take_live(
        [s for s in shots if s["view"] == "live"], work / "sample" / "config", out, work / "video"
    )
    for problem in failed:
        print(problem, file=sys.stderr)
    print(f"{len(shots) - len(failed)} of {len(shots)} entries in {out}")
    return 1 if failed else 0


def take_media(
    lakes: dict[str, Path], shots: list[dict[str, Any]], out: Path, scratch: Path
) -> list[str]:
    """Serve each lake on loopback and shoot or record its entries; returns the failures."""
    import threading

    from playwright.sync_api import sync_playwright

    from flashpoint.lake.paths import LakePaths
    from flashpoint.views.queries import HistoryQueries
    from flashpoint.web.server import FlashpointServer

    failed: list[str] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        for lake_name, root in lakes.items():
            lake = LakePaths(root)
            server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake))
            threading.Thread(target=server.serve_forever, daemon=True).start()
            base = f"http://127.0.0.1:{server.server_address[1]}"
            try:
                for shot in (s for s in shots if s.get("lake", "sample") == lake_name):
                    try:
                        made = _take(browser, base, shot, out, scratch)
                        print("made " + ", ".join(p.name for p in made), flush=True)
                    except ShotError as exc:
                        failed.append(str(exc))
            finally:
                server.shutdown()
                server.server_close()
                server.queries.close()
        browser.close()
    return failed


def content(suffix: str) -> bytes:
    """The placeholder bytes for one media file type."""
    return {".png": png(), ".gif": GIF, ".webm": WEBM}[suffix]


def placeholders(out: Path, manifest: Path = MANIFEST) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name in file_names(load_manifest(manifest)):
        target = out / name
        target.write_bytes(content(target.suffix))
        written.append(target)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    holders = commands.add_parser("placeholders", help="write a tiny stand-in for every entry")
    holders.add_argument("--out", type=Path, default=MEDIA, help=f"default: {MEDIA}")
    check = commands.add_parser("check-site", help="fail on media a built page shows but lacks")
    check.add_argument("site", type=Path, help="the built site folder")
    shoot = commands.add_parser("capture", help="build the sample lake and capture every entry")
    shoot.add_argument("--out", type=Path, default=OUT, help=f"default: {OUT}")
    shoot.add_argument("--corpus-dir", type=Path, help="default: the fetch script's corpus folder")
    shoot.add_argument("--work", type=Path, default=REPO / "build" / "docs-media-lake",
                       help="where the sample lake is built")  # fmt: skip
    joined = commands.add_parser("reel", help="join the captured flows into one video (#81)")
    joined.add_argument("--out", type=Path, default=OUT, help=f"the capture folder; default: {OUT}")
    args = parser.parse_args(argv)
    if args.command == "reel":
        flows = [args.out / f"{s['id']}.webm" for s in load_manifest() if s["kind"] == "animation"]
        missing = [f.name for f in flows if not f.is_file()]
        if missing:
            print(f"not captured yet: {', '.join(missing)}", file=sys.stderr)
            return 1
        print(reel(flows, args.out / "reel.mp4"))
        return 0
    if args.command == "capture":
        return capture(args.out, args.corpus_dir, args.work)
    if args.command == "check-site":
        problems = missing_media(args.site)
        for problem in problems:
            print(problem, file=sys.stderr)
        print(f"{len(problems)} missing media files in {args.site}")
        return 1 if problems else 0
    written = placeholders(args.out)
    print(f"{len(written)} placeholders in {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
