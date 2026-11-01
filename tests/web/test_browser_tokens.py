"""The role-named colour tokens keep today's amber look (spec: team-branding, "Default palette").

Every renamed token, glow and grid line must compute to exactly the colour the amber-named
token or hard-coded value computed to before the rename (design D-a).
"""

from pathlib import Path
from typing import Any

import pytest

from flashpoint.lake.paths import LakePaths
from tests.web.test_browser import _open

pytestmark = pytest.mark.browser
playwright = pytest.importorskip("playwright.sync_api")

# token -> (CSS property to probe it with, today's literal value)
TODAY = {
    "--bg": ("color", "#0A0806"),
    "--surface-1": ("color", "#0C0906"),
    "--surface-2": ("color", "#0D0A07"),
    "--surface-3": ("color", "#0F0B07"),
    "--surface-raised": ("color", "#15100A"),
    "--surface-drag": ("color", "#1A140B"),
    "--line-soft": ("color", "#241A0B"),
    "--line": ("color", "#3D2C10"),
    "--line-strong": ("color", "#7A5715"),
    "--accent": ("color", "#FFB000"),
    "--accent-hi": ("color", "#FFD27A"),
    "--accent-mid": ("color", "#E0A12A"),
    "--accent-dim": ("color", "#C98C1A"),
    "--accent-muted": ("color", "#A27A36"),
    "--prose": ("color", "#F2D49B"),
    "--warn": ("color", "#FF5A1F"),
    "--on-accent": ("color", "#0A0806"),
    "--glow-text": ("box-shadow", "0 0 10px rgba(255, 176, 0, 0.40)"),
    "--glow-select": ("box-shadow", "0 0 18px rgba(255, 176, 0, 0.45)"),
    "--glow-panel": ("box-shadow", "0 0 22px rgba(255, 176, 0, 0.15)"),
    "--glow-line": ("box-shadow", "0 0 6px  rgba(255, 176, 0, 0.60)"),
    "--grid": (
        "background-image",
        "linear-gradient(rgba(255,176,0,0.05) 1px, transparent 1px),"
        " linear-gradient(90deg, rgba(255,176,0,0.05) 1px, transparent 1px)",
    ),
    "--grid-line": ("color", "rgba(255,176,0,.05)"),
    **{
        f"--glow-{a:02d}": ("color", f"rgba(255,176,0,{a / 100})")
        for a in (3, 5, 8, 9, 14, 15, 18, 20, 25, 30, 35, 40, 45, 60, 80)
    },
}

# FP.tokens() key -> today's value (read from the CSS as written)
TODAY_KEYS = {
    "accent": "#FFB000", "accentHi": "#FFD27A", "accentMid": "#E0A12A", "accentDim": "#C98C1A",
    "accentMuted": "#A27A36", "warn": "#FF5A1F", "accentRgb": "255, 176, 0",
    "line": "#3D2C10", "lineSoft": "#241A0B", "lineStrong": "#7A5715", "surface1": "#0C0906",
}  # fmt: skip

PROBE = """(pairs) => pairs.map(([name, prop, literal]) => {
  const probe = (value) => {
    const el = document.createElement('div');
    el.style.setProperty(prop, value);
    document.body.appendChild(el);
    const out = getComputedStyle(el).getPropertyValue(prop);
    el.remove();
    return out;
  };
  const declared = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return [name, declared, probe('var(' + name + ')'), probe(literal)];
})"""


def test_role_tokens_compute_to_todays_colours(page: Any, tmp_path: Path, serve: Any) -> None:
    running = serve(LakePaths(tmp_path / "lake"))
    watch = _open(page, running.base + "/")
    pairs = [[name, prop, literal] for name, (prop, literal) in TODAY.items()]
    for name, declared, token, literal in page.evaluate(PROBE, pairs):
        assert declared, f"{name} is not defined"
        assert token == literal, f"{name}: {token!r} != today's {literal!r}"
    keys = page.evaluate("() => FP.tokens()")
    assert {k: keys.get(k) for k in TODAY_KEYS} == TODAY_KEYS
    assert watch.clean(), watch
