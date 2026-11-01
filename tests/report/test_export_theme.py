"""The static export carries the team's palette and name (team-branding "Static export")."""

import json
import shutil
from pathlib import Path

from flashpoint import config, theme
from flashpoint.report.build import ReportBuilder
from flashpoint.report.export import export_static
from flashpoint.web.branding import theme_css
from tests.report.lake import MatchLake, quiet_rows


def _built(tmp_path: Path) -> MatchLake:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows())
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    builder.build()
    builder.close()
    return lake


def _config(tmp_path: Path, theme_toml: str) -> Path:
    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)  # Ignite: team 6829
    (root / "theme.toml").write_text(theme_toml)
    return root


def _mode(out: Path) -> dict[str, object]:
    text = (out / "static" / "mode.js").read_text()
    return dict(json.loads(text.removeprefix("window.FP_MODE = ").removesuffix(";\n")))


def test_export_writes_theme_css_and_the_team(tmp_path: Path) -> None:
    cfg = _config(tmp_path, 'team_name = "Ignite Robotics"\npalette = "green"\n')
    out = tmp_path / "export"
    export_static(_built(tmp_path).lake, out, config_dir=cfg)
    assert (out / "theme.css").read_text() == theme_css(theme.preset_tokens("green"))
    assert 'href="theme.css"' in (out / "index.html").read_text()
    assert _mode(out) == {
        "static": True,
        "raw": True,
        "team": {"name": "Ignite Robotics", "number": 6829},
    }


def test_export_with_a_broken_theme_uses_the_defaults(tmp_path: Path) -> None:
    cfg = _config(tmp_path, 'team_name = "Ignite Robotics"\nnope = 1\n')
    out = tmp_path / "export"
    export_static(_built(tmp_path).lake, out, config_dir=cfg)
    assert (out / "theme.css").read_text() == ""
    assert _mode(out)["team"] is None


def test_re_export_replaces_theme_css(tmp_path: Path) -> None:
    lake = _built(tmp_path).lake
    out = tmp_path / "export"
    export_static(lake, out, config_dir=_config(tmp_path, 'palette = "green"\n'))
    (tmp_path / "cfg" / "theme.toml").write_text('palette = "blue"\n')
    export_static(lake, out, config_dir=tmp_path / "cfg")
    assert (out / "theme.css").read_text() == theme_css(theme.preset_tokens("blue"))
