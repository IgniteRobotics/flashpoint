import hashlib
import shutil
import subprocess
import tomllib
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.readers import hoot
from flashpoint.setup.checks import CheckContext, run_checks
from flashpoint.setup.fixes import FIXES, FixError

OWLET = b"#!/bin/sh\necho fake owlet\n"


class Commands:
    """Records commands; answers like a machine where they all succeed."""

    def __init__(self, fail: str | None = None) -> None:
        self.calls: list[list[str]] = []
        self.fail = fail

    def __call__(self, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(args))
        code = 1 if self.fail and self.fail in " ".join(args) else 0
        return subprocess.CompletedProcess(list(args), code, "", "boom" if code else "")


def fake_registry(tmp_path: Path) -> Any:
    """A manifest whose one owlet build is a local file, at a file:// URL."""
    source = tmp_path / "owlet-src"
    source.write_bytes(OWLET)
    manifest = tmp_path / "manifest.toml"
    manifest.write_text(
        "[[owlet]]\ncompliancy = 19\nversion = '26.0.0-fake'\n"
        f"[owlet.platforms.{hoot.platform_key()}]\n"
        f"url = '{source.as_uri()}'\nsha256 = '{hashlib.sha256(OWLET).hexdigest()}'\n"
    )
    return lambda cache_root: hoot.OwletRegistry(manifest, cache_root / "owlet")


@pytest.fixture
def ctx(tmp_path: Path) -> CheckContext:
    return CheckContext(
        config_root=tmp_path / "cfg",
        lake=LakePaths(tmp_path / "lake"),
        cache_root=tmp_path / "cache",
        system="Linux",
        season=2026,
        run=Commands(),
        find_advantagescope=lambda root: (None, "not found"),
        registry=fake_registry(tmp_path),
        home=tmp_path / "home",
        which=lambda name: f"/opt/pipx/bin/{name}",
    )


def _check(ctx: CheckContext, check_id: str) -> Any:
    return next(r for r in run_checks(ctx) if r.id == check_id)


def test_allow_list() -> None:
    assert set(FIXES) == {"init", "owlet", "advantagescope", "service-install", "service-start"}


def test_init_twice(ctx: CheckContext, tmp_path: Path) -> None:
    assert _check(ctx, "config-root").status == "problem"
    first = FIXES["init"](ctx, {"team": 254, "lake": str(tmp_path / "frc-lake")})
    assert first["ok"] and "acquire.toml" in first["changed"]
    assert _check(ctx, "config-root").status == "ok"
    second = FIXES["init"](ctx, {"team": 1678})
    assert second["ok"] and second["changed"] == [] and "already" in second["message"]
    assert tomllib.loads((ctx.config_root / "acquire.toml").read_text())["team"] == 254


@pytest.mark.parametrize("body", [{}, {"team": 0}, {"team": "abc"}, None, [254]])
def test_init_refuses_a_bad_team(ctx: CheckContext, body: Any) -> None:
    with pytest.raises(FixError, match="team|JSON object"):
        FIXES["init"](ctx, body)
    assert not ctx.config_root.exists()


def test_download_owlet_from_the_view(ctx: CheckContext) -> None:
    assert _check(ctx, "owlet:C19").status == "warning"
    first = FIXES["owlet"](ctx, {})
    assert first["ok"] and first["changed"] == ["owlet 26.0.0-fake (C19)"]
    assert _check(ctx, "owlet:C19").status == "ok"
    second = FIXES["owlet"](ctx, {})
    assert second["ok"] and second["changed"] == []


def test_owlet_replaces_a_corrupt_download(ctx: CheckContext) -> None:
    registry = ctx.registry(ctx.cache_root)
    target = registry.cached_path(19)
    target.parent.mkdir(parents=True)
    target.write_bytes(b"corrupt")
    assert "checksum" in _check(ctx, "owlet:C19").message
    assert FIXES["owlet"](ctx, {})["changed"] == ["owlet 26.0.0-fake (C19)"]
    assert target.read_bytes() == OWLET


def test_owlet_download_failure(ctx: CheckContext, tmp_path: Path) -> None:
    (tmp_path / "owlet-src").unlink()
    with pytest.raises(FixError, match="download"):
        FIXES["owlet"](ctx, {})


def test_advantagescope_path_twice(ctx: CheckContext, tmp_path: Path) -> None:
    app = tmp_path / "AdvantageScope.app"
    app.mkdir()
    ctx.config_root.mkdir()
    first = FIXES["advantagescope"](ctx, {"path": str(app)})
    assert first["ok"] and first["changed"] == ["report.toml"]
    text = (ctx.config_root / "report.toml").read_text()
    assert tomllib.loads(text)["advantagescope"]["path"] == str(app)
    assert "# Thresholds for Replay rule markers" in text  # started from the bundled default
    second = FIXES["advantagescope"](ctx, {"path": str(app)})
    assert second["ok"] and second["changed"] == []


@pytest.mark.parametrize("path", ["", None, 3, "/no/such/AdvantageScope.app"])
def test_advantagescope_bad_path(ctx: CheckContext, path: Any) -> None:
    with pytest.raises(FixError):
        FIXES["advantagescope"](ctx, {"path": path})
    assert not (ctx.config_root / "report.toml").exists()


def test_service_install_linux_twice(ctx: CheckContext) -> None:
    commands = ctx.run
    assert isinstance(commands, Commands)
    first = FIXES["service-install"](ctx, {})
    unit = ctx.home / ".config" / "systemd" / "user" / "flashpoint-acquire.service"
    assert first["ok"] and first["changed"] == [str(unit)]
    assert "ExecStart=/opt/pipx/bin/flashpoint acquire --watch" in unit.read_text()
    assert ["systemctl", "--user", "daemon-reload"] in commands.calls
    assert ["systemctl", "--user", "enable", "flashpoint-acquire"] in commands.calls
    second = FIXES["service-install"](ctx, {})
    assert second["ok"] and second["changed"] == []


def test_service_install_windows(ctx: CheckContext) -> None:
    ctx.system = "Windows"
    commands = ctx.run
    assert isinstance(commands, Commands)
    ctx.which = lambda name: rf"C:\Users\pit\.local\bin\{name}.EXE"
    for _ in range(2):
        assert FIXES["service-install"](ctx, {})["ok"]
    create = [c for c in commands.calls if c[:2] == ["schtasks", "/Create"]]
    assert len(create) == 2 and create[0][-3:] == ["/TN", "flashpoint-acquire", "/F"]
    xml = Path(create[0][create[0].index("/XML") + 1])
    assert not xml.exists()  # the filled-in task is a temp file


def test_service_install_windows_task_text(ctx: CheckContext) -> None:
    from flashpoint.setup.fixes import windows_task

    text = windows_task(r"C:\Users\pit & co\.local\bin\flashpoint.EXE")
    assert r"<Command>C:\Users\pit &amp; co\.local\bin\flashpoint.EXE</Command>" in text
    assert "<Arguments>acquire --watch</Arguments>" in text


def test_service_fixes_on_macos(ctx: CheckContext) -> None:
    ctx.system = "Darwin"
    for name in ("service-install", "service-start"):
        with pytest.raises(FixError, match="macOS"):
            FIXES[name](ctx, {})


def test_service_install_without_the_command(ctx: CheckContext) -> None:
    ctx.which = lambda name: None
    with pytest.raises(FixError, match="flashpoint"):
        FIXES["service-install"](ctx, {})


def test_service_start_twice(ctx: CheckContext) -> None:
    commands = ctx.run
    assert isinstance(commands, Commands)
    for _ in range(2):
        assert FIXES["service-start"](ctx, {})["ok"]
    assert commands.calls == [["systemctl", "--user", "enable", "--now", "flashpoint-acquire"]] * 2
    ctx.system = "Windows"
    FIXES["service-start"](ctx, {})
    assert commands.calls[-1] == ["schtasks", "/Run", "/TN", "flashpoint-acquire"]


def test_service_command_failure_is_reported(ctx: CheckContext) -> None:
    ctx.run = Commands(fail="enable")
    with pytest.raises(FixError, match="boom"):
        FIXES["service-start"](ctx, {})


def test_fixes_never_touch_the_lake(ctx: CheckContext, tmp_path: Path) -> None:
    lake = ctx.lake.root
    lake.mkdir(parents=True)
    (lake / "marker").write_text("x")
    app = tmp_path / "AS.app"
    app.mkdir()
    FIXES["init"](ctx, {"team": 254, "lake": str(lake)})
    FIXES["owlet"](ctx, {})
    FIXES["advantagescope"](ctx, {"path": str(app)})
    assert sorted(p.name for p in lake.iterdir()) == ["marker"]


def test_default_context_uses_the_real_registry(tmp_path: Path) -> None:
    ctx = CheckContext.default(tmp_path, LakePaths(tmp_path / "lake"))
    assert ctx.registry is hoot.default_registry
    shutil.rmtree(tmp_path)
    assert config.cache_root() == ctx.cache_root
