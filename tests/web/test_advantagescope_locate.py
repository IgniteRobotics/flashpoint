import os
from pathlib import Path

import pytest

from flashpoint.report.settings import ReportConfigError, advantagescope_path
from flashpoint.web.advantagescope import (
    Install,
    LocateError,
    doctor_lines,
    find,
    locate,
    staging_mode,
)

MAC_APP = "AdvantageScope (WPILib).app"
WIN_EXE = "AdvantageScope (WPILib).exe"


def _mac_wpilib(home: Path, year: str) -> Path:
    app = home / "wpilib" / year / "advantagescope" / MAC_APP
    (app / "Contents").mkdir(parents=True)
    return app


def _none(_: str) -> str | None:
    return None


def test_newest_wpilib_year_wins_on_macos(tmp_path: Path) -> None:
    _mac_wpilib(tmp_path, "2025")
    newest = _mac_wpilib(tmp_path, "2026")
    (tmp_path / "wpilib" / "tools").mkdir()  # not a year: ignored
    found = locate(None, platform="darwin", home=tmp_path, env={}, which=_none)
    assert found == Install(newest, "WPILib 2026")


def test_year_order_is_numeric(tmp_path: Path) -> None:
    _mac_wpilib(tmp_path, "999")
    newest = _mac_wpilib(tmp_path, "2026")
    assert locate(None, platform="darwin", home=tmp_path, env={}, which=_none) == Install(
        newest, "WPILib 2026"
    )


def test_year_without_advantagescope_is_skipped(tmp_path: Path) -> None:
    older = _mac_wpilib(tmp_path, "2025")
    (tmp_path / "wpilib" / "2026" / "advantagescope").mkdir(parents=True)
    found = locate(None, platform="darwin", home=tmp_path, env={}, which=_none)
    assert found == Install(older, "WPILib 2025")


def test_windows_wpilib_under_public(tmp_path: Path) -> None:
    public = tmp_path / "Public"
    exe = public / "wpilib" / "2026" / "advantagescope" / WIN_EXE
    exe.parent.mkdir(parents=True)
    exe.write_bytes(b"MZ")
    found = locate(None, platform="win32", home=tmp_path / "me", env={"PUBLIC": str(public)},
                   which=_none)  # fmt: skip
    assert found == Install(exe, "WPILib 2026")


def test_linux_wpilib(tmp_path: Path) -> None:
    exe = tmp_path / "wpilib" / "2026" / "advantagescope" / "AdvantageScope (WPILib)"
    exe.parent.mkdir(parents=True)
    exe.write_bytes(b"\x7fELF")
    found = locate(None, platform="linux", home=tmp_path, env={}, which=_none)
    assert found == Install(exe, "WPILib 2026")


def test_standalone_macos(tmp_path: Path) -> None:
    app = tmp_path / "Applications" / "AdvantageScope.app"
    app.mkdir(parents=True)
    found = locate(None, platform="darwin", home=tmp_path / "me", env={}, which=_none,
                   applications=tmp_path / "Applications")  # fmt: skip
    assert found == Install(app, "standalone")


def test_standalone_windows(tmp_path: Path) -> None:
    exe = tmp_path / "Local" / "Programs" / "AdvantageScope" / "AdvantageScope.exe"
    exe.parent.mkdir(parents=True)
    exe.write_bytes(b"MZ")
    env = {"LOCALAPPDATA": str(tmp_path / "Local"), "PUBLIC": str(tmp_path / "Public")}
    found = locate(None, platform="win32", home=tmp_path, env=env, which=_none)
    assert found == Install(exe, "standalone")


def test_standalone_linux_on_path(tmp_path: Path) -> None:
    exe = tmp_path / "bin" / "advantagescope"
    exe.parent.mkdir()
    exe.write_bytes(b"\x7fELF")

    def which(name: str) -> str | None:
        return str(exe) if name == "advantagescope" else None

    found = locate(None, platform="linux", home=tmp_path, env={}, which=which)
    assert found == Install(exe, "standalone")


def test_wpilib_beats_standalone(tmp_path: Path) -> None:
    wpilib = _mac_wpilib(tmp_path, "2026")
    (tmp_path / "Applications" / "AdvantageScope.app").mkdir(parents=True)
    found = locate(None, platform="darwin", home=tmp_path, env={}, which=_none,
                   applications=tmp_path / "Applications")  # fmt: skip
    assert found == Install(wpilib, "WPILib 2026")


def test_explicit_path_wins(tmp_path: Path) -> None:
    _mac_wpilib(tmp_path, "2026")
    custom = tmp_path / "custom" / "AdvantageScope.app"
    custom.mkdir(parents=True)
    found = locate(custom, platform="darwin", home=tmp_path, env={}, which=_none)
    assert found == Install(custom, "config")


def test_explicit_path_missing_is_an_error_without_fallback(tmp_path: Path) -> None:
    _mac_wpilib(tmp_path, "2026")
    missing = tmp_path / "nope" / "AdvantageScope.app"
    with pytest.raises(LocateError, match="nope"):
        locate(missing, platform="darwin", home=tmp_path, env={}, which=_none)


def test_nothing_installed(tmp_path: Path) -> None:
    assert locate(None, platform="darwin", home=tmp_path, env={}, which=_none,
                  applications=tmp_path / "Applications") is None  # fmt: skip
    assert locate(None, platform="win32", home=tmp_path, env={}, which=_none) is None
    assert locate(None, platform="linux", home=tmp_path, env={}, which=_none) is None


def test_config_path(tmp_path: Path) -> None:
    assert advantagescope_path(tmp_path) is None  # no report.toml
    (tmp_path / "report.toml").write_text("[thresholds]\ntemp_warn_c = 60\n")
    assert advantagescope_path(tmp_path) is None
    (tmp_path / "report.toml").write_text('[advantagescope]\npath = ""\n')
    assert advantagescope_path(tmp_path) is None
    (tmp_path / "report.toml").write_text('[advantagescope]\npath = "~/AS.app"\n')
    assert advantagescope_path(tmp_path) == Path.home() / "AS.app"


@pytest.mark.parametrize("text", ["[advantagescope]\npath = 3\n", '[advantagescope]\napp = "x"\n'])
def test_config_path_invalid(tmp_path: Path, text: str) -> None:
    (tmp_path / "report.toml").write_text(text)
    with pytest.raises(ReportConfigError):
        advantagescope_path(tmp_path)


def test_find_reports_problems_instead_of_raising(tmp_path: Path) -> None:
    (tmp_path / "report.toml").write_text(f"[advantagescope]\npath = '{tmp_path / 'gone.app'}'\n")
    install, problem = find(tmp_path)
    assert install is None and problem is not None and "gone.app" in problem
    (tmp_path / "report.toml").write_text("[advantagescope]\npath = 3\n")
    install, problem = find(tmp_path)
    assert install is None and problem is not None and "string" in problem
    (tmp_path / "report.toml").write_text('[advantagescope]\npath = ""\n')
    install, problem = find(tmp_path, platform="darwin", home=tmp_path, env={}, which=_none,
                            applications=tmp_path / "Applications")  # fmt: skip
    assert (install, problem) == (None, "AdvantageScope not found")


def test_doctor_lines(tmp_path: Path) -> None:
    app = tmp_path / "wpilib" / "2026" / "advantagescope" / MAC_APP
    found = doctor_lines(Install(app, "WPILib 2026"), None, tmp_path / "stage", tmp_path / "raw")
    assert found[0] == f"AdvantageScope: {app} (WPILib 2026)"
    mode = "copy" if os.name == "nt" else "link"  # same volume as the raw store
    assert found[1] == f"  staging {tmp_path / 'stage'} ({mode})"
    assert doctor_lines(None, "AdvantageScope not found", tmp_path, tmp_path) == [
        "AdvantageScope: not found"
    ]
    error = doctor_lines(None, "configured AdvantageScope path not found: /x", tmp_path, tmp_path)
    assert error == ["AdvantageScope: ERROR configured AdvantageScope path not found: /x"]


def test_staging_mode(tmp_path: Path) -> None:
    expected = "copy" if os.name == "nt" else "link"
    assert staging_mode(tmp_path / "raw", tmp_path / "stage") == expected


def test_doctor_prints_advantagescope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from flashpoint.cli import main

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    app = tmp_path / "Custom AS.app"
    app.mkdir()
    (config_dir / "report.toml").write_text(
        f"[advantagescope]\npath = '{app}'\n"
    )  # literal: Windows paths
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(config_dir))
    assert main(["doctor", "--lake", str(tmp_path / "lake")]) == 0
    out = capsys.readouterr().out
    assert f"AdvantageScope: {app} (config)" in out
    assert "  staging " in out
    (config_dir / "report.toml").write_text(f"[advantagescope]\npath = '{tmp_path / 'x.app'}'\n")
    assert main(["doctor", "--lake", str(tmp_path / "lake")]) == 0
    assert (
        "AdvantageScope: ERROR configured AdvantageScope path not found" in capsys.readouterr().out
    )
