"""`flashpoint report` and `flashpoint serve` (spec: match-reports / Served and static modes)."""

import signal
import subprocess
import sys
from pathlib import Path

import pytest

from flashpoint.cli import EXIT_FAILED, EXIT_USAGE, main
from tests.report.lake import MatchLake, quiet_rows


@pytest.fixture
def lake(tmp_path: Path) -> MatchLake:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows())
    lake.add_match("2026gadal_qm1", "d1", quiet_rows())
    lake.add_match(None, "practice", quiet_rows())
    lake.write_meta()
    return lake


def test_report_builds_and_summarises(lake: MatchLake, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["report", "--lake", str(lake.root)]) == 0
    out = capsys.readouterr().out
    assert "built 2, unchanged 0" in out and "2 match(es) listed" in out
    assert "1 session(s) without a match key" in out
    assert main(["report", "--lake", str(lake.root)]) == 0
    assert "built 0, unchanged 2" in capsys.readouterr().out
    assert main(["report", "--lake", str(lake.root), "--all", "--event", "2026gadal"]) == 0
    assert "built 1, unchanged 0" in capsys.readouterr().out
    assert main(["report", "--lake", str(lake.root), "--all", "--match", "2026gacmp_qm7"]) == 0
    assert "built 1," in capsys.readouterr().out


def test_report_static(lake: MatchLake, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "usb"
    assert main(["report", "--lake", str(lake.root), "--static", str(out)]) == 0
    assert "exported 2 match(es)" in capsys.readouterr().out
    assert (out / "index.html").is_file() and len(list((out / "raw").iterdir())) == 6
    assert main(["report", "--lake", str(lake.root), "--static", str(out), "--no-raw"]) == 0
    assert "not included" in capsys.readouterr().out and not (out / "raw").exists()


def test_report_usage_errors(lake: MatchLake, tmp_path: Path) -> None:
    assert main(["report", "--lake", str(lake.root), "--no-raw"]) == EXIT_USAGE
    assert main(["report", "--lake", str(tmp_path / "nothing")]) == EXIT_FAILED
    foreign = tmp_path / "docs"
    foreign.mkdir()
    (foreign / "keep.txt").write_text("x")
    assert main(["report", "--lake", str(lake.root), "--static", str(foreign)]) == EXIT_FAILED
    assert (foreign / "keep.txt").exists()


def test_report_bad_config(
    lake: MatchLake, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "report.toml").write_text("[thresholds]\nnope = 1\n")
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "config"))
    assert main(["report", "--lake", str(lake.root)]) == EXIT_USAGE


_SERVE = "import sys\nfrom flashpoint.cli import main\nsys.exit(main(sys.argv[1:]))\n"


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT semantics differ on Windows")
@pytest.mark.parametrize("build", [True, False])
def test_serve_builds_then_stops_cleanly(lake: MatchLake, build: bool) -> None:
    args = [sys.executable, "-c", _SERVE, "serve", "--lake", str(lake.root), "--port", "0"]
    process = subprocess.Popen(  # noqa: S603
        args + ([] if build else ["--no-build"]),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        lines = [process.stdout.readline() for _ in range(3 if build else 1)]
        assert lines[-1].startswith(f"Flashpoint (lake {lake.root}) at http://127.0.0.1:")
        assert any("built 2" in line for line in lines) is build
        assert (lake.root / "report/data/matches.js").exists() is build
        process.send_signal(signal.SIGINT)
        assert process.wait(timeout=10) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_serve_host_warning_and_bad_port(
    lake: MatchLake, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    import flashpoint.web.server as server

    monkeypatch.setattr(server, "run", lambda s: (s.server_close(), 0)[1])
    assert main(["serve", "--lake", str(lake.root), "--no-build", "--host", "0.0.0.0",  # noqa: S104
                 "--port", "0"]) == 0  # fmt: skip
    captured = capsys.readouterr()
    assert "pit network" in captured.err and "no password" in captured.err
    assert main(["serve", "--lake", str(lake.root), "--no-build", "--port", "-1"]) == EXIT_FAILED


def test_serve_wires_the_launcher(
    lake: MatchLake, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os
    import time

    import flashpoint.web.advantagescope as advantagescope
    import flashpoint.web.server as server

    root = tmp_path / "flashpoint-as"
    old = root / "2026gacmp_qm1"
    old.mkdir(parents=True)
    then = time.time() - advantagescope.MAX_AGE_S - 60
    os.utime(old, (then, then))
    monkeypatch.setattr(advantagescope, "staging_root", lambda: root)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    app = tmp_path / "AdvantageScope.app"
    app.mkdir()
    (config_dir / "report.toml").write_text(f'[advantagescope]\npath = "{app}"\n')
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(config_dir))
    seen: list[dict[str, object]] = []

    def capture(s: server.FlashpointServer) -> int:
        seen.append(s.launcher.availability(s.bound_local, "127.0.0.1"))
        s.server_close()
        return 0

    monkeypatch.setattr(server, "run", capture)
    assert main(["serve", "--lake", str(lake.root), "--no-build", "--port", "0"]) == 0
    assert seen[-1] == {"available": True, "reason": None, "app": str(app), "found_by": "config"}
    assert not old.exists()  # stale staging removed at start
    assert main(["serve", "--lake", str(lake.root), "--no-build", "--host", "0.0.0.0",  # noqa: S104
                 "--port", "0"]) == 0  # fmt: skip
    assert seen[-1]["available"] is False and seen[-1]["reason"] == "shared on the network"
