from pathlib import Path

import pytest

from flashpoint.cli import main
from tests.wpilog_builder import WpilogBuilder


def test_ingest_then_doctor(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    log = WpilogBuilder().start(1, "x", "double").double(1, 1, 1.0).write(tmp_path / "a.wpilog")
    lake = tmp_path / "lake"

    assert main(["ingest", "--lake", str(lake), str(log)]) == 0
    assert "1 ingested, 0 skipped, 0 quarantined" in capsys.readouterr().out

    assert main(["doctor", "--lake", str(lake)]) == 0
    out = capsys.readouterr().out
    assert "success" in out
    assert "owlet cache" in out
    assert "2026-comp" in out and "23 slots" in out


def test_ingest_missing_path_is_usage_error(tmp_path: Path) -> None:
    assert main(["ingest", "--lake", str(tmp_path / "lake"), str(tmp_path / "nope")]) == 2


def test_quarantine_exit_code(tmp_path: Path) -> None:
    bad = tmp_path / "bad.wpilog"
    bad.write_bytes(b"x" * 20)
    assert main(["ingest", "--lake", str(tmp_path / "lake"), str(bad)]) == 1


def test_doctor_lists_season_roots_and_signal_counts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "config"
    (config / "seasons").mkdir(parents=True)
    (config / "robots").mkdir()
    (config / "seasons" / "2026.toml").write_text(
        "season = 2026\n[roots]\n"
        'robot = "NT:Robot/m_robotContainer/"\nphotonvision = "NT:/photonvision/"\n'
        'camera_publisher = "NT:/CameraPublisher/"\npreferences = "NT:/Preferences/"\n'
        'fms = "NT:/FMSInfo/"\n'
    )
    (config / "robots" / "r.toml").write_text(
        'robot = "r"\nseason = 2026\nproject = "P"\n'
        '[[slot]]\nid = "a"\nbus = "rio"\nmodel = "TalonFX"\ncan_id = 1\n'
        'subsystem = "s"\nrole = "x"\n'
        '[[signal]]\nid = "b"\nroot = "robot"\nentry = "Intake/Beam"\n'
        'subsystem = "intake"\nmetric = "is_detected"\n'
    )
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(config))

    assert main(["doctor", "--lake", str(tmp_path / "lake")]) == 0
    out = capsys.readouterr().out
    assert "seasons" in out
    assert "2026" in out and "robot=NT:Robot/m_robotContainer/" in out
    assert "photonvision=NT:/photonvision/" in out
    assert "r " in out and "1 slots, 1 signals" in out
