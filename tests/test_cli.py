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


def _gold(root: Path) -> Path:
    from tests.anomaly.gold_builder import GoldLake, steady

    gold = GoldLake(root, matches=25, slots=["hood", "drive-fl"])
    gold.set("hood", "temp_max_c", steady(45.0, 20) + steady(55.0, 5))
    gold.set("drive-fl", "temp_max_c", [40.0] * 3)
    gold.swap("drive-fl", 3, "ctre:NEW-DRIVE")
    gold.write()
    return root


def test_detect_summary_line(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    lake = _gold(tmp_path / "lake")
    assert main(["detect", "--lake", str(lake)]) == 0
    out = capsys.readouterr().out
    assert "1 HIGH" in out and "anomal" in out
    assert "1 unit(s) building baseline" in out  # the drive unit swapped out after 3 matches
    assert "scanned through 2026syn_qm25" in out
    assert main(["detect", "--lake", str(lake)]) == 0
    assert "unchanged" in capsys.readouterr().out
    assert main(["detect", "--all", "--lake", str(lake)]) == 0
    assert "unchanged" not in capsys.readouterr().out


def test_detect_config_error_is_usage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import shutil

    from flashpoint import config

    cfg = tmp_path / "config"
    shutil.copytree(config.config_root(), cfg)
    (cfg / "anomaly.toml").write_text("[baseline]\ndrift_z_score = 2\n")
    monkeypatch.setenv(config.CONFIG_ENV, str(cfg))
    assert main(["detect", "--lake", str(_gold(tmp_path / "lake"))]) == 2
    assert "drift_z_score" in capsys.readouterr().err


def test_detect_lock_held(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from flashpoint.acquire.watch import EXIT_LOCKED, AcquireLock

    lake = _gold(tmp_path / "lake")
    (lake / "meta").mkdir(exist_ok=True)
    with AcquireLock(lake / "meta" / "detect.lock"):
        assert main(["detect", "--lake", str(lake)]) == EXIT_LOCKED
    assert "detect" in capsys.readouterr().err
