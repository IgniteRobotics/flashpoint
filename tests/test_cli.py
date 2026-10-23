import shutil
from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint import config
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

    assert main(["doctor", "--lake", str(tmp_path / "lake")]) == 1  # no team number
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


def _ingested(tmp_path: Path) -> Path:
    log = WpilogBuilder().start(1, "x", "double").double(1, 1, 1.0).write(tmp_path / "a.wpilog")
    lake = tmp_path / "lake"
    assert main(["ingest", "--no-derive", "--lake", str(lake), str(log)]) == 0
    return lake


def test_derive_without_configuration_warns(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """flashpoint-private#54: an empty configuration root no longer derives silently."""
    lake = _ingested(tmp_path)
    (tmp_path / "empty").mkdir()
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "empty"))
    capsys.readouterr()
    assert main(["derive", "--lake", str(lake)]) == 0
    err = capsys.readouterr().err
    assert "flashpoint init" in err and str(tmp_path / "empty") in err


def test_derive_without_a_robot_warns(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    lake = _ingested(tmp_path)
    root = tmp_path / "cfg"
    root.mkdir()
    (root / "acquire.toml").write_text("team = 254\n")
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    capsys.readouterr()
    assert main(["derive", "--lake", str(lake)]) == 0
    err = capsys.readouterr().err
    assert "no robot" in err and "flashpoint init" not in err


def test_derive_with_configuration_is_quiet(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake = _ingested(tmp_path)
    capsys.readouterr()
    assert main(["derive", "--lake", str(lake)]) == 0
    err = capsys.readouterr().err
    assert "flashpoint init" not in err and "no robot" not in err


@pytest.mark.corpus
def test_derive_corpus_q7_without_configuration_warns(
    tmp_path: Path,
    corpus_group: Callable[[str], list[Path]],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = tmp_path / "lake"
    files = [str(p) for p in corpus_group("2026-gacmp-q7")]
    assert main(["ingest", "--no-derive", "--lake", str(lake), *files]) == 0
    (tmp_path / "empty").mkdir()
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "empty"))
    capsys.readouterr()
    assert main(["derive", "--lake", str(lake)]) == 0
    captured = capsys.readouterr()
    assert "flashpoint init" in captured.err
    assert "derived 1 of 1 session(s)" in captured.out


def test_doctor_missing_configuration(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "none"))
    assert main(["doctor", "--lake", str(tmp_path / "lake")]) == 1
    out = capsys.readouterr().out
    assert "PROBLEM" in out and "flashpoint init" in out
    assert not (tmp_path / "none").exists()


def test_doctor_invalid_robot_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)
    robot = root / "robots" / "2026-comp.toml"
    text = robot.read_text()
    end = text.index("can_id", text.index('id = "steer-fl"'))
    robot.write_text(text[:end] + "can_id = 11" + text[text.index("\n", end) :])
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    assert main(["doctor", "--lake", str(tmp_path / "lake")]) == 1
    out = capsys.readouterr().out
    line = next(x for x in out.splitlines() if "PROBLEM" in x and "2026-comp.toml" in x)
    assert "drive-fl" in line and "steer-fl" in line


def test_doctor_per_user_folder_and_defaults(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per-user folder: no FLASHPOINT_CONFIG, team 254 in the per-user acquire.toml."""
    monkeypatch.delenv("FLASHPOINT_CONFIG")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("APPDATA", str(tmp_path / "xdg"))
    root = tmp_path / "xdg" / "flashpoint"
    root.mkdir(parents=True)
    (root / "acquire.toml").write_text("team = 254\n")
    main(["doctor", "--lake", str(tmp_path / "lake")])
    out = capsys.readouterr().out
    assert f"config     {root}" in out
    assert "team 254" in out
    defaults = next(x for x in out.splitlines() if x.startswith("defaults"))
    assert "report.toml" in defaults and "anomaly.toml" in defaults
