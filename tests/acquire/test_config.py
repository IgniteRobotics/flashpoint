from pathlib import Path

import pytest

from flashpoint.acquire.config import AcquireConfig, AcquireConfigError, BackupConfig, team_hosts
from flashpoint.cli import main

FULL_TOML = """
hosts = ["192.0.2.1"]
user = "pit"
password = "secret"
roots = ["/data/logs"]
poll_s = 10
settle_s = 2
low_space_mb = 250
removable_media = false
[backup]
remote = "gdrive:flashpoint"
interval_min = 5
overdue_h = 48
"""


def test_defaults_when_no_file(tmp_path: Path) -> None:
    config = AcquireConfig.load(tmp_path)
    assert config.hosts == []
    assert config.team is None and config.lake is None
    assert config.robot_hosts() == []
    assert config.user == "lvuser"
    assert config.password == ""
    assert config.roots == ["/home/lvuser/logs", "/u/logs"]
    assert config.poll_s == 30
    assert config.settle_s == 5
    assert config.low_space_mb == 100
    assert config.removable_media is True
    assert config.backup == BackupConfig(remote=None, interval_min=15, overdue_h=24)


def test_full_file_load(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text(FULL_TOML)
    config = AcquireConfig.load(tmp_path)
    assert config.hosts == ["192.0.2.1"]
    assert config.user == "pit"
    assert config.password == "secret"
    assert config.roots == ["/data/logs"]
    assert (config.poll_s, config.settle_s, config.low_space_mb) == (10, 2, 250)
    assert config.removable_media is False
    assert config.backup.remote == "gdrive:flashpoint"
    assert config.backup.interval_min == 5
    assert config.backup.overdue_h == 48


def test_overdue_below_one_hour_names_the_key(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text("[backup]\noverdue_h = 0\n")
    with pytest.raises(AcquireConfigError, match="overdue_h"):
        AcquireConfig.load(tmp_path)


def test_partial_file_keeps_other_defaults(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text("poll_s = 7\n")
    config = AcquireConfig.load(tmp_path)
    assert config.poll_s == 7
    assert config.settle_s == 5


def test_unknown_key_names_the_key(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text("pol_s = 7\n")
    with pytest.raises(AcquireConfigError, match="pol_s"):
        AcquireConfig.load(tmp_path)


def test_unknown_nested_key_names_the_key(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text('[backup]\nremot = "x"\n')
    with pytest.raises(AcquireConfigError, match=r"backup\.remot"):
        AcquireConfig.load(tmp_path)


def test_wrong_type_names_the_key(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text('poll_s = "soon"\n')
    with pytest.raises(AcquireConfigError, match="poll_s"):
        AcquireConfig.load(tmp_path)


def test_malformed_toml_is_a_config_error(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text("hosts = [\n")
    with pytest.raises(AcquireConfigError, match="acquire.toml"):
        AcquireConfig.load(tmp_path)


def test_cli_overrides_win_over_file(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text(FULL_TOML)
    config = AcquireConfig.load(tmp_path).with_overrides(
        hosts=["198.51.100.7"], removable_media=False, poll_s=None
    )
    assert config.hosts == ["198.51.100.7"]
    assert config.poll_s == 10  # None means "flag not given"
    assert config.user == "pit"
    assert config.backup.remote == "gdrive:flashpoint"


def test_override_of_unknown_field_is_rejected() -> None:
    with pytest.raises(AcquireConfigError, match="nope"):
        AcquireConfig().with_overrides(nope=1)


def test_override_is_validated() -> None:
    with pytest.raises(AcquireConfigError, match="poll_s"):
        AcquireConfig().with_overrides(poll_s="soon")


def test_four_digit_team(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text("team = 6829\n")
    config = AcquireConfig.load(tmp_path)
    assert config.robot_hosts() == ["10.68.29.2", "roborio-6829-frc.local", "172.22.11.2"]


@pytest.mark.parametrize(
    ("team", "first"), [(254, "10.2.54.2"), (12345, "10.123.45.2"), (1, "10.0.1.2")]
)
def test_short_and_long_team_numbers(team: int, first: str) -> None:
    assert team_hosts(team)[0] == first
    assert team_hosts(team)[1] == f"roborio-{team}-frc.local"


def test_configured_hosts_replace_the_team_addresses(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text('team = 254\nhosts = ["192.0.2.9"]\n')
    assert AcquireConfig.load(tmp_path).robot_hosts() == ["192.0.2.9"]


@pytest.mark.parametrize("team", ["0", "100000", "-5", '"254"'])
def test_bad_team_is_refused(tmp_path: Path, team: str) -> None:
    (tmp_path / "acquire.toml").write_text(f"team = {team}\n")
    with pytest.raises(AcquireConfigError, match="team"):
        AcquireConfig.load(tmp_path)


def test_lake_is_read(tmp_path: Path) -> None:
    (tmp_path / "acquire.toml").write_text("lake = '/data/lake'\n")
    assert AcquireConfig.load(tmp_path).lake == Path("/data/lake")


def test_no_team_refuses_to_acquire(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "cfg"))
    lake = tmp_path / "lake"
    assert main(["acquire", "--no-usb", "--lake", str(lake)]) != 0
    assert "flashpoint init" in capsys.readouterr().err
    assert not lake.exists()  # nothing transferred, not even a lock file
