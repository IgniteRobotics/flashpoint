import hashlib
import json
import logging
import os
import socket
from collections.abc import Iterator
from pathlib import Path

import pytest

from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.robot import (
    AuthErrorThrottle,
    LowSpaceWarning,
    RemoteFile,
    RobotClient,
    low_space_warning,
)
from tests.acquire.fake_robot import FakeRobot

LOGGER = "flashpoint.acquire.robot"
INTERNAL = "/home/lvuser/logs"
USB = "/u/logs"
MB = 1024 * 1024


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(tmp_path: Path) -> Path:
    return tmp_path / "cache" / "known-robots.json"


def _own(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Records from the client itself; paramiko logs its own noise when sockets close."""
    return [r for r in caplog.records if r.name == LOGGER]


def _address(robot: FakeRobot) -> str:
    return f"{robot.host}:{robot.port}"


def _config(*hosts: str, password: str = "") -> AcquireConfig:
    return AcquireConfig(hosts=list(hosts), password=password)


def _dead_address() -> str:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def _connect(
    config: AcquireConfig, store: Path, clock: Clock, throttle: AuthErrorThrottle | None = None
) -> RobotClient | None:
    return RobotClient.connect(
        config,
        known_robots=store,
        throttle=throttle or AuthErrorThrottle(clock),
        tcp_timeout=0.5,
        auth_timeout=5.0,
    )


@pytest.fixture
def client(fake_robot: FakeRobot, store: Path, clock: Clock) -> Iterator[RobotClient]:
    connected = _connect(_config(_address(fake_robot)), store, clock)
    assert connected is not None
    yield connected
    connected.close()


def _write(robot: FakeRobot, path: str, data: bytes = b"x", mtime_ns: int | None = None) -> Path:
    target = robot.root / path.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    if mtime_ns is not None:
        os.utime(target, ns=(mtime_ns, mtime_ns))
    return target


# --- 3.1 connect ---------------------------------------------------------------------------


def test_connect_uses_first_reachable_candidate_in_order(
    fake_robot: FakeRobot, store: Path, clock: Clock
) -> None:
    dead = _dead_address()
    client = _connect(_config(dead, _address(fake_robot)), store, clock)
    assert client is not None
    with client:
        assert client.host == _address(fake_robot)  # the tether answered after the radio failed
    assert fake_robot.connection_count == 1


def test_connect_prefers_earlier_candidate(tmp_path: Path, store: Path, clock: Clock) -> None:
    first, second = FakeRobot(tmp_path / "a"), FakeRobot(tmp_path / "b")
    first.start()
    second.start()
    try:
        client = _connect(_config(_address(first), _address(second)), store, clock)
        assert client is not None
        client.close()
        assert (first.connection_count, second.connection_count) == (1, 0)
    finally:
        first.stop()
        second.stop()


def test_no_robot_is_not_an_error(
    store: Path, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    assert _connect(_config(_dead_address(), _dead_address()), store, clock) is None
    assert not [r for r in _own(caplog) if r.levelno >= logging.WARNING]


def test_default_port_is_22() -> None:
    from flashpoint.acquire.robot import split_address

    assert split_address("10.68.29.2") == ("10.68.29.2", 22)
    assert split_address("roborio-6829-frc.local:2222") == ("roborio-6829-frc.local", 2222)


def test_auth_none_is_tried_first(fake_robot: FakeRobot, store: Path, clock: Clock) -> None:
    fake_robot.password = "secret"  # would also work, but auth_none must win
    client = _connect(_config(_address(fake_robot), password="wrong"), store, clock)
    assert client is not None
    client.close()


def test_password_after_auth_none_rejected(
    fake_robot: FakeRobot, store: Path, clock: Clock
) -> None:
    fake_robot.allow_none = False
    fake_robot.password = "secret"
    client = _connect(_config(_address(fake_robot), password="secret"), store, clock)
    assert client is not None
    client.close()


def test_auth_failure_skips_to_next_host(
    tmp_path: Path, store: Path, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    bad, good = FakeRobot(tmp_path / "bad"), FakeRobot(tmp_path / "good")
    bad.auth_reject = True
    bad.start()
    good.start()
    try:
        client = _connect(_config(_address(bad), _address(good)), store, clock)
        assert client is not None
        assert client.host == _address(good)
        client.close()
    finally:
        bad.stop()
        good.stop()
    assert [r for r in _own(caplog) if r.levelno == logging.ERROR and _address(bad) in r.message]


def test_auth_error_logged_once_per_host_per_hour(
    fake_robot: FakeRobot, store: Path, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    fake_robot.auth_reject = True
    throttle = AuthErrorThrottle(clock)
    config = _config(_address(fake_robot))

    def errors() -> int:
        return len([r for r in _own(caplog) if r.levelno == logging.ERROR])

    assert _connect(config, store, clock, throttle) is None
    assert errors() == 1
    clock.now += 600
    assert _connect(config, store, clock, throttle) is None
    assert errors() == 1  # suppressed
    clock.now += 3000  # 3600 s after the first error
    assert _connect(config, store, clock, throttle) is None
    assert errors() == 2


def test_throttle_is_per_host(clock: Clock) -> None:
    throttle = AuthErrorThrottle(clock)
    assert throttle.should_log("a")
    assert throttle.should_log("b")
    assert not throttle.should_log("a")


def test_host_key_recorded_on_first_sight(
    fake_robot: FakeRobot, store: Path, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    client = _connect(_config(_address(fake_robot)), store, clock)
    assert client is not None
    client.close()
    assert json.loads(store.read_text()) == {_address(fake_robot): fake_robot.host_key_fingerprint}
    assert not [r for r in _own(caplog) if r.levelno >= logging.WARNING]


def test_changed_host_key_warns_and_is_replaced(
    fake_robot: FakeRobot, store: Path, clock: Clock, caplog: pytest.LogCaptureFixture
) -> None:
    config = _config(_address(fake_robot))
    first = _connect(config, store, clock)
    assert first is not None
    first.close()
    old = fake_robot.host_key_fingerprint
    fake_robot.rotate_host_key()
    second = _connect(config, store, clock)
    assert second is not None  # a reimaged rio is accepted
    second.close()
    warnings = [r for r in _own(caplog) if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert _address(fake_robot) in warnings[0].message
    assert json.loads(store.read_text())[_address(fake_robot)] == fake_robot.host_key_fingerprint
    assert old != fake_robot.host_key_fingerprint
    caplog.clear()
    third = _connect(config, store, clock)
    assert third is not None
    third.close()
    assert not _own(caplog)


def test_corrupt_store_is_treated_as_empty(
    fake_robot: FakeRobot, store: Path, clock: Clock
) -> None:
    store.parent.mkdir(parents=True)
    store.write_text("{not json")
    client = _connect(_config(_address(fake_robot)), store, clock)
    assert client is not None
    client.close()
    assert _address(fake_robot) in json.loads(store.read_text())


# --- 3.3 listing, hashing, free space -----------------------------------------------------------


def test_list_logs_recurses_both_roots_and_keeps_relpaths(
    fake_robot: FakeRobot, client: RobotClient
) -> None:
    _write(fake_robot, f"{INTERNAL}/FRC_20260314_102233_GACMP_Q7.wpilog", b"abc", 5_000_000_000)
    _write(fake_robot, f"{INTERNAL}/2026-03-14_10-22-33/rio_2026-03-14_10-22-33.hoot", b"hoot")
    _write(fake_robot, f"{INTERNAL}/notes.txt")
    _write(fake_robot, f"{USB}/deep/er/x.WPILOG", b"upper")
    entries = client.list_logs()
    assert entries == sorted(entries, key=lambda e: (e.root, e.relpath))
    assert {(e.root, e.relpath) for e in entries} == {
        (INTERNAL, "FRC_20260314_102233_GACMP_Q7.wpilog"),
        (INTERNAL, "2026-03-14_10-22-33/rio_2026-03-14_10-22-33.hoot"),
        (USB, "deep/er/x.WPILOG"),
    }
    wpilog = next(e for e in entries if e.relpath.endswith("Q7.wpilog"))
    assert wpilog == RemoteFile(INTERNAL, "FRC_20260314_102233_GACMP_Q7.wpilog", 3, 5_000_000_000)
    assert wpilog.remote_path == f"{INTERNAL}/FRC_20260314_102233_GACMP_Q7.wpilog"


def test_missing_root_is_skipped_silently(
    fake_robot: FakeRobot, client: RobotClient, caplog: pytest.LogCaptureFixture
) -> None:
    _write(fake_robot, f"{INTERNAL}/a.wpilog")
    caplog.clear()
    entries = client.list_logs()  # /u/logs does not exist
    assert [e.relpath for e in entries] == ["a.wpilog"]
    assert not [r for r in _own(caplog) if r.levelno >= logging.WARNING]


def test_list_logs_with_no_roots_present_is_empty(client: RobotClient) -> None:
    assert client.list_logs() == []


def test_listing_never_writes(fake_robot: FakeRobot, client: RobotClient) -> None:
    _write(fake_robot, f"{INTERNAL}/a.wpilog")
    client.list_logs()
    client.remote_sha256(f"{INTERNAL}/a.wpilog")
    client.free_bytes(INTERNAL)
    assert fake_robot.write_attempts == []


def test_remote_sha256(fake_robot: FakeRobot, client: RobotClient) -> None:
    data = os.urandom(5000)
    _write(fake_robot, f"{INTERNAL}/a.wpilog", data)
    assert client.remote_sha256(f"{INTERNAL}/a.wpilog") == hashlib.sha256(data).hexdigest()


def test_remote_sha256_quotes_paths(fake_robot: FakeRobot, client: RobotClient) -> None:
    _write(fake_robot, f"{INTERNAL}/it's a log.wpilog", b"q")
    assert client.remote_sha256(f"{INTERNAL}/it's a log.wpilog") == hashlib.sha256(b"q").hexdigest()


def test_remote_sha256_none_when_command_missing(
    fake_robot: FakeRobot, client: RobotClient
) -> None:
    _write(fake_robot, f"{INTERNAL}/a.wpilog")
    fake_robot.no_sha256sum = True
    assert client.remote_sha256(f"{INTERNAL}/a.wpilog") is None


def test_remote_sha256_missing_file_raises(client: RobotClient) -> None:
    with pytest.raises(OSError, match="sha256sum"):
        client.remote_sha256(f"{INTERNAL}/gone.wpilog")


def test_free_bytes_from_df(fake_robot: FakeRobot, client: RobotClient) -> None:
    fake_robot.df_free_kb = 4321
    assert client.free_bytes(INTERNAL) == 4321 * 1024
    assert fake_robot.exec_log == [f"df -Pk -- {INTERNAL}"]


def test_free_bytes_falls_back_to_statvfs(
    fake_robot: FakeRobot, client: RobotClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_robot.df_free_kb = 777
    # df is broken on this robot: every exec of it fails
    monkeypatch.setattr(client, "_exec", lambda command: (127, "", "df: not found"))
    assert client.free_bytes(INTERNAL) == 777 * 1024


def test_free_bytes_none_when_both_fail(
    client: RobotClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client, "_exec", lambda command: (1, "", "boom"))
    monkeypatch.setattr(client, "_statvfs_free", lambda path: None)
    assert client.free_bytes(INTERNAL) is None


# --- 3.4 low-space warning -------------------------------------------------------------------


def test_low_space_warning_sets_and_clears() -> None:
    warning = low_space_warning("10.68.29.2", INTERNAL, 80 * MB, 100)
    assert warning == LowSpaceWarning("10.68.29.2", INTERNAL, 80 * MB)
    assert low_space_warning("10.68.29.2", INTERNAL, 500 * MB, 100) is None
    assert low_space_warning("10.68.29.2", INTERNAL, 100 * MB, 100) is None  # at threshold: ok


def test_client_low_space_warnings_follow_free_space(
    fake_robot: FakeRobot, client: RobotClient
) -> None:
    fake_robot.df_free_kb = 80 * 1024
    warnings = client.low_space_warnings()
    assert {w.root for w in warnings} == {INTERNAL, USB}  # df answers for both paths
    assert all(w.host == client.host and w.free_bytes == 80 * MB for w in warnings)
    fake_robot.df_free_kb = 500 * 1024
    assert client.low_space_warnings() == []
