"""Self-test: every fault switch on the fake robot does what it says."""

import hashlib
import shlex
import socket
import struct
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import paramiko
import pytest
from paramiko.sftp import CMD_EXTENDED

from tests.acquire.fake_robot import FakeRobot

LOG_DIR = "/home/lvuser/logs"
LOG_PATH = f"{LOG_DIR}/a.wpilog"
PAYLOAD = bytes(range(256)) * 400  # 102,400 bytes


@pytest.fixture
def robot(fake_robot: FakeRobot) -> FakeRobot:
    log_dir = fake_robot.root / "home/lvuser/logs"
    log_dir.mkdir(parents=True)
    (log_dir / "a.wpilog").write_bytes(PAYLOAD)
    return fake_robot


@contextmanager
def connect(
    robot: FakeRobot, *, password: str | None = None, username: str = "lvuser"
) -> Iterator[paramiko.Transport]:
    transport = paramiko.Transport((robot.host, robot.port))
    try:
        transport.start_client(timeout=10)
        if password is None:
            transport.auth_none(username)
        else:
            transport.auth_password(username, password)
        yield transport
    finally:
        transport.close()


def run(transport: paramiko.Transport, command: str) -> tuple[int, str, str]:
    channel = transport.open_session()
    channel.exec_command(command)
    out = channel.makefile("rb").read().decode()
    err = channel.makefile_stderr("rb").read().decode()
    return channel.recv_exit_status(), out, err


def sftp(transport: paramiko.Transport) -> paramiko.SFTPClient:
    client = paramiko.SFTPClient.from_transport(transport)
    assert client is not None
    return client


def test_auth_none_and_listing_and_read(robot: FakeRobot) -> None:
    with connect(robot) as t:
        client = sftp(t)
        names = [a.filename for a in client.listdir_attr(LOG_DIR)]
        assert names == ["a.wpilog"]
        assert client.stat(LOG_PATH).st_size == len(PAYLOAD)
        assert client.lstat(LOG_PATH).st_size == len(PAYLOAD)
        with client.open(LOG_PATH, "rb") as f:
            f.prefetch()
            assert f.read() == PAYLOAD
    assert robot.bytes_served == len(PAYLOAD)
    assert robot.write_attempts == []


def test_password_auth_when_configured(robot: FakeRobot) -> None:
    robot.password = "hunter2"
    robot.allow_none = False
    with connect(robot, password="hunter2") as t:
        assert t.is_authenticated()
    with pytest.raises(paramiko.AuthenticationException), connect(robot):
        pass
    with pytest.raises(paramiko.AuthenticationException), connect(robot, password="nope"):
        pass


def test_auth_reject(robot: FakeRobot) -> None:
    robot.password = "hunter2"
    robot.auth_reject = True
    with pytest.raises(paramiko.AuthenticationException), connect(robot):
        pass
    with pytest.raises(paramiko.AuthenticationException), connect(robot, password="hunter2"):
        pass


def test_sha256sum_matches_file(robot: FakeRobot) -> None:
    with connect(robot) as t:
        code, out, _ = run(t, f"sha256sum -- {shlex.quote(LOG_PATH)}")
    assert code == 0
    assert out == f"{hashlib.sha256(PAYLOAD).hexdigest()}  {LOG_PATH}\n"


def test_sha256sum_missing_file_exits_1(robot: FakeRobot) -> None:
    with connect(robot) as t:
        code, out, err = run(t, "sha256sum -- /home/lvuser/logs/nope")
    assert (code, out) == (1, "")
    assert "No such file" in err


def test_sha256sum_quoted_path_with_spaces(robot: FakeRobot) -> None:
    (robot.root / "home/lvuser/logs/my log.wpilog").write_bytes(b"x")
    path = f"{LOG_DIR}/my log.wpilog"
    with connect(robot) as t:
        code, out, _ = run(t, f"sha256sum -- {shlex.quote(path)}")
    assert code == 0
    assert out == f"{hashlib.sha256(b'x').hexdigest()}  {path}\n"


def test_wrong_hash(robot: FakeRobot) -> None:
    robot.wrong_hash = True
    with connect(robot) as t:
        code, out, _ = run(t, f"sha256sum -- {shlex.quote(LOG_PATH)}")
    assert code == 0
    digest = out.split()[0]
    assert len(digest) == 64
    assert digest != hashlib.sha256(PAYLOAD).hexdigest()


def test_no_sha256sum(robot: FakeRobot) -> None:
    robot.no_sha256sum = True
    with connect(robot) as t:
        code, out, err = run(t, f"sha256sum -- {shlex.quote(LOG_PATH)}")
    assert code == 127
    assert out == ""
    assert "not found" in err


def test_df_posix_format_and_low_df(robot: FakeRobot) -> None:
    with connect(robot) as t:
        code, out, _ = run(t, "df -Pk -- /home/lvuser")
        assert code == 0
        header, row = out.splitlines()
        assert header.startswith("Filesystem")
        assert int(row.split()[3]) == robot.df_free_kb
        robot.df_free_kb = 1234
        _, out, _ = run(t, "df -Pk -- /home/lvuser")
        assert int(out.splitlines()[1].split()[3]) == 1234


def test_statvfs_reflects_df_free_kb(robot: FakeRobot) -> None:
    robot.df_free_kb = 4321
    with connect(robot) as t:
        client: Any = sftp(t)
        _, reply = client._request(CMD_EXTENDED, "statvfs@openssh.com", "/home/lvuser")
        fields = struct.unpack(">11Q", reply.get_remainder()[:88])
    assert fields[1] * fields[4] == 4321 * 1024  # f_frsize * f_bavail


def test_unknown_exec_command_exits_127(robot: FakeRobot) -> None:
    with connect(robot) as t:
        code, _, err = run(t, "rm -rf /")
    assert code == 127
    assert "not found" in err
    assert robot.exec_log == ["rm -rf /"]


def test_drop_after_bytes(robot: FakeRobot) -> None:
    robot.drop_after_bytes = 40_000
    received = bytearray()
    with (
        connect(robot) as t,
        pytest.raises((OSError, EOFError, paramiko.SSHException)),
        sftp(t).open(LOG_PATH, "rb") as f,
    ):
        f.prefetch()
        while chunk := f.read(8192):
            received.extend(chunk)
    assert len(received) <= 40_000
    assert robot.bytes_served == 40_000
    robot.drop_after_bytes = None
    with connect(robot) as t, sftp(t).open(LOG_PATH, "rb") as f:
        assert f.read() == PAYLOAD


def test_rotate_host_key(robot: FakeRobot) -> None:
    def seen_fingerprint() -> str:
        with connect(robot) as t:
            return str(t.get_remote_server_key().fingerprint)

    first = seen_fingerprint()
    assert first == robot.host_key_fingerprint
    robot.rotate_host_key()
    second = seen_fingerprint()
    assert second == robot.host_key_fingerprint
    assert second != first


def test_every_write_is_refused_and_recorded(robot: FakeRobot) -> None:
    before = (robot.root / "home/lvuser/logs/a.wpilog").read_bytes()
    with connect(robot) as t:
        client = sftp(t)
        for op in (
            lambda: client.remove(LOG_PATH),
            lambda: client.rename(LOG_PATH, f"{LOG_DIR}/b"),
            lambda: client.posix_rename(LOG_PATH, f"{LOG_DIR}/c"),
            lambda: client.mkdir(f"{LOG_DIR}/d"),
            lambda: client.rmdir(LOG_DIR),
            lambda: client.chmod(LOG_PATH, 0o600),
            lambda: client.symlink(LOG_PATH, f"{LOG_DIR}/e"),
            lambda: client.open(f"{LOG_DIR}/new", "wb"),
            lambda: client.open(LOG_PATH, "ab"),
        ):
            with pytest.raises(OSError):  # noqa: PT011
                op()
    ops = [op for op, _ in robot.write_attempts]
    assert ops == [
        "remove", "rename", "rename", "mkdir", "rmdir", "chattr", "symlink",
        "open-write", "open-write",
    ]  # fmt: skip
    assert sorted(p.name for p in (robot.root / "home/lvuser/logs").iterdir()) == ["a.wpilog"]
    assert (robot.root / "home/lvuser/logs/a.wpilog").read_bytes() == before


def test_paths_cannot_escape_root(robot: FakeRobot, tmp_path: Path) -> None:
    (tmp_path / "secret").write_text("x")
    with connect(robot) as t, pytest.raises(OSError):  # noqa: PT011
        sftp(t).stat("/../secret")


def test_reset_counters(robot: FakeRobot) -> None:
    with connect(robot) as t:
        run(t, "df -Pk -- /")
        with sftp(t).open(LOG_PATH, "rb") as f:
            f.read()
    assert robot.bytes_served > 0
    robot.reset_counters()
    assert (robot.bytes_served, robot.exec_log, robot.write_attempts) == (0, [], [])


def test_fingerprint_has_single_sha256_prefix(robot: FakeRobot) -> None:
    assert robot.host_key_fingerprint.startswith("SHA256:")
    assert not robot.host_key_fingerprint.startswith("SHA256:SHA256:")


def test_lifecycle_distinct_ports_and_prompt_stop_with_silent_client(tmp_path: Path) -> None:
    before = {t.ident for t in threading.enumerate()}
    one, two = FakeRobot(tmp_path / "one"), FakeRobot(tmp_path / "two")
    one.start()
    two.start()
    silent = socket.create_connection((one.host, one.port))
    try:
        assert one.port > 0
        assert two.port > 0
        assert one.port != two.port
        # A stalled peer must not block later connections.
        with connect(one) as t:
            assert t.is_authenticated()
        started = time.monotonic()
        one.stop()
        assert time.monotonic() - started < 1
    finally:
        silent.close()
        two.stop()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        leftover = [t for t in threading.enumerate() if t.ident not in before and t.is_alive()]
        if not leftover:
            break
        time.sleep(0.01)
    assert leftover == []
