import os
from pathlib import Path

import pytest

from flashpoint.acquire.pulls import PullKey, PullLedger, PullStatus
from flashpoint.acquire.robot import RemoteFile, RobotClient
from flashpoint.acquire.transfer import active_files, select_robot_files, settle
from tests.acquire.fake_robot import FakeRobot

ROOT = "/home/lvuser/logs"
USB = "/u/logs"
SHA = "b" * 64
S = 10**9  # SFTP mtimes have second resolution


def _rf(relpath: str, mtime_s: int, size: int = 10, root: str = ROOT) -> RemoteFile:
    return RemoteFile(root, relpath, size, mtime_s * S)


def _dirs(**mtimes_s: int) -> dict[str, int]:
    return {f"{ROOT}/{name}": t * S for name, t in mtimes_s.items()}


# --- active rule (pure) --------------------------------------------------------------------


def test_newest_wpilog_per_directory_is_active() -> None:
    old, new = _rf("FRC_a.wpilog", 100), _rf("FRC_b.wpilog", 200)
    other_dir_only = _rf("sub/FRC_c.wpilog", 50)
    usb_only = _rf("FRC_d.wpilog", 10, root=USB)
    files = [old, new, other_dir_only, usb_only]
    assert active_files(files, _dirs().__getitem__) == {new, other_dir_only, usb_only}


def test_every_hoot_in_newest_session_directory_is_active() -> None:
    old_rio = _rf("2026-03-14_10-00-00/rio.hoot", 100)
    new_rio = _rf("2026-03-14_11-00-00/rio.hoot", 300)
    new_canivore = _rf("2026-03-14_11-00-00/canivore.hoot", 150)  # older file, same session
    dirs = _dirs(**{"2026-03-14_10-00-00": 100, "2026-03-14_11-00-00": 140})
    active = active_files([old_rio, new_rio, new_canivore], dirs.__getitem__)
    assert active == {new_rio, new_canivore}


def test_hoot_session_chosen_by_directory_mtime_not_file_mtime() -> None:
    # The rio clock jumps when the DS connects; only the directory order counts.
    a = _rf("A/rio.hoot", 900)
    b = _rf("B/rio.hoot", 100)
    active = active_files([a, b], _dirs(A=10, B=20).__getitem__)
    assert active == {b}


def test_wpilogs_and_hoots_are_independent() -> None:
    wpilog = _rf("FRC_a.wpilog", 100)
    hoot = _rf("S1/rio.hoot", 50)
    assert active_files([wpilog, hoot], _dirs(S1=50).__getitem__) == {wpilog, hoot}


# --- settle (pure) -------------------------------------------------------------------------


class Sleep:
    def __init__(self, action: object = None) -> None:
        self.calls: list[float] = []
        self.action = action

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        if callable(self.action):
            self.action()


def test_settle_waits_once_and_keeps_unchanged_files() -> None:
    a, b = _rf("a.wpilog", 1, size=10), _rf("b.wpilog", 1, size=10)
    b_grown = _rf("b.wpilog", 2, size=20)
    sleep = Sleep()
    kept = settle([a, b], lambda: [a, b_grown], settle_s=5, sleep=sleep)
    assert kept == [a]
    assert sleep.calls == [5]


def test_settle_drops_vanished_files() -> None:
    a = _rf("a.wpilog", 1)
    assert settle([a], list, settle_s=5, sleep=Sleep()) == []


def test_settle_without_candidates_does_not_wait() -> None:
    sleep = Sleep()
    assert settle([], lambda: pytest.fail("no relist"), settle_s=5, sleep=sleep) == []
    assert sleep.calls == []


# --- selection against the fake robot ------------------------------------------------------


def _put(robot: FakeRobot, path: str, data: bytes, mtime_s: int) -> Path:
    target = robot.root / path.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    os.utime(target, ns=(mtime_s * S, mtime_s * S))
    return target


def _set_dir_mtime(robot: FakeRobot, path: str, mtime_s: int) -> None:
    os.utime(robot.root / path.lstrip("/"), ns=(mtime_s * S, mtime_s * S))


def _names(files: list[RemoteFile]) -> list[str]:
    return sorted(f.relpath for f in files)


def _robot_session(robot: FakeRobot) -> None:
    """Two finished wpilogs plus the current one; an old hoot session and the current one."""
    _put(robot, f"{ROOT}/FRC_1.wpilog", b"one", 1000)
    _put(robot, f"{ROOT}/FRC_2.wpilog", b"two", 2000)
    _put(robot, f"{ROOT}/FRC_3.wpilog", b"three", 3000)
    _put(robot, f"{ROOT}/S1/rio.hoot", b"rio-old", 1000)
    _put(robot, f"{ROOT}/S2/rio.hoot", b"rio-new", 3000)
    _put(robot, f"{ROOT}/S2/canivore.hoot", b"can-new", 2900)
    _set_dir_mtime(robot, f"{ROOT}/S1", 1000)
    _set_dir_mtime(robot, f"{ROOT}/S2", 2900)


def test_selection_skips_active_files(
    fake_robot: FakeRobot, robot_client: RobotClient, pull_ledger: PullLedger
) -> None:
    _robot_session(fake_robot)
    sleep = Sleep()
    selection = select_robot_files(
        robot_client, pull_ledger, include_active=False, settle_s=5, sleep=sleep
    )
    assert _names(selection.pull) == ["FRC_1.wpilog", "FRC_2.wpilog", "S1/rio.hoot"]
    assert _names(selection.skipped_active) == [
        "FRC_3.wpilog",
        "S2/canivore.hoot",
        "S2/rio.hoot",
    ]
    assert selection.active == frozenset()
    assert sleep.calls == [5]  # one wait for the whole source, not one per file


def test_growing_file_is_not_selected_until_stable(
    fake_robot: FakeRobot, robot_client: RobotClient, pull_ledger: PullLedger
) -> None:
    _robot_session(fake_robot)
    grow = lambda: _put(fake_robot, f"{ROOT}/FRC_2.wpilog", b"two-and-more", 2005)  # noqa: E731
    first = select_robot_files(
        robot_client, pull_ledger, include_active=False, settle_s=5, sleep=Sleep(grow)
    )
    assert "FRC_2.wpilog" not in _names(first.pull)
    assert _names(first.unstable) == ["FRC_2.wpilog"]
    second = select_robot_files(
        robot_client, pull_ledger, include_active=False, settle_s=5, sleep=Sleep()
    )
    assert "FRC_2.wpilog" in _names(second.pull)


def test_current_hoot_session_waits_for_a_newer_session(
    fake_robot: FakeRobot, robot_client: RobotClient, pull_ledger: PullLedger
) -> None:
    _robot_session(fake_robot)
    _put(fake_robot, f"{ROOT}/S2/rio.hoot", b"rio-new-grown", 3100)  # only the rio hoot grew
    _set_dir_mtime(fake_robot, f"{ROOT}/S2", 2900)
    selection = select_robot_files(
        robot_client, pull_ledger, include_active=False, settle_s=5, sleep=Sleep()
    )
    assert not [f for f in selection.pull if f.relpath.startswith("S2/")]
    _put(fake_robot, f"{ROOT}/S3/rio.hoot", b"next", 4000)  # robot code restarted
    _set_dir_mtime(fake_robot, f"{ROOT}/S3", 4000)
    selection = select_robot_files(
        robot_client, pull_ledger, include_active=False, settle_s=5, sleep=Sleep()
    )
    assert {"S2/rio.hoot", "S2/canivore.hoot"} <= set(_names(selection.pull))
    assert "S3/rio.hoot" not in _names(selection.pull)


def test_include_active_pulls_active_files_without_settling(
    fake_robot: FakeRobot, robot_client: RobotClient, pull_ledger: PullLedger
) -> None:
    _robot_session(fake_robot)
    grow_current = lambda: _put(fake_robot, f"{ROOT}/FRC_3.wpilog", b"three+", 3001)  # noqa: E731
    selection = select_robot_files(
        robot_client, pull_ledger, include_active=True, settle_s=5, sleep=Sleep(grow_current)
    )
    assert _names(selection.pull) == [
        "FRC_1.wpilog",
        "FRC_2.wpilog",
        "FRC_3.wpilog",
        "S1/rio.hoot",
        "S2/canivore.hoot",
        "S2/rio.hoot",
    ]
    assert _names(list(selection.active)) == ["FRC_3.wpilog", "S2/canivore.hoot", "S2/rio.hoot"]
    assert selection.skipped_active == []


def test_already_pulled_files_are_not_selected_and_idle_source_does_not_wait(
    fake_robot: FakeRobot, robot_client: RobotClient, pull_ledger: PullLedger
) -> None:
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", b"one", 1000)
    _put(fake_robot, f"{ROOT}/FRC_2.wpilog", b"two", 2000)
    for f in robot_client.list_logs():
        key = PullKey("robot", robot_client.host, f.remote_path, f.size, f.mtime_ns)
        pull_ledger.record_success(key, SHA, PullStatus.VERIFIED, include_active=False)
    sleep = Sleep()
    selection = select_robot_files(
        robot_client, pull_ledger, include_active=False, settle_s=5, sleep=sleep
    )
    assert selection.pull == []
    assert sleep.calls == []


def test_failed_files_are_not_selected(
    fake_robot: FakeRobot, robot_client: RobotClient, pull_ledger: PullLedger
) -> None:
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", b"one", 1000)
    _put(fake_robot, f"{ROOT}/FRC_2.wpilog", b"two", 2000)
    first = next(f for f in robot_client.list_logs() if f.relpath == "FRC_1.wpilog")
    key = PullKey("robot", robot_client.host, first.remote_path, first.size, first.mtime_ns)
    for _ in range(3):
        pull_ledger.record_failure(key, "hash-mismatch")
    selection = select_robot_files(
        robot_client, pull_ledger, include_active=False, settle_s=5, sleep=Sleep()
    )
    assert selection.pull == []
