"""Read-only SSH/SFTP client for a roboRIO: discovery, log listing, remote hash, free space."""

import json
import logging
import posixpath
import shlex
import socket
import stat
import struct
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import paramiko
from paramiko.sftp import CMD_EXTENDED

from flashpoint.acquire.config import AcquireConfig
from flashpoint.config import cache_root

log = logging.getLogger(__name__)

DEFAULT_PORT = 22
TCP_TIMEOUT_S = 2.0
AUTH_TIMEOUT_S = 10.0
SFTP_TIMEOUT_S = 30.0
EXEC_TIMEOUT_S = 300.0  # sha256sum of a large hoot on the rio's slow flash
AUTH_ERROR_INTERVAL_S = 3600.0
LOG_SUFFIXES = (".wpilog", ".hoot")
KNOWN_ROBOTS_FILE = "known-robots.json"
COMMAND_NOT_FOUND = 127
SHA256_HEX_LEN = 64
_STATVFS_FORMAT = ">11Q"  # f_bsize, f_frsize, f_blocks, f_bfree, f_bavail, ...
_BYTES_PER_MB = 1024 * 1024


@dataclass(frozen=True)
class RemoteFile:
    """A log file on the robot: `root` as configured, `relpath` POSIX relative to it."""

    root: str
    relpath: str
    size: int
    mtime_ns: int

    @property
    def remote_path(self) -> str:
        return posixpath.join(self.root, self.relpath)


@dataclass(frozen=True)
class LowSpaceWarning:
    host: str
    root: str
    free_bytes: int


def low_space_warning(
    host: str, root: str, free_bytes: int, threshold_mb: int
) -> LowSpaceWarning | None:
    """A warning when `free_bytes` is below the threshold; None means no warning (or cleared)."""
    if free_bytes < threshold_mb * _BYTES_PER_MB:
        return LowSpaceWarning(host, root, free_bytes)
    return None


def split_address(candidate: str) -> tuple[str, int]:
    """`host` or `host:port` to (host, port); the port defaults to 22."""
    host, sep, port = candidate.rpartition(":")
    if sep and port.isdigit():
        return host, int(port)
    return candidate, DEFAULT_PORT


class AuthErrorThrottle:
    """Allows one auth/session error per host per interval. Share one across cycles."""

    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        interval_s: float = AUTH_ERROR_INTERVAL_S,
    ) -> None:
        self._clock = clock
        self._interval_s = interval_s
        self._last: dict[str, float] = {}

    def should_log(self, host: str) -> bool:
        now = self._clock()
        last = self._last.get(host)
        if last is not None and now - last < self._interval_s:
            return False
        self._last[host] = now
        return True


_DEFAULT_THROTTLE = AuthErrorThrottle()


class KnownRobots:
    """Host key fingerprints by candidate address, in `<cache_root>/known-robots.json`."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def _load(self) -> dict[str, str]:
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if not isinstance(data, dict):
            return {}
        return {str(k): str(v) for k, v in data.items()}

    def check_and_record(self, host: str, fingerprint: str) -> None:
        """Record a new key; a changed key is replaced with a warning (reimaged rio)."""
        known = self._load()
        previous = known.get(host)
        if previous == fingerprint:
            return
        if previous is not None:
            log.warning(
                "host key for %s changed (%s -> %s); accepting it (robot reimaged?)",
                host,
                previous,
                fingerprint,
            )
        known[host] = fingerprint
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(json.dumps(known, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)


class RobotClient:
    """A connected robot. Only reads: listing, stat, `sha256sum`, `df`, `statvfs`."""

    def __init__(
        self,
        host: str,
        transport: paramiko.Transport,
        sftp: paramiko.SFTPClient,
        config: AcquireConfig,
    ) -> None:
        self.host = host
        self._transport = transport
        self._sftp = sftp
        self._config = config

    @classmethod
    def connect(
        cls,
        config: AcquireConfig,
        *,
        known_robots: Path | None = None,
        throttle: AuthErrorThrottle | None = None,
        tcp_timeout: float = TCP_TIMEOUT_S,
        auth_timeout: float = AUTH_TIMEOUT_S,
    ) -> Self | None:
        """Connect to the first candidate that answers; None when no robot is reachable.

        An unreachable host is silent. An auth or session failure is logged at most once per
        host per hour (via `throttle`) and the next candidate is tried.
        """
        store = KnownRobots(known_robots or cache_root() / KNOWN_ROBOTS_FILE)
        gate = throttle or _DEFAULT_THROTTLE
        for candidate in config.hosts:
            host, port = split_address(candidate)
            try:
                sock = socket.create_connection((host, port), timeout=tcp_timeout)
            except OSError as exc:
                log.debug("robot %s unreachable: %s", candidate, exc)
                continue
            try:
                return cls._open(candidate, sock, config, store, auth_timeout)
            except (paramiko.SSHException, OSError, EOFError) as exc:
                sock.close()
                if gate.should_log(candidate):
                    log.error("robot %s: cannot open a session: %s", candidate, exc)
        return None

    @classmethod
    def _open(
        cls,
        candidate: str,
        sock: socket.socket,
        config: AcquireConfig,
        store: KnownRobots,
        auth_timeout: float,
    ) -> Self:
        transport = paramiko.Transport(sock)
        transport.banner_timeout = auth_timeout
        transport.auth_timeout = auth_timeout
        try:
            transport.start_client(timeout=auth_timeout)
            store.check_and_record(candidate, transport.get_remote_server_key().fingerprint)
            _authenticate(transport, config)
            sftp = paramiko.SFTPClient.from_transport(transport)
            if sftp is None:
                raise paramiko.SSHException("SFTP subsystem refused")
            sftp.get_channel().settimeout(SFTP_TIMEOUT_S)  # type: ignore[union-attr]
        except BaseException:
            transport.close()
            raise
        return cls(candidate, transport, sftp, config)

    def close(self) -> None:
        self._sftp.close()
        self._transport.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def list_logs(self) -> list[RemoteFile]:
        """Every `.wpilog`/`.hoot` under the configured roots, sorted by (root, relpath)."""
        found: list[RemoteFile] = []
        for root in self._config.roots:
            self._walk(root, "", root, found)
        return sorted(found, key=lambda e: (e.root, e.relpath))

    def _walk(self, root: str, rel: str, directory: str, found: list[RemoteFile]) -> None:
        try:
            attrs = self._sftp.listdir_attr(directory)
        except FileNotFoundError:
            return  # a missing root (no USB stick in the rio) is normal
        for attr in attrs:
            child_rel = posixpath.join(rel, attr.filename) if rel else attr.filename
            mode = attr.st_mode or 0
            if stat.S_ISDIR(mode):
                self._walk(root, child_rel, posixpath.join(root, child_rel), found)
            elif stat.S_ISREG(mode) and attr.filename.lower().endswith(LOG_SUFFIXES):
                found.append(
                    RemoteFile(root, child_rel, attr.st_size or 0, (attr.st_mtime or 0) * 10**9)
                )

    def remote_sha256(self, remote_path: str) -> str | None:
        """The robot's SHA-256 of a file; None when the robot has no `sha256sum`."""
        code, out, err = self._exec(f"sha256sum -- {shlex.quote(remote_path)}")
        if code == COMMAND_NOT_FOUND:
            return None
        digest = out.split()[0].lower() if out.split() else ""
        if code != 0 or len(digest) != SHA256_HEX_LEN:
            raise OSError(f"sha256sum {remote_path} failed ({code}): {err.strip() or out.strip()}")
        return digest

    def free_bytes(self, remote_root: str) -> int | None:
        """Free bytes on the filesystem holding `remote_root`: `df -Pk`, else SFTP `statvfs`."""
        try:
            code, out, _ = self._exec(f"df -Pk -- {shlex.quote(remote_root)}")
            if code == 0:
                return int(out.splitlines()[1].split()[3]) * 1024
        except (OSError, ValueError, IndexError, paramiko.SSHException):
            pass
        return self._statvfs_free(remote_root)

    def _statvfs_free(self, remote_path: str) -> int | None:
        # paramiko has no statvfs; send the OpenSSH extension by hand. Best-effort: a rio may
        # not implement it.
        sftp: Any = self._sftp
        try:
            _, reply = sftp._request(CMD_EXTENDED, "statvfs@openssh.com", remote_path)
            fields = struct.unpack(_STATVFS_FORMAT, reply.get_remainder()[:88])
        except (OSError, struct.error, paramiko.SSHException):
            return None
        return int(fields[1] * fields[4])

    def low_space_warnings(self) -> list[LowSpaceWarning]:
        """One warning per configured root below `low_space_mb`; unreadable roots are skipped."""
        warnings: list[LowSpaceWarning] = []
        for root in self._config.roots:
            free = self.free_bytes(root)
            if free is None:
                continue
            warning = low_space_warning(self.host, root, free, self._config.low_space_mb)
            if warning is not None:
                warnings.append(warning)
        return warnings

    def _exec(self, command: str) -> tuple[int, str, str]:
        channel = self._transport.open_session(timeout=AUTH_TIMEOUT_S)
        try:
            channel.settimeout(EXEC_TIMEOUT_S)
            channel.exec_command(command)
            out = channel.makefile("rb").read().decode("utf-8", "replace")
            err = channel.makefile_stderr("rb").read().decode("utf-8", "replace")
            return channel.recv_exit_status(), out, err
        finally:
            channel.close()


def _authenticate(transport: paramiko.Transport, config: AcquireConfig) -> None:
    """`auth_none` first (the rio's default), then the configured password."""
    try:
        if not transport.auth_none(config.user):
            return
    except paramiko.AuthenticationException:
        pass
    transport.auth_password(config.user, config.password)
