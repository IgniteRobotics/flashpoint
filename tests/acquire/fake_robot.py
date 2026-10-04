"""In-process fake roboRIO: a paramiko SSH server with read-only SFTP and a tiny exec shell.

The server maps absolute remote paths (``/home/lvuser/logs/x.wpilog``) into ``root``, so a test
places files under ``root / "home/lvuser/logs"``. It supports only read operations; every write,
remove, rename or similar attempt is refused and recorded in ``FakeRobot.write_attempts``.

Fault switches are plain attributes and may be changed at any time; they apply to the next
request (``rotate_host_key`` and ``auth_*`` to the next connection).
"""

import hashlib
import os
import shlex
import socket
import struct
import threading
import time
from pathlib import Path, PurePosixPath
from typing import Any

import paramiko
from paramiko.common import (
    AUTH_FAILED,
    AUTH_SUCCESSFUL,
    OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED,
    OPEN_SUCCEEDED,
)
from paramiko.sftp import (
    CMD_EXTENDED,
    CMD_EXTENDED_REPLY,
    SFTP_FAILURE,
    SFTP_NO_SUCH_FILE,
    SFTP_PERMISSION_DENIED,
)
from paramiko.sftp_attr import SFTPAttributes
from paramiko.sftp_handle import SFTPHandle
from paramiko.sftp_server import SFTPServer
from paramiko.sftp_si import SFTPServerInterface

_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC
_STATVFS_BLOCK = 1024


class FakeRobot:
    """A threaded SSH/SFTP/exec server on a random localhost port serving ``root``."""

    host = "127.0.0.1"

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.port = 0
        self.username = "lvuser"
        self.password: str | None = None  # accepted by password auth when set
        self.allow_none = True  # accept auth_none for ``username`` (the real rio does)

        # Fault switches.
        self.drop_after_bytes: int | None = None  # kill the connection mid-file, per open file
        self.wrong_hash = False  # sha256sum prints a hash that matches no file
        self.no_sha256sum = False  # sha256sum exits 127, "not found"
        self.df_free_kb = 10_000_000  # free space reported by df and statvfs
        self.auth_reject = False  # reject every auth method

        # Observations.
        self.bytes_served = 0  # SFTP file bytes sent to clients
        self.connection_count = 0
        self.exec_log: list[str] = []
        self.write_attempts: list[tuple[str, str]] = []  # (operation, remote path)

        self._lock = threading.Lock()
        self._host_key = paramiko.ECDSAKey.generate()
        self._stopping = threading.Event()
        self._listener: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._transports: list[paramiko.Transport] = []

    # -- lifecycle -------------------------------------------------------------------------

    def start(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind((self.host, 0))
        listener.listen(8)
        listener.settimeout(0.1)
        self._listener = listener
        self.port = listener.getsockname()[1]
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name="fake-robot-accept", daemon=True
        )
        self._accept_thread.start()

    def stop(self) -> None:
        self._stopping.set()
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=5)
        if self._listener is not None:
            self._listener.close()
        with self._lock:
            transports = list(self._transports)
        for transport in transports:
            transport.close()

    # -- switches and observations ---------------------------------------------------------

    def rotate_host_key(self) -> None:
        """Generate a new host key; connections made afterwards present it (a reimaged rio)."""
        self._host_key = paramiko.ECDSAKey.generate()

    @property
    def host_key_fingerprint(self) -> str:
        """The host key's SHA256 fingerprint, OpenSSH style (``SHA256:<base64>``)."""
        return f"SHA256:{self._host_key.fingerprint}"

    def reset_counters(self) -> None:
        with self._lock:
            self.bytes_served = 0
            self.exec_log.clear()
            self.write_attempts.clear()

    # -- internals -------------------------------------------------------------------------

    def resolve(self, remote_path: str) -> Path | None:
        """Map an absolute remote path into ``root``; None if it would escape."""
        pure = PurePosixPath("/") / remote_path
        parts: list[str] = []
        for part in pure.parts[1:]:
            if part == "..":
                if not parts:
                    return None
                parts.pop()
            elif part != ".":
                parts.append(part)
        return self.root.joinpath(*parts)

    def add_bytes(self, count: int) -> None:
        with self._lock:
            self.bytes_served += count

    def record_exec(self, command: str) -> None:
        with self._lock:
            self.exec_log.append(command)

    def record_write(self, operation: str, path: str) -> None:
        with self._lock:
            self.write_attempts.append((operation, path))

    def _accept_loop(self) -> None:
        assert self._listener is not None
        while not self._stopping.is_set():
            try:
                conn, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            conn.settimeout(None)
            transport = paramiko.Transport(conn)
            transport.add_server_key(self._host_key)
            transport.set_subsystem_handler(
                "sftp", _StatvfsSFTPServer, _ReadOnlySFTP, robot=self, transport=transport
            )
            with self._lock:
                self.connection_count += 1
                self._transports.append(transport)
            try:
                transport.start_server(server=_Server(self))
            except (paramiko.SSHException, OSError):
                transport.close()


def _packets_sent(channel: paramiko.Channel) -> int:
    packetizer: Any = channel.get_transport().packetizer
    return int(packetizer._Packetizer__sequence_number_out)


class _Server(paramiko.ServerInterface):
    def __init__(self, robot: FakeRobot) -> None:
        self.robot = robot

    def get_allowed_auths(self, username: str) -> str:
        return "none,password"

    def check_auth_none(self, username: str) -> int:
        ok = self.robot.allow_none and not self.robot.auth_reject
        return AUTH_SUCCESSFUL if ok and username == self.robot.username else AUTH_FAILED

    def check_auth_password(self, username: str, password: str) -> int:
        robot = self.robot
        ok = robot.password is not None and password == robot.password and not robot.auth_reject
        return AUTH_SUCCESSFUL if ok and username == robot.username else AUTH_FAILED

    def check_channel_request(self, kind: str, chanid: int) -> int:
        return OPEN_SUCCEEDED if kind == "session" else OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_channel_exec_request(self, channel: paramiko.Channel, command: bytes) -> bool:
        sent_before = _packets_sent(channel)
        threading.Thread(
            target=self._run,
            args=(channel, command.decode(), sent_before),
            name="fake-robot-exec",
            daemon=True,
        ).start()
        return True

    def _run(self, channel: paramiko.Channel, command: str, sent_before: int) -> None:
        self.robot.record_exec(command)
        # paramiko sends the exec request's success reply after this handler returns; output
        # sent before it makes the client see "Channel closed". Wait for one more packet out.
        deadline = time.monotonic() + 2
        while _packets_sent(channel) <= sent_before and time.monotonic() < deadline:
            time.sleep(0.001)
        try:
            code, out, err = self._execute(command)
            if out:
                channel.sendall(out)
            if err:
                channel.sendall_stderr(err)
            channel.send_exit_status(code)
        except (OSError, EOFError, paramiko.SSHException):
            pass
        finally:
            channel.close()

    def _execute(self, command: str) -> tuple[int, bytes, bytes]:
        try:
            argv = shlex.split(command)
        except ValueError:
            return 2, b"", b"sh: syntax error\n"
        if not argv:
            return 0, b"", b""
        name, args = argv[0], [a for a in argv[1:] if a != "--"]
        if name == "sha256sum":
            return self._sha256sum(args)
        if name == "df":
            return self._df(args)
        return 127, b"", f"sh: {name}: not found\n".encode()

    def _sha256sum(self, args: list[str]) -> tuple[int, bytes, bytes]:
        if self.robot.no_sha256sum:
            return 127, b"", b"sh: sha256sum: not found\n"
        out, err, code = [], [], 0
        for arg in args:
            real = self.robot.resolve(arg)
            if real is None or not real.is_file():
                err.append(f"sha256sum: {arg}: No such file or directory\n")
                code = 1
                continue
            digest = hashlib.sha256(real.read_bytes()).hexdigest()
            if self.robot.wrong_hash:
                digest = hashlib.sha256(digest.encode()).hexdigest()
            out.append(f"{digest}  {arg}\n")
        return code, "".join(out).encode(), "".join(err).encode()

    def _df(self, args: list[str]) -> tuple[int, bytes, bytes]:
        paths = [a for a in args if not a.startswith("-")] or ["/"]
        total = max(self.robot.df_free_kb * 2, 1)
        used = total - self.robot.df_free_kb
        lines = ["Filesystem     1024-blocks      Used Available Capacity Mounted on\n"]
        for _ in paths:
            pct = used * 100 // total
            lines.append(
                f"/dev/root {total:>15} {used:>9} {self.robot.df_free_kb:>9} {pct:>7}% /\n"
            )
        return 0, "".join(lines).encode(), b""


class _ReadHandle(SFTPHandle):
    def __init__(self, robot: FakeRobot, transport: paramiko.Transport, path: Path) -> None:
        super().__init__()
        self._robot = robot
        self._transport = transport
        self._file = path.open("rb")
        self._served = 0

    def read(self, offset: int, length: int) -> bytes | int:
        limit = self._robot.drop_after_bytes
        if limit is not None:
            allowed = limit - self._served
            if allowed <= 0:
                self._transport.close()
                return SFTP_FAILURE
            length = min(length, allowed)
        self._file.seek(offset)
        data = self._file.read(length)
        self._served += len(data)
        self._robot.add_bytes(len(data))
        return data

    def stat(self) -> SFTPAttributes | int:
        return SFTPAttributes.from_stat(os.fstat(self._file.fileno()))

    def close(self) -> None:
        self._file.close()
        super().close()


class _ReadOnlySFTP(SFTPServerInterface):
    def __init__(
        self,
        server: paramiko.ServerInterface,
        *args: Any,
        robot: FakeRobot,
        transport: paramiko.Transport,
        **kwargs: Any,
    ) -> None:
        super().__init__(server, *args, **kwargs)
        self.robot = robot
        self.transport = transport

    def canonicalize(self, path: str) -> str:
        return str(PurePosixPath("/") / path)

    def _attrs(self, path: str, follow: bool) -> SFTPAttributes | int:
        real = self.robot.resolve(path)
        if real is None:
            return SFTP_NO_SUCH_FILE
        try:
            st = real.stat() if follow else real.lstat()
        except OSError:
            return SFTP_NO_SUCH_FILE
        attrs = SFTPAttributes.from_stat(st)
        attrs.filename = PurePosixPath(path).name
        return attrs

    def stat(self, path: str) -> SFTPAttributes | int:
        return self._attrs(path, follow=True)

    def lstat(self, path: str) -> SFTPAttributes | int:
        return self._attrs(path, follow=False)

    def list_folder(self, path: str) -> list[SFTPAttributes] | int:
        real = self.robot.resolve(path)
        if real is None or not real.is_dir():
            return SFTP_NO_SUCH_FILE
        entries = []
        for child in sorted(real.iterdir()):
            attrs = SFTPAttributes.from_stat(child.lstat())
            attrs.filename = child.name
            entries.append(attrs)
        return entries

    def open(self, path: str, flags: int, attr: SFTPAttributes) -> SFTPHandle | int:
        if flags & _WRITE_FLAGS:
            self.robot.record_write("open-write", path)
            return SFTP_PERMISSION_DENIED
        real = self.robot.resolve(path)
        if real is None or not real.is_file():
            return SFTP_NO_SUCH_FILE
        return _ReadHandle(self.robot, self.transport, real)

    def remove(self, path: str) -> int:
        self.robot.record_write("remove", path)
        return SFTP_PERMISSION_DENIED

    def rename(self, oldpath: str, newpath: str) -> int:
        self.robot.record_write("rename", oldpath)
        return SFTP_PERMISSION_DENIED

    def posix_rename(self, oldpath: str, newpath: str) -> int:
        self.robot.record_write("rename", oldpath)
        return SFTP_PERMISSION_DENIED

    def mkdir(self, path: str, attr: SFTPAttributes) -> int:
        self.robot.record_write("mkdir", path)
        return SFTP_PERMISSION_DENIED

    def rmdir(self, path: str) -> int:
        self.robot.record_write("rmdir", path)
        return SFTP_PERMISSION_DENIED

    def chattr(self, path: str, attr: SFTPAttributes) -> int:
        self.robot.record_write("chattr", path)
        return SFTP_PERMISSION_DENIED

    def symlink(self, target_path: str, path: str) -> int:
        self.robot.record_write("symlink", path)
        return SFTP_PERMISSION_DENIED

    def readlink(self, path: str) -> str | int:
        return SFTP_FAILURE

    def statvfs(self, path: str) -> bytes | None:
        """Pack an OpenSSH ``statvfs@openssh.com`` reply body from ``df_free_kb``."""
        free_blocks = self.robot.df_free_kb
        total = free_blocks * 2
        return struct.pack(
            ">11Q",
            _STATVFS_BLOCK,  # f_bsize
            _STATVFS_BLOCK,  # f_frsize
            total,  # f_blocks
            free_blocks,  # f_bfree
            free_blocks,  # f_bavail
            1_000_000,  # f_files
            900_000,  # f_ffree
            900_000,  # f_favail
            0,  # f_fsid
            0,  # f_flag
            255,  # f_namemax
        )


class _StatvfsSFTPServer(SFTPServer):
    """paramiko's SFTP server lacks ``statvfs@openssh.com``; answer it from the interface."""

    def _process(self, t: int, request_number: int, msg: paramiko.Message) -> None:
        me: Any = self  # paramiko's stubs omit the private SFTPServer helpers used here
        if t == CMD_EXTENDED:
            start = msg.packet.tell()
            tag = msg.get_text()
            if tag == "statvfs@openssh.com":
                path = msg.get_text()
                body = me.server.statvfs(path)
                if body is None:
                    me._send_status(request_number, SFTP_FAILURE)
                    return
                reply = paramiko.Message()
                reply.add_int(request_number)
                reply.add_bytes(body)
                me._send_packet(CMD_EXTENDED_REPLY, reply)
                return
            msg.packet.seek(start)
        super()._process(t, request_number, msg)  # type: ignore[misc]
