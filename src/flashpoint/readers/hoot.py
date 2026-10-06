"""CTRE Phoenix 6 hoot logs: header probing, owlet registry, and conversion to wpilog.

owlet only converts hoots whose format version (compliancy) it was built for. The
compliancy is byte 70 of the hoot header (the same probe AdvantageScope uses). Binaries
come from a committed manifest generated from CTRE's redist index
(tools/update-owlet-manifest.py). They are verified by SHA-256 and cached for offline use.
"""

import hashlib
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import tomllib
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).with_name("owlet-manifest.toml")
PLATFORMS = ("macosuniversal", "linuxx86-64", "linuxarm64", "windowsx86-64")

HEADER_SIZE = 72
COMPLIANCY_OFFSET = 70
MIN_COMPLIANCY = 6  # Phoenix 2024; older hoots cannot be decoded
OWLET_TIMEOUT_S = 600
OWLET_ATTEMPTS = 2  # one retry: the Linux owlet fails at random on healthy hoots (#17)
# owlet also drops the tail of an export at random, with exit 0 and no marker (#17). Export
# twice, a third time if the two differ, and keep the largest output.
OWLET_EXPORT_RUNS_MAX = 3
# owlet prints this when a hoot ends mid-record (e.g. the robot lost power). It still
# writes everything it could read, so the partial data is kept and flagged.
INCOMPLETE_READ_MARKER = "Could not read to end of input file"

PROFILES = ("health", "all")
# Per-device health signals plus robot-level state. Raw hoots are kept, so this list
# can widen later and `flashpoint rebuild` recovers the extra signals.
HEALTH_PATTERN = re.compile(
    r"^(RobotEnable|RobotMode|DS:IsFMSAttached)$"
    r"|^DriveState/Pose$"  # also published to NetworkTables: anchors hoot-to-wpilog clock alignment
    r"|/(MotorVoltage|SupplyVoltage|StatorCurrent|SupplyCurrent|TorqueCurrent"
    r"|DeviceTemp|ProcessorTemp|AncillaryDeviceTemp|Velocity|RotorVelocity|Position|DutyCycle"
    r"|DeviceEnable|FaultField|StickyFaultField|ConnectedMotor|Version"
    r"|MotorKT|MotorKV|MotorStallCurrent"  # device-reported motor model for current residuals
    r"|Fault_\w+)$"
)


class HootError(Exception):
    """A hoot that can't be converted. `reason` is a stable quarantine code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


@dataclass(frozen=True)
class HootHeader:
    compliancy: int
    bus_description: str


@dataclass(frozen=True)
class HootConversion:
    wpilog: Path
    compliancy: int
    owlet_version: str
    pro_licensed: bool
    profile: str
    signal_count: int
    bus_description: str
    warnings: tuple[str, ...] = ()


def read_header(path: Path) -> HootHeader:
    with path.open("rb") as f:
        head = f.read(HEADER_SIZE)
    if not head:
        raise HootError("empty-file")
    if len(head) < HEADER_SIZE:
        raise HootError("invalid-header", f"{len(head)} bytes")
    compliancy = head[COMPLIANCY_OFFSET]
    if compliancy < MIN_COMPLIANCY:
        raise HootError("too-old", f"compliancy {compliancy}")
    bus = head[:COMPLIANCY_OFFSET].split(b"\x00", 1)[0].decode(errors="replace")
    return HootHeader(compliancy, bus)


def platform_key() -> str:
    machine = platform.machine().lower()
    if sys.platform == "darwin":
        return "macosuniversal"
    if sys.platform.startswith("win"):
        return "windowsx86-64"
    return "linuxarm64" if machine in ("aarch64", "arm64") else "linuxx86-64"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


class OwletRegistry:
    """Resolve, download, and verify the owlet build for a hoot compliancy."""

    def __init__(self, manifest: Path, cache_dir: Path) -> None:
        with manifest.open("rb") as f:
            data: dict[str, Any] = tomllib.load(f)
        self._entries = {int(e["compliancy"]): e for e in data.get("owlet", [])}
        self.cache_dir = cache_dir
        self._lock = threading.Lock()  # parallel conversions share the first download

    def compliancies(self) -> list[int]:
        return sorted(self._entries)

    def version_for(self, compliancy: int) -> str:
        return str(self._entry(compliancy)["version"])

    def _entry(self, compliancy: int) -> dict[str, Any]:
        if compliancy not in self._entries:
            raise HootError(f"unsupported-compliancy:{compliancy}")
        entry: dict[str, Any] = self._entries[compliancy]
        return entry

    def cached_path(self, compliancy: int) -> Path:
        entry = self._entry(compliancy)
        suffix = ".exe" if platform_key() == "windowsx86-64" else ""
        return self.cache_dir / f"owlet-{entry['version']}-C{compliancy}{suffix}"

    def binary_for(self, compliancy: int, download: bool = True) -> Path:
        with self._lock:
            return self._resolve(compliancy, download)

    def _resolve(self, compliancy: int, download: bool) -> Path:
        entry = self._entry(compliancy)
        build = entry.get("platforms", {}).get(platform_key())
        if build is None:
            raise HootError("owlet-unavailable-for-platform", platform_key())
        target = self.cached_path(compliancy)
        if target.is_file():
            if _sha256(target) == build["sha256"]:
                return target
            if not download:
                raise HootError("owlet-checksum-mismatch", str(target))
        elif not download:
            raise HootError("owlet-not-cached", str(target))
        self._download(build["url"], build["sha256"], target)
        return target

    def _download(self, url: str, sha256: str, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        # Unique per process and thread, so concurrent downloaders never share a temp file.
        tmp = target.with_name(f"{target.name}.{os.getpid()}.{threading.get_ident()}.part")
        try:
            with urllib.request.urlopen(url, timeout=60) as response, tmp.open("wb") as out:
                shutil.copyfileobj(response, out, 1 << 20)
        except OSError as exc:
            tmp.unlink(missing_ok=True)
            raise HootError("owlet-download-failed", f"{url}: {exc}") from exc
        if _sha256(tmp) != sha256:
            tmp.unlink(missing_ok=True)
            raise HootError("owlet-checksum-mismatch", url)
        tmp.chmod(0o755)
        tmp.replace(target)


def default_registry(cache_root: Path) -> OwletRegistry:
    return OwletRegistry(MANIFEST_PATH, cache_root / "owlet")


@dataclass(frozen=True)
class _OwletOutput:
    stdout: str
    incomplete: bool


def _run(args: list[str]) -> _OwletOutput:
    """Run owlet; a failed run is retried once (#17: the Linux owlet fails at random).

    An incomplete read is a result, not a failure, and a timeout is not retried.
    """
    detail = ""
    for attempt in range(1, OWLET_ATTEMPTS + 1):
        try:
            done = subprocess.run(args, capture_output=True, text=True, timeout=OWLET_TIMEOUT_S)
        except subprocess.TimeoutExpired as exc:
            raise HootError("owlet-timeout", " ".join(args[:2])) from exc
        incomplete = INCOMPLETE_READ_MARKER in done.stderr or INCOMPLETE_READ_MARKER in done.stdout
        if done.returncode == 0 or incomplete:
            return _OwletOutput(done.stdout, incomplete)
        last = (done.stderr or done.stdout).strip().splitlines()[-1:] or ["no output"]
        detail = f"exit {done.returncode}: {last[0]}"
        if attempt < OWLET_ATTEMPTS:
            LOG.warning(
                "owlet failed on %s (%s); retrying", args[1] if len(args) > 1 else "", detail
            )
    raise HootError("owlet-failed", detail)


def scan_signals(owlet: Path, hoot_path: Path) -> dict[str, str]:
    """Map signal name -> owlet signal id (from `owlet --scan`)."""
    signals: dict[str, str] = {}
    for line in _run([str(owlet), str(hoot_path), "--scan"]).stdout.splitlines():
        name, sep, signal_id = line.rpartition(":")
        if sep and name.strip() and signal_id.strip():
            signals[name.strip()] = signal_id.strip()
    return signals


def select_signals(signals: dict[str, str], profile: str) -> list[str] | None:
    """Signal ids to export for a profile; None means export everything."""
    if profile not in PROFILES:
        raise ValueError(f"unknown profile {profile!r}; expected one of {PROFILES}")
    if profile == "all":
        return None
    return [sid for name, sid in signals.items() if HEALTH_PATTERN.search(name)]


def check_pro(owlet: Path, hoot_path: Path) -> bool:
    return "is pro-licensed" in _run([str(owlet), str(hoot_path), "--check-pro"]).stdout.lower()


def _export_once(
    owlet: Path, hoot_path: Path, out_dir: Path, selected: list[str] | None
) -> tuple[Path, _OwletOutput]:
    with tempfile.NamedTemporaryFile(dir=out_dir, suffix=".wpilog", delete=False) as tmp:
        wpilog = Path(tmp.name)
    args = [str(owlet), str(hoot_path), str(wpilog), "-f", "wpilog"]
    if selected is not None:
        args += ["-s", ",".join(selected)]
    try:
        output = _run(args)
        if not wpilog.is_file() or wpilog.stat().st_size == 0:
            raise HootError("owlet-failed", "no output written")
    except HootError:
        wpilog.unlink(missing_ok=True)
        raise
    return wpilog, output


def _export(
    owlet: Path, hoot_path: Path, out_dir: Path, selected: list[str] | None
) -> tuple[Path, _OwletOutput]:
    """Export until two runs match in size (at most OWLET_EXPORT_RUNS_MAX); keep the largest."""
    runs = [_export_once(owlet, hoot_path, out_dir, selected)]
    for _ in range(OWLET_EXPORT_RUNS_MAX - 1):
        try:
            runs.append(_export_once(owlet, hoot_path, out_dir, selected))
        except HootError as exc:
            LOG.warning(
                "extra owlet export of %s failed (%s); keeping earlier output", hoot_path, exc
            )
            continue
        sizes = sorted(p.stat().st_size for p, _ in runs)
        if sizes[-1] == sizes[-2]:
            break
    sizes = [p.stat().st_size for p, _ in runs]
    if len(set(sizes)) > 1:
        LOG.info("owlet exports of %s differed in size %s; keeping the largest", hoot_path, sizes)
    best = max(runs, key=lambda run: run[0].stat().st_size)
    for path, _ in runs:
        if path != best[0]:
            path.unlink(missing_ok=True)
    return best


def convert(
    hoot_path: Path, out_dir: Path, registry: OwletRegistry, profile: str = "health"
) -> HootConversion:
    """Convert a hoot to wpilog in out_dir using the owlet that matches its compliancy."""
    if profile not in PROFILES:
        raise ValueError(f"unknown profile {profile!r}; expected one of {PROFILES}")
    header = read_header(hoot_path)
    owlet = registry.binary_for(header.compliancy)
    signals = scan_signals(owlet, hoot_path)
    selected = select_signals(signals, profile)
    if selected is not None and not selected:
        raise HootError("no-signals-for-profile", profile)
    out_dir.mkdir(parents=True, exist_ok=True)
    wpilog, output = _export(owlet, hoot_path, out_dir, selected)
    return HootConversion(
        wpilog=wpilog,
        compliancy=header.compliancy,
        owlet_version=registry.version_for(header.compliancy),
        pro_licensed=check_pro(owlet, hoot_path),
        profile=profile,
        signal_count=len(signals) if selected is None else len(selected),
        bus_description=header.bus_description,
        warnings=("incomplete-read",) if output.incomplete else (),
    )
