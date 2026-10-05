import hashlib
import stat
import sys
from pathlib import Path

import pytest

from flashpoint.readers import hoot
from flashpoint.readers.hoot import HootError, OwletRegistry


def _hoot_bytes(compliancy: int, bus: str = "roboRIO Native CAN Bus") -> bytes:
    header = bus.encode().ljust(70, b"\x00") + bytes([compliancy, 1]) + b"\x00" * 8
    return header + b"\xaa" * 32


def test_reads_header(tmp_path: Path) -> None:
    path = tmp_path / "a.hoot"
    path.write_bytes(_hoot_bytes(19))
    header = hoot.read_header(path)
    assert header.compliancy == 19
    assert header.bus_description == "roboRIO Native CAN Bus"


@pytest.mark.parametrize(
    ("content", "reason"),
    [(b"", "empty-file"), (b"short", "invalid-header"), (_hoot_bytes(5), "too-old")],
)
def test_header_errors(tmp_path: Path, content: bytes, reason: str) -> None:
    path = tmp_path / "bad.hoot"
    path.write_bytes(content)
    with pytest.raises(HootError) as exc:
        hoot.read_header(path)
    assert exc.value.reason == reason


def test_platform_key_known() -> None:
    assert hoot.platform_key() in hoot.PLATFORMS


# --- registry --------------------------------------------------------------------------

FAKE_OWLET = """#!{python}
import sys
args = sys.argv[1:]
if "--check-pro" in args:
    print("hoot file is Pro-licensed")
elif "--scan" in args:
    print("RobotEnable: 1ff00")
    print("TalonFX-1/SupplyCurrent: 6f40c00")
    print("TalonFX-1/PIDDutyCycle_Output: 71f0c00")
    print("TalonFX-1/DeviceTemp: 6f60c00")
else:
    src, dst = args[0], args[1]
    sel = args[args.index("-s") + 1] if "-s" in args else "all"
    open(dst, "w").write("converted " + sel)
"""


@pytest.fixture
def fake_registry(tmp_path: Path) -> OwletRegistry:
    binary = tmp_path / "dist" / "owlet"
    binary.parent.mkdir()
    binary.write_text(FAKE_OWLET.format(python=sys.executable))
    binary.chmod(binary.stat().st_mode | stat.S_IEXEC)
    sha = hashlib.sha256(binary.read_bytes()).hexdigest()
    manifest = tmp_path / "manifest.toml"
    manifest.write_text(
        f"""[[owlet]]
channel = "test"
compliancy = 19
version = "9.9.9"
[owlet.platforms."{hoot.platform_key()}"]
url = "{binary.as_uri()}"
sha256 = "{sha}"
"""
    )
    return OwletRegistry(manifest, tmp_path / "cache")


def test_registry_downloads_verifies_and_caches(fake_registry: OwletRegistry) -> None:
    first = fake_registry.binary_for(19)
    assert first.is_file()
    assert first.name.startswith("owlet-9.9.9-C19")
    assert fake_registry.binary_for(19) == first


def test_registry_works_offline_once_cached(fake_registry: OwletRegistry, tmp_path: Path) -> None:
    cached = fake_registry.binary_for(19)
    (tmp_path / "dist" / "owlet").unlink()  # the "network" is gone
    assert fake_registry.binary_for(19) == cached


def test_registry_refuses_tampered_binary(fake_registry: OwletRegistry) -> None:
    cached = fake_registry.binary_for(19)
    cached.write_text("#!/bin/sh\necho evil\n")
    with pytest.raises(HootError) as exc:
        fake_registry.binary_for(19, download=False)
    assert exc.value.reason == "owlet-checksum-mismatch"


def test_registry_unknown_compliancy(fake_registry: OwletRegistry) -> None:
    with pytest.raises(HootError) as exc:
        fake_registry.binary_for(42)
    assert exc.value.reason == "unsupported-compliancy:42"


def test_health_profile_selects_health_signals() -> None:
    signals = {
        "RobotEnable": "1ff00",
        "TalonFX-1/SupplyCurrent": "6f40c00",
        "TalonFX-1/PIDDutyCycle_Output": "71f0c00",
        "TalonFX-1/DeviceTemp": "6f60c00",
    }
    assert set(hoot.select_signals(signals, "health") or []) == {"1ff00", "6f40c00", "6f60c00"}
    assert hoot.select_signals(signals, "all") is None


def test_convert_with_profile(
    fake_registry: OwletRegistry, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if sys.platform == "win32":
        # Windows can't exec a shebang script; hand the fake owlet to the interpreter.
        run = hoot._run
        monkeypatch.setattr(hoot, "_run", lambda args: run([sys.executable, *args]))
    src = tmp_path / "x.hoot"
    src.write_bytes(_hoot_bytes(19))
    result = hoot.convert(src, tmp_path / "out", fake_registry, profile="health")

    assert result.wpilog.read_text() == "converted 1ff00,6f40c00,6f60c00"
    assert result.compliancy == 19
    assert result.owlet_version == "9.9.9"
    assert result.pro_licensed is True
    assert result.profile == "health"
    assert result.signal_count == 3


def test_convert_unknown_profile(fake_registry: OwletRegistry, tmp_path: Path) -> None:
    src = tmp_path / "x.hoot"
    src.write_bytes(_hoot_bytes(19))
    with pytest.raises(ValueError, match="profile"):
        hoot.convert(src, tmp_path / "out", fake_registry, profile="bogus")


def test_concurrent_first_download_is_safe(fake_registry: OwletRegistry) -> None:
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=8) as pool:
        paths = list(pool.map(lambda _: fake_registry.binary_for(19), range(16)))

    assert len(set(paths)) == 1
    assert fake_registry.binary_for(19, download=False) == paths[0]
    assert not list(paths[0].parent.glob("*.part"))


def test_health_profile_includes_alignment_and_physics_signals() -> None:
    signals = {
        "DriveState/Pose": "a",
        "DriveState/ModuleStates": "b",
        "TalonFX-1/RotorVelocity": "c",
        "TalonFX-1/MotorKT": "d",
        "TalonFX-1/MotorKV": "e",
        "TalonFX-1/MotorStallCurrent": "f",
        "TalonFX-1/PIDVelocity_Reference": "g",
    }
    assert set(hoot.select_signals(signals, "health") or []) == {"a", "c", "d", "e", "f"}
