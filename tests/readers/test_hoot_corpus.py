"""Real owlet builds from the manifest against corpus hoots (downloads once, then cached)."""

from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.readers import hoot
from flashpoint.readers.hoot import HootError
from flashpoint.readers.wpilog import read_wpilog

pytestmark = pytest.mark.corpus


@pytest.fixture(scope="module")
def registry() -> hoot.OwletRegistry:
    return hoot.default_registry(config.cache_root())


def _pick(paths: list[Path], bus: str) -> Path:
    return next(p for p in paths if p.suffix == ".hoot" and (("_rio_" in p.name) == (bus == "rio")))


@pytest.mark.parametrize(
    ("group", "bus", "compliancy"),
    [("2026-gacmp-q7", "rio", 19), ("2025-gacmp-q19", "rio", 13)],
)
def test_converts_with_matching_owlet(
    group: str,
    bus: str,
    compliancy: int,
    corpus_group: Callable[[str], list[Path]],
    registry: hoot.OwletRegistry,
    tmp_path: Path,
) -> None:
    result = hoot.convert(_pick(corpus_group(group), bus), tmp_path, registry)

    assert result.compliancy == compliancy
    assert result.owlet_version == registry.version_for(compliancy)
    assert result.pro_licensed is True
    assert result.bus_description == "roboRIO Native CAN Bus"
    log = read_wpilog(result.wpilog)
    names = {e.name for e in log.catalog}
    assert any(n.endswith("/SupplyCurrent") for n in names)
    assert not any(n.endswith("_ProportionalOutput") for n in names)  # not in the health profile
    assert len(log) > 0


def test_health_profile_is_smaller_than_all(
    corpus_group: Callable[[str], list[Path]], registry: hoot.OwletRegistry
) -> None:
    path = _pick(corpus_group("2026-gacmp-q7"), "canivore")
    signals = hoot.scan_signals(registry.binary_for(19), path)
    health = hoot.select_signals(signals, "health")

    assert health is not None
    assert 0 < len(health) < len(signals) / 2


def test_empty_hoot_is_rejected_before_running_owlet(
    corpus_group: Callable[[str], list[Path]], registry: hoot.OwletRegistry, tmp_path: Path
) -> None:
    (path,) = corpus_group("2026-gadal-q11-empty")
    with pytest.raises(HootError) as exc:
        hoot.convert(path, tmp_path, registry)
    assert exc.value.reason == "empty-file"


def test_health_output_has_alignment_and_motor_constants(
    corpus_group: Callable[[str], list[Path]], registry: hoot.OwletRegistry, tmp_path: Path
) -> None:
    group = corpus_group("2026-gacmp-q7")
    rio = hoot.convert(_pick(group, "rio"), tmp_path / "rio", registry)
    canivore = hoot.convert(_pick(group, "canivore"), tmp_path / "can", registry)
    rio_names = {e.name for e in read_wpilog(rio.wpilog).catalog}
    can_names = {e.name for e in read_wpilog(canivore.wpilog).catalog}

    # Custom signals (the alignment anchor) are written to the rio hoot; the buses share a clock.
    assert "DriveState/Pose" in rio_names
    for signal in ("RotorVelocity", "MotorKT", "MotorKV", "MotorStallCurrent"):
        assert f"Phoenix6/TalonFX-11/{signal}" in can_names
        assert f"Phoenix6/TalonFX-1/{signal}" in rio_names
