"""Non-Pro hoots convert with the free signal set (hoot-conversion, mvp-outside-team-logs 6.1)."""

import struct
from pathlib import Path

import duckdb
import pytest

from flashpoint.ingest import Ingestor
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.readers import hoot
from flashpoint.readers.hoot import HootError, OwletRegistry
from tests.readers.test_hoot import _hoot_bytes
from tests.synthetic_lake import derive, query
from tests.wpilog_builder import WpilogBuilder

STAMP = "2026-04-09_21-51-06"
CANIVORE = "6E9415C3394C485320202050101C18FF"
SCAN = {
    "RobotEnable": "1ff00",
    "TalonFX-1/SupplyCurrent": "6f40c00",
    "TalonFX-1/SupplyVoltage": "6f41c00",
    "TalonFX-1/Position": "6f42c00",
    "TalonFX-1/Velocity": "6f43c00",
    "TalonFX-1/DeviceTemp": "6f60c00",
}
NAMES = {sid: name for name, sid in SCAN.items()}


class FakeOwlet:
    """`hoot._run` stand-in. A hoot file ending in b"PRO" is Pro-licensed. `refuse` makes an
    export that asks for a Pro-only signal from a non-Pro hoot fail."""

    def __init__(self, refuse: bool) -> None:
        self.refuse = refuse
        self.exports: list[list[str]] = []

    def __call__(self, args: list[str]) -> hoot._OwletOutput:
        pro = Path(args[1]).read_bytes().endswith(b"PRO")
        if "--check-pro" in args:
            return hoot._OwletOutput(f"hoot file is {'' if pro else 'not '}Pro-licensed\n", False)
        if "--scan" in args:
            return hoot._OwletOutput("".join(f"{n}: {s}\n" for n, s in SCAN.items()), False)
        selected = args[args.index("-s") + 1].split(",")
        self.exports.append(selected)
        if self.refuse and not pro and SCAN["TalonFX-1/DeviceTemp"] in selected:
            raise HootError("owlet-failed", "exit 1: signal requires a Pro license")
        b = WpilogBuilder()
        entry = 0
        for sid in selected:
            name = NAMES[sid]
            if name.endswith("DeviceTemp") and not pro:
                continue  # the free export has no device temperature
            entry += 1
            b.start(entry, f"Phoenix6/{name}" if "/" in name else name, "double")
            for i in range(5):
                b.double(entry, 1_000_000 + i * 10_000, 1.0 + i)
        Path(args[2]).write_bytes(b.raw_bytes())
        return hoot._OwletOutput("", False)


class LocalRegistry(OwletRegistry):
    def __init__(self, tmp_path: Path) -> None:
        manifest = tmp_path / "manifest.toml"
        manifest.write_text("")
        super().__init__(manifest, tmp_path / "owlet-cache")

    def binary_for(self, compliancy: int, download: bool = True) -> Path:
        return Path("owlet")

    def version_for(self, compliancy: int) -> str:
        return "fake"


def _hoot(folder: Path, bus: str, pro: bool) -> Path:
    path = folder / f"GACMP_Q7_{bus}_{STAMP}.hoot"
    path.write_bytes(_hoot_bytes(19) + struct.pack("<I", len(bus)) + (b"PRO" if pro else b"FREE"))
    return path


def _ingest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, refuse: bool,
            *hoots: tuple[str, bool]) -> tuple[LakePaths, FakeOwlet]:  # fmt: skip
    fake = FakeOwlet(refuse)
    monkeypatch.setattr(hoot, "_run", fake)
    folder = tmp_path / "logs"
    folder.mkdir()
    paths = [_hoot(folder, bus, pro) for bus, pro in hoots]
    lake = LakePaths(tmp_path / "lake")
    ingestor = Ingestor(lake, LocalRegistry(tmp_path))
    try:
        report = ingestor.ingest(paths)
    finally:
        ingestor.close()
    assert [r.status for r in report.results] == ["success"] * len(paths)
    return lake, fake


def _hoot_rows(lake: LakePaths) -> dict[str, dict[str, object]]:
    rows = query(
        lake,
        "SELECT h.bus, h.pro_licensed, h.dropped_signals, f.reason, f.warnings, h.integrity"
        " FROM hoot_logs h JOIN files f ON f.sha256 = h.log_id",
    )
    return {str(r["bus"]): r for r in rows}


def _bronze_signals(lake: LakePaths) -> set[str]:
    glob = (lake.bronze / "season=*" / "log_id=*" / "*.parquet").as_posix()
    rel = duckdb.sql(f"SELECT DISTINCT signal FROM '{glob}'")
    assert rel is not None
    return {r[0] for r in rel.fetchall()}


def test_non_pro_hoot_with_a_converter_that_reports_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake, fake = _ingest(tmp_path, monkeypatch, False, ("rio", False))
    row = _hoot_rows(lake)["rio"]
    assert row["pro_licensed"] == 0
    assert (row["reason"], row["warnings"], row["dropped_signals"]) == (None, None, None)
    assert row["integrity"] in ("unchecked", "ok")
    signals = _bronze_signals(lake)
    assert {"Phoenix6/TalonFX-1/SupplyCurrent", "Phoenix6/TalonFX-1/SupplyVoltage"} <= signals
    assert not any(s.endswith("DeviceTemp") for s in signals)
    assert len(fake.exports) >= 1


def test_converter_refuses_a_pro_only_signal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake, fake = _ingest(tmp_path, monkeypatch, True, ("rio", False))
    row = _hoot_rows(lake)["rio"]
    assert row["pro_licensed"] == 0
    assert row["dropped_signals"] == "DeviceTemp"
    assert row["reason"] is None
    assert SCAN["TalonFX-1/DeviceTemp"] not in fake.exports[-1]
    assert "Phoenix6/TalonFX-1/SupplyCurrent" in _bronze_signals(lake)


def test_a_pro_hoot_that_fails_is_not_retried_without_signals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def always_fails(args: list[str]) -> hoot._OwletOutput:
        if "--check-pro" in args:
            return hoot._OwletOutput("hoot file is Pro-licensed\n", False)
        if "--scan" in args:
            return hoot._OwletOutput("".join(f"{n}: {s}\n" for n, s in SCAN.items()), False)
        raise HootError("owlet-failed", "exit 139: Segmentation fault")

    monkeypatch.setattr(hoot, "_run", always_fails)
    src = _hoot(tmp_path, "rio", True)
    with pytest.raises(HootError, match="owlet-failed"):
        hoot.convert(src, tmp_path / "out", LocalRegistry(tmp_path))


def test_mixed_licensing_in_one_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lake, _ = _ingest(tmp_path, monkeypatch, True, (CANIVORE, True), ("rio", False))
    rows = _hoot_rows(lake)
    assert (rows[CANIVORE]["pro_licensed"], rows["rio"]["pro_licensed"]) == (1, 0)
    assert rows[CANIVORE]["dropped_signals"] is None
    derive(lake, tmp_path / "config")
    ledger = Ledger(lake.ledger)
    try:
        sessions = {r["session_id"] for r in ledger.query("SELECT session_id FROM session_hoots")}
    finally:
        ledger.close()
    assert len(sessions) == 1
