"""Small lakes for derive tests: real ingest of synthetic wpilogs, hoots added as ledger rows
plus a one-row bronze partition (no owlet needed)."""

from pathlib import Path

import pyarrow as pa

from flashpoint.ingest import Ingestor
from flashpoint.lake import bronze
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.meta.extract import parse_hoot_filename
from flashpoint.readers.hoot import OwletRegistry
from flashpoint.semantics.derive import Deriver


def ingest_wpilogs(tmp_path: Path, paths: list[Path]) -> LakePaths:
    lake = LakePaths(tmp_path / "lake")
    manifest = tmp_path / "empty-manifest.toml"
    manifest.write_text("")
    ingestor = Ingestor(lake, OwletRegistry(manifest, tmp_path / "owlet-cache"))
    try:
        ingestor.ingest(paths)
    finally:
        ingestor.close()
    return lake


def add_hoot(
    lake: LakePaths,
    log_id: str,
    filename: str,
    pro_licensed: bool = True,
    signal: str = "Phoenix6/TalonFX-1/SupplyVoltage",
    last_ts_us: int = 100_000_000,
) -> None:
    """A hoot as ingest would leave it: files, logs and hoot_logs rows, and a bronze sample."""
    parsed = parse_hoot_filename(filename)
    assert parsed.session_stamp is not None, filename
    table = pa.table(
        {
            "signal": pa.array([signal]).dictionary_encode(),
            "type": pa.array(["double"]).dictionary_encode(),
            "ts_us": pa.array([1_000_000], pa.int64()),
            "v_f64": pa.array([12.0], pa.float64()),
            "v_i64": pa.nulls(1, pa.int64()),
            "v_bool": pa.nulls(1, pa.bool_()),
            "v_str": pa.nulls(1, pa.large_string()),
            "v_bytes": pa.nulls(1, pa.large_binary()),
        }
    )
    bronze.write(lake, table, season=parsed.session_stamp[:4], log_id=log_id, run_id="t")
    ledger = Ledger(lake.ledger)
    try:
        ledger.execute(
            "INSERT INTO files (sha256, kind, size, stage, first_seen, updated)"
            " VALUES (?, 'hoot', 1, 'success', 't', 't')",
            (log_id,),
        )
        ledger.execute(
            "INSERT INTO logs (log_id, kind, season, filename, file_event, first_ts_us,"
            " last_ts_us) VALUES (?, 'hoot', ?, ?, ?, 0, ?)",
            (log_id, parsed.session_stamp[:4], filename, parsed.event, last_ts_us),
        )
        ledger.execute(
            "INSERT INTO hoot_logs (log_id, bus, session_stamp, pro_licensed) VALUES (?, ?, ?, ?)",
            (log_id, parsed.bus, parsed.session_stamp, int(pro_licensed)),
        )
    finally:
        ledger.close()


def derive(lake: LakePaths, config_root: Path) -> Deriver:
    """Run a whole derive; the returned (closed) deriver keeps its warnings and re-reads."""
    (config_root / "robots").mkdir(parents=True, exist_ok=True)
    (config_root / "seasons").mkdir(exist_ok=True)
    deriver = Deriver(lake, config_root)
    try:
        deriver.run()
    finally:
        deriver.close()
    return deriver


def query(lake: LakePaths, sql: str, args: tuple[object, ...] = ()) -> list[dict[str, object]]:
    ledger = Ledger(lake.ledger)
    try:
        return ledger.query(sql, args)
    finally:
        ledger.close()
