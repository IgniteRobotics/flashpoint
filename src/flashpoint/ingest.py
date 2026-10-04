"""Ingest orchestration: discover → hash → ledger → raw → convert → decode → bronze → metadata.

Each file succeeds or is quarantined on its own; one bad file never stops a run.
"""

import os
import shutil
import tempfile
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa

from flashpoint import config
from flashpoint.lake import bronze, raw
from flashpoint.lake.ledger import Ledger, Stage
from flashpoint.lake.paths import LakePaths
from flashpoint.meta import extract
from flashpoint.readers import hoot
from flashpoint.readers.hoot import HootConversion, HootError, OwletRegistry
from flashpoint.readers.wpilog import CatalogEntry, WpilogError, WpilogReader

SUFFIX_KIND = {".wpilog": "wpilog", ".hoot": "hoot"}
SHORT_COVERAGE_RATIO = 0.8  # a hoot covering < 80% of its longest session sibling is flagged
_EMPTY_METADATA_FRAME = pl.DataFrame(
    schema={
        "signal": pl.String,
        "type": pl.String,
        "ts_us": pl.Int64,
        "v_f64": pl.Float64,
        "v_i64": pl.Int64,
        "v_bool": pl.Boolean,
        "v_str": pl.String,
        "v_bytes": pl.Binary,
    }
)


@dataclass(frozen=True)
class _Streamed:
    span: tuple[int, int] | None
    records: int
    truncated_bytes: int
    orphan_records: int
    catalog: list[CatalogEntry]
    metadata_frame: pl.DataFrame


@dataclass
class FileResult:
    path: Path
    sha256: str
    kind: str
    status: str  # success | skipped | quarantined
    reason: str | None = None
    warnings: list[str] = field(default_factory=list)


@dataclass
class IngestReport:
    results: list[FileResult] = field(default_factory=list)
    elapsed_s: float = 0.0

    def count(self, status: str) -> int:
        return sum(1 for r in self.results if r.status == status)

    @property
    def exit_code(self) -> int:
        return 1 if self.count("quarantined") else 0


def discover(paths: Iterable[Path]) -> list[Path]:
    found: list[Path] = []
    for path in paths:
        candidates = path.rglob("*") if path.is_dir() else [path]
        for candidate in candidates:
            if (
                candidate.is_file()
                and candidate.suffix.lower() in SUFFIX_KIND
                and not candidate.name.startswith(".")
            ):
                found.append(candidate)
    return sorted(set(found))


def _run_id() -> str:
    return f"{time.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}"


class Ingestor:
    def __init__(
        self,
        lake: LakePaths,
        registry: OwletRegistry,
        profile: str = "health",
        jobs: int | None = None,
        log: Callable[[str], None] = lambda _msg: None,
    ) -> None:
        if profile not in hoot.PROFILES:
            raise ValueError(f"unknown profile {profile!r}")
        self.lake = lake
        self.registry = registry
        self.profile = profile
        self.jobs = jobs or max(1, (os.cpu_count() or 2) // 2)
        self.log = log
        self.run_id = _run_id()
        lake.ensure()
        bronze.clean_staging(lake)
        self.ledger = Ledger(lake.ledger)
        self._scratch = Path(tempfile.mkdtemp(prefix="flashpoint-"))

    def close(self) -> None:
        shutil.rmtree(self._scratch, ignore_errors=True)
        bronze.clean_staging(self.lake)
        self.ledger.export_snapshots(self.lake.meta)
        self.ledger.close()

    # --- public entry points ----------------------------------------------------------

    def ingest(self, paths: Iterable[Path]) -> IngestReport:
        started = time.perf_counter()
        report = IngestReport()
        todo: list[tuple[Path, str, str]] = []
        seen: set[str] = set()
        for path in discover(paths):
            sha = raw.file_sha256(path)
            kind = SUFFIX_KIND[path.suffix.lower()]
            self.ledger.register(sha, kind, path.stat().st_size, path)
            if sha in seen or not self.ledger.needs_processing(sha, config.PIPELINE_VERSION):
                report.results.append(FileResult(path, sha, kind, "skipped"))
                continue
            seen.add(sha)
            todo.append((path, sha, kind))
        report.results += self._process(todo)
        self._check_hoot_coverage(report)
        report.elapsed_s = time.perf_counter() - started
        return report

    def rebuild(self) -> IngestReport:
        """Reprocess files from an older pipeline version, from raw storage."""
        started = time.perf_counter()
        todo: list[tuple[Path, str, str]] = []
        for row in self.ledger.files():
            if row["pipeline_version"] == config.PIPELINE_VERSION:
                continue
            suffix = "." + row["kind"]
            path = raw.raw_path(self.lake, row["sha256"], suffix)
            if path.is_file():
                todo.append((path, row["sha256"], row["kind"]))
        report = IngestReport(self._process(todo))
        self._check_hoot_coverage(report)
        report.elapsed_s = time.perf_counter() - started
        return report

    # --- processing -------------------------------------------------------------------

    def _process(self, todo: list[tuple[Path, str, str]]) -> list[FileResult]:
        results: list[FileResult] = []
        with ThreadPoolExecutor(max_workers=self.jobs) as pool:
            conversions: dict[str, Future[HootConversion]] = {}
            stored: dict[str, Path] = {}
            for path, sha, kind in todo:
                stored[sha] = raw.store(self.lake, path, sha, "." + kind)
                if kind == "hoot" and path.stat().st_size > 0:
                    out = self._scratch / sha
                    conversions[sha] = pool.submit(
                        hoot.convert, stored[sha], out, self.registry, self.profile
                    )
            for path, sha, kind in todo:
                results.append(self._finish(path, sha, kind, stored[sha], conversions.get(sha)))
        return results

    def _finish(
        self,
        path: Path,
        sha: str,
        kind: str,
        raw_path: Path,
        conversion: Future[HootConversion] | None,
    ) -> FileResult:
        name = self._display_name(sha, path)
        warnings: list[str] = []
        try:
            if raw_path.stat().st_size == 0:
                raise (HootError if kind == "hoot" else WpilogError)("empty-file")
            if kind == "wpilog":
                self._ingest_wpilog(sha, raw_path, name)
            else:
                assert conversion is not None
                converted = conversion.result()
                warnings = list(converted.warnings)
                self._ingest_hoot(sha, converted, name)
        except (WpilogError, HootError) as exc:
            return self._quarantine(path, sha, kind, name, exc.reason, str(exc))
        except Exception as exc:  # noqa: BLE001 - one bad file must never stop the batch
            reason = f"internal-error:{type(exc).__name__}"
            return self._quarantine(path, sha, kind, name, reason, repr(exc))
        joined = ",".join(warnings) or None
        self.ledger.set_stage(sha, Stage.SUCCESS, config.PIPELINE_VERSION, warnings=joined)
        self.log(f"ingested    {name}" + (f" (warnings: {joined})" if joined else ""))
        return FileResult(path, sha, kind, "success", warnings=warnings)

    def _quarantine(
        self, path: Path, sha: str, kind: str, name: str, reason: str, detail: str
    ) -> FileResult:
        self.ledger.quarantine(sha, reason, config.PIPELINE_VERSION)
        bronze.remove(self.lake, sha)
        self.ledger.replace_metadata(sha, {})
        self.log(f"quarantined {name}: {detail}")
        return FileResult(path, sha, kind, "quarantined", reason)

    def _display_name(self, sha: str, path: Path) -> str:
        """Original filename (metadata comes from names; raw copies are named by hash)."""
        for alias in self.ledger.aliases(sha):
            alias_name = Path(alias).name
            if not alias_name.startswith(sha):
                return alias_name
        return path.name

    def _stream_to_bronze(
        self,
        sha: str,
        path: Path,
        season_of: Callable[["_Streamed"], str],
        collect_metadata: bool,
    ) -> "_Streamed":
        """Decode `path` window by window into bronze; one pass, bounded memory."""
        reader = WpilogReader(path)
        frames: list[pl.DataFrame] = []
        span: list[int] = []

        def tables() -> Iterator[pa.Table]:
            for chunk in reader:
                chunk_span = chunk.ts_range()
                if chunk_span is not None:
                    span[:] = (
                        [min(span[0], chunk_span[0]), max(span[1], chunk_span[1])]
                        if span
                        else list(chunk_span)
                    )
                if collect_metadata:
                    wanted = chunk.subset(extract.is_metadata_entry)
                    if len(wanted):
                        frames.append(
                            wanted.samples().with_columns(
                                pl.col("signal").cast(pl.String), pl.col("type").cast(pl.String)
                            )
                        )
                yield chunk.sorted().to_arrow()

        streamed: list[_Streamed] = []

        def season() -> str:
            result = _Streamed(
                span=(span[0], span[1]) if span else None,
                records=reader.records,
                truncated_bytes=reader.truncated_bytes,
                orphan_records=reader.orphan_records,
                catalog=reader.catalog,
                metadata_frame=pl.concat(frames) if frames else _EMPTY_METADATA_FRAME,
            )
            streamed.append(result)
            return season_of(result)

        bronze.write(self.lake, tables(), season, sha, self.run_id, presorted=True)
        self.ledger.set_stage(sha, Stage.BRONZE, config.PIPELINE_VERSION)
        return streamed[0]

    @staticmethod
    def _entries(sha: str, catalog: list[CatalogEntry]) -> list[dict[str, Any]]:
        return [
            {"log_id": sha, "idx": e.index, "entry_id": e.entry_id, "name": e.name,
             "type": e.type, "metadata": e.metadata}
            for e in catalog
        ]  # fmt: skip

    def _ingest_wpilog(self, sha: str, raw_path: Path, name: str) -> None:
        metas: list[extract.WpilogMetadata] = []

        def season_of(streamed: _Streamed) -> str:
            metas.append(extract.metadata_from_frame(streamed.metadata_frame, streamed.span, name))
            return metas[0].season

        streamed = self._stream_to_bronze(sha, raw_path, season_of, collect_metadata=True)
        meta = metas[0]
        span = streamed.span
        logs_row = {
            "log_id": sha, "kind": "wpilog", "season": meta.season, "filename": name,
            "fms_event": meta.fms_event, "fms_match_type": meta.fms_match_type,
            "fms_match_number": meta.fms_match_number, "fms_replay": meta.fms_replay,
            "fms_red_alliance": meta.fms_red_alliance, "fms_station": meta.fms_station,
            "file_event": meta.file.event, "file_match_type": meta.file.match_type,
            "file_match_number": meta.file.match_number, "project": meta.project,
            "build_date": meta.build.get("Build Date"),
            "commit_hash": meta.build.get("Commit Hash"),
            "git_branch": meta.git_branch, "git_dirty": meta.git_dirty,
            "utc_start": meta.utc_start.isoformat() if meta.utc_start else None,
            "utc_end": meta.utc_end.isoformat() if meta.utc_end else None,
            "anchor_source": meta.anchor_source, "utc_offset_us": meta.utc_offset_us,
            "first_ts_us": span[0] if span else None, "last_ts_us": span[1] if span else None,
            "record_count": streamed.records, "truncated_bytes": streamed.truncated_bytes,
            "orphan_records": streamed.orphan_records, "inventory": meta.inventory_status,
        }  # fmt: skip
        inventory = [
            {"log_id": sha, "ts_us": r.ts_us, "payload": r.payload, "valid": r.valid,
             "error": r.error}
            for r in meta.inventory
        ]  # fmt: skip
        self.ledger.replace_metadata(
            sha,
            {
                "logs": [logs_row],
                "entries": self._entries(sha, streamed.catalog),
                "inventory": inventory,
            },
        )

    def _ingest_hoot(self, sha: str, conversion: HootConversion, name: str) -> None:
        try:
            parsed = extract.parse_hoot_filename(name)
            season = parsed.session_stamp[:4] if parsed.session_stamp else "unknown"
            streamed = self._stream_to_bronze(
                sha, conversion.wpilog, lambda _s: season, collect_metadata=False
            )
            span = streamed.span
            logs_row = {
                "log_id": sha, "kind": "hoot", "season": season, "filename": name,
                "file_event": parsed.event, "first_ts_us": span[0] if span else None,
                "last_ts_us": span[1] if span else None, "record_count": streamed.records,
                "truncated_bytes": streamed.truncated_bytes,
                "orphan_records": streamed.orphan_records, "inventory": "absent",
            }  # fmt: skip
            hoot_row = {
                "log_id": sha, "bus": parsed.bus, "bus_description": conversion.bus_description,
                "session_stamp": parsed.session_stamp, "compliancy": conversion.compliancy,
                "owlet_version": conversion.owlet_version, "pro_licensed": conversion.pro_licensed,
                "profile": conversion.profile, "signal_count": conversion.signal_count,
                "read_status": "incomplete" if conversion.warnings else "complete",
                "integrity": "unchecked",
            }  # fmt: skip
            self.ledger.replace_metadata(
                sha,
                {
                    "logs": [logs_row],
                    "hoot_logs": [hoot_row],
                    "entries": self._entries(sha, streamed.catalog),
                },
            )
        finally:
            shutil.rmtree(conversion.wpilog.parent, ignore_errors=True)

    def _check_hoot_coverage(self, report: IngestReport) -> None:
        """Flag hoots that end well short of their session siblings (owlet accepts truncation)."""
        rows = self.ledger.query(
            "SELECT h.log_id, h.session_stamp, l.first_ts_us, l.last_ts_us, f.warnings"
            " FROM hoot_logs h"
            " JOIN logs l USING (log_id) JOIN files f ON f.sha256 = h.log_id"
            " WHERE f.stage = 'success' AND h.session_stamp IS NOT NULL"
        )
        sessions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            sessions[row["session_stamp"]].append(row)
        flagged: set[str] = set()
        for members in sessions.values():
            durations = {
                m["log_id"]: (m["last_ts_us"] or 0) - (m["first_ts_us"] or 0) for m in members
            }
            longest = max(durations.values())
            for log_id, duration in durations.items():
                if len(members) == 1:
                    integrity = "unchecked"
                elif duration < SHORT_COVERAGE_RATIO * longest:
                    integrity = "short-coverage"
                    flagged.add(log_id)
                else:
                    integrity = "ok"
                self.ledger.execute(
                    "UPDATE hoot_logs SET integrity = ? WHERE log_id = ?", (integrity, log_id)
                )
                existing = next(m["warnings"] for m in members if m["log_id"] == log_id)
                kept = [w for w in (existing or "").split(",") if w and w != "short-coverage"]
                if log_id in flagged:
                    kept.append("short-coverage")
                self.ledger.execute(
                    "UPDATE files SET warnings = ? WHERE sha256 = ?",
                    (",".join(kept) or None, log_id),
                )
        for result in report.results:
            if result.sha256 in flagged and "short-coverage" not in result.warnings:
                result.warnings.append("short-coverage")
