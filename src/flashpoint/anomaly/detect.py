"""`flashpoint detect`: rebuild `gold/anomalies/` from gold, configuration, and triage.

One pass over a time-ordered frame of whole-match rows judges every unit, metric and method
with no lookahead, and writes `anomalies.parquet` (one row per episode) and `summary.json`
atomically. A fingerprint of every input skips the run when nothing changed.
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from flashpoint.anomaly import power
from flashpoint.anomaly.catalogue import CatalogueError, load_causes
from flashpoint.anomaly.episodes import episodes
from flashpoint.anomaly.explain import Context, describe
from flashpoint.anomaly.findings import Finding
from flashpoint.anomaly.frame import Frame, load_frame
from flashpoint.anomaly.restarts import restart_pending as restart_pending
from flashpoint.anomaly.restarts import restarts as restarts_of
from flashpoint.anomaly.settings import (
    POWER,
    AnomalyConfig,
    AnomalyConfigError,
    load_anomaly_config,
)
from flashpoint.anomaly.shapes import Rule, unit_findings
from flashpoint.anomaly.siblings import Member, sibling_findings
from flashpoint.anomaly.spec_rules import spec_finding
from flashpoint.lake.paths import LakePaths
from flashpoint.report.settings import ReportConfig, ReportConfigError, load_report_config
from flashpoint.semantics.robot_config import ConfigError, RobotConfig, load_robots

DETECT_VERSION = 2  # bump when anomaly rows change for the same inputs
ANOMALIES_FILE = "anomalies.parquet"
SUMMARY_FILE = "summary.json"
LOCK_FILE = "detect.lock"
GOLD_INPUTS = ("match_features", "can_health", "unit_usage")
UNIT_SPEC_METRICS = ("temp_max_c", "temp_mean_c", "stall_s", "reboots", "longest_gap_ms")
SCHEMA: dict[str, pl.DataType] = {
    "id": pl.String(), "subject": pl.String(), "subject_key": pl.String(),
    "subject_kind": pl.String(), "robot": pl.String(), "season": pl.String(),
    "slot_id": pl.String(), "role": pl.String(), "subsystem": pl.String(), "model": pl.String(),
    "method": pl.String(), "metric": pl.String(), "metric_name": pl.String(),
    "metric_unit": pl.String(), "severity": pl.String(), "sensitivity": pl.String(),
    "onset_match_key": pl.String(), "last_match_key": pl.String(),
    "matches_affected": pl.Int64(), "detected_match_key": pl.String(),
    "detected_normal_match_key": pl.String(), "actual": pl.String(), "expected": pl.String(),
    "title": pl.String(), "why": pl.String(), "actual_value": pl.Float64(),
    "confidence": pl.String(), "baseline_n": pl.Int64(), "max_z": pl.Float64(),
    "peer_name": pl.String(), "flagged_keys": pl.String(), "low_alignment": pl.Boolean(),
    "evidence": pl.String(), "last_start": pl.String(), "event_boundary": pl.String(),
}  # fmt: skip


class DetectConfigError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    anomaly: AnomalyConfig
    robots: list[RobotConfig]
    report: ReportConfig
    digest: str


@dataclass
class Computed:
    rows: list[dict[str, Any]]
    frame: Frame
    building: list[dict[str, Any]]
    restart_seq: int
    notes: list[str] = field(default_factory=list)


@dataclass
class DetectResult:
    ran: bool
    summary: dict[str, Any]


def anomalies_dir(lake: LakePaths) -> Path:
    return power.anomalies_dir(lake)


def load_settings(config_dir: Path) -> Settings:
    """Every detection input from configuration, validated; errors name the file and key."""
    try:
        anomaly = load_anomaly_config(config_dir)
        robots_dir = config_dir / "robots"
        robots = load_robots(robots_dir) if robots_dir.is_dir() else []
        report = load_report_config(config_dir)
        load_causes(config_dir)  # validated here; resolved when an anomaly is read
    except (AnomalyConfigError, ConfigError, ReportConfigError, CatalogueError) as exc:
        raise DetectConfigError(str(exc)) from exc
    digest = hashlib.sha256()
    digest.update(anomaly.digest().encode())
    for robot in sorted(robots, key=lambda r: r.robot):
        digest.update(robot.model_dump_json(include={"robot", "slots", "siblings"}).encode())
    digest.update(power.thresholds_hash(report).encode())
    return Settings(anomaly, robots, report, digest.hexdigest())


def _rules(cfg: AnomalyConfig, metric: str) -> tuple[Rule, Rule]:
    b, s = cfg.baseline, cfg.strict
    floor = b.floors[metric]
    normal = Rule(b.window, floor, b.min_baseline, b.step_z, b.step_persist, b.drift_z,
                  b.drift_span)  # fmt: skip
    strict = Rule(b.window, floor, b.min_baseline, s.step_z, s.persist, s.drift_z, b.drift_span)
    return normal, strict


def _column(rows: pl.DataFrame, metric: str) -> np.ndarray:
    return rows.get_column(metric).cast(pl.Float64).fill_null(np.nan).to_numpy()


def compute(
    lake: LakePaths,
    settings: Settings,
    restarts: bool = True,
    frame: Frame | None = None,
    counts: power.PowerCounts | None = None,
) -> Computed:
    """Every anomaly the lake's gold supports, as rows (no files written)."""
    cfg = settings.anomaly
    frame = frame if frame is not None else load_frame(lake)
    counts = counts if counts is not None else power.brownout_counts(lake, settings.report)
    restart_at, restart_seq = restarts_of(lake) if restarts else ({}, 0)
    robots = {r.robot: r for r in settings.robots}
    rows: list[dict[str, Any]] = []
    building: list[dict[str, Any]] = []
    own_z: dict[str, dict[str, dict[str, float]]] = {}  # metric -> unit -> key -> z
    members: dict[tuple[str, str, str], list[Member]] = {}  # (robot, group, metric)
    contexts = _contexts(frame, robots, cfg)
    for (unit,), unit_rows in frame.rows.group_by(["unit_id"], maintain_order=True):
        unit = str(unit)
        keys = unit_rows.get_column("match_key").to_list()
        ctx = contexts[unit]
        robot = robots.get(ctx.robot)
        if len(keys) < cfg.baseline.min_baseline:
            building.append({"unit": unit, "robot": ctx.robot, "slot_id": ctx.slot_id,
                             "matches": len(keys)})  # fmt: skip
        findings: list[Finding] = []
        for metric in UNIT_SPEC_METRICS:
            enabled = _column(unit_rows, "reboots_enabled") if metric == "reboots" else None
            findings.append(
                spec_finding(unit, metric, keys, _column(unit_rows, metric), cfg.spec, enabled)
            )
        for metric in cfg.baseline.metrics:
            normal, strict = _rules(cfg, metric)
            step, drift, z = unit_findings(
                unit, metric, keys, _column(unit_rows, metric), normal, strict,
                restart_at.get((unit, metric)),
            )  # fmt: skip
            findings += [step, drift]
            own_z.setdefault(metric, {})[unit] = {
                k: float(v) for k, v in zip(keys, z, strict=True) if not np.isnan(v)
            }
        rows += _describe(findings, ctx)
        if robot is None:
            continue
        for (slot_id,), slot_rows in unit_rows.group_by(["slot_id"], maintain_order=True):
            group = robot.sibling_group(str(slot_id))
            if group is None:
                continue
            slot_keys = slot_rows.get_column("match_key").to_list()
            for metric in cfg.baseline.metrics:
                members.setdefault((robot.robot, group.name, metric), []).append(
                    Member(str(slot_id), unit, slot_keys, _column(slot_rows, metric))
                )
    for (robot_name, group_name, metric), group_members in sorted(members.items()):
        order = frame.matches.get(robot_name, [])
        slots = next(g.slots for g in robots[robot_name].siblings if g.name == group_name)
        found = sibling_findings(metric, group_members, order, cfg, own_z.get(metric, {}), slots)
        for finding in found:
            if finding.flagged.any():
                rows += _describe([finding], contexts[finding.subject])
    notes: list[str] = []
    if cfg.model.enabled:
        from flashpoint.anomaly import model

        for finding in model.model_findings(frame, cfg, notes.append):
            rows += _describe([finding], contexts[finding.subject])
    for robot_name, keys in frame.matches.items():
        values = np.array([
            float(counts.counts[k]["brownouts"]) if k in counts.counts else np.nan for k in keys
        ])  # fmt: skip
        if np.isnan(values).all():
            continue
        low = frame.rows.filter(
            (pl.col("robot") == robot_name) & (pl.col("alignment") != "high")
        ).get_column("match_key")
        ctx = Context(robot=robot_name, season=_season(frame, robot_name), slot_id=POWER,
                      role=None, subsystem=POWER, model=None, cfg=cfg,
                      low_alignment=frozenset(low))  # fmt: skip
        finding = spec_finding(f"{POWER}:{robot_name}", "brownouts", keys, values, cfg.spec)
        rows += _describe([finding], ctx)
    for row in rows:
        row["last_start"] = frame.starts.get(row["last_match_key"])
    rows.sort(key=lambda r: r["id"])
    return Computed(rows, frame, sorted(building, key=lambda b: b["unit"]), restart_seq, notes)


def _season(frame: Frame, robot: str) -> str:
    seasons = frame.rows.filter(pl.col("robot") == robot).get_column("season")
    return str(seasons[-1]) if seasons.len() else ""


def _contexts(frame: Frame, robots: dict[str, RobotConfig], cfg: AnomalyConfig
              ) -> dict[str, Context]:  # fmt: skip
    out = {}
    for (unit,), unit_rows in frame.rows.group_by(["unit_id"], maintain_order=True):
        latest = unit_rows.row(-1, named=True)
        robot = robots.get(latest["robot"])
        slot = next((s for s in robot.slots if s.id == latest["slot_id"]), None) if robot else None
        out[str(unit)] = Context(
            robot=latest["robot"], season=latest["season"], slot_id=latest["slot_id"],
            role=latest["role"], subsystem=latest["subsystem"],
            model=slot.model if slot else None, cfg=cfg,
            low_alignment=frozenset(
                unit_rows.filter(pl.col("alignment") != "high").get_column("match_key")
            ),
        )  # fmt: skip
    return out


def _describe(findings: list[Finding], ctx: Context) -> list[dict[str, Any]]:
    return [describe(f, e, ctx) for f in findings for e in episodes(f.flagged)]


def _gold_files(lake: LakePaths) -> list[tuple[str, int, int]]:
    files = []
    for table in GOLD_INPUTS:
        base = lake.root / "gold" / table
        for path in sorted(base.glob("season=*/session_id=*/*.parquet")):
            stat = path.stat()
            files.append((path.relative_to(base.parent).as_posix(), stat.st_size,
                          stat.st_mtime_ns))  # fmt: skip
    return files


def fingerprint(
    lake: LakePaths, settings: Settings, counts: power.PowerCounts, restarts: bool = True
) -> str:
    restart_at, seq = restarts_of(lake) if restarts else ({}, 0)
    model_ready = False
    if settings.anomaly.model.enabled:  # installing the extra later must rerun detection
        from flashpoint.anomaly import model

        model_ready = model.available()
    parts = [DETECT_VERSION, settings.digest, _gold_files(lake), counts.digest(),
             sorted(restart_at.items()), seq, model_ready]  # fmt: skip
    return hashlib.sha256(json.dumps(parts, default=str).encode()).hexdigest()


def read_summary(lake: LakePaths) -> dict[str, Any] | None:
    path = anomalies_dir(lake) / SUMMARY_FILE
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def read_anomalies(lake: LakePaths) -> pl.DataFrame:
    path = anomalies_dir(lake) / ANOMALIES_FILE
    if not path.is_file():
        return pl.DataFrame(schema=SCHEMA)
    return pl.read_parquet(path)


def summarize(computed: Computed, fp: str) -> dict[str, Any]:
    counts = {s: 0 for s in ("HIGH", "MED", "LOW")}
    sensitivities = {s: 0 for s in ("relaxed", "normal", "strict")}
    for row in computed.rows:
        counts[row["severity"]] += 1
        sensitivities[row["sensitivity"]] += 1
    frame = computed.frame
    return {
        "version": DETECT_VERSION,
        "fingerprint": fp,
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "scanned_through": frame.scanned_through,
        "matches": len(frame.starts),
        "units": frame.rows.get_column("unit_id").n_unique() if frame.rows.height else 0,
        "anomalies": len(computed.rows),
        "counts": counts,
        "by_sensitivity": sensitivities,
        "building_baseline": {"count": len(computed.building), "units": computed.building},
        "skipped": dict(sorted(frame.skipped.items())),
        "missing": dict(sorted(frame.missing.items())),
        "restart_seq": computed.restart_seq,
        "notes": computed.notes,
    }


def write(lake: LakePaths, computed: Computed, summary: dict[str, Any]) -> None:
    out = anomalies_dir(lake)
    out.mkdir(parents=True, exist_ok=True)
    table = pl.DataFrame(computed.rows, schema=SCHEMA) if computed.rows else pl.DataFrame(
        schema=SCHEMA
    )  # fmt: skip
    staged = out / f"{SUMMARY_FILE}.part"
    staged.write_text(json.dumps(summary, indent=1, sort_keys=True), encoding="utf-8")
    power.write_parquet_atomic(out / ANOMALIES_FILE, table)
    staged.replace(out / SUMMARY_FILE)


def detect(
    lake: LakePaths, config_dir: Path, force: bool = False, restarts: bool = True
) -> DetectResult:
    """Run detection unless nothing changed since the last run (`force` runs anyway).
    Raises DetectConfigError, leaving the previous table in place."""
    settings = load_settings(config_dir)
    counts = power.brownout_counts(lake, settings.report)
    fp = fingerprint(lake, settings, counts, restarts)
    previous = read_summary(lake)
    if (
        not force
        and previous is not None
        and previous.get("fingerprint") == fp
        and (anomalies_dir(lake) / ANOMALIES_FILE).is_file()
    ):
        return DetectResult(False, previous)
    computed = compute(lake, settings, restarts, counts=counts)
    summary = summarize(computed, fp)
    write(lake, computed, summary)
    return DetectResult(True, summary)
