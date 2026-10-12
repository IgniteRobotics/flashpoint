"""`flashpoint backtest`: would detection have caught the team's known failures, how early, and
how often does it flag things nobody labelled?

One detection pass with triage ignored (no baseline restarts) gives the as-of state at every
match, because every flag uses only earlier matches: an anomaly existed at match t once its
detection match is at or before t. Each label is scored at every sensitivity.
"""

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from flashpoint.anomaly import detect
from flashpoint.anomaly.catalogue import CatalogueError, Label, load_labels
from flashpoint.anomaly.episodes import SENSITIVITIES, shown_at
from flashpoint.anomaly.settings import POWER
from flashpoint.lake.paths import LakePaths

WINDOW = 5  # a label is caught by an anomaly flagged at its match or the 5 matches before it
METHODS = ("SPEC", "DRIFT", "STEP", "SIBLING", "MODEL")


@dataclass
class BacktestResult:
    report: dict[str, Any]
    json_text: str
    json_path: Path
    md_path: Path
    message: str | None = None


def backtest_dir(lake: LakePaths) -> Path:
    return lake.root / "report" / "backtest"


def _label_match(label: Label, keys: list[str], starts: dict[str, str]) -> str | None:
    """The label's match in the robot's order: its key, or the last match on its date."""
    if label.match is not None:
        return label.match if label.match in keys else None
    assert label.date is not None
    day: date = label.date
    on_or_before = [k for k in keys if datetime.fromisoformat(starts[k]).date() <= day]
    return on_or_before[-1] if on_or_before else None


def _about(label: Label, row: dict[str, Any]) -> bool:
    if row["robot"] != label.robot:
        return False
    if label.metric is not None and row["metric"] != label.metric:
        return False
    if label.unit is not None and row["subject_key"] == label.unit:
        return True
    return label.slot is not None and row["slot_id"] == label.slot


def _detected(row: dict[str, Any], sens: str) -> str | None:
    if not shown_at(row["sensitivity"], sens):
        return None
    if sens == "strict":
        return str(row["detected_match_key"])
    return str(row["detected_normal_match_key"] or row["detected_match_key"])


def _history_at(computed: detect.Computed, label: Label, key: str) -> int:
    """Matches the label's unit (or the slot's unit at that match) played before `key`."""
    rows = computed.frame.rows
    order = computed.frame.matches[label.robot]
    at = order.index(key)
    if label.unit is not None:
        unit = label.unit
    else:
        here = rows.filter((rows["robot"] == label.robot) & (rows["slot_id"] == label.slot)
                           & (rows["match_key"] == key))  # fmt: skip
        if here.is_empty():
            return 0
        unit = str(here["unit_id"][0])
    played = set(rows.filter(rows["unit_id"] == unit)["match_key"].to_list())
    return sum(1 for k in order[:at] if k in played)


def _score(
    computed: detect.Computed, labels: list[Label], sens: str, min_baseline: int
) -> tuple[list[dict[str, Any]], set[str]]:
    frame = computed.frame
    scored, matched = [], set()
    for n, label in enumerate(labels, start=1):
        keys = frame.matches.get(label.robot, [])
        entry: dict[str, Any] = {
            "label": n, "robot": label.robot, "unit": label.unit, "slot": label.slot,
            "kind": label.kind, "metric": label.metric, "note": label.note,
            "match": label.match, "result": None, "reason": None, "anomaly": None,
            "method": None, "lead_matches": None,
        }  # fmt: skip
        key = _label_match(label, keys, frame.starts)
        if key is None:
            where = label.match or str(label.date)
            entry.update(result="not evaluable", reason=f"{where} is not in the lake")
            scored.append(entry)
            continue
        entry["match"] = key
        at = keys.index(key)
        window = set(keys[max(0, at - WINDOW) : at + 1])
        best = None
        for row in computed.rows:
            seen = _detected(row, sens)
            if not _about(label, row) or seen is None or seen not in keys:
                continue
            if keys.index(seen) > at or not window & set(json.loads(row["flagged_keys"])):
                continue
            matched.add(row["id"])
            lead = at - keys.index(seen)
            if best is None or lead > best[0] or (lead == best[0] and row["id"] < best[1]["id"]):
                best = (lead, row)
        if best is not None:
            entry.update(result="caught", anomaly=best[1]["id"], method=best[1]["method"],
                         lead_matches=best[0])  # fmt: skip
        else:
            history = _history_at(computed, label, key) if label.slot != POWER else min_baseline
            reason = (
                f"building baseline ({history} of {min_baseline})"
                if history < min_baseline
                else f"no anomaly within the {WINDOW} matches before {key}"
            )
            entry.update(result="missed", reason=reason)
        scored.append(entry)
    return scored, matched


def _false_positives(computed: detect.Computed, sens: str, matched: set[str]) -> dict[str, Any]:
    robot_matches = sum(len(keys) for keys in computed.frame.matches.values())
    shown = [r for r in computed.rows if _detected(r, sens) is not None]
    by_method: dict[str, dict[str, Any]] = {}
    for method in METHODS:
        count = sum(1 for r in shown if r["method"] == method and r["id"] not in matched)
        rate = round(count / robot_matches * 10, 6) if robot_matches else None
        by_method[method] = {"count": count, "per_10_robot_matches": rate}
    total = sum(m["count"] for m in by_method.values())
    return {"robot_matches": robot_matches, "total": total, "by_method": by_method}


def _markdown(report: dict[str, Any], taken: str) -> str:
    lines = [f"# Backtest ({taken})", "", f"Lake: `{report['lake']}`", ""]
    if report.get("message"):
        lines += [report["message"], ""]
    for sens, part in report["sensitivities"].items():
        lines += [f"## {sens.capitalize()} sensitivity", "",
                  "| # | Robot | Unit / slot | Match | Kind | Result | By | Lead (matches) |",
                  "|---|---|---|---|---|---|---|---|"]  # fmt: skip
        for lb in part["labels"]:
            result = lb["result"] + (f" ({lb['reason']})" if lb["reason"] else "")
            lines.append(
                f"| {lb['label']} | {lb['robot']} | {lb['unit'] or lb['slot']} | {lb['match']}"
                f" | {lb['kind']} | {result} | {lb['method'] or ''}"
                f" | {'' if lb['lead_matches'] is None else lb['lead_matches']} |"
            )
        fp = part["false_positives"]
        lines += ["", f"False positives: {fp['total']} over {fp['robot_matches']} robot-matches",
                  "", "| Method | Count | Per 10 robot-matches |", "|---|---|---|"]  # fmt: skip
        for method, values in fp["by_method"].items():
            rate = values["per_10_robot_matches"]
            lines.append(
                f"| {method} | {values['count']} | {'' if rate is None else f'{rate:.2f}'} |"
            )
        lines.append("")
    cov = report["coverage"]
    lines += ["## Coverage", "", f"- Matches judged: {cov['matches_judged']}",
              f"- Unit-matches skipped: {cov['skipped'] or 'none'}",
              "- Units that never left the building-baseline state: "
              + (", ".join(u["unit"] for u in cov["units_building_baseline"]) or "none"),
              ""]  # fmt: skip
    return "\n".join(lines)


def backtest(lake: LakePaths, config_dir: Path) -> BacktestResult:
    """Raises detect.DetectConfigError for bad configuration or labels."""
    settings = detect.load_settings(config_dir)
    try:
        labels = load_labels(config_dir, settings.robots)
    except CatalogueError as exc:
        raise detect.DetectConfigError(str(exc)) from exc
    computed = detect.compute(lake, settings, restarts=False)
    message = None
    if computed.frame.rows.is_empty():
        message = f"no gold in {lake.root}: nothing to backtest (ingest and derive logs first)"
    min_baseline = settings.anomaly.baseline.min_baseline
    sensitivities = {}
    for sens in SENSITIVITIES:
        scored, matched = _score(computed, labels, sens, min_baseline)
        counts = {r: sum(1 for lb in scored if lb["result"] == r)
                  for r in ("caught", "missed", "not evaluable")}  # fmt: skip
        sensitivities[sens] = {
            "labels": scored, "counts": counts,
            "false_positives": _false_positives(computed, sens, matched),
        }  # fmt: skip
    report = {
        "lake": str(lake.root),
        "message": message,
        "sensitivities": sensitivities,
        "appearances": [
            {
                "id": r["id"],
                "subject": r["subject_key"],
                "method": r["method"],
                "metric": r["metric"],
                "severity": r["severity"],
                "sensitivity": r["sensitivity"],
                "onset": r["onset_match_key"],
                "appeared_at": r["detected_match_key"],
                "appeared_at_normal": r["detected_normal_match_key"],
            }
            for r in computed.rows
        ],  # fmt: skip
        "coverage": {
            "matches_judged": len(computed.frame.starts),
            "skipped": dict(sorted(computed.frame.skipped.items())),
            "units_building_baseline": computed.building,
        },
    }
    json_text = json.dumps(report, sort_keys=True, indent=1)
    out = backtest_dir(lake)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    json_path, md_path = out / f"{stamp}.json", out / f"{stamp}.md"
    json_path.write_text(json_text, encoding="utf-8")
    md_path.write_text(_markdown(report, stamp), encoding="utf-8")
    return BacktestResult(report, json_text, json_path, md_path, message)
