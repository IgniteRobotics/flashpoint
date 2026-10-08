"""History JSON API: thin, read-only endpoints over `views.queries`, plus Replay's windowed
detail over one match's silver partition.

Unknown or repeated parameters are refused (400); values bind as query parameters, never text.
"""

import json
import math
from collections.abc import Callable
from typing import Any
from urllib.parse import unquote

import duckdb

from flashpoint.report import envelope
from flashpoint.report.paths import data_dir, file_stem
from flashpoint.views.queries import Filters, HistoryQueries, QueryError

Response = tuple[int, dict[str, Any]]
_FILTERS = Filters.names()


class ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def _single(params: dict[str, list[str]], allowed: set[str]) -> dict[str, str]:
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise ApiError(400, f"unknown parameter(s): {', '.join(unknown)}")
    repeated = sorted(k for k, v in params.items() if len(v) != 1)
    if repeated:
        raise ApiError(400, f"repeated parameter(s): {', '.join(repeated)}")
    return {k: v[0] for k, v in params.items()}


def _filters(values: dict[str, str]) -> Filters:
    return Filters(**{k: v for k, v in values.items() if k in _FILTERS and v != ""})


class Api:
    def __init__(self, queries: HistoryQueries) -> None:
        self.queries = queries
        self.routes: dict[str, Callable[[str, dict[str, list[str]]], dict[str, Any]]] = {
            "filters": self.filters,
            "summary": self.summary,
            "trend": self.trend,
            "devices": self.devices,
            "unit": self.unit,
            "match": self.match,
            "envelope": self.envelope,
        }

    def handle(self, path: str, params: dict[str, list[str]]) -> Response:
        """`path` is the part after /api/ (still percent-encoded)."""
        name, _, rest = path.partition("/")
        route = self.routes.get(name)
        if route is None:
            return 404, {"error": f"no such endpoint: /api/{name}"}
        try:
            return 200, route(unquote(rest), params)
        except ApiError as exc:
            return exc.status, {"error": str(exc)}
        except QueryError as exc:
            return 400, {"error": str(exc)}

    def _lake(self) -> dict[str, Any]:
        return {"lake": str(self.queries.lake.root), "empty": self.queries.is_empty()}

    @staticmethod
    def _no_rest(rest: str) -> None:
        if rest:
            raise ApiError(404, "no such endpoint")

    def filters(self, rest: str, params: dict[str, list[str]]) -> dict[str, Any]:
        self._no_rest(rest)
        _single(params, set())
        return {**self._lake(), "values": self.queries.filter_values()}

    def summary(self, rest: str, params: dict[str, list[str]]) -> dict[str, Any]:
        self._no_rest(rest)
        values = _single(params, _FILTERS)
        return {**self._lake(), "tiles": self.queries.summary(_filters(values))}

    def trend(self, rest: str, params: dict[str, list[str]]) -> dict[str, Any]:
        self._no_rest(rest)
        values = _single(params, _FILTERS | {"metric", "by"})
        return self.queries.trend(
            values.get("metric", "peak_temp"), values.get("by", "unit"), _filters(values)
        )

    def devices(self, rest: str, params: dict[str, list[str]]) -> dict[str, Any]:
        self._no_rest(rest)
        values = _single(params, _FILTERS | {"metric"})
        metric = values.get("metric", "peak_temp")
        return {"metric": metric, "rows": self.queries.devices(metric, _filters(values))}

    def unit(self, rest: str, params: dict[str, list[str]]) -> dict[str, Any]:
        if not rest:
            raise ApiError(404, "a unit id is required: /api/unit/<id>")
        values = _single(params, {"season", "robot"})
        return self.queries.unit(rest, values.get("season") or None, values.get("robot") or None)

    def match(self, rest: str, params: dict[str, list[str]]) -> dict[str, Any]:
        if not rest:
            raise ApiError(404, "a match key is required: /api/match/<key>")
        _single(params, set())
        stem = file_stem(rest)
        built = (data_dir(self.queries.lake) / f"{stem}.js").is_file()
        return {
            "match_key": rest,
            "built": built,
            "file": f"data/{stem}.js" if built else None,
            "build_command": f"flashpoint report --match {rest}",
            "sources": self.queries.sources(rest),
        }

    def _built_payload(self, key: str) -> dict[str, Any]:
        """The built Replay data for `key`; 404 unless it exists and is that match's file."""
        path = data_dir(self.queries.lake) / f"{file_stem(key)}.js"
        prefix = "FP.register("
        try:
            text = path.read_text(encoding="utf-8")
            payload = json.loads(text[len(prefix) : -3]) if text.startswith(prefix) else None
        except (OSError, ValueError):
            payload = None
        if not isinstance(payload, dict) or payload.get("match_key") != key:
            raise ApiError(404, f"no Replay data for {key}: run flashpoint report --match {key}")
        return payload

    @staticmethod
    def _seconds(values: dict[str, str], name: str) -> float:
        try:
            value = float(values[name])
        except KeyError:
            raise ApiError(400, f"{name} is required (seconds from match start)") from None
        except ValueError:
            raise ApiError(400, f"{name} is not a number") from None
        if not math.isfinite(value):
            raise ApiError(400, f"{name} must be finite")
        return value

    def envelope(self, rest: str, params: dict[str, list[str]]) -> dict[str, Any]:
        """Finer envelopes for `from`..`to` (seconds from match start), clamped to the match
        window, from the primary session's silver partition only."""
        if not rest:
            raise ApiError(404, "a match key is required: /api/envelope/<key>")
        values = _single(params, {"from", "to"})
        start_s, end_s = self._seconds(values, "from"), self._seconds(values, "to")
        if start_s >= end_s:
            raise ApiError(400, "from must be before to")
        payload = self._built_payload(rest)
        if payload.get("status") != "ok":
            return {"available": False, "reason": payload.get("reason") or "no aligned samples"}
        if "t0_us" not in payload:
            return {
                "available": False,
                "reason": "Rebuild with flashpoint report to zoom past bucket resolution.",
            }
        stored = payload["window"]
        lo_s, hi_s = stored["t0"], stored["t0"] + stored["n"] * stored["width"]
        if end_s <= lo_s or start_s >= hi_s:
            raise ApiError(400, f"the window lies outside the match ({lo_s:g} to {hi_s:g} s)")
        match_start = int(payload["t0_us"]) - round(stored["t0"] * envelope.US_PER_S)
        window = envelope.window_between(
            match_start + round(max(start_s, lo_s) * envelope.US_PER_S),
            match_start + round(min(end_s, hi_s) * envelope.US_PER_S),
            match_start,
        )
        silver = (
            self.queries.lake.silver
            / f"season={payload['season']}"
            / f"session_id={payload['session_id']}"
        )
        if not any(silver.glob("*.parquet")):
            return {
                "available": False,
                "reason": "This match has no silver data in the lake; finer data is unavailable.",
            }
        match_end = match_start + round(payload["duration_s"] * envelope.US_PER_S)
        con = duckdb.connect()
        try:
            con.execute("SET memory_limit = '256MB'")
            con.execute("SET threads = 2")
            temps = envelope.temperature_points(con, silver, window)
            detail = envelope.series_payload(con, silver, window, temps, match_end)
        finally:
            con.close()
        keys = ("window", "series", "battery", "temps", "not_logged")
        return {"available": True, **{k: detail[k] for k in keys}}
