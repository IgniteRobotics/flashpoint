"""History JSON API: thin, read-only endpoints over `views.queries`.

Unknown or repeated parameters are refused (400); values bind as query parameters, never text.
"""

from collections.abc import Callable
from typing import Any
from urllib.parse import unquote

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
