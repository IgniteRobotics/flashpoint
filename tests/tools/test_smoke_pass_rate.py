"""The nightly first-run smoke report (issue #104, for mvp-install-and-first-run task 9.5)."""

import datetime as dt
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "smoke-pass-rate.py"
NOW = dt.datetime(2026, 11, 27, 12, 0, tzinfo=dt.UTC)


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("smoke_pass_rate", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rate = _load()


def _run(day: int, ubuntu: str, macos: str, windows: str) -> dict[str, Any]:
    return {
        "created": dt.datetime(2026, 11, day, 7, 17, tzinfo=dt.UTC),
        "url": f"https://example.invalid/runs/{day}",
        "jobs": {"ubuntu-latest": ubuntu, "macos-latest": macos, "windows-latest": windows},
    }


def test_pass_rate_per_os_and_failures_listed() -> None:
    runs = [
        _run(21, "success", "success", "failure"),
        _run(22, "success", "success", "success"),
        _run(23, "success", "timed_out", "failure"),
        _run(24, "success", "success", "cancelled"),  # cancelled: not a verdict
    ]
    report = rate.summarize(runs, days=7, now=NOW)
    assert "4 nightly runs" in report
    rows = {
        line.split("|")[1].strip(): line
        for line in report.splitlines()
        if line.startswith("| ") and "-latest" in line
    }
    assert "4 of 4" in rows["ubuntu-latest"] and "100%" in rows["ubuntu-latest"]
    assert "3 of 4" in rows["macos-latest"] and "75%" in rows["macos-latest"]
    assert "1 of 3" in rows["windows-latest"] and "33%" in rows["windows-latest"]
    failures = [line for line in report.splitlines() if line.startswith("- ")]
    assert len(failures) == 3
    assert any("2026-11-23" in f and "macos-latest" in f and "timed_out" in f for f in failures)
    assert all("https://example.invalid/runs/" in f for f in failures)


def test_runs_outside_the_window_are_left_out() -> None:
    runs = [_run(10, "failure", "failure", "failure"), _run(26, "success", "success", "success")]
    report = rate.summarize(runs, days=7, now=NOW)
    assert "1 nightly run" in report and "failure" not in report


def test_no_runs_says_so() -> None:
    report = rate.summarize([], days=7, now=NOW)
    assert "no nightly runs" in report.lower()


def test_job_names_map_to_their_os() -> None:
    assert rate.os_of("first-run smoke (windows-latest)") == "windows-latest"
    assert rate.os_of("something else") is None
