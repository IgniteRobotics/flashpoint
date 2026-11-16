"""CI probe for #97: log GC passes and the backup_status() time inside the timed test."""

import gc
import time

import pytest

EVENTS: list[str] = []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):  # type: ignore[no-untyped-def]
    if "fast_enough_for_every_navigation" not in item.name:
        yield
        return
    module = item.module
    original = module.backup_status
    starts: dict[int, float] = {}

    def on_gc(phase: str, info: dict) -> None:  # type: ignore[type-arg]
        if phase == "start":
            starts[info["generation"]] = time.perf_counter()
        else:
            took = (time.perf_counter() - starts.pop(info["generation"], time.perf_counter())) * 1000
            EVENTS.append(f"gc gen{info['generation']} {took:.1f} ms collected={info['collected']}")

    def timed(*args, **kwargs):  # type: ignore[no-untyped-def]
        gc.callbacks.append(on_gc)
        started = time.perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            EVENTS.append(f"backup_status {(time.perf_counter() - started) * 1000:.1f} ms")
            gc.callbacks.remove(on_gc)

    module.backup_status = timed
    try:
        yield
    finally:
        module.backup_status = original
        counts = gc.get_count()
        started = time.perf_counter()
        collected = gc.collect()
        EVENTS.append(
            f"full gc.collect() after the call: {(time.perf_counter() - started) * 1000:.1f} ms, "
            f"collected={collected}, counts before={counts}, tracked objects={len(gc.get_objects())}"
        )


def pytest_terminal_summary(terminalreporter):  # type: ignore[no-untyped-def]
    for line in EVENTS:
        terminalreporter.write_line(f"GCPROBE {line}")
