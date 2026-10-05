"""Guard for the watch RSS budget: the acquire entry path must not load the heavy libraries."""

import subprocess
import sys

HEAVY = ("polars", "duckdb", "pyarrow")


def test_acquire_entry_path_does_not_import_heavy_libraries() -> None:
    code = (
        "import sys\n"
        "import flashpoint.cli, flashpoint.acquire.watch\n"
        f"print([m for m in {HEAVY!r} if m in sys.modules])\n"
    )
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert done.stdout.strip() == "[]"


def test_report_and_serve_imports_stay_deferred() -> None:
    """P4: `report` and `serve` add handlers to cli.py; their modules load only when run."""
    code = (
        "import sys\n"
        "import flashpoint.cli\n"
        "print([m for m in ('flashpoint.report.build', 'flashpoint.web.server',"
        " 'flashpoint.views.queries', 'numpy') if m in sys.modules])\n"
    )
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert done.stdout.strip() == "[]"
