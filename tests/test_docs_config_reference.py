"""Config reference drift check (spec "Config and views reference", design D-e).

Each `reference/config/<file>.md` names every key its loader accepts, as `` `key` ``. The keys
come from the models (by their TOML names, so `[[slot]]` not `slots`), so a new key with no
docs fails here.
"""

import csv
import dataclasses
import subprocess
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

from pydantic import BaseModel

from flashpoint.acquire.config import AcquireConfig, BackupConfig
from flashpoint.anomaly import settings as anomaly_settings
from flashpoint.anomaly.catalogue import Cause, Label
from flashpoint.lake.paths import LakePaths
from flashpoint.report.settings import ReportConfig
from flashpoint.semantics.robot_config import (
    Bus,
    RobotConfig,
    SeasonConfig,
    SeasonRoots,
    SiblingGroup,
    Signal,
    Slot,
)
from flashpoint.setup.checks import CheckContext, run_checks
from flashpoint.setup.init import init_config
from flashpoint.theme import CustomPalette, ThemeConfig

REPO = Path(__file__).resolve().parents[1]
CONFIG_REFERENCE = REPO / "docs" / "guide" / "reference" / "config"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "config-reference"
UNITS_SEED = REPO / "src" / "flashpoint" / "defaults" / "units" / "units-seed.csv"


def model_keys(*models: type[BaseModel]) -> list[str]:
    """The TOML key of every field (its alias when it has one)."""
    return [field.alias or name for model in models for name, field in model.model_fields.items()]


def _anomaly_sections() -> list[type[BaseModel]]:
    base = anomaly_settings._Section
    return [
        cls
        for cls in vars(anomaly_settings).values()
        if isinstance(cls, type) and issubclass(cls, base) and cls is not base
    ]


def _report_keys() -> list[str]:
    return ["thresholds", "advantagescope", "path"] + [
        f.name for f in dataclasses.fields(ReportConfig)
    ]


def _units_keys() -> list[str]:
    with UNITS_SEED.open(encoding="utf-8", newline="") as f:
        return next(csv.reader(f))


# Page -> the keys it must name. Found by name: a new loader model shows up as an edit here.
CONFIG_PAGES: dict[str, Callable[[], Iterable[str]]] = {
    "acquire.md": lambda: model_keys(AcquireConfig, BackupConfig),
    "theme.md": lambda: model_keys(ThemeConfig, CustomPalette),
    "robots.md": lambda: model_keys(RobotConfig, Slot, Bus, Signal, SiblingGroup),
    "seasons.md": lambda: model_keys(SeasonConfig, SeasonRoots),
    "report.md": _report_keys,
    "anomaly.md": lambda: model_keys(*_anomaly_sections()),
    "anomaly-causes.md": lambda: ["cause", *model_keys(Cause)],
    "failure-labels.md": lambda: ["label", *model_keys(Label)],
    "units.md": _units_keys,
}


def config_key_problems(page: Path, keys: Iterable[str], root: Path) -> list[str]:
    """Keys the page doesn't name as inline code: `key`, or a table as `[key]` or `[[key]]`."""
    name = page.relative_to(root).as_posix()
    if not page.is_file():
        return [f"{name}: missing"]
    text = page.read_text(encoding="utf-8")
    return [
        f"{name}: doesn't name `{key}`"
        for key in keys
        if not any(form in text for form in (f"`{key}`", f"`[{key}]`", f"`[[{key}]]`"))
    ]


class _Widget(BaseModel):
    widget: list[int] = []  # a `[[widget]]` table counts as naming it
    size: int = 3
    colour: str = "red"
    shape: str = "round"


def test_fixture_page_missing_a_key_is_named() -> None:
    assert config_key_problems(FIXTURES / "widget.md", model_keys(_Widget), FIXTURES) == [
        "widget.md: doesn't name `shape`"
    ]


def test_fixture_missing_page_is_named() -> None:
    assert config_key_problems(FIXTURES / "gone.md", ["x"], FIXTURES) == ["gone.md: missing"]


def test_every_config_key_is_documented() -> None:
    problems = [
        problem
        for page, keys in CONFIG_PAGES.items()
        for problem in config_key_problems(CONFIG_REFERENCE / page, keys(), REPO)
    ]
    assert problems == [], "\n".join(problems)


# --- robots.md "Common errors" quotes the real message (spec "Bad key message is documented")

ROBOTS_PAGE = CONFIG_REFERENCE / "robots.md"
UNKNOWN_KEY_ROBOT = """
robot = "2026-comp"
season = 2026
project = "Robot-2026"

[[slot]]
id = "intake"
bus = "rio"
model = "TalonFX"
can_id = 5
subsystem = "intake"
role = "roller"
gearing = 3.0
"""


def _ok_run(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(list(args), 0, "active\n", "")


def test_robot_unknown_key_message_is_the_one_documented(tmp_path: Path) -> None:
    root = tmp_path / "cfg"
    init_config(root, team=254, lake=tmp_path / "lake")
    (root / "robots" / "2026-comp.toml").write_text(UNKNOWN_KEY_ROBOT, encoding="utf-8")
    ctx = CheckContext(
        root, LakePaths(tmp_path / "lake"), tmp_path / "cache", system="Darwin", season=2026,
        run=_ok_run, find_advantagescope=lambda r: (None, "not found"),
    )  # fmt: skip
    result = {r.id: r for r in run_checks(ctx)}["file:robots/2026-comp.toml"]
    assert result.status == "problem"
    errors = ROBOTS_PAGE.read_text(encoding="utf-8").split("\n## Common errors\n", 1)[1]
    quoted = [line.strip() for line in errors.splitlines()]
    assert result.message in quoted, f"robots.md doesn't quote {result.message!r}"


# --- views reference: one page per view, with its screenshot -------------------------------

VIEWS_JS = REPO / "src" / "flashpoint" / "web" / "static" / "views"
VIEWS_REFERENCE = REPO / "docs" / "guide" / "reference" / "views"


def view_page_problems(views: Path, pages: Path, root: Path) -> list[str]:
    """Each `<view>.js` needs `<view>.md`, and the page shows the view's manifest screenshot."""
    problems = []
    for script in sorted(views.glob("*.js")):
        page = pages / f"{script.stem}.md"
        name = page.relative_to(root).as_posix()
        if not page.is_file():
            problems.append(f"{name}: missing (for {script.name})")
        elif f"](../../assets/media/{script.stem}.png" not in page.read_text(encoding="utf-8"):
            problems.append(f"{name}: doesn't show assets/media/{script.stem}.png")
    return problems


def test_fixture_view_pages() -> None:
    site = FIXTURES / "views"
    assert view_page_problems(site / "js", site / "pages", FIXTURES) == [
        "views/pages/gauge.md: missing (for gauge.js)",
        "views/pages/meter.md: doesn't show assets/media/meter.png",
    ]


def test_every_view_has_a_reference_page() -> None:
    problems = view_page_problems(VIEWS_JS, VIEWS_REFERENCE, REPO)
    assert problems == [], "\n".join(problems)
