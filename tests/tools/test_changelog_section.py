"""tools/changelog-section.py (spec public-release, "A tag publishes ... a GitHub Release")."""

import importlib.util
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "changelog-section.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("changelog_section", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


changelog_section = _load()

CHANGELOG = """\
# Changelog

## Unreleased

- Something not released yet.

## 0.3.0 - 2026-12-19

### Added

- Feature A. (#70)

```sql
## not a heading inside a code block
```

- Feature B. (#71)

## 0.3.0rc1 - 2026-12-07

- Release candidate.

## 0.2.0 - 2026-11-13

- Older entry.
"""


def test_notes_stop_at_the_next_section() -> None:
    notes = changelog_section.section(CHANGELOG, "0.3.0")
    assert notes.startswith("### Added")
    assert "Feature A. (#70)" in notes
    assert "## not a heading inside a code block" in notes
    assert notes.endswith("- Feature B. (#71)")
    assert "Unreleased" not in notes
    assert "not released yet" not in notes
    assert "Release candidate" not in notes
    assert "Older entry" not in notes


def test_notes_are_word_for_word() -> None:
    notes = changelog_section.section(CHANGELOG, "0.3.0")
    start = CHANGELOG.index("### Added")
    end = CHANGELOG.index("- Feature B. (#71)") + len("- Feature B. (#71)")
    assert notes == CHANGELOG[start:end]


def test_heading_without_a_date() -> None:
    text = "# Changelog\n\n## 0.3.1\n\n- Fix.\n"
    assert changelog_section.section(text, "0.3.1") == "- Fix."


def test_tag_name_is_accepted() -> None:
    assert changelog_section.section(CHANGELOG, "v0.2.0") == "- Older entry."


def test_pre_release_version() -> None:
    assert changelog_section.section(CHANGELOG, "0.3.0rc1") == "- Release candidate."


def test_final_version_does_not_match_a_pre_release_heading() -> None:
    text = "# Changelog\n\n## 0.3.0rc1 - 2026-12-07\n\n- Release candidate.\n"
    with pytest.raises(changelog_section.MissingSection, match=re.escape("0.3.0")):
        changelog_section.section(text, "0.3.0")


def test_missing_changelog_section() -> None:
    with pytest.raises(changelog_section.MissingSection, match=re.escape("0.3.1")):
        changelog_section.section(CHANGELOG, "0.3.1")


def test_empty_section() -> None:
    text = "# Changelog\n\n## 0.3.0 - 2026-12-19\n\n## 0.2.0 - 2026-11-13\n\n- Older.\n"
    with pytest.raises(changelog_section.MissingSection, match=re.escape("0.3.0")):
        changelog_section.section(text, "0.3.0")


def test_wrong_heading_level() -> None:
    text = (
        "# Changelog\n\n### 0.3.0 - 2026-12-19\n\n- Entry.\n\n## 0.2.0 - 2026-11-13\n\n- Older.\n"
    )
    with pytest.raises(changelog_section.MissingSection, match=re.escape("0.3.0")):
        changelog_section.section(text, "0.3.0")


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("0.3.0", False),
        ("0.3.1", False),
        ("0.3.0rc1", True),
        ("0.3.0a2", True),
        ("0.3.0b1", True),
        ("0.3.0.dev1", True),
    ],
)
def test_pre_release_detection(version: str, expected: bool) -> None:
    assert changelog_section.is_pre_release(version) is expected


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False
    )


def test_cli_prints_the_section(tmp_path: Path) -> None:
    path = tmp_path / "CHANGELOG.md"
    path.write_text(CHANGELOG, encoding="utf-8")
    done = _run("0.3.0rc1", "--changelog", str(path))
    assert done.returncode == 0, done.stderr
    assert done.stdout == "- Release candidate.\n"


def test_cli_fails_naming_the_version(tmp_path: Path) -> None:
    path = tmp_path / "CHANGELOG.md"
    path.write_text(CHANGELOG, encoding="utf-8")
    done = _run("0.3.1", "--changelog", str(path))
    assert done.returncode == 1
    assert done.stdout == ""
    assert "0.3.1" in done.stderr


def test_cli_prints_the_real_0_2_0_section() -> None:
    done = _run("0.2.0")
    assert done.returncode == 0, done.stderr
    assert done.stdout.startswith("### Added")
    assert "Live CAN health view" in done.stdout
    assert "## Unreleased" not in done.stdout
