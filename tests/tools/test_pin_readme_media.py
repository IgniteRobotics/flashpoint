"""Pinning the README for a release (mvp-documentation design D-g; spec public-release, "Release
build refuses moving-branch documentation links")."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "pin-readme-media.py"
FIXTURES = REPO / "tests" / "fixtures" / "pin-readme"


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("pin_readme_media", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("version", ["0.3.0", "0.3.0rc1"])
def test_pins_images_and_repository_links(version: str) -> None:
    text = (FIXTURES / "README.md").read_text(encoding="utf-8")
    expected = (FIXTURES / f"expected-{version}.md").read_text(encoding="utf-8")
    assert _tool().pin(text, version) == expected


def test_a_leading_v_is_the_same_version() -> None:
    text = (FIXTURES / "README.md").read_text(encoding="utf-8")
    assert _tool().pin(text, "v0.3.0") == _tool().pin(text, "0.3.0")


def test_pinned_readme_passes_the_release_link_check() -> None:
    spec = importlib.util.spec_from_file_location(
        "check", REPO / "tools" / "check-release-links.py"
    )
    assert spec is not None and spec.loader is not None
    check = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(check)
    text = (FIXTURES / "README.md").read_text(encoding="utf-8")
    pinned = _tool().pin(text, "0.3.0")
    # The dev image is left alone, so the check still names it: pinning never hides a bad link.
    assert check.problems(pinned, "0.3.0", "README.md") == [
        "README.md:15: image from another docs version"
        " (https://igniterobotics.github.io/flashpoint/dev/assets/media/setup.png); use /0.3/"
    ]


def test_main_rewrites_the_file_in_place(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text((FIXTURES / "README.md").read_text(encoding="utf-8"), encoding="utf-8")
    assert _tool().main(["0.3.0", "--readme", str(readme)]) == 0
    assert readme.read_text(encoding="utf-8") == (FIXTURES / "expected-0.3.0.md").read_text(
        encoding="utf-8"
    )


def test_a_malformed_version_fails() -> None:
    with pytest.raises(ValueError, match="'three'"):
        _tool().pin("", "three")
