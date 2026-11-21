"""Docs version for a tag (mvp-documentation design D-c; spec project-documentation,
"Published, versioned docs site")."""

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "docs-version.py"


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_version", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _published(*versions: tuple[str, str]) -> list[dict[str, object]]:
    """A mike versions.json: (version, title) pairs."""
    return [{"version": v, "title": t, "aliases": []} for v, t in versions]


def test_first_final_release_moves_latest() -> None:
    deploy = _tool().for_tag("v0.3.0", [])
    assert (deploy.version, deploy.latest, deploy.banner, deploy.title) == ("0.3", True, "", "0.3")


def test_pre_release_leaves_latest_and_carries_the_banner() -> None:
    deploy = _tool().for_tag("v0.3.0rc1", _published(("dev", "dev")))
    assert deploy.version == "0.3"
    assert not deploy.latest
    assert deploy.banner == "pre-release"
    assert deploy.title == "0.3 (pre-release)"


def test_patch_to_an_older_minor_leaves_latest() -> None:
    deploy = _tool().for_tag("v0.2.5", _published(("0.3", "0.3"), ("0.2", "0.2")))
    assert (deploy.version, deploy.latest) == ("0.2", False)


def test_patch_to_the_newest_minor_moves_latest() -> None:
    deploy = _tool().for_tag("v0.3.1", _published(("0.3", "0.3"), ("0.2", "0.2")))
    assert (deploy.version, deploy.latest, deploy.banner) == ("0.3", True, "")


def test_a_published_pre_release_does_not_hold_latest_back() -> None:
    """0.3 is only rc1 so far: a 0.2 patch is still the highest final release."""
    published = _published(("0.3", "0.3 (pre-release)"), ("0.2", "0.2"), ("dev", "dev"))
    assert _tool().for_tag("v0.2.5", published).latest


def test_alpha_and_beta_are_pre_releases() -> None:
    tool = _tool()
    for tag in ("v1.0.0a1", "v1.0.0b2"):
        assert (tool.for_tag(tag, []).version, tool.for_tag(tag, []).latest) == ("1.0", False)


@pytest.mark.parametrize("tag", ["v0.3", "0.3.0", "v0.3.0-rc1", "v0.3.0.dev1", "corpus-v1", ""])
def test_malformed_tag_fails_naming_it(tag: str) -> None:
    with pytest.raises(ValueError, match=f"'{tag}'"):
        _tool().for_tag(tag, [])


def test_main_writes_github_outputs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    versions = tmp_path / "versions.json"
    versions.write_text(json.dumps(_published(("0.3", "0.3"))), encoding="utf-8")
    assert _tool().main(["v0.3.1", "--versions", str(versions)]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "version=0.3",
        "latest=true",
        "banner=",
        "title=0.3",
    ]


def test_main_with_no_versions_file_and_a_bad_tag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    tool = _tool()
    assert tool.main(["v0.4.0rc2", "--versions", str(tmp_path / "missing.json")]) == 0
    assert "latest=false" in capsys.readouterr().out
    assert tool.main(["v0.4"]) == 2
    assert "'v0.4'" in capsys.readouterr().err
