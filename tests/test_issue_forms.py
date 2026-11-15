"""The repository's issue forms (spec public-release, "Issue forms for Flashpoint users")."""

from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
FORMS = REPO / ".github" / "ISSUE_TEMPLATE"
DISCUSSIONS_QA = "https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a"

# form file -> the field ids it must require
REQUIRED = {
    "bug.yml": {"version", "os", "python", "doctor", "logs", "pro", "what-happened"},
    "feature.yml": {"what", "where"},
}


def _form(name: str) -> dict[str, Any]:
    form: dict[str, Any] = yaml.safe_load((FORMS / name).read_text(encoding="utf-8"))
    return form


def _fields(form: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["id"]: item for item in form["body"] if "id" in item}


def _text(item: dict[str, Any]) -> str:
    attributes = item.get("attributes", {})
    parts = [attributes.get(key, "") for key in ("label", "description", "value", "placeholder")]
    return " ".join([*parts, *map(str, attributes.get("options", []))])


def required_problems(name: str) -> list[str]:
    """Each required field that is missing from the form or not marked required."""
    fields = _fields(_form(name))
    return [
        f"{name}: field '{field}' is missing or not required"
        for field in sorted(REQUIRED[name])
        if not fields.get(field, {}).get("validations", {}).get("required")
    ]


@pytest.mark.parametrize("name", sorted(REQUIRED))
def test_form_requires_its_fields(name: str) -> None:
    assert required_problems(name) == []


def test_removing_a_required_field_names_the_form_and_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    form = _form("bug.yml")
    form["body"] = [item for item in form["body"] if item.get("id") != "version"]
    (tmp_path / "bug.yml").write_text(yaml.safe_dump(form), encoding="utf-8")
    monkeypatch.setattr(f"{__name__}.FORMS", tmp_path)
    assert required_problems("bug.yml") == ["bug.yml: field 'version' is missing or not required"]


def test_bug_form_asks_for_each_thing_by_name() -> None:
    fields = _fields(_form("bug.yml"))
    assert "flashpoint --version" in _text(fields["version"])
    assert "flashpoint doctor" in _text(fields["doctor"])
    assert {"macOS", "Linux", "Windows"} <= set(fields["os"]["attributes"]["options"])
    logs = " ".join(fields["logs"]["attributes"]["options"])
    for kind in ("wpilog", "roboRIO", "CANivore", "AdvantageKit"):
        assert kind in logs, kind
    assert fields["logs"]["attributes"].get("multiple") is True
    assert set(fields["pro"]["attributes"]["options"]) == {"Yes", "No", "Don't know"}


def test_bug_form_asks_for_the_ingest_line_with_stage_and_reason() -> None:
    text = _text(_fields(_form("bug.yml"))["ingest-line"]).lower()
    assert "quarantined" in text
    assert "incomplete-read" in text
    assert "reason" in text


def test_bug_form_warns_against_attaching_logs() -> None:
    form = _form("bug.yml")
    notes = " ".join(
        item["attributes"]["value"] for item in form["body"] if item["type"] == "markdown"
    )
    assert "attach" in notes.lower()
    assert ".wpilog" in notes
    assert ".hoot" in notes
    assert "private" in notes.lower()


def test_feature_form_asks_which_view_or_command() -> None:
    text = _text(_fields(_form("feature.yml"))["where"]).lower()
    assert "view" in text
    assert "command" in text


def test_blank_issues_off_and_questions_go_to_discussions() -> None:
    config: dict[str, Any] = yaml.safe_load((FORMS / "config.yml").read_text(encoding="utf-8"))
    assert config["blank_issues_enabled"] is False
    assert DISCUSSIONS_QA in [link["url"] for link in config["contact_links"]]


def test_only_flashpoint_forms() -> None:
    assert sorted(path.name for path in FORMS.iterdir()) == ["bug.yml", "config.yml", "feature.yml"]
