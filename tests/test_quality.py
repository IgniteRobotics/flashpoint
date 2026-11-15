"""The quality gates test themselves (specs: coding-standards, ci-quality-gates).

Each deliberately bad fixture under tests/fixtures/quality/ must fail the linter with the rule
it is named for, so a rule switched off by accident breaks the build. The suppression and
workflow rules that no linter can express are checked here directly.
"""

import io
import json
import re
import shutil
import subprocess
import sys
import tokenize
import tomllib
from pathlib import Path

import pytest
import yaml

from tests.web.test_static_lint import MODULE_SYNTAX, SINKS

REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "tests" / "fixtures" / "quality"
PRE_COMMIT_CONFIG = REPO / ".pre-commit-config.yaml"

# fixture file -> the rule code ruff must report for it
PY_RULES = {
    "arg001.py": "ARG001",
    "ble001.py": "BLE001",
    "c901.py": "C901",
    "dtz005.py": "DTZ005",
    "pgh004.py": "PGH004",
    "pt011.py": "PT011",
    "ruf100.py": "RUF100",
    "s602.py": "S602",
    "s603.py": "S603",
    "s608.py": "S608",
    "t201.py": "T201",
    "tid251-os-system.py": "TID251",
    "tid251-sys-exit.py": "TID251",
}
# fixture file -> the Biome diagnostic category it must produce
BIOME_RULES = {
    "no-double-equals.js": "lint/suspicious/noDoubleEquals",
    "no-undeclared-variables.js": "lint/correctness/noUndeclaredVariables",
    "no-unused-variables.js": "lint/correctness/noUnusedVariables",
    "unformatted.js": "format",
}
# fixture file -> the static-lint pattern it must trip
STATIC_RULES = {"html-sink.js": SINKS, "module-syntax.js": MODULE_SYNTAX}


def _fixtures(kind: str, suffix: str) -> set[str]:
    return {p.name for p in (FIXTURES / kind).iterdir() if p.suffix == suffix}


def test_every_fixture_has_a_rule() -> None:
    assert _fixtures("python", ".py") == set(PY_RULES)
    assert _fixtures("js", ".js") == set(BIOME_RULES) | set(STATIC_RULES)


# --- Python ---------------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(PY_RULES))
def test_python_fixture_fails_its_rule(name: str, tmp_path: Path) -> None:
    # Copied out of tests/ so the tests/ per-file ignores don't apply.
    target = tmp_path / name
    shutil.copy(FIXTURES / "python" / name, target)
    done = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "--no-cache", "--output-format", "concise",
         "--config", str(REPO / "pyproject.toml"), str(target)],
        capture_output=True, text=True, encoding="utf-8", check=False,
    )  # fmt: skip
    assert done.returncode == 1, done.stdout + done.stderr
    assert re.search(rf":\d+:\d+: {PY_RULES[name]}\b", done.stdout), (
        f"{name} no longer fails {PY_RULES[name]}; is the rule still enabled?\n{done.stdout}"
    )


# --- frontend -------------------------------------------------------------------------------


def _biome_version() -> str:
    text = PRE_COMMIT_CONFIG.read_text(encoding="utf-8")
    found = re.search(r"@biomejs/biome@(\d+\.\d+\.\d+)", text)
    assert found, f"{PRE_COMMIT_CONFIG.name} pins no @biomejs/biome version"
    return found.group(1)


@pytest.fixture(scope="module")
def biome_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The JS fixtures beside a copy of the repo's Biome config, minus its file selection."""
    npx = shutil.which("npx")
    if npx is None:
        pytest.skip("npx (Node.js) is not installed; CI always has it")
    root = tmp_path_factory.mktemp("biome")
    config = json.loads((REPO / "biome.json").read_text(encoding="utf-8"))
    config.pop("files", None)
    config.pop("$schema", None)
    (root / "biome.json").write_text(json.dumps(config), encoding="utf-8")
    for name in BIOME_RULES:
        shutil.copy(FIXTURES / "js" / name, root / name)
    return root


@pytest.mark.parametrize("name", sorted(BIOME_RULES))
def test_js_fixture_fails_its_rule(name: str, biome_dir: Path) -> None:
    npx = shutil.which("npx")
    assert npx is not None
    done = subprocess.run(
        [npx, "--yes", f"@biomejs/biome@{_biome_version()}", "check", "--error-on-warnings",
         "--colors=off", name],
        capture_output=True, text=True, encoding="utf-8", check=False, cwd=biome_dir,
    )  # fmt: skip
    output = done.stdout + done.stderr
    assert done.returncode != 0, output
    assert BIOME_RULES[name] in output, f"{name} no longer fails {BIOME_RULES[name]}\n{output}"


@pytest.mark.parametrize("name", sorted(STATIC_RULES))
def test_js_fixture_fails_static_lint(name: str) -> None:
    text = (FIXTURES / "js" / name).read_text(encoding="utf-8")
    assert STATIC_RULES[name].search(text), f"{name} no longer trips the static-lint check"


# --- suppressions ---------------------------------------------------------------------------

NOQA = re.compile(r"#\s*noqa\b(?P<rest>.*)")
NOQA_OK = re.compile(r"^: [A-Z]+[0-9]+(?:, [A-Z]+[0-9]+)* - \S")
TYPE_IGNORE = re.compile(r"#\s*type:\s*ignore(?P<rest>.*)")
TYPE_IGNORE_OK = re.compile(r"^\[[a-z-]+(?:, [a-z-]+)*\]\s+#\s*\S")
NEVER_SUPPRESSED = re.compile(r"\b(S602|S604|S605)\b")


def _python_files() -> list[Path]:
    files = [p for top in ("src", "tests", "tools") for p in (REPO / top).rglob("*.py")]
    return sorted(p for p in files if FIXTURES not in p.parents)


def _comments(path: Path) -> list[tuple[int, str]]:
    source = path.read_text(encoding="utf-8")
    tokens = tokenize.generate_tokens(io.StringIO(source).readline)
    return [(tok.start[0], tok.string) for tok in tokens if tok.type == tokenize.COMMENT]


def _offenders(check: "re.Pattern[str]", ok: "re.Pattern[str]") -> list[str]:
    found = []
    for path in _python_files():
        for line, comment in _comments(path):
            match = check.search(comment)
            if match and not ok.search(match.group("rest")):
                found.append(f"{path.relative_to(REPO).as_posix()}:{line}: {comment}")
    return found


def test_noqa_names_rules_and_a_reason() -> None:
    offenders = _offenders(NOQA, NOQA_OK)
    assert not offenders, "use '# noqa: CODE - reason':\n" + "\n".join(offenders)


def test_type_ignore_names_codes_and_a_reason() -> None:
    offenders = _offenders(TYPE_IGNORE, TYPE_IGNORE_OK)
    assert not offenders, "use '# type: ignore[code]  # reason':\n" + "\n".join(offenders)


def test_shell_rules_are_never_suppressed() -> None:
    offenders = [
        f"{path.relative_to(REPO).as_posix()}:{line}: {comment}"
        for path in _python_files()
        for line, comment in _comments(path)
        if (match := NOQA.search(comment)) and NEVER_SUPPRESSED.search(match.group("rest"))
    ]
    assert not offenders, "a shell subprocess can't be suppressed:\n" + "\n".join(offenders)


def test_suppression_checks_catch_bad_comments(tmp_path: Path) -> None:
    sample = tmp_path / "sample.py"
    sample.write_text(
        "a = 1  # noqa\nb = 2  # noqa: E501\nc = 3  # noqa: E501 - long URL\n"
        "d = 4  # type: ignore\ne = 5  # type: ignore[misc]\nf = 6  # type: ignore[misc]  # ok\n"
    )
    flagged = {
        line
        for line, comment in _comments(sample)
        for check, ok in ((NOQA, NOQA_OK), (TYPE_IGNORE, TYPE_IGNORE_OK))
        if (match := check.search(comment)) and not ok.search(match.group("rest"))
    }
    assert flagged == {1, 2, 4, 5}


# --- workflows ------------------------------------------------------------------------------

WORKFLOWS = sorted((REPO / ".github" / "workflows").glob("*.yml"))
USES = re.compile(r"^\s*(?:-\s+)?uses:\s*(?P<ref>[^\s#]+)(?P<comment>.*)$")
PINNED = re.compile(r"@[0-9a-f]{40}$")


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_workflow_sets_permissions(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert re.search(r"^permissions:", text, re.MULTILINE), f"{path.name} has no permissions block"


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_workflow_actions_pinned_by_commit(path: Path) -> None:
    unpinned = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        match = USES.match(line)
        if not match or match.group("ref").startswith(("./", "docker://")):
            continue
        if not PINNED.search(match.group("ref")) or "# v" not in match.group("comment"):
            unpinned.append(f"{path.name}:{number}: {match.group('ref')}")
    assert not unpinned, "pin to a full commit with a '# vX.Y.Z' comment:\n" + "\n".join(unpinned)


# --- dependencies ---------------------------------------------------------------------------

DEPENDABOT = REPO / ".github" / "dependabot.yml"


def _runtime_dependencies() -> list[str]:
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    extras = [dep for group in project.get("optional-dependencies", {}).values() for dep in group]
    return [*project["dependencies"], *extras]


def test_runtime_dependencies_have_an_upper_bound() -> None:
    unbounded = [dep for dep in _runtime_dependencies() if "<" not in dep]
    assert not unbounded, "cap below the next major (next minor for 0.x):\n" + "\n".join(unbounded)


def test_dependabot_groups_leave_majors_out() -> None:
    config = yaml.safe_load(DEPENDABOT.read_text(encoding="utf-8"))
    offenders = [
        f"{update['package-ecosystem']}: {name}"
        for update in config["updates"]
        for name, group in update.get("groups", {}).items()
        if "major" in group.get("update-types", ["major", "minor", "patch"])
    ]
    assert not offenders, "group minor and patch only:\n" + "\n".join(offenders)
