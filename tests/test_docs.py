"""Documentation drift checks: relative links resolve, and every CLI subcommand is documented."""

import argparse
import importlib.util
import re
from pathlib import Path

import yaml

from flashpoint.cli import _parser

REPO = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "docs"
GUIDE = REPO / "docs" / "guide"
MKDOCS = REPO / "mkdocs.yml"
CLI_REFERENCE = GUIDE / "reference" / "cli.md"
# A page that moved keeps a stub at its old path (for 0.2.0's links); its first line is fixed.
STUB_FIRST_LINE = "# This page moved"


def _media_names() -> set[str]:
    """The files `tools/docs-media.py` writes into `assets/media/` (they're gitignored)."""
    spec = importlib.util.spec_from_file_location("docs_media", REPO / "tools" / "docs-media.py")
    assert spec is not None and spec.loader is not None
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    return set(tool.file_names(tool.load_manifest()))


MEDIA_NAMES = _media_names()

LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]*)[^)]*\)")
UNCLOSED_LINK = re.compile(r"\[[^\]]*\]\([^)]*$")
INLINE_CODE = re.compile(r"`[^`]*`")
SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


REPO_URL = re.compile(r"^https://github\.com/IgniteRobotics/flashpoint/(?:blob|tree)/main/([^#]*)")


def doc_files() -> list[Path]:
    return [REPO / "README.md", REPO / "deploy" / "README.md", *sorted(GUIDE.rglob("*.md"))]


def is_stub(path: Path) -> bool:
    with path.open(encoding="utf-8") as text:
        return text.readline().rstrip("\n") == STUB_FIRST_LINE


def link_problems(path: Path, root: Path, site: Path | None = None) -> list[str]:
    """Every broken or malformed link in one Markdown file, as `file:line: message`.

    A relative link must name a file that exists; on a page of the docs site (`site`, the
    source folder) it must also stay inside the site. Media under the site's `assets/media/` is
    generated, so a link there must name a file the media manifest produces. A link to this
    repository on GitHub (`blob/main`, `tree/main`) must name a path that exists in it.
    """
    media = (site / "assets" / "media").resolve() if site is not None else None
    on_site = site is not None and path.is_relative_to(site) and not is_stub(path)
    problems: list[str] = []
    in_fence = False
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if raw.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        line = INLINE_CODE.sub("", raw)
        where = f"{path.relative_to(root).as_posix()}:{number}"
        for match in LINK.finditer(line):
            target = match[1]
            if (repo := REPO_URL.match(target)) and not (REPO / repo[1]).exists():
                problems.append(f"{where}: {target} names a path that doesn't exist")
            if not target or target.startswith("#") or SCHEME.match(target):
                continue
            resolved = (path.parent / target.split("#", 1)[0]).resolve()
            if media is not None and resolved.parent == media:
                if resolved.name not in MEDIA_NAMES:
                    problems.append(f"{where}: {target} isn't in docs/media-manifest.toml")
            elif not resolved.exists():
                problems.append(f"{where}: broken link {target}")
            elif on_site and site is not None and not resolved.is_relative_to(site.resolve()):
                problems.append(
                    f"{where}: {target} leaves the docs site"
                    " (link it as https://github.com/IgniteRobotics/flashpoint/blob/main/...)"
                )
        if UNCLOSED_LINK.search(LINK.sub("", line)):
            problems.append(f"{where}: malformed link (no closing parenthesis)")
    return problems


def nav_entries(nav: object) -> list[str]:
    """The page paths an `mkdocs.yml` nav lists, in order (external links are skipped)."""
    if isinstance(nav, str):
        return [] if SCHEME.match(nav) else [nav]
    if isinstance(nav, list):
        return [entry for item in nav for entry in nav_entries(item)]
    if isinstance(nav, dict):
        return [entry for value in nav.values() for entry in nav_entries(value)]
    return []


def nav_problems(config: Path) -> list[str]:
    """Pages missing from the nav, nav entries with no page, and stubs the build would publish."""
    settings = yaml.safe_load(config.read_text(encoding="utf-8"))
    site = config.parent / settings.get("docs_dir", "docs")
    listed = nav_entries(settings.get("nav", []))
    excluded = {line.strip().lstrip("/") for line in settings.get("exclude_docs", "").splitlines()}
    problems = []
    for entry in listed:
        page = site / entry
        if not page.is_file():
            problems.append(f"{entry}: in the {config.name} nav but doesn't exist")
        elif is_stub(page):
            problems.append(f"{entry}: in the {config.name} nav but is a moved stub")
    for page in sorted(site.rglob("*.md")):
        name = page.relative_to(site).as_posix()
        if is_stub(page):
            if name not in excluded:
                problems.append(f"{name}: moved stub missing from {config.name} exclude_docs")
        elif name not in listed:
            problems.append(f"{name}: not in the {config.name} nav")
    return problems


def subcommands() -> list[str]:
    actions = [a for a in _parser()._actions if isinstance(a, argparse._SubParsersAction)]
    return sorted(name for action in actions for name in action.choices)


def test_fixture_good_links_pass() -> None:
    assert link_problems(FIXTURES / "good.md", FIXTURES) == []
    assert link_problems(FIXTURES / "sub" / "target.md", FIXTURES) == []


def test_fixture_broken_and_malformed_links_are_reported() -> None:
    assert link_problems(FIXTURES / "broken.md", FIXTURES) == [
        "broken.md:4: broken link missing/page.md",
        "broken.md:5: malformed link (no closing parenthesis)",
    ]


def test_fixture_nested_page_links() -> None:
    site = FIXTURES / "site"
    assert link_problems(site / "how-to" / "nested.md", FIXTURES, site) == [
        "site/how-to/nested.md:3: broken link missing-sibling.md",
        "site/how-to/nested.md:5: https://github.com/IgniteRobotics/flashpoint/blob/main/docs/adr/"
        "nope.md names a path that doesn't exist",
        "site/how-to/nested.md:6: ../../readme.md leaves the docs site"
        " (link it as https://github.com/IgniteRobotics/flashpoint/blob/main/...)",
        "site/how-to/nested.md:7: ../assets/media/nope.png isn't in docs/media-manifest.toml",
    ]


def test_fixture_stub() -> None:
    site = FIXTURES / "site"
    assert is_stub(site / "old.md")
    assert not is_stub(site / "index.md")
    assert link_problems(site / "old.md", FIXTURES, site) == []


def test_fixture_nav_reports_unlisted_pages_and_missing_entries() -> None:
    assert nav_problems(FIXTURES / "site" / "mkdocs.yml") == [
        "how-to/gone.md: in the mkdocs.yml nav but doesn't exist",
        "unlisted.md: not in the mkdocs.yml nav",
    ]


def test_relative_links_resolve() -> None:
    problems = [
        p for path in doc_files() if path.exists() for p in link_problems(path, REPO, GUIDE)
    ]
    assert problems == [], "\n".join(problems)


def test_every_site_page_is_in_the_nav() -> None:
    problems = nav_problems(MKDOCS)
    assert problems == [], "\n".join(problems)


def readme_link_problems(path: Path, root: Path) -> list[str]:
    """README is also the PyPI page, where relative links break: every link must be absolute,
    and a link into this repository must name a file or folder that exists."""
    problems: list[str] = []
    in_fence = False
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if raw.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        for match in LINK.finditer(INLINE_CODE.sub("", raw)):
            target = match[1]
            if not SCHEME.match(target):
                problems.append(f"{number}: relative link {target} (breaks on PyPI)")
            elif (repo := REPO_URL.match(target)) and not (root / repo[1]).exists():
                problems.append(f"{number}: {target} names a path that doesn't exist")
    return problems


def test_fixture_readme_links() -> None:
    assert readme_link_problems(FIXTURES / "readme.md", REPO) == [
        "3: relative link docs/guide/README.md (breaks on PyPI)",
        "4: https://github.com/IgniteRobotics/flashpoint/blob/main/docs/nope.md"
        " names a path that doesn't exist",
    ]


def test_readme_links_work_on_pypi() -> None:
    problems = readme_link_problems(REPO / "README.md", REPO)
    assert problems == [], "\n".join(problems)


def test_readme_says_where_to_get_help() -> None:
    """Spec public-release, "Questions have a public place": the README points questions at Q&A
    discussions and bugs at the issue forms."""
    text = (REPO / "README.md").read_text(encoding="utf-8")
    line = next((line for line in text.splitlines() if "Getting help" in line), "")
    assert "https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a" in line
    assert "https://github.com/IgniteRobotics/flashpoint/issues/new/choose" in line


def test_every_subcommand_is_in_the_cli_reference() -> None:
    text = CLI_REFERENCE.read_text(encoding="utf-8") if CLI_REFERENCE.exists() else ""
    headings = set(re.findall(r"^## `flashpoint ([a-z-]+)`", text, flags=re.MULTILINE))
    missing = [name for name in subcommands() if name not in headings]
    assert missing == [], f"undocumented in {CLI_REFERENCE.name}: {', '.join(missing)}"


# --- live-can-health: "Guide matches the contract" ----------------------------------------

LIVE_GUIDE = GUIDE / "how-to" / "live-can-health.md"
GUIDE_TYPES = {"integer": "int", "double": "double", "boolean": "boolean", "string": "string"}
CONTRACT_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*(\w+)\s*\|")


def test_live_guide_states_the_experimental_status() -> None:
    """Spec live-can-health, "Live CAN is marked experimental": "Guide states the status"."""
    text = LIVE_GUIDE.read_text(encoding="utf-8")
    first = text.split("\n\n", 2)[1]
    assert not first.startswith("#"), "the guide's first paragraph should follow its title"
    lowered = first.lower()
    assert "experimental" in lowered
    assert "robot code" in lowered
    assert "false dropouts" in lowered
    assert "fast bus" in lowered


def guide_contract() -> dict[str, str]:
    """The guide's contract table: topic pattern (relative to the root) -> contract type."""
    text = LIVE_GUIDE.read_text(encoding="utf-8")
    section = text.split("\n## The contract\n", 1)[1].split("\n## ", 1)[0]
    rows = [CONTRACT_ROW.match(line) for line in section.splitlines()]
    return {m[1]: GUIDE_TYPES.get(m[2].lower(), m[2]) for m in rows if m}


def test_guide_matches_the_contract() -> None:
    from flashpoint.live import contract

    expected = dict(contract.TOP_FIELDS)
    expected |= {f"<bus>/<Model>-<id>/{f}": t for f, t in contract.DEVICE_FIELDS.items()}
    expected |= {f"<bus>/{contract.BUS_DIR}/{f}": t for f, t in contract.BUS_FIELDS.items()}
    assert guide_contract() == expected
    text = LIVE_GUIDE.read_text(encoding="utf-8")
    assert f"`{contract.ROOT}`" in text
    assert f"Contract version: `{contract.CONTRACT_VERSION}`" in text
    for bit, name in contract.FAULT_BITS.items():
        assert f"{bit} {name}" in text


# --- release checklist (spec public-release) ------------------------------------------------

DEVELOPER_GUIDE = GUIDE / "under-the-hood" / "developer-guide.md"
CHECKLIST_STEPS = (
    ("promote", re.compile(r"`development`.*`main`|promot", re.IGNORECASE)),
    ("TestPyPI dry run", re.compile(r"TestPyPI.*dry run|dry run.*TestPyPI", re.IGNORECASE)),
    ("smoke on the tag", re.compile(r"smoke", re.IGNORECASE)),
    ("clean installs", re.compile(r"clean install.*macOS.*Linux.*Windows", re.IGNORECASE)),
    ("tag and approve", re.compile(r"\btag\b.*approv", re.IGNORECASE)),
    ("GitHub Release and latest", re.compile(r"GitHub Release.*latest", re.IGNORECASE)),
    ("yank policy", re.compile(r"\byank", re.IGNORECASE)),
    ("docs deploy", re.compile(r"documentation.*deploy|docs.*deploy", re.IGNORECASE)),
)
NUMBERED = re.compile(r"^(\d+)\. (.*)$")
NEXT_PATCH = re.compile(
    r"^\*\*Next patch release:\*\* "
    r"(?:none scheduled\.|\d+\.\d+\.\d+, owner .+, target \d{4}-\d{2}-\d{2}\.)$",
    re.MULTILINE,
)


def _guide_section(heading: str) -> str:
    text = DEVELOPER_GUIDE.read_text(encoding="utf-8")
    assert f"\n{heading}\n" in text, f"developer guide has no '{heading}' section"
    return text.split(f"\n{heading}\n", 1)[1].split("\n## ", 1)[0]


def test_release_checklist_lists_the_eight_steps_in_order() -> None:
    section = _guide_section("## Releasing")
    items = [m for line in section.splitlines() if (m := NUMBERED.match(line))]
    assert [int(m[1]) for m in items] == list(range(1, 9))
    for (name, pattern), item in zip(CHECKLIST_STEPS, items, strict=True):
        assert pattern.search(item[2]), f"step {item[1]} should be '{name}': {item[2]}"


def test_release_checklist_forbids_closing_keywords_in_prep_prs() -> None:
    section = _guide_section("## Releasing")
    assert "closing keyword" in section
    assert "`development`" in section
    assert re.search(r"close.*by hand", section, re.IGNORECASE)


def test_release_checklist_names_the_next_patch_release() -> None:
    assert NEXT_PATCH.search(_guide_section("## Releasing"))


def test_corpus_releases_are_never_latest() -> None:
    assert "--latest=false" in _guide_section("### The golden corpus")
