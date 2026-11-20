"""Documentation links (spec: team-setup "Documentation links").

Every pointer to the docs that users see is an address on the published, versioned docs site,
built by `flashpoint.docs_links`; no user-facing text names a path inside the repository.
"""

import io
import re
import subprocess
import tokenize
import unicodedata
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.setup.checks import CheckContext, CheckResult, run_checks

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src" / "flashpoint"
GUIDE = REPO / "docs" / "guide"
PUBLISHED = "https://igniterobotics.github.io/flashpoint/"
URL = re.compile(r"https?://[^\s)`'\"]+")
# Install pages of other projects that a fix may name; everything else must be our docs site.
THIRD_PARTY = ("https://rclone.org/", "https://docs.astral.sh/")
DOCS_URL_CALL = re.compile(r"""docs_url\(\s*["']([a-z0-9-]+)["'](?:\s*,\s*["']([a-z0-9-]+)["'])?""")
JS_COMMENT = re.compile(r"//[^\n]*|/\*.*?\*/", re.DOTALL)


# --- no repository paths reach users -------------------------------------------------------


def _python_strings(path: Path) -> list[tuple[int, str]]:
    tokens = tokenize.generate_tokens(io.StringIO(path.read_text("utf-8")).readline)
    return [(t.start[0], t.string) for t in tokens if t.type == tokenize.STRING]


def test_no_user_facing_string_names_a_repo_docs_path() -> None:
    offenders = []
    for path in sorted(SRC.rglob("*.py")):
        for line, text in _python_strings(path):
            if "docs/guide/" in text:
                offenders.append(f"{path.relative_to(REPO)}:{line}")
    for path in sorted(SRC.rglob("*")):
        if path.suffix not in (".js", ".html") or "vendor" in path.parts:
            continue
        if "docs/guide/" in JS_COMMENT.sub("", path.read_text("utf-8")):
            offenders.append(str(path.relative_to(REPO)))
    assert not offenders, "use flashpoint.docs_links.docs_url:\n" + "\n".join(offenders)


# --- every check's fix ----------------------------------------------------------------------


def _not_ok_context(tmp_path: Path, root: Path, **kwargs: Any) -> CheckContext:
    def run(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(list(args), 1, "", "not found")  # nothing installed

    defaults: dict[str, Any] = {
        "config_root": root,
        "lake": LakePaths(tmp_path / "lake"),
        "cache_root": tmp_path / "cache",  # no owlet downloaded
        "system": "Linux",  # has an acquire service to be missing
        "season": 2026,
        "python": (3, 10),
        "run": run,
        "find_advantagescope": lambda root: (None, "not found"),
        "which": lambda name: None,
    }
    return CheckContext(**(defaults | kwargs))


def _not_ok_results(tmp_path: Path) -> list[CheckResult]:
    results = run_checks(_not_ok_context(tmp_path, tmp_path / "no-config"))
    # A configuration with no team and no robot, an unwritable lake, and logs not backed up.
    root = tmp_path / "cfg"
    root.mkdir()
    (root / "acquire.toml").write_text("", encoding="utf-8")
    lake = LakePaths(tmp_path / "lake")
    lake.ensure()
    ledger = Ledger(lake.ledger)
    try:
        ledger.register("a" * 64, "wpilog", 1, lake.raw / "a")
    finally:
        ledger.close()
    results += run_checks(_not_ok_context(tmp_path, root, lake=LakePaths(Path("/proc/nope/x"))))
    results += run_checks(_not_ok_context(tmp_path, root))  # last: its backup has a log waiting
    return results


def test_every_check_fix_links_the_published_docs(tmp_path: Path) -> None:
    results = _not_ok_results(tmp_path)
    shown = [f"{r.id}: {r.message} | {r.command or ''}" for r in results]
    assert not [s for s in shown if "docs/guide" in s]
    addresses = [url for s in shown for url in URL.findall(s)]
    ours = [url for url in addresses if not url.startswith(THIRD_PARTY)]
    assert ours, "expected the backup and service fixes to link the guide"
    assert all(url.startswith(PUBLISHED) for url in ours), ours
    by_id = {r.id: r for r in results}
    assert PUBLISHED in (by_id["backup"].command or "")
    assert PUBLISHED in (by_id["acquire-service"].command or "")


# --- links match the installed version ------------------------------------------------------


@pytest.mark.parametrize(
    ("version", "segment"), [("0.3.1", "0.3"), ("0.3.0rc1", "0.3"), ("0.4.0.dev2", "dev")]
)
def test_links_match_the_installed_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, version: str, segment: str
) -> None:
    import flashpoint

    monkeypatch.setattr(flashpoint, "__version__", version)
    backup = {r.id: r for r in _not_ok_results(tmp_path)}["backup"]
    (address,) = [u for u in URL.findall(backup.command or "") if u.startswith(PUBLISHED)]
    assert address.startswith(f"{PUBLISHED}{segment}/")


def test_local_and_prerelease_versions() -> None:
    from flashpoint.docs_links import docs_version

    assert docs_version("0.3.0") == "0.3"
    assert docs_version("1.2.0b3") == "1.2"
    assert docs_version("0.3.0a1") == "0.3"
    assert docs_version("0.3.0+g1234") == "dev"
    assert docs_version("0.0.0") == "dev"  # a checkout without a release version


# --- every link names a page and heading that exist -----------------------------------------


def slugify(heading: str) -> str:
    """The docs site's heading anchor (Python-Markdown's toc slugify)."""
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def anchors(page: Path) -> set[str]:
    found, in_fence = set(), False
    for line in page.read_text("utf-8").splitlines():
        if line.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
        elif not in_fence and (match := re.match(r"#{1,6}\s+(.+?)\s*#*\s*$", line)):
            found.add(slugify(match[1]))
    return found


def test_slugify_matches_the_site() -> None:
    assert slugify("Backup and restore") == "backup-and-restore"
    assert slugify("Look at the data: `flashpoint serve`") == "look-at-the-data-flashpoint-serve"
    assert slugify("Configure it: `acquire.toml`") == "configure-it-acquiretoml"


def _used_links() -> set[tuple[str, str | None]]:
    from flashpoint.docs_links import APP_LINKS

    used: set[tuple[str, str | None]] = set(APP_LINKS.values())
    for path in sorted(SRC.rglob("*.py")):
        for match in DOCS_URL_CALL.finditer(path.read_text("utf-8")):
            used.add((match[1], match[2]))
    return used


def test_every_docs_link_names_a_page_and_heading_that_exist() -> None:
    from flashpoint.docs_links import PAGES

    used = _used_links()
    assert used, "no docs links found"
    problems = []
    for page_id, section in sorted(used, key=str):
        if page_id not in PAGES:
            problems.append(f"{page_id}: not in docs_links.PAGES")
            continue
        source = GUIDE / f"{PAGES[page_id].path}.md"
        if not source.is_file():
            source = GUIDE / PAGES[page_id].path / "index.md"
        if not source.is_file():
            problems.append(f"{page_id}: no page {PAGES[page_id].path} under docs/guide")
        elif section is not None and section not in anchors(source):
            problems.append(f"{page_id}#{section}: no such heading in {source.name}")
    assert not problems, "\n".join(problems)
