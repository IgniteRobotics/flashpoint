"""Addresses on the published docs site, for every docs pointer the CLI, checks and app show.

The site is versioned (`mvp-documentation` design D-b, D-c): `<base><version>/<page>/`, where a
page's path is its source path in the guide folder, without `.md`. The version matches the
installed package: `X.Y` for a final `X.Y.Z` and its pre-releases (both tags deploy `X.Y`), `dev`
for development builds. The served app gets these addresses from `GET /api/docs`; the shipped JS
names no remote host (D6, ADR-0013).

`mvp-documentation` owns the page paths: when it moves guide pages, it updates `PAGES` here.
"""

import re
from dataclasses import dataclass

import flashpoint

DOCS_BASE = "https://igniterobotics.github.io/flashpoint/"
_FINAL_OR_PRE = re.compile(r"^(\d+)\.(\d+)(?:\.\d+)?(?:(?:a|b|rc)\d+)?$")


@dataclass(frozen=True)
class Page:
    path: str  # in the guide folder, without `.md`
    title: str


PAGES: dict[str, Page] = {
    "user-guide": Page("user-guide", "User guide"),
    "team-setup": Page("team-setup", "Setting up Flashpoint for your team"),
    "auto-import": Page("auto-import", "Automatic import guide"),
    "live-can-health": Page("live-can-health", "Live CAN health guide"),
    "cli-reference": Page("cli-reference", "CLI reference"),
    "robot-side": Page("robot-side", "Robot-side guide"),
}
# Named links the app's views use, as (page id, section anchor). /api/docs serves them.
APP_LINKS: dict[str, tuple[str, str | None]] = {
    "live-can-publisher": ("live-can-health", "adding-it-to-robot-code"),
    "match-identification": ("user-guide", "how-matches-are-identified"),
    "import-logs": ("user-guide", "import-logs-by-hand"),
}


def docs_version(version: str) -> str:
    """The docs version segment for an installed package version."""
    match = _FINAL_OR_PRE.match(version)
    if match is None or (match[1], match[2]) == ("0", "0"):
        return "dev"
    return f"{match[1]}.{match[2]}"


def docs_url(page: str, section: str | None = None) -> str:
    """The published address of a guide page (and heading), for the installed version."""
    address = f"{DOCS_BASE}{docs_version(flashpoint.__version__)}/{PAGES[page].path}/"
    return f"{address}#{section}" if section else address


def app_links() -> dict[str, str]:
    """Every page and named link, by id: the body of `GET /api/docs`."""
    links = {page: docs_url(page) for page in PAGES}
    links |= {name: docs_url(page, section) for name, (page, section) in APP_LINKS.items()}
    return links
