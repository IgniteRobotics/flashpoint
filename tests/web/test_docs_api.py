"""`GET /api/docs`: the docs addresses the served app links to (spec: team-setup
"Documentation links")."""

from pathlib import Path
from typing import Any

from flashpoint.lake.paths import LakePaths

PUBLISHED = "https://igniterobotics.github.io/flashpoint/"


def test_docs_returns_the_page_map(serve: Any, tmp_path: Path) -> None:
    from flashpoint.docs_links import APP_LINKS, PAGES, docs_url

    app = serve(LakePaths(tmp_path / "lake"))
    status, docs = app.json("/api/docs")
    assert status == 200
    assert set(docs) == set(PAGES) | set(APP_LINKS)
    assert all(str(url).startswith(PUBLISHED) for url in docs.values())
    assert docs["live-can-health"] == docs_url("live-can-health")
    page, section = APP_LINKS["live-can-publisher"]
    assert docs["live-can-publisher"] == docs_url(page, section)
