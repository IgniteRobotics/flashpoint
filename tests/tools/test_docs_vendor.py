"""The docs site's own copy of the diagram renderer (spec project-documentation, "Diagrams render
as pictures"; docs-visual-polish task 2.1). `docs-media.py vendor` fetches the pinned
`mermaid.min.js` by SHA-256, and `check-site` refuses a site that shows a diagram without it."""

import hashlib
import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[2]
DIAGRAM_PAGE = '<pre class="mermaid"><code>flowchart LR\n  a --&gt; b</code></pre>'


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_media", REPO / "tools" / "docs-media.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fake_fetch(tool: ModuleType, served: bytes, fetched: list[str]) -> SimpleNamespace:
    def fake_download(name: str, release_url: str, out: Path) -> None:
        fetched.append(f"{release_url}/{name}")
        out.write_bytes(served)

    return SimpleNamespace(download=fake_download, sha256_of=tool._fetch_corpus().sha256_of)


def test_mermaid_is_pinned_to_one_release() -> None:
    tool = _tool()
    url, digest = tool.VENDORED["mermaid.min.js"]
    assert url == f"https://unpkg.com/mermaid@{tool.MERMAID_VERSION}/dist"
    assert tool.MERMAID_VERSION.startswith("12.")
    assert tool.MERMAID_VERSION.count(".") == 2  # an exact release, not a range
    assert len(digest) == 64


def test_vendor_writes_the_file_when_its_checksum_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tool = _tool()
    served = b"globalThis.mermaid = {};"
    url, _ = tool.VENDORED["mermaid.min.js"]
    monkeypatch.setattr(
        tool, "VENDORED", {"mermaid.min.js": (url, hashlib.sha256(served).hexdigest())}
    )
    fetched: list[str] = []
    fake = _fake_fetch(tool, served, fetched)
    monkeypatch.setattr(tool, "_fetch_corpus", lambda: fake)
    assert tool.main(["vendor", "--out", str(tmp_path)]) == 0
    assert (tmp_path / "mermaid.min.js").read_bytes() == served
    assert fetched == [f"{url}/mermaid.min.js"]
    # Already there with the right checksum: nothing is fetched again.
    assert tool.main(["vendor", "--out", str(tmp_path)]) == 0
    assert len(fetched) == 1
    assert [p.name for p in tmp_path.iterdir()] == ["mermaid.min.js"]


def test_vendor_refuses_a_file_that_doesnt_match_its_pin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tool = _tool()
    served = b"not the pinned release"
    _, pinned = tool.VENDORED["mermaid.min.js"]
    got = hashlib.sha256(served).hexdigest()
    fake = _fake_fetch(tool, served, [])
    monkeypatch.setattr(tool, "_fetch_corpus", lambda: fake)
    with pytest.raises(SystemExit) as raised:
        tool.main(["vendor", "--out", str(tmp_path)])
    message = str(raised.value.code)
    assert "mermaid.min.js" in message
    assert pinned in message
    assert got in message
    assert list(tmp_path.iterdir()) == []


def test_check_site_fails_a_diagram_without_the_renderer(tmp_path: Path) -> None:
    tool = _tool()
    (tmp_path / "under-the-hood" / "how-it-works").mkdir(parents=True)
    page = tmp_path / "under-the-hood" / "how-it-works" / "index.html"
    page.write_text(f"<article>{DIAGRAM_PAGE}</article>", encoding="utf-8")
    (tmp_path / "index.html").write_text("<p>no diagram here</p>", encoding="utf-8")
    assert tool.missing_renderer(tmp_path) == [
        "under-the-hood/how-it-works/index.html: shows a diagram, and the site has no"
        " assets/vendor/mermaid.min.js"
    ]
    assert tool.main(["check-site", str(tmp_path)]) == 1
    (tmp_path / "assets" / "vendor").mkdir(parents=True)
    (tmp_path / "assets" / "vendor" / "mermaid.min.js").write_text("x", encoding="utf-8")
    assert tool.missing_renderer(tmp_path) == []
    assert tool.main(["check-site", str(tmp_path)]) == 0


def test_check_site_passes_a_site_with_no_diagram_and_no_renderer(tmp_path: Path) -> None:
    tool = _tool()
    (tmp_path / "index.html").write_text("<pre><code>mermaid</code></pre>", encoding="utf-8")
    assert tool.missing_renderer(tmp_path) == []
