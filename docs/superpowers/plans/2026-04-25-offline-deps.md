# Offline Dependencies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bundle Plotly.js as a static package asset so the generated site works with no internet connection.

**Architecture:** Commit `plotly-2.35.2.min.js` to `utils/static/`. `ensure_index` in `site_builder.py` copies it to `site/plotly.min.js` on every run. The HTML template references the local file instead of the CDN.

**Tech Stack:** Python 3.11+, pytest, shutil (stdlib)

**Working directory:** `/Users/y0shi/workspace/ignite/flashpoint/.worktrees/static-site`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `utils/static/plotly-2.35.2.min.js` | Create | Bundled Plotly.js — downloaded once, committed |
| `utils/site_builder.py` | Modify | Add `_PLOTLY_PATH`; update `ensure_index` to copy it |
| `utils/templates/index.html` | Modify | Swap CDN `<script src>` for local `plotly.min.js` |
| `tests/utils/test_site_builder.py` | Modify | Add 2 tests; patch `_PLOTLY_PATH` in existing tests |

---

## Task 1: Download and commit Plotly.js

**Files:**
- Create: `utils/static/plotly-2.35.2.min.js`

- [ ] **Step 1: Create the static directory and download Plotly**

```bash
mkdir -p utils/static
curl -o utils/static/plotly-2.35.2.min.js \
  https://cdn.plot.ly/plotly-2.35.2.min.js
```

- [ ] **Step 2: Verify the file downloaded correctly**

```bash
wc -c utils/static/plotly-2.35.2.min.js
```

Expected: a number around `3400000` (3.4 MB). If it's tiny (a few hundred bytes), the download redirected or failed — re-run the curl command.

- [ ] **Step 3: Commit**

```bash
git add utils/static/plotly-2.35.2.min.js
git commit -m "chore: bundle plotly-2.35.2.min.js as static asset"
```

---

## Task 2: Update `site_builder.py` and tests

**Files:**
- Modify: `utils/site_builder.py:90-124`
- Modify: `tests/utils/test_site_builder.py`

- [ ] **Step 1: Write the two new failing tests**

Open `tests/utils/test_site_builder.py` and append these two tests at the end of the file:

```python
def test_ensure_index_copies_plotly(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    plotly_path = tmp_path / "plotly.src.js"
    plotly_path.write_text("var Plotly={};")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    monkeypatch.setattr("utils.site_builder._PLOTLY_PATH", plotly_path)
    ensure_index(tmp_path)
    assert (tmp_path / "plotly.min.js").exists()
    assert (tmp_path / "plotly.min.js").read_text() == "var Plotly={};"


def test_ensure_index_overwrites_plotly(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    plotly_path = tmp_path / "plotly.src.js"
    plotly_path.write_text("var Plotly={};")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    monkeypatch.setattr("utils.site_builder._PLOTLY_PATH", plotly_path)
    (tmp_path / "plotly.min.js").write_text("OLD")
    ensure_index(tmp_path)
    assert (tmp_path / "plotly.min.js").read_text() == "var Plotly={};"
```

- [ ] **Step 2: Run new tests to confirm they fail**

```bash
pytest tests/utils/test_site_builder.py::test_ensure_index_copies_plotly \
       tests/utils/test_site_builder.py::test_ensure_index_overwrites_plotly -v
```

Expected: both FAIL — `AttributeError: module 'utils.site_builder' has no attribute '_PLOTLY_PATH'`

- [ ] **Step 3: Add `_PLOTLY_PATH` constant to `site_builder.py`**

In `utils/site_builder.py`, find lines 90-91:

```python
_TEMPLATE_PATH: Path | None = Path(__file__).parent / "templates" / "index.html"
_LOGO_PATH: Path | None = Path(__file__).parent.parent / "media" / "logo.png"
```

Replace with:

```python
_TEMPLATE_PATH: Path | None = Path(__file__).parent / "templates" / "index.html"
_LOGO_PATH: Path | None = Path(__file__).parent.parent / "media" / "logo.png"
_PLOTLY_PATH: Path | None = Path(__file__).parent / "static" / "plotly-2.35.2.min.js"
```

- [ ] **Step 4: Patch the two existing `ensure_index` tests to also mock `_PLOTLY_PATH`**

In `tests/utils/test_site_builder.py`, find `test_ensure_index_writes_html` and `test_ensure_index_does_not_overwrite`. Add a `monkeypatch.setattr` for `_PLOTLY_PATH` to each so they don't copy the real 3.4 MB file during tests.

Replace:

```python
def test_ensure_index_writes_html(tmp_path: Path, monkeypatch):
    # Patch the template path to a minimal HTML string
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    ensure_index(tmp_path)
    assert (tmp_path / "index.html").exists()
    assert "TEMPLATE" in (tmp_path / "index.html").read_text()


def test_ensure_index_does_not_overwrite(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    (tmp_path / "index.html").write_text("EXISTING")
    ensure_index(tmp_path)
    assert (tmp_path / "index.html").read_text() == "EXISTING"
```

With:

```python
def test_ensure_index_writes_html(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    monkeypatch.setattr("utils.site_builder._PLOTLY_PATH", None)
    ensure_index(tmp_path)
    assert (tmp_path / "index.html").exists()
    assert "TEMPLATE" in (tmp_path / "index.html").read_text()


def test_ensure_index_does_not_overwrite(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    monkeypatch.setattr("utils.site_builder._PLOTLY_PATH", None)
    (tmp_path / "index.html").write_text("EXISTING")
    ensure_index(tmp_path)
    assert (tmp_path / "index.html").read_text() == "EXISTING"
```

- [ ] **Step 5: Update `ensure_index` to copy Plotly unconditionally**

In `utils/site_builder.py`, replace the `ensure_index` function (lines 116-124):

```python
def ensure_index(site_dir: Path) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)
    if _PLOTLY_PATH and _PLOTLY_PATH.exists():
        shutil.copy(_PLOTLY_PATH, site_dir / "plotly.min.js")
    index = site_dir / "index.html"
    if index.exists():
        return
    if _TEMPLATE_PATH and _TEMPLATE_PATH.exists():
        shutil.copy(_TEMPLATE_PATH, index)
    if _LOGO_PATH and _LOGO_PATH.exists():
        shutil.copy(_LOGO_PATH, site_dir / "logo.png")
```

- [ ] **Step 6: Run the full test suite**

```bash
pytest tests/utils/test_site_builder.py -v
```

Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
git add utils/site_builder.py tests/utils/test_site_builder.py
git commit -m "feat: bundle plotly — ensure_index copies it to site dir"
```

---

## Task 3: Update the HTML template

**Files:**
- Modify: `utils/templates/index.html:7`

- [ ] **Step 1: Swap the CDN script tag for a local reference**

In `utils/templates/index.html`, replace line 7:

```html
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
```

With:

```html
<script src="plotly.min.js"></script>
```

- [ ] **Step 2: Smoke-test the site with no internet**

```bash
python -m utils path/to/match.hoot --site /tmp/fp-site
ls /tmp/fp-site/
```

Expected output includes `plotly.min.js` in the listing alongside `index.html`, `manifest.json`, `logo.png`.

Open the site in a browser (served via local HTTP server) and verify charts render:

```bash
python -m http.server 8765 --directory /tmp/fp-site
open http://localhost:8765
```

Add a match, click through all four tabs — Stats, Motor & Supply Charts, Total Power, Supply W/RPS — and confirm charts render without any network requests.

- [ ] **Step 3: Commit**

```bash
git add utils/templates/index.html
git commit -m "feat: load plotly from local bundle, remove CDN dependency"
```
