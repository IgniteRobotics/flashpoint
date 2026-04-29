# Logo/Header Title + `--hoot-dir` Flag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Flashpoint" title to the site topbar and add a `--hoot-dir` CLI flag for directory-based site rebuilds that auto-loads `motors.toml`.

**Architecture:** The topbar title is a pure HTML/CSS change to the `index.html` template. The CLI changes live entirely in `cli.py` — no new modules needed. Both features are independent and can be implemented sequentially.

**Tech Stack:** Python 3.11, argparse, pathlib, pytest (subprocess-based CLI tests)

---

## Prerequisites

`site_builder.py` and `utils/templates/index.html` exist in the `feature/static-site` worktree but not yet on `feature/power-tracking`. Both are required before this work starts. Merge or cherry-pick from `feature/static-site`, or copy manually:

```bash
# from repo root
git merge feature/static-site
# OR copy manually:
cp .worktrees/static-site/utils/site_builder.py utils/site_builder.py
cp -r .worktrees/static-site/utils/templates utils/templates
cp -r .worktrees/static-site/utils/static utils/static
```

Verify `utils/site_builder.py`, `utils/templates/index.html`, and `utils/static/plotly-*.min.js` all exist before proceeding.

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `utils/templates/index.html` | Modify | Add `#app-title` span + CSS |
| `utils/cli.py` | Modify | Add `--hoot-dir`, `--clean`; make `files` optional |
| `tests/utils/test_cli.py` | Modify | Tests for `--hoot-dir` and `--clean` flag behaviour |

---

## Task 1: Add "Flashpoint" title to the topbar

**Files:**
- Modify: `utils/templates/index.html`

- [ ] **Step 1: Verify the template exists and find the topbar**

```bash
grep -n "id=\"logo\"\|id=\"topbar\"\|id=\"app-title\"" utils/templates/index.html
```

Expected: lines for `id="topbar"` and `id="logo"`. If `id="app-title"` already appears, this task is done — skip to Task 2.

- [ ] **Step 2: Add the CSS rule for `#app-title`**

Find the `#logo` CSS block and add immediately after it:

```css
  #app-title {
    font-size: 16px;
    font-weight: 600;
    color: var(--text);
    letter-spacing: 0.5px;
  }
```

The `#logo` block looks like:
```css
  #logo { height: 32px; object-fit: contain; }
```

Add the new rule on the next line.

- [ ] **Step 3: Add the `<span>` to the topbar HTML**

Find this line in the topbar HTML:
```html
  <img id="logo" src="logo.png" alt="Flashpoint" onerror="this.style.display='none'">
```

Add the span immediately after it:
```html
  <img id="logo" src="logo.png" alt="Flashpoint" onerror="this.style.display='none'">
  <span id="app-title">Flashpoint</span>
```

- [ ] **Step 4: Verify the template renders correctly**

Rebuild the site to a temp dir and open it:

```bash
python -m utils data/*.hoot --site /tmp/fp-site-test --motor-names utils/motors.toml
open /tmp/fp-site-test/index.html
```

Confirm "Flashpoint" appears as text next to the logo in the topbar.

- [ ] **Step 5: Commit**

```bash
git add utils/templates/index.html
git commit -m "feat: add Flashpoint title to site topbar"
```

---

## Task 2: Add `--hoot-dir` and `--clean` flags to CLI

**Files:**
- Modify: `utils/cli.py`
- Modify: `tests/utils/test_cli.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/utils/test_cli.py`:

```python
def test_hoot_dir_and_positional_files_are_mutually_exclusive(tmp_path: Path) -> None:
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    dummy = tmp_path / "dummy.hoot"
    dummy.touch()
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         str(dummy), "--hoot-dir", str(hoot_dir),
         "--site", str(tmp_path / "site")],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "mutually exclusive" in result.stderr


def test_hoot_dir_nonexistent_exits_nonzero(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         "--hoot-dir", str(tmp_path / "no-such-dir"),
         "--site", str(tmp_path / "site")],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "not a directory" in result.stderr


def test_hoot_dir_requires_site(tmp_path: Path) -> None:
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    result = subprocess.run(
        [sys.executable, "-m", "utils", "--hoot-dir", str(hoot_dir)],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "--site" in result.stderr


def test_hoot_dir_empty_dir_exits_nonzero(tmp_path: Path) -> None:
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         "--hoot-dir", str(hoot_dir),
         "--site", str(tmp_path / "site")],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "no .hoot files" in result.stderr


def test_clean_removes_existing_json(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    site_dir = tmp_path / "site"
    data_dir = site_dir / "data"
    data_dir.mkdir(parents=True)
    stale = data_dir / "STALE_Q99.json"
    stale.write_text('{"match_id":"STALE_Q99"}')
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    for f in HOOT_FILES:
        import shutil
        shutil.copy(f, hoot_dir / f.name)
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         "--hoot-dir", str(hoot_dir),
         "--site", str(site_dir),
         "--clean"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert not stale.exists(), "stale JSON should have been removed by --clean"


def test_hoot_dir_merge_preserves_existing_json(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    site_dir = tmp_path / "site"
    data_dir = site_dir / "data"
    data_dir.mkdir(parents=True)
    preserved = data_dir / "PREV_Q1.json"
    preserved.write_text('{"match_id":"PREV_Q1","duration":90.0,"timestamps":[],"motors":{},"totals":{}}')
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    for f in HOOT_FILES:
        import shutil
        shutil.copy(f, hoot_dir / f.name)
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         "--hoot-dir", str(hoot_dir),
         "--site", str(site_dir)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert preserved.exists(), "pre-existing JSON should be preserved without --clean"
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/utils/test_cli.py::test_hoot_dir_and_positional_files_are_mutually_exclusive \
       tests/utils/test_cli.py::test_hoot_dir_nonexistent_exits_nonzero \
       tests/utils/test_cli.py::test_hoot_dir_requires_site \
       tests/utils/test_cli.py::test_hoot_dir_empty_dir_exits_nonzero \
       -v
```

Expected: all FAIL (unrecognized argument `--hoot-dir`).

- [ ] **Step 3: Implement the CLI changes**

Replace the argument parser setup and validation section in `utils/cli.py`. The new `main()` function body:

```python
def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m utils",
        description="Generate FRC motor power analysis PDF from hoot telemetry logs.",
    )
    parser.add_argument(
        "files", nargs="*", type=Path, metavar="FILE",
        help="Telemetry files (.hoot; one or more). Omit when using --hoot-dir.",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=Path("report.pdf"),
        help="Output PDF path (default: report.pdf)",
    )
    parser.add_argument(
        "--site", type=Path, default=None, metavar="DIR",
        help="Output site directory (accumulates matches; creates dir if needed)",
    )
    parser.add_argument(
        "--hoot-dir", type=Path, default=None, metavar="DIR",
        help="Directory to search recursively for .hoot files (mutually exclusive with FILE args)",
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Remove existing match JSONs from site before rebuilding (only with --hoot-dir --site)",
    )
    parser.add_argument(
        "--per-motor-graphs", action="store_true",
        help="Include a per-motor graph page for each motor",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.5,
        help="Voltage threshold (V) for match start/end detection (default: 0.5)",
    )
    parser.add_argument(
        "--motor-names", type=Path, default=None, metavar="PATH",
        help="TOML config mapping TalonFX IDs to display names (optional)",
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("converted_data/power_cache"),
        metavar="DIR",
        help="Directory for cached hoot conversions (default: converted_data/power_cache/)",
    )
    args = parser.parse_args()

    # ── Input resolution ──────────────────────────────────────────────────
    if args.hoot_dir is not None and args.files:
        print("error: FILE args and --hoot-dir are mutually exclusive", file=sys.stderr)
        sys.exit(1)

    if args.hoot_dir is not None:
        if not args.hoot_dir.is_dir():
            print(f"error: not a directory: {args.hoot_dir}", file=sys.stderr)
            sys.exit(1)
        if args.site is None:
            print("error: --hoot-dir requires --site", file=sys.stderr)
            sys.exit(1)
        hoot_files = sorted(args.hoot_dir.rglob("*.hoot"))
        if not hoot_files:
            print(f"error: no .hoot files found in {args.hoot_dir}", file=sys.stderr)
            sys.exit(1)
        # Auto-load bundled motors.toml unless caller provides --motor-names
        if args.motor_names is None:
            _bundled = Path(__file__).parent / "motors.toml"
            if _bundled.exists():
                args.motor_names = _bundled
    else:
        if not args.files:
            print("error: provide FILE arguments or use --hoot-dir", file=sys.stderr)
            sys.exit(1)
        hoot_files = list(args.files)

    # ── File validation ───────────────────────────────────────────────────
    missing = [f for f in hoot_files if not f.exists()]
    if missing:
        for f in missing:
            print(f"error: file not found: {f}", file=sys.stderr)
        sys.exit(1)

    non_hoot = [f for f in hoot_files if f.suffix != ".hoot"]
    if non_hoot:
        for f in non_hoot:
            print(f"error: expected .hoot file, got: {f.name}", file=sys.stderr)
        sys.exit(1)

    # ── Optional clean ────────────────────────────────────────────────────
    if args.clean and args.site is not None:
        data_dir = args.site / "data"
        if data_dir.is_dir():
            for stale in data_dir.glob("*.json"):
                stale.unlink()

    # ── Processing ────────────────────────────────────────────────────────
    match_groups = hoot_loader.find_matches(hoot_files)
    matches = []

    for match_id, files in sorted(match_groups.items()):
        print(f"Loading {match_id} ({len(files)} file(s))...")

        dfs: list[pd.DataFrame] = []
        for f in files:
            cached = hoot_loader.convert_hoot(f, args.cache_dir)
            if cached:
                dfs.append(pd.read_csv(cached, na_values=["null"]))
            else:
                print(f"  warning: skipping {f.name} (conversion produced no usable data)", file=sys.stderr)

        if not dfs:
            print(f"  No usable files for {match_id}, skipping.", file=sys.stderr)
            continue

        df = hoot_loader.merge_dataframes(dfs)
        df = trimmer.trim_to_match(df, voltage_threshold=args.threshold)
        df = analyzer.normalize(df)
        match = analyzer.build_match(match_id, df)
        matches.append(match)
        print(f"  {len(match.motors)} motors · {match.timestamps[-1]:.1f}s match duration")

    if not matches:
        print("error: no usable matches found in provided files", file=sys.stderr)
        sys.exit(1)

    names_config = _names.load(args.motor_names) if args.motor_names else None

    build_pdf = args.site is None or args.output != Path("report.pdf")
    if build_pdf:
        print(f"Building report → {args.output}")
        reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs, names_config=names_config)
        print("Done.")

    if args.site is not None:
        for match in matches:
            mn = _names.resolve(match.match_id, names_config) if names_config else None
            match_dict = site_builder.serialize_match(match, mn)
            site_builder.write_match(match_dict, args.site)
        site_builder.update_manifest(args.site)
        site_builder.ensure_index(args.site)
        print(f"Site updated → {args.site / 'index.html'}")
```

- [ ] **Step 4: Run all new tests**

```bash
pytest tests/utils/test_cli.py -v -k "hoot_dir or clean"
```

Expected: all 6 new tests PASS (integration tests skip automatically if no hoot files).

- [ ] **Step 5: Run the full test suite to check for regressions**

```bash
pytest tests/utils/test_cli.py -v
```

Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add utils/cli.py tests/utils/test_cli.py
git commit -m "feat: add --hoot-dir and --clean flags to CLI"
```
