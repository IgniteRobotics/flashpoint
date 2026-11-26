## 1. Tests first

- [x] 1.1 In `tests/report/test_export.py`, add a test that exports the two-match fixture lake with raw files and with `include_raw=False`, then reads every text file under the export (`.js`, `.html`, `.css`, the marker) and asserts none contains `str(lake.root)` or `lake.root.as_posix()`, and that the exported `data/matches.js` payload has no `lake` key but the same `matches` list as the lake's. Verify it fails against the current code with `PYENV_VERSION=flashpoint poetry run pytest tests/report/test_export.py`.
- [x] 1.2 In the same file, add a test that after the export the lake's own `report/data/matches.js` still has `lake == str(lake.root)` (served mode keeps the path). Verify it passes before and after the fix.
- [x] 1.3 In `tests/web/test_browser_replay_empty.py`, extend `test_static_export_of_practice_logs` (or add a sibling test) to assert the page text does not contain the lake path and the header shows "STATIC EXPORT". Verify with `PYENV_VERSION=flashpoint poetry run pytest -m browser tests/web/test_browser_replay_empty.py`.

## 2. Export without the lake path

- [x] 2.1 In `src/flashpoint/report/export.py`, after copying the data folder, rewrite `out/data/matches.js` from the parsed index without the `lake` key, using `to_script("FP.index", ...)` from `report/build.py`. Verify 1.1 and 1.2 pass.
- [x] 2.2 In `src/flashpoint/web/static/views/replay.js`, render the no-sessions path `<code>` only when `index.lake` is set; keep the `<lake>` placeholder in the ingest command. Verify the 1.3 test and `test_blank_lake` (served, path still shown) pass.
- [x] 2.3 Run `PYENV_VERSION=flashpoint poetry run pytest`, `PYENV_VERSION=flashpoint poetry run pytest -m browser tests/web`, `poetry run ruff check .`, and `poetry run mypy`; all pass.

## 3. Docs

- [x] 3.1 In `docs/guide/how-to/replay-on-a-usb-stick.md`, add one sentence (near "What works on the stick") that the export holds no folder path from the laptop that made it, so it is safe to hand out. Verify with `PYENV_VERSION=flashpoint poetry run pytest tests/test_docs*.py`.
- [x] 3.2 Confirm `docs/rewrite` needs no update (D6 unchanged; no design decision changes) and note that in the PR.

## 4. Field check

- [ ] 4.1 [HUMAN] Run `flashpoint report --static <folder>` on a real lake, then `grep -rIl "$HOME" <folder>` (text files only; PowerShell: `Get-ChildItem <folder> -Recurse -Include *.js,*.html,*.css | Select-String -SimpleMatch $env:USERPROFILE`) and confirm no match; open `index.html` and confirm Replay loads. Then `flashpoint serve` and confirm the header still shows the lake path.
