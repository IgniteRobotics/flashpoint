## 1. Baseline (before any code change)

- [ ] 1.1 On the reference laptop, with the corpus fetched, run `PYENV_VERSION=flashpoint poetry run pytest -m perf tests/test_ingest_perf.py` and record Q7 wall time, peak RSS, and the Q7 silver size on disk (`du -sh` of its `session_id=` folder). Verify: the three numbers are written into the PR description under "Before".

## 2. Silver decodes the enable string (design D1, D2)

- [x] 2.1 Test first, in a new file `tests/semantics/test_silver_device_enable.py`, with its own bronze helper that takes `v_str` (don't edit `test_silver.py`). Cases: one TalonFX's `DeviceEnable` logged as `Enabled` then `Disabled` gives two `device_enable` rows valued 1.0 and 0.0, with phase and match time set. An unknown string (`"Foo"`) gives no row while that device's `SupplyCurrent` row is still written. A numeric metric on the same device keeps its value. Verify: the string cases fail on `development` with no `device_enable` rows, and the existing boolean case in `test_silver.py` passes.
- [x] 2.2 In `src/flashpoint/semantics/silver.py`, append the `DeviceEnable`-gated `CASE` on `v_str` as the last `COALESCE` argument of the device value. Verify: 2.1 passes, `tests/semantics/test_silver.py` passes unchanged, and so do `ruff check .` and `mypy`.

## 3. Lakes upgrade on derive (design D3)

- [x] 3.1 Test first, in the new file from 2.1: derive a synthetic session, then derive again with `DERIVE_VERSION` monkeypatched one higher, and assert the session is rebuilt (`rebuilt == 1`). Verify: it passes once 3.2 lands, and a second derive at the same version rebuilds 0.
- [x] 3.2 Bump `DERIVE_VERSION` in `src/flashpoint/config.py` from 5 to 6 and add `v6: silver device_enable` to its comment. Verify: 3.1 passes, and full `poetry run pytest` passes.

## 4. Corpus check on Q7 (spec scenario "Q7 enable state follows the match")

- [x] 4.1 Test first, in a new file `tests/semantics/test_silver_device_enable_corpus.py` (`corpus` marker, reusing the `corpus_lake` fixture). Assert that on Q7 every TalonFX slot has `device_enable` rows, that values are only 0.0 or 1.0, that at least 95 % of the `auto` and `teleop` rows are 1, and that at most 1 % of the `pre`, `gap` and `post` rows are 1. Verify: it fails on `development` and passes after group 2.
- [x] 4.2 Run `pytest -m corpus tests/semantics/test_usage_corpus.py tests/semantics/test_derive_corpus.py tests/report/test_build_corpus.py`, unchanged. Verify: all pass, so `powered_s` and `enabled_s` are unchanged and reports still build.

## 5. Budget check (design Risks)

- [x] 5.1 Re-run the 1.1 measurements on the same laptop. Verify: Q7 wall time grows by at most 1 s and peak RSS by at most 50 MB over "Before", and silver size is recorded under "After" in the PR. If either budget fails, filter gold's per-motor load in `src/flashpoint/semantics/gold.py` to the metrics it reads, then re-measure until both pass.

## 6. Records and wrap-up

- [x] 6.1 In `docs/rewrite/05-target-architecture.md` §4 (data model), note on the silver samples line that `device_enable` is decoded from Phoenix 6's `Enabled`/`Disabled` text and that unknown text is dropped. Verify: `openspec validate silver-device-enable --strict` and the docs link check pass.
- [ ] 6.2 Run the full gate: `ruff check .`, `mypy`, `pytest`, `pytest -m corpus`, and `openspec validate silver-device-enable --strict`. Verify: all pass, and CI `ci-ok` is green on the PR (into `development`, body says `Closes #5`).
- [ ] 6.3 [HUMAN] On the main lake, run `flashpoint derive` and open one GACMP qualification match in DuckDB or Replay data. Verify: `device_enable` exists for TalonFX slots and the derive reports every session rebuilt. Josh's OK is recorded on the PR.
- [ ] 6.4 Run `openspec archive silver-device-enable` as the PR's last commit. Verify: `openspec validate --all` passes.
