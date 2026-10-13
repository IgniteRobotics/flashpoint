## 1. Shared scale

- [x] 1.1 Add a History fixture (in `tests/web/`) with a count metric where one unit's values are 3, 0, 1, 3 and another's are 7, 20, 1, 42; a unit with no value (null) in two matches and values 10 to 12 otherwise; a unit with a single value; and a metric whose values run from −2 to 5. Verify the fixture builds and `/api/devices` returns those `spark` lists (an API test)
- [x] 1.2 Write failing browser tests (`tests/web/test_browser.py`): Rows share one scale (every canvas has `data-scale="0 42"`, and the 0–3 unit's pixels stay in the bottom tenth of its cell, checked with `getImageData`); Negative values in range (`data-scale="-2 5"`); Unit missing values in some matches (scale 0 30, gaps kept); One value in range (no line, scale unchanged). Then add the `range` option to `FP.spark` (design §2) and compute the shared range in the History device table (design §3). Verify the tests pass, and that the existing History browser tests still pass

## 2. Description

- [x] 2.1 Write failing browser tests: the 0–3 unit's trend cell description reads its own range and the shared scale with the metric's unit (`This unit 0–3 · shared scale 0–42 …`), the single-value unit's reads its value, and the canvas exposes the same text as its accessible label. Then replace the fixed "This unit's own range" title (design §4). Verify the tests pass and `tests/web/test_static_lint.py` passes

## 3. Docs and gate

- [x] 3.1 Add a dated follow-up note under P4 in docs/rewrite/06-roadmap.md (History sparklines share one scale; closes #39), and update the README's History paragraph if it describes the trend column. Verify the links resolve
- [x] 3.2 Run the full gate locally (ruff, mypy, unit, corpus, browser), open a PR into `rewrite` that closes #39, and verify CI is green on all blocking jobs
- [ ] 3.3 [HUMAN] On `~/flashpoint-lake`, open History on the can-health metrics (isolated CAN gaps, reboots) and on peak temperature: confirm rows compare at a glance and temperature rows stay readable against the zero baseline (design, Risks). Record the result in design.md
