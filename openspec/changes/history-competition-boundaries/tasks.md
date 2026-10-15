## 1. Shared competition helpers

- [x] 1.1 Write failing browser tests (`tests/web/test_browser.py`, on the History page) that `page.evaluate` `FP.competitions` / `FP.competitionsText` / `FP.competitionMarks` (design §1): one event gives "COMPETITION · GADAL"; `2026gadal` ×3 then `2026gacmp` ×2 gives "COMPETITIONS · GADAL · 3 MATCHES → GACMP · 2 MATCHES" and marks `[{x:-0.5,label:'GADAL',line:false},{x:2.5,label:'GACMP',line:true}]`; `2025gaalb` ×2 then `2026gadal` ×3 gives "COMPETITIONS · 2025 GAALB · 2 MATCHES → 2026 GADAL · 3 MATCHES"; `2025gacmp` then `2026gacmp` is two runs. Then add the helpers to `shell.js`. Verify the tests pass
- [x] 1.2 Switch `anomalies.js` to the shared helpers (drop its private `competitions`; keep the local regex for the EVENT BOUNDARY badge). Verify `test_one_competition` and `test_evidence_spans_competitions` in `tests/web/test_browser_anomalies.py` pass unchanged

## 2. Labels that don't collide

- [x] 2.1 Write a failing browser test that mounts `FP.track` in a narrow box with 100 points and marks at x = 49.5 (`AAAA`) and 50.5 (`BBBB`), and asserts the chart's `drawnMarks` is `['AAAA']`, and that with marks at 9.5 and 59.5 it is both. Then add the overlap check and `drawnMarks` to `FP.track` (design §2). Verify it passes, and the Replay track and anomaly evidence tests still pass

## 3. History boundaries

- [x] 3.1 Write failing browser tests on `history_site` for the lifetime-trends scenarios: "Season spans two competitions" (`#competitions` text; `FP.historyChart().marks` is GADAL at -0.5 with no line and GACMP at 29.5 with a line; `drawn` holds both labels), "One competition" (`event=2026gacmp`), "Boundaries follow the filters" (clear the event filter in the bar; marks return without a reload), "No matches in range" (no `#competitions` text, no marks), and per-slot lines give the same marks. Then wire History (design §3) and extend the `FP.historyChart()` hook with `marks` and `drawn` (design §4). Verify the new tests and the existing History tests pass

## 4. Docs and gate

- [x] 4.1 Add a dated follow-up note under P4 in `docs/rewrite/06-roadmap.md` (History chart marks competition boundaries; shared with anomaly evidence). Update the History section of `docs/guide/user-guide.md` and the README's History paragraph if either describes the chart. Verify the docs link/drift tests pass
- [ ] 4.2 Run the full gate locally (ruff, ruff format, mypy, unit, corpus, browser), open a PR from `feature/history-competition-boundaries` into `rewrite`, and verify CI is green on all blocking jobs
- [ ] 4.3 [HUMAN] On `~/flashpoint-lake`, open History for the 2026 season with no event filter: confirm the GADAL/GACMP (and any other event) boundaries sit where the match numbers restart, the labels are readable, and the competitions line matches. Record the result in design.md
