## 1. Hover identifies a line

- [x] 1.1 Add `at(series, index)` to the `FP.historyChart()` test hook (design §5), returning a plotted point's page coordinates. Verify with a browser test on the History fixture (`history_site`) that hovering `at('ctre:FLYWHEEL-RIGHT', <index of 2026gacmp_qm13>)` lands on the chart
- [x] 1.2 Write failing browser tests (`tests/web/test_browser.py`): "Hover names the line under the pointer" (the readout names `ctre:FLYWHEEL-RIGHT`, 66.0 °C, its slot, and its alignment; the hook reports it as the highlighted series) and "Hover near no line" (the readout gives only the match and time, and nothing is highlighted). Then add `highlight` to `FP.track` (design §2) and the nearest-line rule and readout to History (design §1, §4). Verify the tests pass and the existing History and Replay track tests still pass

## 2. Selecting a line

- [x] 2.1 Write failing browser tests: "Selecting a line selects its unit" (unit in the address, row marked, unit panel shown, still on History); "Selecting another unit's line while one is selected" (`ctre:HOOD-G` selected, select the 76 °C point of `2026gacmp_qm21`, and `ctre:INDEXER-MAIN` becomes selected); "Slot line opens Replay" (`by=slot`, select `intake-roller` at `2026gadal_qm5`, and Replay opens with that track). Then route picks as in design §3, acting on pointer-down only. Verify these and the existing `test_worsening_connector` and `test_drill_through_to_replay_not_built` pass

## 3. Docs and gate

- [x] 3.1 Add a dated follow-up note under P4 in docs/rewrite/06-roadmap.md (chart lines are identified by hover and selected by click; closes #46), and update the README's History paragraph if it describes the chart. Verify the links resolve
- [x] 3.2 Run the full gate locally (ruff, ruff format, mypy, unit, corpus, browser), open a PR into `rewrite` that closes #46, and verify CI is green on all blocking jobs
- [x] 3.3 [HUMAN] On `~/flashpoint-lake`, open History on peak temperature for an event: hover the outlier line and confirm it is named correctly, click it to select it, and try a tap on a tablet or touchscreen if one is handy. Record the result in design.md
