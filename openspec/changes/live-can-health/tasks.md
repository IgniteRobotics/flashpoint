## 1. Dependency and contract

- [ ] 1.1 Add `robotpy-ntcore` pinned to the same 2026 line as `robotpy-wpiutil`; verify `poetry lock` and `poetry install` succeed and `python -c "import ntcore"` works under `PYENV_VERSION=flashpoint`
- [ ] 1.2 Write failing tests for a contract module in `flashpoint.live` (topic names and types for version 1, fault bit names, `<bus>/<Model>-<id>` parsing, `<bus>/Bus/` fields); implement it as the single source of topic names; verify tests pass
- [ ] 1.3 Write ADR-0016 (live view: outbound NetworkTables connection from `serve`, new dependency, still GET-only) amending ADR-0013, and add it to `docs/adr/README.md`; verify `pytest tests/test_docs.py` passes

## 2. Fake robot

- [ ] 2.1 Write failing tests for `flashpoint.live.fake_robot`: a local NetworkTables server publishing the contract from a list of steps (set totals, set fault bits, toggle `Enabled`, restart, publish wrong type, publish version 2); implement; verify a test client reads back each step's values
- [ ] 2.2 Add `python -m flashpoint.live.fake_robot --scenario {shake,drive,restart}` for local work on the view; verify each scenario runs and stops cleanly on interrupt

## 3. Detector

- [ ] 3.1 Write failing tests for baselines and rising totals: "Baseline is not an event", "Shake-test dropout while disabled", "Drive-test reboot while enabled", connected/disconnected, fault bit 0→1 only, and bus error counts; implement the pure detector (snapshot in, events out); verify tests pass
- [ ] 3.2 Write failing tests for the sibling rule by batch ("One device drops", "Whole bus drops", unmapped devices count as siblings); implement; verify tests pass, and grep the module to confirm no equality matching on timestamps
- [ ] 3.3 Write failing tests for "Robot code restarts" and "Wrong type" (plus negative counts); implement re-baselining and per-device notes; verify the detector never raises on fuzzed topic values (a hypothesis or parametrised test)
- [ ] 3.4 Write failing tests for slot mapping with the 2026 robot config ("Mapped device", "Missing slot", "Unknown project", latest season wins); implement over `[[bus]]` and `[[slot]]`; verify tests pass
- [ ] 3.5 Add a detector timing test: one batch of 40 devices in under 5 ms; verify it passes

## 4. Live service in `serve`

- [ ] 4.1 Write failing tests for the connection lifecycle against the fake robot: no connection until the first subscriber, one connection shared by two, closed 30 s (configurable in tests) after the last leaves, states connecting / connected / not publishing / unsupported version; implement in `flashpoint.live`; verify tests pass
- [ ] 4.2 Write failing tests for `GET /api/live/can/stream` (server-sent events: snapshot first, then events and snapshots at most 2 Hz, bounded queue with slow-subscriber disconnect) and `GET /api/live/can/state`; implement in `web/server.py`; verify tests pass, and that `POST` to these paths is refused
- [ ] 4.3 Add `serve --robot-host`, defaulting to the `acquire` hosts; write a test that Replay and History keep responding while the robot is unreachable ("No robot"); verify `pytest tests/web tests/test_cli.py` passes
- [ ] 4.4 Write the "Live session leaves the lake unchanged" test (hash every file under a corpus lake before and after a fake-robot run through every step); verify it passes

## 5. Live CAN view

- [ ] 5.1 Add `web/static/views/live-can.js` registered with `FP.view({ needsApi: true, ... })`: connection banner, enabled state, per-bus utilisation and error counts, device grid by slot with the seven states, totals and `MinSupplyV`; text only through `h()`; verify a browser test against the fake robot shows five devices under two buses ("Contract accepted")
- [ ] 5.2 Add the event log (newest first, enabled/disabled tag, slot, bus-wide events naming their devices) and the mark control; verify browser tests "Marked shake test" and "Reload drops marks", and that an event appears within 1 s of the fake robot's step
- [ ] 5.3 Add browser tests for "Unknown contract version", "Robot code without the publisher" (links to the guide) and "Static export" (no Live CAN in the navigation); verify keyboard focus reaches the mark control and the event log; verify `pytest -m browser` passes

## 6. Robot-side guide and sample

- [ ] 6.1 Write `examples/robot-java/CanHealthPublisher.java` (Phoenix 6 + WPILib 2026; devices plus the signal to watch for each; `refreshAll` once per loop; totals; bus status from `CANBus.getStatus`; header comment saying it's a suggestion, not compiled in CI); verify by review against the contract module's topic list
- [ ] 6.2 Write `docs/guide/live-can-health.md`: what the view is for, running a shake test and a drive test, the contract table, why the tree must not move under `/Flashpoint/CANBus/`, the loop-time check, and running the fake robot; link it from `docs/guide/README.md` and the user guide; add `serve --robot-host` to `cli-reference.md`
- [ ] 6.3 Add the "Guide matches the contract" drift test to `tests/test_docs.py` (guide table vs contract module); verify `pytest tests/test_docs.py` passes

## 7. Roadmap docs and wrap-up

- [ ] 7.1 Update `docs/rewrite/06-roadmap.md` §Live & Pit views to say live CAN health ships separately in `live-can-health`, and `04-ecosystem-and-tools.md` where it calls live dashboards out of scope; verify `pytest tests/test_docs.py` passes
- [ ] 7.2 Run the full suite (`PYENV_VERSION=flashpoint poetry run pytest` and `-m browser`) and `openspec validate live-can-health --strict`; verify both pass
- [ ] 7.3 [HUMAN] Pit check with the Robot-2026 PR deployed: shake test disabled (wiggle a drive module harness, see an isolated dropout with the mark) and drive test enabled; record the results in the change and on the tracking issue
