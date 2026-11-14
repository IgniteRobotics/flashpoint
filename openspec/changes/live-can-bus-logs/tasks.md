## 1. Tests first

- [ ] 1.1 Add browser tests for "Events stay in their bus's log" and "Restart shown on every bus", and update "Marked shake test", "Reload drops marks" and the keyboard-focus test to the per-bus logs; verify the new and updated tests fail before 2.1 (`PYENV_VERSION=flashpoint poetry run pytest -m browser`)

## 2. View

- [ ] 2.1 In `web/static/views/live-can.js`, render one event log per bus section, newest first, route each event by its `bus`, put restart events in every bus's log, and apply the log cap per bus; style in `shell.css`; verify 1.1 passes and the other Live CAN browser tests still pass
- [ ] 2.2 Check each bus's log is labelled with its bus for screen readers and reachable by keyboard; verify by the keyboard-focus test and a manual tab-through against the fake robot

## 3. Docs and wrap-up

- [ ] 3.1 Update `docs/guide/live-can-health.md` (the event log paragraph and the marks paragraph) for per-bus logs; verify `pytest tests/test_docs.py` passes
- [ ] 3.2 Run the full suite (`PYENV_VERSION=flashpoint poetry run pytest`, `-m browser`), `ruff check .`, `mypy`, and `openspec validate live-can-bus-logs --strict`; verify all pass
- [ ] 3.3 [HUMAN] Pit check on the robot: a shake test with marks, checking each bus's log only shows its own events. Record on #67
