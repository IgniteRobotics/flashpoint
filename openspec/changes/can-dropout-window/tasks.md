## 1. Tests first

- [ ] 1.1 Add robot-config tests for "Window declared" and "Bad window" (zero, negative, string); verify they fail before 2.1
- [ ] 1.2 Add can-health tests for "Jitter under the bus window", "Gap over the bus window" and "Slow device on a windowed bus", plus a trailing gap under the window; verify they fail before 2.2
- [ ] 1.3 Add live detector tests for "Jitter under the bus window", "Dropout over the bus window", "Bus without a window", a missing `MaxAgeMs` (kept), and short jitter on several devices not turning a real isolated dropout bus-wide; verify they fail before 2.3

## 2. Implementation

- [ ] 2.1 Add `dropout_window_ms` (optional float > 0) to `Bus` in `semantics/robot_config.py`, plus a lookup from robot-code bus name and from hoot bus; set DriveTrain to 35 ms in `config/robots/2026-comp.toml`; verify 1.1 passes and the "Valid 2026 configuration" test still passes
- [ ] 2.2 Apply the window in `semantics/can_health.py`'s gap rule (interval and trailing gaps) per hoot bus; verify 1.2 and the existing can-health tests pass, and the Q7 budget test stays under 3 s
- [ ] 2.3 Pass the window through `live/slots.py` to `live/detector.py`, and filter dropout rises before the bus-wide check; verify 1.3 and the existing live tests pass

## 3. Docs and wrap-up

- [ ] 3.1 Document `dropout_window_ms` in the robot-file section of the user guide and in `live-can-health.md` (what it hides, live on reconnect, gold on re-derive); verify `pytest tests/test_docs.py` passes
- [ ] 3.2 Run the full suite (`PYENV_VERSION=flashpoint poetry run pytest`, `-m browser`), `ruff check .`, `mypy`, and `openspec validate can-dropout-window --strict`; verify all pass
- [ ] 3.3 [HUMAN] With Robot-2026 counting at the plain 2.5× rule, repeat the disabled, untouched minute on the robot with DriveTrain at 35 ms and check LIVE CAN stays quiet; then re-derive one session and check Replay's DriveTrain gaps agree. Record on #66
