## Why

On 2026-comp's CANivore (DriveTrain), status signals update at about 250 Hz, so 2.5 update periods is only 10 ms, inside normal timing jitter. In the 2026-11-14 pit session, LIVE CAN counted dropouts with the robot disabled and untouched, so a shake test couldn't separate a loose harness from noise. The stopgap was a minimum window hard-coded in robot code (Robot-2026 PR #115). That needs a redeploy for every change, and after-match CAN health on the hoots kept the plain 2.5× rule, so live and Replay disagreed. (Issue #66.)

## What Changes

- A robot file's `[[bus]]` table can set `dropout_window_ms`, a minimum dropout length for that bus. A bus without it keeps the plain 2.5× rule.
- LIVE CAN ignores a dropout whose `MaxAgeMs` is under its bus's window. It is not an event, it is not in the device totals, and it plays no part in the bus-wide check.
- After-match CAN health applies the same window: a hoot frame gap must be at least 2.5 nominal periods *and* at least its bus's window.
- Ignite's `2026-comp` robot file sets DriveTrain to 35 ms, the value tested on the robot.
- The robot-side contract is unchanged. Robot code keeps counting with the plain 2.5× rule, and Flashpoint applies the window. Robot-2026 PR #115's hard-coded window has to come out, or Flashpoint never sees the shorter dropouts.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `robot-config`: buses gain an optional dropout window, validated on load.
- `can-health`: frame gaps respect the bus's dropout window.
- `live-can-health`: live dropout events respect the bus's dropout window.

## Impact

- `semantics/robot_config.py` (`Bus`), `semantics/can_health.py` (gap rule per hoot bus), `live/slots.py` and `live/detector.py` (window lookup and filter).
- `config/robots/2026-comp.toml`; the robot-file docs in `docs/guide/` (the drift tests cover the guide).
- A changed window takes effect live on the next robot connection, and in gold on the next re-derive. CAN health is already rebuilt on every re-derive.
- No lake schema change.
