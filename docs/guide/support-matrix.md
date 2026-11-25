# Support matrix and known limits

What Flashpoint works with, and how we know. Every "supported" cell names its evidence: a group of the public [log corpus](https://github.com/IgniteRobotics/flashpoint/blob/main/tests/corpus/manifest.toml) that the test suite runs on, a test, or a dated check on an outside team's logs. The corpus is Ignite's own 2025 and 2026 robots, so anything Ignite's robots don't have reads "not tested" until someone's logs show it working.

| Value | Means |
|---|---|
| supported | Tested on real logs; the evidence is named |
| supported (needs Phoenix Pro) | The same, but only with a Phoenix Pro license on the device |
| rough | Works end to end with a workaround, named with its issue |
| not tested on real logs | Only synthetic test data covers it |
| not tested | Nobody has checked it yet. It may well work: [tell us](https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a) |
| not supported | Doesn't work; the linked issue tracks it |

The tables are generated from [`support-matrix.toml`](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/support-matrix.toml).

## Devices

<!-- generated:matrix:devices -->
| Device | Replay tracks | History and motor health | Temperature | CAN health | Draft robot | Live CAN |
|---|---|---|---|---|---|---|
| Talon FX (Falcon 500, Kraken) | supported<br><small>corpus `2026-gacmp-q7`; corpus `2026-gacmp-e10`</small> | supported<br><small>corpus `2026-gacmp-q7`; corpus `2026-gacmp-e10`</small> | supported (needs Phoenix Pro)<br><small>corpus `2026-gacmp-q7`</small> | supported<br><small>corpus `2026-gacmp-q7`</small> | supported<br><small>corpus `2026-gacmp-q7`</small> | not tested on real logs<br><small>fake robot only; experimental</small> |
| Talon FXS | not tested | not tested | not tested | not tested | not tested | not tested<br><small>the sample publisher has no Talon FXS overload</small> |
| Pigeon 2 | supported<br><small>corpus `2026-gacmp-q7`; voltage and CAN timing; it's Ignite's battery proxy</small> | supported<br><small>corpus `2026-gacmp-q7`; CAN gaps and voltage</small> | not supported ([#75](https://github.com/IgniteRobotics/flashpoint/issues/75))<br><small>not tracked: its IMU temperature isn't exported; History reads N/A</small> | supported<br><small>corpus `2026-gacmp-q7`</small> | supported<br><small>corpus `2026-gacmp-q7`</small> | not tested on real logs<br><small>fake robot only; experimental</small> |
| CANcoder | supported<br><small>corpus `2026-gacmp-q7`; CAN timing</small> | supported<br><small>corpus `2026-gacmp-q7`; CAN gaps</small> | not supported ([#75](https://github.com/IgniteRobotics/flashpoint/issues/75))<br><small>not tracked: no temperature signal; History reads N/A</small> | supported<br><small>corpus `2026-gacmp-q7`</small> | supported<br><small>corpus `2026-gacmp-q7`</small> | not tested on real logs<br><small>fake robot only; experimental</small> |
| CANrange | not tested | not tested | not tested | not tested | not tested | not tested<br><small>the sample publisher has no CANrange overload</small> |
| CANdi | not tested | not tested | not tested | not tested | not tested | not tested<br><small>the sample publisher has no CANdi overload</small> |
| CANivore | supported<br><small>corpus `2026-gacmp-q7`; the devices on it</small> | supported<br><small>corpus `2026-gacmp-q7`; the devices on it</small> | supported (needs Phoenix Pro)<br><small>corpus `2026-gacmp-q7`; the devices on it</small> | supported<br><small>corpus `2026-gacmp-q7`; bus status needs the robot-side logger</small> | supported<br><small>corpus `2026-gacmp-q7`; bus names come from bus status, when logged</small> | not tested on real logs<br><small>fake robot only; experimental</small> |
| Phoenix 5 devices ([#109](https://github.com/IgniteRobotics/flashpoint/issues/109)) | not supported<br><small>they don't write hoots</small> | not supported | not supported | not supported | not supported | not supported |
| REV Spark MAX and Spark Flex ([#108](https://github.com/IgniteRobotics/flashpoint/issues/108)) | not supported | not supported | not supported | not supported | not supported | not supported |
| REV PDH ([#6](https://github.com/IgniteRobotics/flashpoint/issues/6)) | not supported<br><small>Replay uses a battery proxy</small> | not supported | not supported | not supported | not supported | not supported |
| CTRE PDP ([#6](https://github.com/IgniteRobotics/flashpoint/issues/6)) | not supported<br><small>Replay uses a battery proxy</small> | not supported | not supported | not supported | not supported | not supported |
<!-- /generated:matrix:devices -->

## Log setups

What you get from each combination of logs a robot writes. [Robot side](robot-side/index.md) says how to get to the first row.

<!-- generated:matrix:log-setups -->
| Logs | Match identity | Clock alignment | Motor tracks | Temperature | Anomalies | Draft robot |
|---|---|---|---|---|---|---|
| Pro hoots with wpilog | supported<br><small>corpus `2026-gacmp-q7`; FMS info in the log</small> | supported<br><small>corpus `2026-gacmp-q7`; payload match; enable edges as the fallback</small> | supported<br><small>corpus `2026-gacmp-q7`; corpus `2026-gacmp-e10`</small> | supported (needs Phoenix Pro)<br><small>corpus `2026-gacmp-q7`</small> | supported<br><small>corpus `2026-gacmp-q7`; [test](https://github.com/IgniteRobotics/flashpoint/blob/main/tests/anomaly/test_detect_corpus.py); beta</small> | supported<br><small>corpus `2026-gacmp-q7`</small> |
| non-Pro hoots with wpilog | not tested on real logs<br><small>the same paths as Pro logs</small> | not tested on real logs<br><small>the same paths as Pro logs</small> | not tested on real logs<br><small>the free signal set: current, voltage, position, velocity</small> | not supported ([#75](https://github.com/IgniteRobotics/flashpoint/issues/75))<br><small>free hoots carry no temperature: NEEDS PRO per bus</small> | not tested on real logs<br><small>beta; temperature is skipped, never flagged</small> | not tested on real logs |
| wpilog only | supported<br><small>corpus `2026-gacmp-p2`; corpus `2025-gadal-q30`; corpus `2025-nofms`; a log with no FMS info or match in its name is a non-match session</small> | supported<br><small>corpus `2026-gacmp-p2`; the wpilog's own clock; no hoots to align</small> | not supported ([#109](https://github.com/IgniteRobotics/flashpoint/issues/109))<br><small>motor data comes from hoots</small> | not supported ([#109](https://github.com/IgniteRobotics/flashpoint/issues/109)) | not supported ([#109](https://github.com/IgniteRobotics/flashpoint/issues/109)) | not supported ([#109](https://github.com/IgniteRobotics/flashpoint/issues/109))<br><small>drafts read the hoots' devices</small> |
| hoots only ([#8](https://github.com/IgniteRobotics/flashpoint/issues/8)) | supported<br><small>corpus `2025-gacmp-q19`; from the hoot file names</small> | not supported<br><small>no wpilog clock</small> | not supported<br><small>ingest warns: no aligned samples</small> | not supported | not supported | not tested on real logs |
| AdvantageKit wpilog with hoots | not tested on real logs<br><small>/DriverStation fields, akit_ names, then the hoots' names</small> | not tested on real logs<br><small>wall-clock only: very low confidence</small> | not tested on real logs<br><small>from the hoots</small> | not tested on real logs<br><small>from the hoots; needs Phoenix Pro</small> | not tested on real logs<br><small>beta</small> | not tested on real logs |
| rio bus only | not tested | not tested | not tested | not tested | not tested on real logs<br><small>beta; rio rates keep the configured CAN-gap floors</small> | not tested |
<!-- /generated:matrix:log-setups -->

## Platforms

<!-- generated:matrix:platforms -->
| Computer | Install | Ingest | Serve | Automatic import service | Live CAN |
|---|---|---|---|---|---|
| macOS | supported<br><small>corpus `2026-gacmp-e10`; first-run smoke in CI</small> | supported<br><small>corpus `2026-gacmp-e10`; first-run smoke in CI</small> | supported<br><small>corpus `2026-gacmp-e10`; first-run smoke in CI</small> | not supported ([#111](https://github.com/IgniteRobotics/flashpoint/issues/111))<br><small>run the watch in a terminal</small> | not tested on real logs<br><small>fake robot only; experimental</small> |
| Linux | supported<br><small>corpus `2026-gacmp-e10`; first-run smoke in CI</small> | supported<br><small>corpus `2026-gacmp-e10`; corpus `2026-gacmp-q7`; first-run smoke and corpus tests in CI</small> | supported<br><small>corpus `2026-gacmp-e10`; first-run smoke in CI</small> | not tested on real logs<br><small>systemd user service; robot pull is beta</small> | not tested on real logs<br><small>fake robot only; experimental</small> |
| Windows | supported<br><small>corpus `2026-gacmp-e10`; first-run smoke in CI</small> | supported<br><small>corpus `2026-gacmp-e10`; first-run smoke in CI</small> | supported<br><small>corpus `2026-gacmp-e10`; first-run smoke in CI</small> | not tested on real logs<br><small>scheduled task; robot pull is beta</small> | not tested |
<!-- /generated:matrix:platforms -->

## Known limits

### The 2027 season and Systemcore

This version ships a 2027 season file, so `doctor` stays green after January 1. But 2027 hoots will need a newer owlet than this version knows, and Live CAN's NetworkTables library is held below its 2027 release. Both come in Flashpoint 0.3.1, planned for kickoff week (2027-01-16): update then ([Start a new season](how-to/start-a-new-season.md)). Systemcore, the 2027 control system, is untested ([#110](https://github.com/IgniteRobotics/flashpoint/issues/110)). Season tracking: [#73](https://github.com/IgniteRobotics/flashpoint/issues/73).

### The first hoot needs internet

Flashpoint converts hoots with CTRE's `owlet`, which it downloads the first time it meets a hoot version and then keeps. A pit laptop that has never read a hoot can't read one offline. Ingest one hoot at home before the event, or [install offline](how-to/install-offline.md) with owlet included. `flashpoint doctor` checks that the owlet for each hoot format in your lake is cached.

### Hoots without a wpilog

A session with hoots but no wpilog gets no motor history: no Replay tracks, usage or match features. Ingest warns `session(s) with hoots but no wpilog`, and when every session of a season is like that, `doctor`'s "Hoots without a wpilog" check warns too. One such session among many is usually the hoot logger restarting mid-event, and is safe to ignore. The fix is on the robot: [turn on the WPILib DataLog](robot-side/datalog.md). Hoot-only sessions are also missing from motor usage totals ([#8](https://github.com/IgniteRobotics/flashpoint/issues/8)).

### Beta and experimental features

| Feature | Label | Why |
|---|---|---|
| [Robot pull](how-to/auto-import.md) (automatic import from the robot) | beta, tested on one robot | Run on Ignite's robot only so far ([#74](https://github.com/IgniteRobotics/flashpoint/issues/74)). USB import has no label |
| [Anomalies](reference/views/anomalies.md) | beta | Thresholds are tuned on one team's logs; expect findings to need judgement ([#77](https://github.com/IgniteRobotics/flashpoint/issues/77)) |
| [Live CAN](how-to/live-can-health.md) | experimental | Needs robot code, and a fast bus can show false dropouts |

### Known issues

- **Linux owlet can crash on power-loss-truncated hoots.** On Linux, CTRE's owlet sometimes exits with an error on a hoot whose tail was cut off by a power loss (macOS doesn't). Flashpoint retries the conversion, so the file usually still imports with `incomplete-read`. If a hoot stays `failed (owlet-failed)`, run `flashpoint rebuild` once. History: [#4](https://github.com/IgniteRobotics/flashpoint/issues/4) (closed), tracked end to end in [#71](https://github.com/IgniteRobotics/flashpoint/issues/71).
