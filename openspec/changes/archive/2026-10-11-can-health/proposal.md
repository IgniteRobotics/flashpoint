## Why

A loose CAN or power connector costs matches, and Flashpoint can't see one. The evidence is already in the lake: Q105 at Johnson has the intake roller leader (rio TalonFX 2) silent for 820 ms, then back with `ResetCount` 1 and `Fault_BootDuringEnable`. In that same match its neighbour, the intake-extension follower (rio TalonFX 14), had 97 isolated frame gaps, against 6–43 in earlier matches. Neither shows in Replay or History.

The obvious detector is wrong. Every CANivore hoot shows 50–60 ms gaps on all devices at once, many times per match, and the robot's odometry on the same bus doesn't dip during them (1 of 90 events), so the logger is the cause. Counting raw gaps would flag every device in every match.

This is a precursor to P5 Tier 1 ("CAN dropout and brownout counts", `06-roadmap.md` §P5), counting dropouts from the hoots instead of the Driver Station log that line assumes. It builds on gold and per-unit identity (`05-target-architecture.md` §4, §4a).

## What Changes

- **Reboot events:** a device `ResetCount` increase, or `Fault_BootDuringEnable` turning on, is a reboot event, with its time and whether the robot was enabled.
- **Isolated frame gaps:**
  - Each device's frame stream is measured against its own nominal period, while enabled, skipping each log's start-up.
  - A gap counts against a device only when most bus siblings kept talking. Bus-wide gaps are reported as logger gaps, never charged to a device.
  - Gaps are matched across devices by overlapping time windows, never by equal timestamps (#14, #38).
- **Per-match counts in gold**, for each unit and slot: reboots, isolated gaps, missed frames per enabled minute, the longest isolated gap, and logger gaps per bus.
- **Views:**
  - Replay gets reboot (FAULT) and long-dropout (WARN) markers.
  - History gets per-unit reboot and isolated-gap trends, so a connector that is getting worse shows up across matches.
- **Robot code (Robot-2026#108):** logs rio and CANivore bus status (utilization, bus-off, TX-full, error counts) at 10 Hz under `/Flashpoint/CANBus/<bus>/`; this change maps it, tying the CANivore's code name (`DriveTrain`) to the serial its hoots carry. Rising error counts without gaps point to termination or noise, which gap counting can't see.

## Capabilities

### New Capabilities
- `can-health`: reboot events, isolated frame-gap detection with logger-gap filtering, and per-match CAN health counts per unit and slot.

### Modified Capabilities
- `match-reports`: reboot and long-dropout markers.
- `lifetime-trends`: per-unit reboot and isolated-gap trends.
- `nt-signals`: maps the rio and CANivore bus-status signals once robot code logs them.

## Non-goals

- Anomaly scoring and baselines (z-scores against a unit's history): P5 Tier 2 consumes these counts.
- Live or pit CAN checks during an event: there's no live view (it was cut from P4).
- Driver Station log ingest.
- Odometry-frequency dips: a rio timing signal (loop, CPU, GC), not CAN.
- Repairing or re-exporting hoots to remove logger gaps.
- Chain-order localisation (a run of neighbouring devices gapping together → suspected break). Deferred to the 2027 robot, when the wiring order can be recorded at build time.

## Impact

- New derived step, run by `flashpoint derive`, reading bronze hoot frames and writing a gold CAN-health table. `DERIVE_VERSION` bumps.
- Replay payload: new marker kinds. `REPORT_VERSION` bumps.
- History API: new metrics.
- Corpus checks: Q105 (Johnson lake) and Q7 (corpus) become the labelled cases. The golden corpus has no reboot, so a synthetic fixture covers that rule.
- Pitfalls fixed: none. The detector must not reintroduce #14 or #38.
- No new dependencies.
