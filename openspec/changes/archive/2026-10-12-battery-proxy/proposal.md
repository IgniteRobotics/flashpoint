## Why

The battery proxy is the lowest supply voltage of any device. It reads one motor's own wiring drop under load as a battery brownout. On the 24-match 2026 lake, it flags brownouts in 16 matches. The Pigeon2, which draws almost no current and so sits at the bus voltage, never goes below 7.59 V, so there were probably no real brownouts this season. Most of Replay's brownout FAULTs and detection's HIGH `power` anomalies are false, and a HIGH that is usually wrong teaches students to ignore HIGH.

The per-motor dips still matter. A motor whose supply falls further below the bus for the same current has a worse connection. Dividing the drop by the current measures the connection, whatever the motor's load.

This is follow-up work to roadmap P5 (docs/rewrite/06-roadmap.md §2: deferred battery-resistance metrics; brownouts from the battery proxy) and decision D10 (05-target-architecture.md). It fixes no numbered pitfall.

## What Changes

- **Declared battery proxy.** A robot configuration may name one low-current slot on the main power bus as its battery proxy (the gyro on `2026-comp`). The battery track, the brownout and sag markers, and detection's brownout count use that device's supply voltage.
- **Fallback.** With no declared proxy, or when it was not logged in a match, the proxy is the median supply voltage across devices. Markers and anomalies say which proxy they used.
- **No hold across logging gaps.** A low sample counts only until the next sample, up to a short limit, not up to 1 s.
- **Brownout hold back to 0.02 s,** close to how a roboRIO trips.
- **New metric: supply path resistance.** Per motor, match, and phase: the slope of (bus voltage − motor supply voltage) against the motor's supply current, in milliohms. It is computed only when the current range supports a fit, and is "not available" otherwise. It becomes a baseline metric, so DRIFT, STEP, and SIBLING can flag a degrading connector or lead.
- **Labels.** Remove the GACMP Q7 brownout label: no brownout was seen, and its note says "likely a wiring drop".
- **Corpus scenarios** that relied on a Q7 brownout use a test configuration whose brownout threshold is above Q7's lowest proxy voltage.

## Capabilities

### New Capabilities
- (none)

### Modified Capabilities
- `robot-config`: an optional battery-proxy slot, validated on load.
- `match-reports`: the battery track and power markers use the declared proxy or the median fallback, with no hold across gaps; the Q7 badge scenarios use the test threshold.
- `anomaly-detection`: brownouts use the same proxy; supply path resistance is a baseline metric; the corpus brownout scenarios use the test threshold.
- `match-features`: the feature table gains supply path resistance.
- `motor-physics`: how supply path resistance is computed, and when it is not available.
- `detection-backtest`: the seeded labels drop GACMP Q7.
- `anomaly-review`: the corpus queue scenario uses the test threshold.

## Non-goals

- A Driver Station log reader (real brownout flags); it stays deferred.
- Robot-code changes to log roboRIO or PDH voltage. That's a recommendation for the robot code, not Flashpoint work.
- History trend lines for path resistance.
- Battery internal resistance.

## Impact

- **Code:**
  - `semantics/robot_config.py`, `config/robots/2026-comp.toml`;
  - `report/markers.py` and the battery track in `report/build.py`;
  - `anomaly/power.py`, `semantics/physics.py`, the gold feature writer, and `anomaly/settings.py`;
  - `config/report.toml` and `config/failure-labels.toml`.
- **Versions:** `DERIVE_VERSION` and `REPORT_VERSION` bump, so there is one re-derive and a Replay rebuild. The brownout cache invalidates through its thresholds hash.
- **Tests:** the marker, power, and corpus Replay, Anomalies, and badge tests move to the test threshold. The backtest has two seeded labels.
