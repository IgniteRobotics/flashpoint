## Why

The pit crew hunts loose CAN and power connectors with shake tests (disabled, wiggle each harness) and drive tests (enabled, drive hard). Today the only feedback is a stuttering motor, or `can-health` counts minutes later, after the hoots are pulled. A 40 ms dropout is invisible to the eye and to a 2 Hz dashboard. The crew needs to see, while they work, which device dropped, rebooted or faulted, and whether it was one device or the whole bus.

This is the first slice of **Live & Pit views** (`06-roadmap.md` §Live & Pit views, #75), split out as its own change. It reads the robot, not the lake, and reuses the CAN-health ideas (`can-health` spec: own-period gaps, the sibling rule) and device identity (`05-target-architecture.md` §4a).

## What Changes

- **Robot-side contract.** Robot code publishes, over NetworkTables under `/Flashpoint/Live/CANHealth/`, running totals for each CTRE device: dropouts (loops where its status signal is older than 2.5× its update period), worst signal age, reset count, fault bits and lowest supply voltage. Plus bus status, `Enabled`, and a contract version. Totals, not samples, so a 40 ms dropout counts even though NetworkTables sends updates about every 100 ms.
- **Documentation and a suggested sample:** a guide page for the contract and a Java sample (`examples/robot-java/`). The robot change itself is a separate Robot-2026 PR.
- **Live CAN service in `flashpoint serve`:**
  - one NetworkTables connection to the robot, opened while a Live CAN view is open;
  - compares each update with the previous one to produce events (dropout, reboot, fault raised, bus errors rising), maps each device to its slot through the robot configuration, and classifies dropouts as isolated or bus-wide with the sibling rule;
  - pushes state and events to browsers over server-sent events.
- **Live CAN view** in the app shell: a device grid by bus and slot (ok / dropped / rebooted / faulted / missing / unmapped), a live event log, and marks ("now wiggling: FL module") that label the events that follow. Every event is tagged enabled or disabled, so shake tests and drive tests use the same view with no mode switch.
- **Missing devices:** a slot in the robot configuration that the robot never reports shows as missing.
- **Fake robot:** a Python publisher of the contract, for tests and for work without a robot.

## Capabilities

### New Capabilities
- `live-can-health`: the robot-side contract, the live service in `flashpoint serve`, event detection, and the Live CAN view.

### Modified Capabilities
None. The live topics are written to the wpilog as `NT:/Flashpoint/Live/...`, which the `flashpoint` root (`/Flashpoint/`) does not match, so `nt-signals` behaviour is unchanged.

## Non-goals

- Writing anything to the lake: no recorded test sessions, no exported event logs, marks live only in the browser.
- The Pit board and its GO / DECIDE / NO-GO verdict (the rest of #75).
- Reading the bus when robot code is not running (the Phoenix diagnostics server was considered and dropped).
- REV and other non-CTRE devices.
- Chain-order localisation of a break (deferred, as in `can-health`).
- The robot-code change itself; this repo carries only the contract, the guide and the sample.

## Impact

- New package `flashpoint.live`; new GET routes in `web/server.py`; new view `web/static/views/live-can.js`.
- New dependency: `robotpy-ntcore`, pinned to the same 2026 release line as `robotpy-wpiutil`.
- Robot address: reuses the `acquire` host list; `flashpoint serve` gets an option to override it.
- Docs: new guide page; `serve` option in the CLI reference.
- Pitfalls fixed: none. Dropouts are compared by update interval, never by matching exact timestamps, so #14 and #38 are not reintroduced.
