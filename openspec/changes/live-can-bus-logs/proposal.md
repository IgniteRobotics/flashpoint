## Why

LIVE CAN has a single event log for every bus. In the 2026-11-14 pit session, the rio and the CANivore produced bursts within the same seconds, and the mixed log made it hard to see which bus a run of events belonged to during a shake test. (Issue #67.)

## What Changes

- Each bus section of LIVE CAN gets its own event log, newest first, under that bus's device grid.
- Every event goes to its bus's log: dropout, bus-wide dropout, disconnect and reconnect, reboot, fault, bus errors.
- A robot code restart belongs to no bus, so it appears in every bus's log.
- Marks work as before: a mark is attached to every event that follows, on every bus.
- The service, the event stream and the robot-side contract are unchanged. Events already carry their bus.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `live-can-health`: the Live CAN view shows one event log per bus instead of one shared log.

## Impact

- `web/static/views/live-can.js` and `web/static/shell.css` (the log moves into each bus section, and the per-log cap is applied per bus).
- Browser tests for the event log, marks, reload and keyboard focus.
- `docs/guide/live-can-health.md` (the "event log" paragraph).
