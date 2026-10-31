# 0016. Live view: an outbound robot connection from `serve`

- Status: Proposed
- Date: 2026-10-31
- Amends: [0013](0013-views-one-local-app.md) (the app reads the lake and nothing else)
- Deciders: Josh
- Refs: openspec change `live-can-health` (design.md D-c, D-d, D-e), `06-roadmap.md` §Live & Pit views, pitfalls #14, #18, #38

## Context
ADR-0013 reserved a place in the shell for Live and Pit, which "read the robot", but the app it describes only reads the lake. The pit crew's shake tests (disabled, wiggle each harness) and drive tests (enabled, drive hard) need to see dropouts, reboots and faults as they happen, not minutes later from the hoots. `live-can-health` adds that view: robot code publishes running totals over NetworkTables under `/Flashpoint/Live/CANHealth/`, and the server works out what changed.

## Decision
`flashpoint serve` may open **one outbound NetworkTables client connection to the robot**:
- only while at least one Live CAN view is subscribed, shared by every subscriber, and closed 30 s after the last one leaves; nothing connects when no live view is open;
- to the `acquire` hosts (`10.TE.AM.2`, `roborio-TEAM-frc.local`, `172.22.11.2`) unless `serve --robot-host` names one;
- through **`pyntcore`** (RobotPy's ntcore, `import ntcore`), a new runtime dependency pinned to the same 2026 line as `robotpy-wpiutil`. It is imported only when the connection opens.

The server compares each NetworkTables update with the previous one in Python (`flashpoint.live`) and sends state and events to browsers over **server-sent events** on `GET /api/live/can/stream` (plus `GET /api/live/can/state`). Both routes are GET-only and change nothing. The connection is a NetworkTables **client that only subscribes**: it publishes nothing to the robot.

**Unchanged:** the app writes nothing new. The live service never touches the lake, the ledger or the configuration; marks and the event log live in the browser. The static export has no live view.

## Consequences
- The pit crew sees a dropout within a second, tagged enabled or disabled, without pulling logs. `can-health` stays the exact, after-the-match picture.
- `serve` now makes a network connection it didn't before, and every install carries `pyntcore` and its native libraries. Its Linux wheels need glibc 2.35 (Ubuntu 22.04 or later); the clean-install CI job covers macOS, Windows and Ubuntu.
- A missing robot must never stall the other views: the connection runs in its own thread, and stream threads are bounded (a slow browser is disconnected and reconnects).
- Served with `--host` on the pit network, the robot's live CAN state is visible to anyone on that network. It is read-only, the same exposure `--host` already warns about.
- Dropouts are grouped by the update the server reads, never by matching exact timestamps, so #14 and #38 are not reintroduced.

## Alternatives considered
- **A NetworkTables 4 client in the browser** (WebSocket on port 5810): needs a msgpack-over-WebSocket client in hand-written JavaScript (the area ADR-0013 keeps small) and moves detection out of tested Python.
- **The Phoenix diagnostics server on the rio:** works without robot code, but is undocumented and changes between Phoenix versions.
- **A separate live tool:** a second thing to install, and a second look; ADR-0013 already chose one app.
