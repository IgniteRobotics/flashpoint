## Context

See proposal.md for the motivation and specs/live-can-health/spec.md for the behaviour. The facts that shape the design:

- **The app is one local server** (ADR-0013, D6/D7): stdlib `ThreadingHTTPServer`, plain-script views registered with `FP.view({...})`, `needsApi: true` hides a view in static mode. It reads the lake and writes only triage rows (ADR-0015). ADR-0013 already reserves a slot for Live and Pit, which "read the robot".
- **Robot-side Flashpoint logging already exists.** Robot code (Robot-2026#108) writes `/Flashpoint/CANBus/<bus>/*` straight to the wpilog at 10 Hz, and `/Flashpoint/CANInventory` at boot. The season config maps the `flashpoint` root to `/Flashpoint/` with no `NT:` prefix (`config/seasons/2026.toml`).
- **The robot config already ties bus names together**: `[[bus]]` maps the robot-code name (`DriveTrain`) to the hoot bus (`canivore`), and `[[slot]]` gives (bus, model, CAN id) → slot.
- **Robot addresses** are in `AcquireConfig.hosts` (`10.68.29.2`, `roborio-6829-frc.local`, `172.22.11.2`).
- **NetworkTables sends updates about every 100 ms by default.** A shake-test dropout lasts 20–200 ms. `can-health` measured CANivore frame periods of about 4 ms and rio periods of 8–10 ms (archive `2026-10-11-can-health/design.md`).

## Goals / Non-Goals

**Goals:**
- Robot code counts dropouts; the laptop only works out what changed between updates. A slow link can delay an event, but it can't hide one.
- All detection lives in Python (`flashpoint.live`) and is tested with pytest against the fake robot. The view only renders.
- No cost when nobody is looking: no robot connection unless a Live CAN view is open.

**Non-Goals:**
- No reuse of the `can-health` gold code: that works on frame streams in bronze, this works on totals. They share the rules (2.5× period, sibling rule at ≥ half), not code.
- No history beyond the server process: state and totals reset when `serve` restarts.

## Decisions

### D-a. Robot publishes running totals, not samples
Each loop (20 ms) the robot checks every device's watched status signal and keeps running totals. It publishes those totals over NetworkTables, and the server compares each update with the one before.
- *Why:* sampling "connected now?" at 10 Hz misses most 40 ms dropouts. A total that rises by 2 can't be missed, whenever it is read.
- *Alternative:* publish every loop's state and detect on the laptop. Rejected: NetworkTables would still batch updates at about 100 ms and drop the in-between values.
- *Alternative:* Phoenix diagnostics server on the rio (works without robot code). Dropped in exploration: undocumented and changes between versions.
- *Cost:* the 20 ms loop limits timing to one loop, so a dropout shorter than one loop on a 4 ms signal can be missed. `can-health` still catches those after the match.

### D-b. Dropout = watched signal older than 2.5× its update period
The sample watches one status signal per device, refreshes all of them once per loop with `BaseStatusSignal.refreshAll`, and compares each signal's age with 2.5× the update frequency it set. This is the same factor as `can-health`'s gap rule (`GAP_FACTOR`), so a live dropout and a hoot gap mean roughly the same thing.
- *Alternative:* `ParentDevice.isConnected()`. Kept, as `Connected`, but its fixed staleness window is too coarse to count short dropouts.

### D-c. Topics live under `/Flashpoint/Live/CANHealth/`, over NetworkTables
NetworkTables topics are written to the wpilog as `NT:/Flashpoint/Live/...` when the robot logs NetworkTables. That doesn't start with `/Flashpoint/`, so the `flashpoint` root and every `nt-signals` match stay unchanged. The bus-status entries are published to NetworkTables as well, under the same root, so the server reads one tree. The DataLog writer for `/Flashpoint/CANBus/` stays as it is.
- *Watch:* nobody should "tidy" the live tree into `/Flashpoint/CANBus/`: the NetworkTables copy would then match a different name and break the bus-status mapping. The guide says so.
- Device names use the hoot form (`TalonFX-11`), so they read the same as Replay and the robot config.

### D-d. One server-side NetworkTables client, server-sent events to browsers
`flashpoint.live` holds one `pyntcore` (RobotPy's ntcore, `import ntcore`) client instance, started by the first `GET /api/live/can/stream` subscriber and stopped 30 s after the last one leaves. A detector thread reads the topic tree on each update, works out what changed, and pushes state snapshots (≤ 2 Hz) and events (as they happen) to every subscriber over server-sent events. A late joiner gets the current snapshot first.
- *Why:* the rules can be tested in Python; there's one robot connection however many people are watching; it needs no new front-end protocol code.
- *Alternative:* a NetworkTables 4 client in the browser (WebSocket on 5810). Rejected: we'd have to vendor or write a msgpack-over-WebSocket client in hand-written JavaScript, the area the team likes least (ADR-0013), and run the detection in JavaScript.
- *Threads:* `ThreadingHTTPServer` gives each stream a thread. Each subscriber gets a bounded queue (256 messages). A slow subscriber that fills it is disconnected, and its browser reconnects and gets a fresh snapshot.
- Stream routes are GET-only and change nothing, so the read-only stance of ADR-0013 holds; ADR-0013 gets a short amendment (ADR-0016) recording that the server now opens an outbound connection to the robot and takes a new dependency.

### D-e. Comparison rules
- The first update from a device or bus is its baseline. Any total that goes down is treated as a robot-code restart for that publisher, and every total is re-baselined.
- **Sibling rule:** a NetworkTables update arrives as a batch of topic changes. Dropouts are grouped by the batch the detector reads them in. A dropout is bus-wide when ≥ half of that bus's other reporting devices also had a dropout in the same batch. This is the live version of `can-health`'s overlap rule, and it never matches timestamps exactly (#14, #38).
- Events carry `Enabled` as published in that batch. This tells shake tests (disabled) from drive tests (enabled) without a mode switch.
- Bad values (wrong type, negative counts) are recorded as a per-device note and skipped. The service never raises on robot input.

### D-f. Robot config selection
The robot config is chosen by the published `Project`, taking the latest season when several match, the same rule wpilog sessions use with `project` plus year. There is no `--robot` override in v1: a team with two robots running the same project picks the latest, which is the robot in the pit.

### D-g. Fake robot is the test fixture and the dev tool
`flashpoint.live.fake_robot` runs a local NetworkTables server publishing the contract from a script of steps (raise dropouts, reboot, set fault, restart, wrong type). Tests drive it in-process. `python -m flashpoint.live.fake_robot --scenario shake` runs it for working on the view, with `flashpoint serve --robot-host 127.0.0.1`. It isn't a CLI subcommand, so the CLI reference only gains `serve --robot-host`.

### D-h. The Java sample is a suggestion, kept small
`examples/robot-java/CanHealthPublisher.java` uses Phoenix 6 2026 and WPILib 2026 APIs. It takes a list of `ParentDevice` plus the signal to watch for each, and its `periodic()` is called from `robotPeriodic`. CI doesn't compile it (that would need Gradle and WPILib). The fake robot and a drift test between the guide's contract table and the topics the service reads keep the contract honest. The actual robot change is a separate PR in Robot-2026.

### Architecture decisions
D1–D4, D9 and D10 are untouched. D5 lists "robot-side NT logging" as a fallback for temperature only; this change doesn't touch temperature. D6/D7 (one local app) are extended, not changed. D11's identity rules are used as-is: live devices map to slots by (bus, model, CAN id). No serial lookup in v1, since the live tree doesn't carry serials.

## Budgets

- Server: the detector handles one batch of 40 devices in under 5 ms. The live service adds under 30 MB of resident memory, and nothing when no view is open.
- Latency: an update shows in the view within 1 s of reaching the server (spec), aiming for about 300 ms.
- Robot (sample): `refreshAll` plus totals for 30 devices should cost under 1 ms per loop. The guide asks teams to check this with loop timing before a competition.
- No lake I/O. No full logs read.

## Risks / Trade-offs

- [Robot code changes are needed before anything shows] → The fake robot makes the view usable and testable now. The guide and sample make the Robot-2026 PR a copy job.
- [The sample goes stale against Phoenix 6 releases] → Kept small, tied to the 2026 releases, and labelled a suggestion. The contract (topic names and types) is the stable part, and it is tested.
- [Dropout counts depend on the update frequencies the robot code sets] → The count is relative to each signal's own period, as in `can-health`. The guide recommends watching a signal at 50 Hz or faster.
- [`pyntcore` (RobotPy's ntcore, `import ntcore`) wheel availability on pit laptops (macOS arm64, Windows, Ubuntu); its Linux wheels are `manylinux_2_35`, so Linux needs glibc 2.35 (Ubuntu 22.04 or later)] → Pin the 2026 line next to `robotpy-wpiutil`, which ships for the same platforms. The CI matrix catches a missing wheel.
- [Serve shared on the pit network makes the robot stream visible to anyone on it] → The data is read-only. It's the same exposure as `--host` today, and is already warned about.
- [Batch boundaries are the server's, not the robot's] → A dropout that crosses an update boundary can show as two isolated dropouts instead of one bus-wide one. Good enough for live use; `can-health` gives the exact picture from the hoot.

## Migration Plan

Additive. A new view, new routes and a new dependency. Nothing changes until robot code publishes the contract. Rollback is a revert; there's no stored state.
