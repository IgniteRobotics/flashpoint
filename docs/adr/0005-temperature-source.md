# 0005. Temperature source: hoot DeviceTemp (the team's logs are Pro-licensed)

- Status: Proposed, amended 2026-11-25
- Date: 2026-10-04
- Refs: 03 §6, D5; P0 findings; openspec change `mvp-outside-team-logs`

## Context
Without a Pro-licensed device in the log, a hoot exports only a free subset of signals, and that subset excludes continuous `DeviceTemp`. Thermal trends are the most valuable early failure signal.

**P0 finding (2026-10-04):** `owlet --check-pro` reports the 2025 and 2026 corpus hoots (CANivore and rio buses) as **Pro-licensed**. All signals export.

## Decision
Use hoot `DeviceTemp` as the primary temperature source. Record `pro_licensed` per log. If a log is not Pro, mark temperature as unavailable for that log rather than inferring it. No robot-side NT temperature logging is required.

## Consequences
- No robot code changes are needed for temperature.
- If the team drops Pro licensing, thermal features silently disappear for new logs. `flashpoint doctor` and a per-log flag make that visible.

## Alternatives considered
- **Robot-side NetworkTables logging of DeviceTemp:** unnecessary while the logs are Pro. Keep it as a fallback.

## Amendment (2026-11-25, mvp-outside-team-logs)
Ignite's logs are Pro-licensed, but many outside teams' aren't. Non-Pro hoots are now an expected input, not an edge case:
- **Non-Pro hoots convert with the free signal set.** If owlet refuses a Pro-only signal (`DeviceTemp`, `ProcessorTemp`, `AncillaryDeviceTemp`) on a non-Pro hoot, conversion runs again without them and records them in `hoot_logs.dropped_signals`. Missing temperature never quarantines a hoot or fails a session.
- **Why temperature is missing is decided once**, in `semantics/temperature.py`, as `temp_status`: `logged`, `needs-pro` (a temperature-capable device on a bus whose hoots aren't Pro-licensed), `not-tracked` (CANcoder and Pigeon 2: Flashpoint tracks no temperature for them), or `missing`. Pro is judged per bus, because one Pro-licensed device unlocks its bus's whole hoot. Gold `match_features` and `can_health` carry it. History reads it (N/A, NEEDS PRO, NO DATA), Replay applies the same rule per bus, and detection counts the `needs-pro` rows it skipped.
- **`doctor` reports Pro coverage** as information, never a warning: a team can't clear it without buying Pro. The earlier consequence ("`flashpoint doctor` makes that visible") is now true.
