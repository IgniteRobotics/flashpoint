# 0005. Temperature source: hoot DeviceTemp (the team's logs are Pro-licensed)

- Status: Proposed
- Date: 2026-10-04
- Refs: 03 §6, D5; P0 findings

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
