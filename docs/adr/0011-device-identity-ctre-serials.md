# 0011. Device identity: slots vs. units, with units keyed by CTRE serial number

- Status: **Accepted** (2026-10-04)
- Date: 2026-10-04
- Deciders: Josh Reddick (team decision: implement on all robots)
- Refs: 05 §4a, D11; [99-ctre-device-serial-numbers.md](../rewrite/99-ctre-device-serial-numbers.md)

## Context
- Phoenix 6 robot APIs and hoot signals identify devices by model and CAN ID. `device_hash` is "not unique across networks".
- A motor swapped in with the same CAN ID is invisible, which corrupts per-motor baselines.
- The Phoenix Diagnostics Server (port 1250, `?action=getdevices`) exposes factory serial numbers.

## Decision
- **Slot:** a role on a robot, `(robot, bus, model, can_id)`.
- **Unit:** a physical device, `ctre:<serial>`.
- Every robot logs a normalized CAN inventory to the wpilog entry `/Flashpoint/CANInventory` (schema v1) at boot, and again after a hot-swap.
- Baselines are per unit; sibling comparisons are per slot.
- Logs without an inventory use legacy epochs, split at swap dates declared in config.

## Consequences
- Wear history follows a physical motor across swaps and robots, and maintenance labels attach to real hardware.
- Requires robot code on every robot (P0 task 6) and depends on the diagnostics server's JSON field names.
- REV devices need a separate inventory source.

## Alternatives considered
- **CAN ID only:** swaps silently corrupt histories.
- **A manual maintenance log only:** error-prone, and it lags reality.

## Implementation notes (P2, 2026-10-04)
- **Slots** come from `config/robots/<robot>.toml`: 2026-comp has 23 slots (18 motors, the Pigeon2 gyro, and 4 CANcoders).
  - A slot with bus `canivore` matches any non-rio bus: hoot files name the bus by its hex id, while the diagnostics server reports its configured name.
  - Inventory model names are normalized ("Talon FX" → `TalonFX`).
- **Units:** `ctre:<serial>` from a valid `/Flashpoint/CANInventory`, otherwise `legacy:<robot>:<slot>:<epoch>`, where the epoch counts the slot's configured swap dates on or before the session day.
- **Mid-session swaps** split attribution at the second inventory's timestamp (`unit_swaps`).
- **Unmapped devices** from logs or inventories are reported (`unmapped_devices`), never dropped. The first corpus run found the gyro and the CANcoders this way.
- Tables: `slot_observations`, `unit_swaps`, `unmapped_devices` (ledger plus `meta/*.parquet`).
