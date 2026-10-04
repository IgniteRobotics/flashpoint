# Unit registry seed

`units-seed.csv` records every **physical** CTRE device the team owns, keyed by factory serial number. Flashpoint uses it to start each motor's history (ADR-0011). After that, the robot's `/Flashpoint/CANInventory` log entry keeps it current automatically. This seed covers:
- spares on the shelf, which never appear in a log;
- giving each unit a human-readable physical label.

## How to fill it in (Tuner X walk)

1. Connect Tuner X to each robot (rio and CANivore) and open each device's **Device Details**. Copy the **Serial No**.
2. Do the same for every spare motor on the bench (connect it to a test board).
3. Put a **label** on each motor, for example a paint-pen or label-maker tag `M-001`, `M-002`, and so on. Write the same label in the CSV. Labels never get reused.

| Column | Example | Notes |
|---|---|---|
| `serial` | `6E9415C3394C4853` | Exactly as Tuner X shows it |
| `model` | `Talon FX` | Device model |
| `robot` | `2026-comp` | `2026-comp`, `2026-practice`, or `shelf` |
| `bus` | `rio` or the CANivore name | Leave blank for `shelf` |
| `can_id` | `11` | Leave blank for `shelf` |
| `role` | `FL drive` | The slot's role on the robot. Leave blank for `shelf` |
| `label` | `M-014` | The physical label on the motor |
| `first_installed` | `2026-01-20` | Best guess is fine |
| `notes` | `rebuilt gearbox 2026-03` | Free text |

Re-walk the robots after any rebuild, until the CAN inventory logger is deployed on every robot.
