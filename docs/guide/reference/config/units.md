# The motor list (units/units-seed.csv)

`units/units-seed.csv` lists every physical CTRE device the team owns, by factory serial number, with the label on it. Flashpoint uses it to start each motor's history, including spares on the shelf that never appear in a log. After that, the robot's [CAN inventory](../../robot-side/can-inventory.md) keeps it current. It's optional: without it, motors still get histories, from the inventory or from slots.

`flashpoint init` writes the file with just its header. Ignite's [README for it](https://github.com/IgniteRobotics/flashpoint/blob/main/config/units/README.md) walks through filling it in from Tuner X.

```text
serial,model,robot,bus,can_id,role,label,first_installed,notes
6E9415C3394C4853,Talon FX,2026-comp,DriveTrain,11,FL drive,M-014,2026-01-20,rebuilt gearbox 2026-03
```

| Column | Example | Notes |
|---|---|---|
| `serial` | `6E9415C3394C4853` | Exactly as Tuner X shows it |
| `model` | `Talon FX` | Device model |
| `robot` | `2026-comp` | A robot name, or `shelf` for a spare |
| `bus` | `rio` or the CANivore name | Blank for `shelf` |
| `can_id` | `11` | Blank for `shelf` |
| `role` | `FL drive` | The slot's role on the robot. Blank for `shelf` |
| `label` | `M-014` | The physical label on the motor. Never reuse one |
| `first_installed` | `2026-01-20` | A best guess is fine |
| `notes` | `rebuilt gearbox 2026-03` | Free text |
