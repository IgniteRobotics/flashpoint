# Battery voltage and the PDH

Flashpoint doesn't read battery voltage or PDH data yet ([#6](https://github.com/IgniteRobotics/flashpoint/issues/6)). Today it uses a **battery proxy**, and there's nothing to add to robot code for it.

## The battery proxy today

Replay's battery track, its brownout markers and brownout findings all use the supply voltage of one device standing in for the battery:

- the robot file's `battery_proxy` slot, a low-current device on the main power bus such as the gyro ([robot file reference](../reference/config/robots.md));
- when there's none, the median `SupplyVoltage` across the robot's CTRE devices, every 20 ms.

Declare a proxy. A motor makes a bad proxy, and so does the median under heavy load: a stalled motor's own wiring drop reads as a brownout. On Ignite's 2026 robot, the old device-minimum proxy reported six brownouts in one match (6.3–6.7 V) that were intake stalls at about 80 A. A single motor's supply dip shows up instead as its supply path resistance, in History.

## What we plan to read

These entry names are **planned, not read yet**. Logging them now does no harm, and the logs you collect this season will have them when Flashpoint does read them:

| Planned entry | Type | Source |
|---|---|---|
| `/Flashpoint/Power/BatteryVoltage` | double | `RobotController.getBatteryVoltage()` |
| `/Flashpoint/Power/TotalCurrent` | double | the PDH's or PDP's `getTotalCurrent()` |

There's no sample file for these yet, because the names may change before Flashpoint reads them. Follow [#6](https://github.com/IgniteRobotics/flashpoint/issues/6) for the final names.
