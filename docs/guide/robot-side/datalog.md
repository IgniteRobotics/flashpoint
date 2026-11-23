# The WPILib DataLog

The WPILib DataLog (`.wpilog`) tells Flashpoint which match a log is, when the match phases happened, and the wall-clock time that lines the hoots up with it. Without a wpilog, a session's hoots still import, but its motors get no history: ingest warns

```text
warning: 1 session(s) with hoots but no wpilog (...): their motors get no history (no aligned samples, usage or match features). Motor history needs the robot to write a wpilog too: turn on WPILib data logging (DataLogManager.start()).
```

## Turn it on

[`SignalLoggerSetup.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/SignalLoggerSetup.java) does both calls, first thing in `robotInit()`:

```java
    DataLogManager.start();
    DriverStation.startDataLog(DataLogManager.getLog());
```

Checked 2026-11-21 against WPILib's [On-Robot Telemetry Recording](https://docs.wpilib.org/en/stable/docs/software/telemetry/datalog.html) page:

- `DataLogManager.start()` writes to a `logs` folder on the USB stick if one is plugged in, and to `/home/lvuser/logs` otherwise. The stick must be FAT32; NTFS and exFAT don't work.
- It records every NetworkTables change, and a `systemTime` entry about every 5 seconds.
- It doesn't record Driver Station data on its own. `DriverStation.startDataLog(DataLogManager.getLog())` adds DS control data (enabled, autonomous, teleop) and joystick data.
- A file is named `FRC_TBD_<random>.wpilog` until the Driver Station connects, then `FRC_yyyyMMdd_HHmmss.wpilog` (UTC), and `FRC_yyyyMMdd_HHmmss_<event>_<match>.wpilog` once the FMS gives a match.
- With less than 50 MB free, it deletes the oldest `FRC_` logs until there's 50 MB free or 10 files left. At startup it deletes logs from runs where no Driver Station connected. See [Disk space](disk-space.md).

## What Flashpoint reads from it

| Entry | Used for |
|---|---|
| `NT:/FMSInfo/...` (NetworkTables, which `DataLogManager` records) | The match: event, match type and number. The file name is the fallback, then The Blue Alliance |
| `DS:enabled`, `DS:autonomous` and the other DS entries | Match phases (auto, teleop, disabled) and when the robot was enabled |
| `systemTime` | The wall clock, which anchors the log's timestamps |
| `MetaData` | [Build info](build-info.md): which robot file the log uses |
| `/Flashpoint/...` | The [CAN inventory](can-inventory.md) and [CAN bus status](bus-status.md) |
| NetworkTables entries your robot file maps | Whatever signals you map ([robot file reference](../reference/config/robots.md)) |

**Lining hoots up with the wpilog.** Flashpoint puts hoot samples on the wpilog's clock. It does that exactly (to well under a millisecond) when one signal is written to both logs, such as `DriveState/Pose`, which CTRE's swerve code writes to the hoot and to NetworkTables. Otherwise it pairs the robot-enable edges in both logs (`DS:enabled` and the hoot's `RobotEnable`), which is good to 30–500 ms, and as a last resort it uses the wall clock.
