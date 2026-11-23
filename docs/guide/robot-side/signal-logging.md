# Signal logging (hoots)

Phoenix 6 signal logging writes every CTRE device's status signals to `.hoot` files: one per CAN bus. They're Flashpoint's main source of motor data: current, voltage, velocity, temperature (with [Phoenix Pro](pro-and-temperature.md)) and the timing that shows CAN dropouts.

## What Phoenix does on its own

Checked 2026-11-21 against CTRE's [signal logging page](https://v6.docs.ctr-electronics.com/en/stable/docs/api-reference/api-usage/signal-logging.html):

- Signal logging is on by default on a roboRIO 2, or a roboRIO 1 with a USB stick. It starts at least 1 second after the robot program starts with the robot enabled, or at least 5 seconds after it starts with the Driver Station connected, whichever comes first. `SignalLogger.enableAutoLogging(false)` turns that off; then you call `SignalLogger.start()` and `SignalLogger.stop()` yourself.
- Each CAN bus gets its own hoot. A session's hoots go in a subfolder named after the date and time the robot program started.
- Logging stops when the disk has less than 5 MB free (see [Disk space](disk-space.md)).

CTRE's page doesn't say which folder hoots go to by default, so set it.

## Put the hoots next to the wpilog

[`SignalLoggerSetup.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/SignalLoggerSetup.java) starts the [WPILib DataLog](datalog.md), then points signal logging at the folder the DataLog chose:

```java
  public static void start() {
    DataLogManager.start();
    DriverStation.startDataLog(DataLogManager.getLog());
    if (RobotBase.isReal()) {
      // Hoots land next to the wpilog: on the USB stick when there is one, otherwise
      // /home/lvuser/logs. Flashpoint's robot pull and USB import look in both.
      SignalLogger.setPath(DataLogManager.getLogDir());
    }
  }
```

So hoots land on the USB stick when one is plugged in, and in `/home/lvuser/logs` otherwise. Those are the two folders Flashpoint's [robot pull](../how-to/auto-import.md) reads (`roots` in [`acquire.toml`](../reference/config/acquire.md)), and a USB stick plugged into the laptop is searched four folders deep.

Call `SignalLoggerSetup.start()` first thing in `robotInit()`. Keep the default auto-start: it begins before the match does, and the hoots line up with the match through the wpilog's clock.

## Check for a hoot after each match

A missing hoot is the most common way to lose a match's motor data, and it's silent on the robot. After each match, in the pit:

1. Look at the newest session folder on the USB stick (or in `/home/lvuser/logs`, over SFTP). It should have one `.hoot` per CAN bus, named like `rio_2026-04-12_15-20-31.hoot`, with a time just before the match.
2. Or let Flashpoint check: after an [import](../how-to/import-from-usb.md), open the match in Replay. Its **Open in AdvantageScope** panel lists the match's logs: one `wpilog` line, and a `hoot · <bus>` line for each bus.

If a hoot is missing, check the disk space first, then that the robot code calls `SignalLoggerSetup.start()` (or that auto-logging wasn't turned off).

## Converting hoots

Flashpoint converts hoots with CTRE's `owlet` tool, which it downloads and caches the first time it reads a hoot. That download needs internet, so ingest one hoot at home before you travel. See the [support matrix](../support-matrix.md) for the hoot versions Flashpoint reads.
