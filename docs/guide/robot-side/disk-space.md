# Disk space

A full disk loses logs quietly. Check free space on the roboRIO and its USB stick before each event day, and look for a new hoot after each match ([Signal logging](signal-logging.md#check-for-a-hoot-after-each-match)).

## What happens when space runs out

Checked 2026-11-21 against the [WPILib DataLog page](https://docs.wpilib.org/en/stable/docs/software/telemetry/datalog.html) and CTRE's [signal logging page](https://v6.docs.ctr-electronics.com/en/stable/docs/api-reference/api-usage/signal-logging.html):

- **wpilogs:** with less than 50 MB free, `DataLogManager` deletes the oldest `FRC_` logs until there's 50 MB free or 10 files left. Old matches can disappear before Flashpoint has copied them.
- **Hoots:** signal logging stops when less than 5 MB is free, printing `Available disk space (3 MB) below 5 MB, stopping log` (with the real figure) to the console. WPILib's clean-up doesn't delete hoots, so hoots are usually what fills the disk.

The roboRIO's internal storage is small, so use a USB stick: [`SignalLoggerSetup.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/SignalLoggerSetup.java) puts both logs on it when it's plugged in. Format the stick FAT32; the roboRIO doesn't read NTFS or exFAT.

## What Flashpoint does

- It **never deletes logs on the robot**. Clearing space is yours to do, after Flashpoint has copied the files.
- [Robot pull](../how-to/auto-import.md) reads the free space of each log folder it copies from. When one is below `low_space_mb` (100 MB by default, in [`acquire.toml`](../reference/config/acquire.md)), the automatic import's log says so: `robot <address>: low space on /u/logs: 62 MB free`. It's only in that log for now, so on an event day check the space yourself too.

## Clearing space safely

1. Import the logs first: [from the USB stick](../how-to/import-from-usb.md) or with [robot pull](../how-to/auto-import.md).
2. Check the import worked: each match you're about to delete opens in Replay.
3. Delete old session folders on the stick (or over SFTP in `/home/lvuser/logs`), oldest first. Keep the current event's.
