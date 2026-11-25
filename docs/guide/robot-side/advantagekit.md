# AdvantageKit

What Flashpoint reads from AdvantageKit logs, and what it doesn't yet. **Not tested on real logs:** everything here is built and tested on synthetic logs made from AdvantageKit's own entry names (v26), and no outside team's AdvantageKit log has been checked yet ([#76](https://github.com/IgniteRobotics/flashpoint/issues/76)).

## What works

- **Match identity.** AdvantageKit logs the driver station's FMS data under `/DriverStation/` (`EventName`, `MatchType`, `MatchNumber`, `ReplayNumber`, `AllianceStation`). Flashpoint reads them as in-log FMS data when the log has no WPILib `FMSInfo` entries, taking the whole set from one place, never mixing the two.
- **The filename.** AdvantageKit names its log `akit_yy-MM-dd_HH-mm-ss_<event>_<p|q|e><n>.wpilog`. Flashpoint reads the event, match and start time from it, like a WPILib `FRC_…` name.
- **The hoots' names.** If the wpilog gives no identity at all, the match comes from the hoots' filenames (`GACMP_Q7_…`), which CTRE's signal logger takes from the same FMS data.
- **Pairing with hoots.** AdvantageKit doesn't write WPILib's `systemTime` entry. Flashpoint anchors the log to wall-clock time with `/SystemStats/EpochTimeMicros` instead, so the wpilog and its hoots form one session.

A lake imported with an earlier Flashpoint picks these up on the next `derive`, which reads the affected logs' metadata again once.

## What doesn't, yet

- **Alignment is very low.** Flashpoint lines a hoot up with the wpilog by a payload both logged (`DriveState/Pose` over NetworkTables) or by enable edges. AdvantageKit's CTRE swerve template publishes neither the way Flashpoint looks for them, so alignment falls back to wall-clock time, with **very low** confidence. Replay shows its low-alignment warning: check the timing of events against AdvantageScope.
- **No enable-edge alignment yet.** Reading AdvantageKit's `/DriverStation/Enabled` for enable edges would lift alignment to low. It waits for a real AdvantageKit log to check against.
- **Match phases** come from the hoots' robot mode, so auto and teleop still frame correctly.

See the [support matrix](../support-matrix.md) for how well each log setup is supported.
