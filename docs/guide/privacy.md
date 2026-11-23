# Privacy and network

Flashpoint runs on your laptop and sends **no telemetry**: no usage statistics, no crash reports, no account, and no server of ours. Everything it works out stays in your lake. This page lists every connection it makes, what your logs contain, and how to share them safely.

Checked by a test: with owlet already downloaded, ingesting, deriving, building Replay data and serving a match makes no connection beyond the laptop itself.

## Every connection Flashpoint makes

| When | To | What goes over it |
|---|---|---|
| The first time it reads a hoot of a new version | CTRE's download server (`redist.ctr-electronics.com`), over HTTPS | A download of the `owlet` converter. Nothing about you or your logs is sent. It's cached in `~/.cache/flashpoint/`, so it happens once per hoot version ([install offline](how-to/install-offline.md) avoids it) |
| [Robot pull](how-to/auto-import.md) (`flashpoint acquire`), only if you use it | Your roboRIO (`10.TE.AM.2`, `roborio-TEAM-frc.local` or `172.22.11.2`), over SSH and SFTP | Listing and copying log files from the robot, and reading its free space. Flashpoint never deletes files on the robot |
| [Backup](how-to/back-up-the-lake.md), only if you set `backup.remote` | Wherever your rclone remote points (Google Drive, a NAS, another disk) | A copy of the lake and your config folder, by rclone |
| [Live CAN](how-to/live-can-health.md), only while the view is open | Your roboRIO, over NetworkTables | Reading the CAN health topics robot code publishes |
| You click a docs link in the app or `doctor` output | The docs site (`igniterobotics.github.io`) | An ordinary web page visit, in your browser |

Installing and updating Flashpoint itself (with uv or pipx) downloads from PyPI; that's your package manager, not Flashpoint. The app's own pages, fonts and scripts are all served from your laptop.

## What a log and a lake contain

A **wpilog** holds whatever your robot code logs. Usually:
- every NetworkTables value, including your own subsystem data and anything a vision coprocessor publishes;
- Driver Station data: enabled, auto, teleop, and joystick inputs;
- match data: event, match number and alliance station;
- messages the robot program logs (`DataLogManager.log()`), and in recent WPILib versions its console output;
- [build info](robot-side/build-info.md) (project name, git branch), and with Flashpoint's samples the [CAN inventory](robot-side/can-inventory.md): every CTRE device's serial number.

A **hoot** holds each CTRE device's status signals, by CAN id and bus, including its firmware version.

The **lake** holds a copy of every raw log you imported, everything worked out from them, and your triage notes. Your team number and robot files are in the config folder.

None of that is secret in the usual sense, but it is your team's robot: your strategy code's NetworkTables values, your hardware list, and your match-by-match performance. Treat logs like your robot code.

## Sharing logs for a bug report

Don't attach logs to a public GitHub issue or discussion. Say in the issue that you have logs to share, and a maintainer will reply with a private way to send them ([Share logs privately](how-to/share-logs-privately.md)). Logs shared with us are used only to test Flashpoint, and are deleted when you ask.

## Showing the app on the pit network

`flashpoint serve --host 0.0.0.0` lets other devices on the same network open the app. **There's no password**: anyone on that network who knows the laptop's address can see everything in the lake. The app is read-only then (for everyone, including the laptop itself), so nobody can change triage or settings, but they can read and download your logs. Only do it on a network you trust, and stop it (Ctrl-C) when you're done. See [Show the app on the pit network](how-to/share-on-the-pit-network.md).
