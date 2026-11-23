# FAQ

Short answers to the questions people ask most. The [support matrix](support-matrix.md) has the detail behind most of them.

## Does it work with REV (Spark MAX, Spark Flex)?

Not yet. Flashpoint reads CTRE hoots and the WPILib wpilog, and has no reader for REV motor data, so REV motors get no Replay tracks, History or health. That's tracked in [#108](https://github.com/IgniteRobotics/flashpoint/issues/108). A robot with a mix of CTRE and REV gets everything for its CTRE devices.

## Does it work with AdvantageKit logs?

Not tested on real AdvantageKit logs yet. An AdvantageKit wpilog with Phoenix 6 hoots is the setup we expect to work, with match identity read from AdvantageKit's Driver Station fields ([#76](https://github.com/IgniteRobotics/flashpoint/issues/76)). See [AdvantageKit](robot-side/advantagekit.md), and if you can share a log, [tell us](https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a).

## We don't have Phoenix Pro. Is it still useful?

Yes. A hoot without Pro has every motor signal Flashpoint uses except temperature: current, voltage, velocity and CAN timing, so Replay, History, stall and brownout detection and CAN health all work. You lose temperature tracks and thermal findings. [Phoenix Pro and temperature](robot-side/pro-and-temperature.md) has the detail.

## What about Systemcore and the 2027 season?

This version handles 2027's season file, and Flashpoint 0.3.1, planned for kickoff week (2027-01-16), adds what 2027 hoots and NetworkTables need: update then. Systemcore, the 2027 control system, hasn't been tested ([#110](https://github.com/IgniteRobotics/flashpoint/issues/110)). See [Known limits](support-matrix.md#the-2027-season-and-systemcore).

## Does it run on Windows?

Yes: install, ingest and serve are tested on Windows in CI, and the automatic import runs as a scheduled task. Windows gets the least real-world use of the three, so [tell us](https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a) what you hit. See [Install on Windows](install/windows.md).

## What does it cost?

Nothing. Flashpoint is free and open source under the [BSD 3-Clause license](https://github.com/IgniteRobotics/flashpoint/blob/main/LICENSE), made by FRC team 6829, Ignite Robotics. There's no paid tier and no account. Phoenix Pro is CTRE's license, and optional.

## Where does our data live?

On your laptop, in three folders:
- the **lake** (`~/flashpoint-lake` by default): a copy of every log you import, and everything worked out from them;
- the **config folder** (`~/.config/flashpoint/`, or `%APPDATA%\flashpoint` on Windows): your team, robot files and settings;
- the **cache** (`~/.cache/flashpoint/`): the downloaded owlet converter.

Nothing is stored anywhere else unless you set up a [backup](how-to/back-up-the-lake.md), which copies the lake to a place you choose.

## Does Flashpoint send our data anywhere?

No. There's no telemetry, no account and no server of ours. Flashpoint connects to other machines only for features you use: downloading owlet, pulling logs from your robot, your own backup, and Live CAN. [Privacy and network](privacy.md) lists each one.

## Can several laptops share one lake?

Not at the same time. Keep one laptop as the one that imports (the pit laptop), and let others look at it:
- over the pit network, with [`flashpoint serve --host 0.0.0.0`](how-to/share-on-the-pit-network.md) (read-only for them);
- with a [Replay export on a USB stick](how-to/replay-on-a-usb-stick.md);
- or with a copy restored from the backup ([Move to another laptop](how-to/move-to-another-laptop.md)).

Don't put the lake in a folder that a sync tool (Dropbox, OneDrive, Google Drive) shares between laptops that both write to it: the lake's ledger is a database file, and two writers can damage it.

## How do we get help?

Ask in [Q&A discussions](https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a): answers there stay searchable for the next team. Report bugs with the [issue forms](https://github.com/IgniteRobotics/flashpoint/issues/new/choose), and never attach match logs to a public issue ([share them privately](how-to/share-logs-privately.md) instead). Check [Troubleshooting](troubleshooting.md) first.

## What support can we expect during build season?

Flashpoint is maintained by volunteers on an FRC team, who are in build season too. From January to April, expect a first reply within a week; competition weekends are slower. See [CONTRIBUTING](https://github.com/IgniteRobotics/flashpoint/blob/main/CONTRIBUTING.md).
