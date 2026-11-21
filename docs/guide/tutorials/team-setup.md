# Setting up Flashpoint for your team

This page takes a new FRC team from nothing to a working Flashpoint, in order. Each step is short and links to the page with the details. Flashpoint was built by Ignite Robotics (6829), but nothing in it is tied to Ignite: you give it your team number, your robot and your colours.

**What you need:**
- a pit laptop running macOS, Linux or Windows, with internet for the first setup ([what each supports](../support-matrix.md#platforms));
- robot code that uses CTRE Phoenix 6 motors and writes `.wpilog` files (most WPILib robots already do);
- a few recent logs from your robot (`.wpilog` and `.hoot` files), copied off the roboRIO or its USB stick;
- about an hour, plus time in the shop to read motor serial numbers with Tuner X.

New to Flashpoint? Try the [Quickstart](../quickstart.md) with sample logs first, or the [first-event tutorial](first-event.md) for a tour of the views.

## 1. What Flashpoint is

Flashpoint keeps every log your robot records and tracks the health of each motor over the robot's whole life: temperature, current, stalls, brownouts, and CAN dropouts and reboots. It follows each physical motor by its serial number, so a motor's history goes with it when you move it to another slot or robot, and it flags motors that are drifting away from normal before they cost you a match. You look at the results in a local web app (Replay, History, Anomalies, Setup and Live CAN). Everything runs on one laptop with no internet. [Why Flashpoint](../why.md) says more about what it is and isn't.

Flashpoint reads WPILib DataLogs (`.wpilog`) and CTRE Phoenix 6 signal logs (`.hoot`). Motor health comes from the hoots, so a robot without Phoenix 6 devices gets much less out of it.

## 2. Install

Install with uv, which brings its own Python:

```sh
uv tool install --python 3.13 flashpoint
```

If you don't have uv yet, or `flashpoint` isn't found afterwards, follow the Install page for [macOS](../install/macos.md), [Linux](../install/linux.md) or [Windows](../install/windows.md). For a laptop with no internet at all, see [Install without internet](../how-to/install-offline.md).

Two optional extras: **rclone**, for off-laptop backups ([section 7](#7-backups-and-moving-to-another-laptop)), and **AdvantageScope** (it comes with WPILib), for Replay's "Open in AdvantageScope" button.

## 3. Create your configuration and import one match

Create your configuration, with your own team number and name:

```sh
flashpoint init --team 1234 --team-name "Robo Rams" --palette green
```

In a terminal, `init` asks for each option you leave out. The example leaves out `--lake`, so it stops at `Lake folder [~/flashpoint-lake]:`. Press Enter, or type another folder: the **lake** holds all your logs and results, so pick a drive with room to grow. `init` never overwrites anything. It writes your **config folder** (`~/.config/flashpoint/` on macOS and Linux, `%APPDATA%\flashpoint` on Windows) and prints what it created:

```
created the configuration in /home/pit/.config/flashpoint:
  acquire.toml
  report.toml
  anomaly.toml
  anomaly-causes.toml
  seasons/2025.toml
  seasons/2026.toml
  units/units-seed.csv
  theme.toml
next: import a match's logs with `flashpoint ingest <folder>` (it drafts your robot from them), then run `flashpoint serve`
```

Each file has a reference page: [acquire.toml](../reference/config/acquire.md) (team, lake, robot addresses, backup), [theme.toml](../reference/config/theme.md), [report.toml](../reference/config/report.md), [anomaly.toml](../reference/config/anomaly.md), [anomaly-causes.toml](../reference/config/anomaly-causes.md), [seasons](../reference/config/seasons.md) and the [motor list](../reference/config/units.md).

**Import one match next**, with internet (the first hoot import downloads owlet, the CTRE tool that reads hoots). Copy one match's `.wpilog` and `.hoot` files off the robot's USB stick into a folder, then:

```sh
flashpoint ingest path/to/one-match/
```

Since `robots/` has no file for that season yet, the import drafts one from the logs and prints `drafted robot 2026-comp from 2026gacmp_qm7 (22 devices) into ...`. You name its slots in section 4. [Import logs from a USB stick](../how-to/import-from-usb.md) explains what `ingest` prints.

Now check the setup:

```sh
flashpoint doctor
```

`doctor` changes nothing. It prints one line per check, marked `ok`, `WARN`, `PROBLEM` or `n/a`, with the fix under anything that isn't ok. After this first import, expect a `WARN` on "Robot for *year*" while the drafted robot's slots are unnamed, and on AdvantageScope if WPILib isn't installed. Each `PROBLEM` and its fix is in [Troubleshooting](../troubleshooting.md#doctor-problems).

## 4. Robot side

**What Flashpoint needs to know about your robot.** One robot file per robot per season, in `robots/` in your config folder. It lists each CAN device as a **slot**. Your first import drafted it; to finish it, follow [Set up a robot file](../how-to/set-up-a-robot-file.md): name each slot's subsystem and role in **Setup > Robots**, and keep the drafted slot ids.

**What robot code should log.** Flashpoint works with the logs your robot already writes, and gets much more from a few additions: the CAN inventory (history follows each motor by serial number), CAN bus status, build info, and status-signal rates high enough to see current spikes. [Robot side: what to log](../robot-side/index.md) ranks them, with Java samples you can copy.

**Your motor list.** `units/units-seed.csv` lists every motor the team owns, including spares on the shelf, by serial number, with a label you write on the motor. Fill it in with Tuner X ([motor list](../reference/config/units.md)).

## 5. Getting logs in

**By hand:** copy logs off the robot or a USB stick into any folder and run `flashpoint ingest <folder>`. It skips files already in the lake, then works out matches, runs anomaly detection and updates Replay on its own. See [Import logs from a USB stick](../how-to/import-from-usb.md).

**Automatically:** `flashpoint acquire --watch` pulls new logs from the robot over the network, and from USB sticks plugged into the laptop. It never deletes or changes anything on the robot or the stick. Robot pull is **beta, tested on one robot**. See [Import logs from the robot automatically](../how-to/auto-import.md), which also sets it up as a service on Linux or Windows.

## 6. Looking at the data

```sh
flashpoint serve
```

Open <http://127.0.0.1:8000/> in a browser, and stop the server with Ctrl-C. Each view has a reference page: [Replay](../reference/views/replay.md), [History](../reference/views/history.md), [Anomalies](../reference/views/anomalies.md) (beta), [Setup](../reference/views/setup.md) and [Live CAN](../reference/views/live-can.md) (experimental).

To show the app to others, [share it on the pit network](../how-to/share-on-the-pit-network.md) (no password, read-only) or [take Replay on a USB stick](../how-to/replay-on-a-usb-stick.md). Settings can only be changed from a browser on the laptop running a plain `flashpoint serve`.

## 7. Backups and moving to another laptop

The lake is the only copy of your logs and your anomaly triage, so back it up off the laptop. Flashpoint backs up with [rclone](https://rclone.org/) to any remote it supports, such as Google Drive, and the watch keeps backing up every 15 minutes. [Back up the lake](../how-to/back-up-the-lake.md) sets it up and walks through the restore drill. Do the drill once before you need it.

When the pit laptop changes, restore from the backup instead of copying folders by hand: [Move Flashpoint to another laptop](../how-to/move-to-another-laptop.md).

## 8. Team name and colours

The app header shows your team name and number, and the app uses your colours: a preset palette or your own. Set them in **Setup > Appearance**, which previews them and refuses colours that would be hard to read. See [Set the team name and colours](../how-to/team-name-and-colours.md), and [theme.toml](../reference/config/theme.md) to edit the file by hand.

## 9. Your first week

- [ ] Install Flashpoint and run `flashpoint init` with your team number, name and colours.
- [ ] With internet, ingest a few recent logs, including at least one `.hoot`.
- [ ] Name your robot's slots in Setup, then run `flashpoint doctor`: "Robot for *year*" should be ok.
- [ ] Walk the robot and the spares shelf with Tuner X, label each motor, and fill in `units/units-seed.csv`.
- [ ] Add the robot-side loggers ([what to log](../robot-side/index.md)), deploy, and ingest a new log.
- [ ] Try `flashpoint acquire --dry-run` with the robot on, then leave `flashpoint acquire --watch` running.
- [ ] Set up `backup.remote`, run `flashpoint backup`, and do the [restore drill](../how-to/back-up-the-lake.md) once.
- [ ] Open Replay, History and Anomalies with the students who'll use them at events.
- [ ] Before your first event, go through the [event-day checklist](../user-guide.md#event-day-checklist), and ingest one hoot at home so owlet is cached.

## Getting help

- Run `flashpoint doctor` first. Most problems show up there with a fix, and [Troubleshooting](../troubleshooting.md) has the rest.
- Every command has `--help`, and the [CLI reference](../reference/cli.md) covers each one.
- Ask questions in [Q&A discussions](https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a), and report bugs with the [issue forms](https://github.com/IgniteRobotics/flashpoint/issues/new/choose). Include the output of `flashpoint doctor` and `flashpoint --version`, but never attach match logs to a public issue ([share them privately](../how-to/share-logs-privately.md)).
- To change Flashpoint itself, start with the [developer guide](../under-the-hood/developer-guide.md).
