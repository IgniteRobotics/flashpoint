# Setting up Flashpoint for your team

This page takes a new FRC team from nothing to a working Flashpoint, in order. Each step links to the guide with the details. Flashpoint was built by Ignite Robotics (6829), but nothing in it is tied to Ignite: you give it your team number, your robot and your colours.

**What you need:**
- a pit laptop running macOS, Linux or Windows, with internet for the first setup;
- Python 3.11, 3.12 or 3.13 on it (not 3.14);
- robot code that uses CTRE Phoenix 6 motors and writes `.wpilog` files (most WPILib robots already do);
- a few recent logs from your robot (`.wpilog` and `.hoot` files), copied off the roboRIO or its USB stick;
- about an hour, plus time in the shop to read motor serial numbers with Tuner X.

## 1. What Flashpoint is

Flashpoint keeps every log your robot records and tracks the health of each motor over the robot's whole life: temperature, current, stalls, brownouts, and CAN dropouts and reboots. It follows each physical motor by its serial number, so a motor's history goes with it when you move it to another slot or robot, and it flags motors that are drifting away from normal before they cost you a match. You look at the results in a local web app (Replay, History, Anomalies, Setup and Live CAN). Everything runs on one laptop with no internet. The [user guide](user-guide.md#what-flashpoint-is-for) says more about what it is and isn't.

Flashpoint reads WPILib DataLogs (`.wpilog`) and CTRE Phoenix 6 signal logs (`.hoot`). Motor health comes from the hoots, so a robot without Phoenix 6 devices gets much less out of it.

## 2. Prerequisites

- **Operating system:** macOS, Linux or Windows. All three can import, serve and back up. Running automatic import as a background service is set up for Linux and Windows; on macOS you leave it running in a terminal ([details](auto-import.md#macos)).
- **Python:** 3.11, 3.12 or 3.13. Check with `python3 --version` (`py --list` on Windows).
- **pipx**, which installs command-line tools in their own space:
  - macOS: `brew install pipx`
  - Debian or Ubuntu: `sudo apt install pipx` (`pip install --user pipx` fails there with "externally-managed-environment")
  - Windows and other systems: `python -m pip install --user pipx`

  Then run `pipx ensurepath` once and open a new terminal. Without that, the `flashpoint` command often isn't found after you install it.
- **rclone**, only if you want off-laptop backups ([section 7](#7-backups-and-moving-to-another-laptop)).
- **AdvantageScope** (comes with WPILib), only if you want the "Open in AdvantageScope" button in Replay.

## 3. Install and create your configuration

The package on PyPI is called `flashpoint`:

```sh
pipx install --python python3.12 flashpoint
```

`--python` picks which Python pipx uses; name any 3.11 to 3.13 you have. On Windows, `python3.12` usually isn't found: run `py -3.12 -c "import sys; print(sys.executable)"` and give that full path instead. If the laptop has no internet at the pit, see [Installing without internet](user-guide.md#installing-without-internet).

**Check your version first.** Team names and colours (`--team-name`, `--palette`, `theme.toml` and [section 8](#8-team-name-and-colours)) are newer than Flashpoint 0.1.0, the only release on PyPI so far. Run `flashpoint --version`. If it says `flashpoint 0.1.0`, leave out `--team-name` and `--palette` below (0.1.0 stops with "unrecognized arguments") and skip section 8, or [install from a checkout](user-guide.md#installing-from-a-checkout) to get them now.

Then create your configuration. Use your own team number and name:

```sh
flashpoint init --team 1234 --team-name "Robo Rams" --palette green
```

| Option | What it sets |
|---|---|
| `--team NUMBER` | Your FRC team number. Flashpoint finds the robot from it: `10.12.34.2`, then `roborio-1234-frc.local`, then the USB tether `172.22.11.2` |
| `--lake PATH` | The folder that holds all your logs and results, called the **lake**. Default `~/flashpoint-lake`. Pick a drive with room to grow |
| `--team-name NAME` | The name in the app header, at most 40 characters. Optional |
| `--palette PRESET` | The app's colours: `amber` (the default), `green`, `blue`, `red` or `white` |

In a terminal, `init` asks for each option you leave out, even when you give the others. The example above leaves out `--lake`, so `init` stops at `Lake folder [~/flashpoint-lake]:`. Press Enter to use that folder, or type another one. It never overwrites anything: run it again and it lists the files that already exist and changes nothing.

`init` writes your **config folder**: `~/.config/flashpoint/` on macOS and Linux, `%APPDATA%\flashpoint` on Windows (or wherever `FLASHPOINT_CONFIG` points). It prints what it created:

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
next: add your robot file to /home/pit/.config/flashpoint/robots, then run `flashpoint doctor`
```

| File | What it's for |
|---|---|
| `acquire.toml` | Your team number and lake folder; later, robot addresses and backup ([every key](auto-import.md#configure-it-acquiretoml)) |
| `theme.toml` | Your team name and colours ([section 8](#8-team-name-and-colours)) |
| `report.toml`, `anomaly.toml`, `anomaly-causes.toml` | Replay limits, anomaly thresholds, and the "likely causes" text. The defaults are a good start |
| `seasons/<year>.toml` | Where each season's NetworkTables entries live |
| `units/units-seed.csv` | Your list of physical motors, empty to start ([section 4](#4-robot-side)) |
| `robots/` | Empty. Your robot file goes here ([section 4](#4-robot-side)) |

Now check the setup:

```sh
flashpoint doctor
```

`doctor` changes nothing. Its `checks` section has one line per check, marked `ok`, `WARN`, `PROBLEM` or `n/a`, with the fix under anything that isn't ok. Right after `init`, expect:
- `PROBLEM Robot for <year>`: no robot file yet. You add one in section 4.
- `WARN owlet for hoot format ...`: the tool that reads hoots isn't downloaded yet. Your first hoot import downloads it (needs internet).
- `WARN AdvantageScope`: only if WPILib isn't installed.

`doctor` exits with code 1 while any `PROBLEM` remains. The [user guide](user-guide.md#check-the-setup-flashpoint-doctor) explains each section, and the [CLI reference](cli-reference.md#flashpoint-doctor) lists every check.

## 4. Robot side

**What robot code must log.** Flashpoint works with the logs your robot already writes:
- `.wpilog` files from WPILib's `DataLogManager` (call `DataLogManager.start()` in `robotInit()`);
- `.hoot` files from Phoenix 6's signal logger, one per CAN bus.

Three small additions to robot code make it much more useful. [Robot-side logging](robot-side.md) has Java samples for each:
- **CAN inventory:** every motor's serial number, so history follows the physical motor through swaps;
- **CAN bus status:** bus use and error counts per bus;
- **Build info:** the project name, which tells a practice bot's logs from a comp bot's.

To watch CAN dropouts live while you wiggle wires, add the publisher from [Live CAN health](live-can-health.md#adding-it-to-robot-code) too. It's optional.

**What Flashpoint needs to know about your robot.** It needs one robot file per robot per season in `robots/` in your config folder, such as `robots/2026-comp.toml`. It lists each motor as a **slot**: its bus, CAN id, model, subsystem and role. The easy way to write it:
1. Import one log from the robot that includes a `.hoot` file ([section 5](#5-getting-logs-in)). Drafting reads the devices from the hoots, so a log without one can't be drafted from. Do this first hoot import with internet.
2. Run `flashpoint serve`, open **Setup**, then the **DRAFT ROBOT** tab.
3. Pick the log and press **DRAFT FROM THIS LOG**. Every CAN device in its hoots becomes a slot.
4. Give each slot an id, subsystem and role, then **SAVE ROBOT**.
5. Run `flashpoint derive`. Logs you imported before the robot file existed stay unmapped (no slots, no motor history) until you do. The save message in Setup reminds you.

The [user guide](user-guide.md#describe-a-robot) shows a whole robot file, with the optional parts: a battery proxy, sibling groups (motors that do the same job, compared with each other), swap dates, bus names and NetworkTables signals.

**Your motor list.** `units/units-seed.csv` lists every motor the team owns, including spares on the shelf, by serial number, with a label you write on the motor. Fill it in with Tuner X: [how to fill it in](../../config/units/README.md). Once the CAN inventory logger runs on the robot, Flashpoint keeps track of motors in the robot on its own; the list still covers spares and labels.

**New seasons.** `init` copies the season files Flashpoint knows (2025 and 2026 for this version). For a later year, copy last year's file to `seasons/<year>.toml` and change `season =`; see [Starting a new season](user-guide.md#starting-a-new-season).

## 5. Getting logs in

**By hand.** Copy logs off the robot or a USB stick into any folder, then:

```sh
flashpoint ingest path/to/logs/
```

It takes files or folders, skips files already in the lake, and then works out matches, runs anomaly detection and updates Replay on its own. Do your **first hoot import with internet**, so Flashpoint can download owlet, the CTRE tool that reads hoots. After that it works offline. The [user guide](user-guide.md#import-logs-by-hand) explains the summary it prints (`WARNING`, `QUARANTINED`, `FAILED`).

**Automatically.** `flashpoint acquire` pulls new logs from the robot over the network, and from USB sticks plugged into the laptop, then ingests them. It never deletes or changes anything on the robot or the stick. Try it once with the robot on:

```sh
flashpoint acquire --dry-run
```

That lists what it would copy and changes nothing. When it works, leave the watch running:

```sh
flashpoint acquire --watch
```

[Automatic import](auto-import.md) covers the settings, running it as a service on Linux or Windows ([service files](../../deploy/README.md)), and troubleshooting. The Setup view can install and start the service for you on Linux and Windows.

## 6. Looking at the data

```sh
flashpoint serve
```

Open <http://127.0.0.1:8000/> in a browser. Stop the server with Ctrl-C. The views:
- **Replay:** one match, with tracks for battery, current and temperature, and markers for heat, brownouts, stalls and CAN faults.
- **History:** every motor across matches, events and seasons.
- **Anomalies:** what Flashpoint flagged, with evidence, severity, likely causes, and triage buttons.
- **Setup:** the same checks as `doctor`, with buttons to fix known problems, and your whole configuration (team, appearance, robots, thresholds, motors) to edit without opening files.
- **Live CAN:** CAN dropouts as they happen, while you shake-test or drive-test ([Live CAN health](live-can-health.md)).

Saves and fixes in Setup only work when `serve` runs the normal way, and only from a browser on the same laptop. If you start it with `flashpoint serve --host 0.0.0.0` so others on the network can look, Setup is read-only for everyone, including on the laptop itself. Stop it and run plain `flashpoint serve` to change settings. The [user guide](user-guide.md#look-at-the-data-flashpoint-serve) explains each view, and [How it works](how-it-works.md) explains what's behind them.

To take Replay to a computer without Flashpoint, export it to a folder:

```sh
flashpoint report --static /Volumes/USB/flashpoint
```

## 7. Backups and moving to another laptop

The lake is the only copy of your logs and your anomaly triage. Back it up off the laptop.

Import at least one log first ([section 5](#5-getting-logs-in)). On an empty lake, `flashpoint backup` stops with "no ledger at ...; nothing to back up", and your config folder isn't backed up either.

**Set up a backup.** Flashpoint backs up with [rclone](https://rclone.org/) to any remote rclone supports, such as Google Drive:
1. Install rclone and run `rclone config` once to make a remote, for example `gdrive`.
2. In `acquire.toml`, add:

   ```toml
   [backup]
   remote = "gdrive:flashpoint"
   ```

3. Run one backup now:

   ```sh
   flashpoint backup
   ```

After that, `flashpoint acquire --watch` backs up every 15 minutes (`interval_min`). A backup holds the raw logs, the lake's metadata (including triage) and your config folder's settings files, robots and motor list. Everything else is rebuilt from the raw logs. Details: [Backup and restore](auto-import.md#backup-and-restore).

**The overdue check.** The **Backup** check in `doctor` and Setup turns into a problem when a log or a configuration change has waited more than 24 hours (`backup.overdue_h`) for a backup. The **Setup** tab in the header then shows a red **!**, whichever view you have open, until a backup succeeds. It's a warning when no `backup.remote` is set and the lake holds logs. See [When a backup is overdue](auto-import.md#when-a-backup-is-overdue).

**Moving to a new laptop.** Restore from the backup instead of copying folders by hand. On the new laptop:
1. Install Flashpoint ([section 3](#3-install-and-create-your-configuration)), but **don't run `init` yet**: `restore` only brings your configuration back into an empty config folder.
2. Install rclone and make the same remote (`rclone config`).
3. Restore into a new lake, then rebuild it:

   ```sh
   flashpoint restore --remote gdrive:flashpoint --lake ~/flashpoint-lake
   flashpoint rebuild --lake ~/flashpoint-lake
   ```

4. Open `acquire.toml` in the config folder and check `lake`: it still names the old laptop's folder. Change it to the new one.
5. Run `flashpoint doctor`.

Practise this once before you need it, using a scratch folder: the [restore drill](auto-import.md#restore-drill) shows how without touching your real setup.

## 8. Team name and colours

The app header shows your team name and number, such as "Robo Rams · 1234", and the page title includes them. The number comes from `team` in `acquire.toml` unless you set another one for display. With no name, the header shows just the number, such as "telemetry // 1234".

The colours come from a **palette**. Pick a preset: `amber` (the default), `green`, `blue`, `red` or `white`, all on a dark background. Or give your own colours: a primary colour, and optionally a background and a warning colour, each as `#RRGGBB`. Flashpoint works out every other shade from them.

**In the Setup view** (easiest): open **Setup**, then **CONFIGURATION**, and find **Appearance**.
1. Type the team name, and a team number if you want a different one shown.
2. Choose a preset under **COLOURS**, or **custom colours** to enter your own.
3. Watch the preview: a header, a small chart and a warning in the new colours. The rest of the app keeps the saved colours until you save.
4. Press **SAVE**, then reload the page. Every view and chart uses the new colours, and so does any static export you write after that.

**By hand:** edit `theme.toml` in your config folder:

```toml
team_name = "Robo Rams"      # optional, at most 40 characters
# team_number = 1234         # optional, display only
palette = "custom"           # amber, green, blue, red, white, or "custom"

[custom]                     # only with palette = "custom"
primary = "#00A3E0"
# background = "#0A0806"     # optional
# warning = "#FF5A1F"        # optional; picked for you when left out
```

**Readable by rule.** Flashpoint refuses a palette that would be hard to read: every text shade needs 4.5:1 contrast on every background, and warnings must look clearly different from your primary colour. A refused palette names each failing colour and its contrast, and suggests the nearest primary that passes. For example, `primary = "#202020"` (dark grey on a dark background) is refused with "nearest primary that passes: #808080". In Setup, the save is refused and `theme.toml` stays as it was. If you edit the file by hand and it doesn't pass, the app still starts, with the default amber and no name, and `doctor` reports a `PROBLEM` on `theme.toml` with the reason.

## 9. Your first week

- [ ] Install Flashpoint and run `flashpoint init` with your team number, name and colours.
- [ ] Run `flashpoint doctor`, and read what it says.
- [ ] With internet, ingest a few recent logs, including at least one `.hoot`.
- [ ] Draft your robot file in Setup, fill in each slot, and save. Run `flashpoint derive`, then `flashpoint doctor` again: "Robot for *year*" should be ok.
- [ ] Walk the robot and the spares shelf with Tuner X, label each motor, and fill in `units/units-seed.csv`.
- [ ] Add the CAN inventory, bus status and build info loggers to robot code ([robot-side logging](robot-side.md)), deploy, and ingest a new log.
- [ ] Try `flashpoint acquire --dry-run` with the robot on, then leave `flashpoint acquire --watch` running.
- [ ] Set up `backup.remote`, run `flashpoint backup`, and do the [restore drill](auto-import.md#restore-drill) once.
- [ ] Open Replay, History and Anomalies with the students who'll use them at events.
- [ ] Before your first event, go through the [event-day checklist](user-guide.md#event-day-checklist).

## Getting help

- Run `flashpoint doctor` first. Most problems show up there with a fix.
- Every command has `--help`, and the [CLI reference](cli-reference.md) covers each one.
- Questions, bugs and ideas: open an issue at <https://github.com/IgniteRobotics/flashpoint/issues>. Include the output of `flashpoint doctor` and the Flashpoint version (`flashpoint --version`).
- To change Flashpoint itself, start with the [developer guide](developer-guide.md).
