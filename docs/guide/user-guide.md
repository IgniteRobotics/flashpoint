# User guide

For students and mentors who run Flashpoint: at the pit, after an event, or on the team laptop. You don't need to know Python. If you want to change the code, read the [developer guide](developer-guide.md) instead.

## What Flashpoint is for

Flashpoint keeps every robot log the team records and tracks the health of each motor over the robot's whole life. It reads:
- **WPILib DataLogs** (`.wpilog`), which the roboRIO writes every time the robot runs;
- **CTRE Phoenix 6 signal logs** (`.hoot`), one per CAN bus, with motor current, voltage, temperature, and CAN health.

From those it answers questions like:
- Which motor ran hottest in Q7, and when?
- Is the front-left drive motor drawing more current than it did last month?
- Did we brown out, and which part of the match was it?
- Which motor keeps losing CAN frames when its neighbours don't?

It flags problems (anomalies) before they cost a match, and it follows each physical motor by its serial number, so a motor's history stays with it when it moves to another slot or robot.

Everything runs on one laptop with no internet.

**What it isn't:**
- It isn't a live dashboard; it reads logs after the robot is off.
- It isn't a replacement for AdvantageScope. For a deep look at one log (every signal, field view, odometry), Flashpoint opens the log in AdvantageScope for you.

## Install

You need a computer running macOS, Linux, or Windows, with:
- Python 3.11, 3.12, or 3.13. Not 3.14: Flashpoint doesn't support it yet. Check with `python3 --version`.
- [pipx](https://pipx.pypa.io/), which installs command-line tools in their own space (`brew install pipx` on macOS, `python3 -m pip install --user pipx` elsewhere).

Then, once:

```sh
pipx install --python python3.12 flashpoint
flashpoint init --team 1234
flashpoint doctor
```

`--python` picks which Python pipx uses. Name any 3.11–3.13 you have installed. On Windows, use `py -3.12` to find it, or give the full path.

`flashpoint init` creates your **config folder** (`~/.config/flashpoint`, or `%APPDATA%\flashpoint` on Windows) with your team number, your lake folder, and copies of the default settings. Without `--team` it asks. It never overwrites a file, so running it again is safe. The robot addresses Flashpoint tries come from the team number.

`doctor` then lists the setup checks. Right after `init`, the only problem should be "no robot configured": add your robot from the [Setup view](#setup-checks-and-configuration), and run `doctor` again.

### Updating

```sh
pipx upgrade flashpoint
```

Your config folder and lake are kept.

### Installing without internet

On a computer with internet, download the package: `python3 -m pip download --no-deps flashpoint -d flashpoint-wheel`. Copy the `.whl` file to a USB stick, then on the pit laptop run `pipx install --python python3.12 /Volumes/USB/flashpoint-0.2.0-py3-none-any.whl` (or `E:\...` on Windows). pipx still needs Flashpoint's dependencies, so do this on a laptop that has installed it once before, or bring their wheels too.

### Installing from a checkout

Until Flashpoint is on PyPI, and for Ignite's own laptops, install from the repository (`gh` and access to IgniteRobotics are needed):

```sh
gh repo clone IgniteRobotics/flashpoint
cd flashpoint
git switch rewrite
pipx install --python python3.12 .
```

Ignite's configuration (robots, units, failure labels) is the checkout's `config/` folder. Point Flashpoint at it in your shell profile (`~/.zshrc` or `~/.bashrc`), or copy it to `~/.config/flashpoint`:

```sh
export FLASHPOINT_CONFIG="$HOME/flashpoint/config"
```

On Windows, set it once with `setx FLASHPOINT_CONFIG "%USERPROFILE%\flashpoint\config"`, then open a new terminal. To update a checkout install: `git pull`, then `pipx install --force --python python3.12 .`.

## Before an event: get ready offline

Hoot files are converted with CTRE's `owlet` tool. Flashpoint downloads the right owlet version the first time it sees a hoot of that format, so **ingest one recent hoot while you still have internet**:

```sh
flashpoint ingest path/to/a/recent.hoot
flashpoint doctor
```

`doctor` checks owlet for each hoot format in your lake: each must say `downloaded, verified`. With no hoots yet, it checks this season's owlet. In the Setup view, **Download owlet** does the same without a hoot.

## Where your data lives: the lake

Flashpoint keeps everything in one folder, the **lake**, at `~/flashpoint-lake` by default. To use another folder, such as a bigger drive:
- put `--lake PATH` on any command, or
- set `lake` in `acquire.toml` in your config folder (`flashpoint init --lake PATH` does this), or
- set `FLASHPOINT_LAKE` in your shell profile.

The lake keeps an untouched copy of every log, so you can delete the copies you imported from. Don't edit files inside the lake by hand. [How it works](how-it-works.md#the-lake) explains what each folder holds.

## Import logs by hand

Copy logs off the robot or a USB stick into any folder, then:

```sh
flashpoint ingest path/to/logs/
```

You can pass files or folders, and folders are searched all the way down. A file that's already in the lake is skipped, so running it twice is fine.

At the end, `ingest` prints a summary like this:

```
  WARNING     GACMP_E10_rio_2026-04-11_19-29-15.hoot: incomplete-read
  QUARANTINED GADAL_Q11_..._2026-03-06_17-52-51.hoot: empty-file
14 ingested, 0 skipped, 1 quarantined in 13.1s
```

- **Ingested** files are in the lake.
- **WARNING** means the file was read but something was off. `incomplete-read` means the end of the file is missing, usually because the robot lost power while writing it. The rest of the data is still used.
- **QUARANTINED** means the file couldn't be read at all, for example because it's empty or corrupt. It's kept in the lake but not used.
- **"declared signal(s) not written"** lines mean the robot config expects a NetworkTables entry that the log doesn't have, which is normal for older code. It only matters if you were looking for that signal.

Ingest then works out matches, aligns the logs, and runs anomaly detection, all on its own.

To have logs pulled from the robot and USB sticks without doing this by hand, set up [automatic import](auto-import.md).

## Look at the data: `flashpoint serve`

```sh
flashpoint serve
```

Open <http://127.0.0.1:8000/> in a browser. Stop the server with Ctrl-C. The app has three views.

### Replay: one match

Pick a match from the list on the left; matches are grouped by event. You'll see:
- **Tracks** over the match: battery voltage, total current, the hottest motor's temperature, and the highest-current motor. Auto and teleop are shaded. You can swap any track for any motor and metric.
- **Markers** on the timeline. A WARN or FAULT marker is a fact, such as "temperature reached 65 °C", "battery dropped below 6.75 V", "stall", "device rebooted", or "CAN frames stopped". Markers don't guess at a cause. Click one to jump to it.
- A **readout** of every motor at the cursor. With no cursor placed, it lists each motor's maximum for the match, hottest first.
- **Open in AdvantageScope** opens the match's logs in AdvantageScope, on the machine running `serve` only. There's also a link to download the raw logs. Flashpoint uses the newest WPILib install of AdvantageScope unless `config/report.toml` names another.

"Not logged" means the data isn't there, which is not the same as zero. A match whose hoots didn't record temperature says so in its header.

### History: every motor over time

Pick a metric (maximum temperature, lowest voltage under load, energy, stall time, current, reboots, CAN gaps) to see one point per match:
- By default there is one line per **unit**, the physical motor, followed by serial number across slots and robots. You can switch to one line per **slot**, the position on the robot.
- Point at a line to see which motor it is.
- Dashed lines mark where each competition starts, labelled with the event (for example GACMP); the line above the chart lists the competitions and their match counts.
- Select a motor to highlight it, see its totals, and jump to its matches in Replay.

### Anomalies: what Flashpoint flagged

A queue of everything detection found. Each anomaly comes with its evidence (the matches and values), a severity (HIGH, MED, or LOW), and likely causes to check.

| Kind | Meaning |
|---|---|
| SPEC | Broke a fixed limit |
| DRIFT | Slowly moving away from this motor's own normal |
| STEP | Jumped and stayed there |
| SIBLING | Different from the motors doing the same job |

The count badge in the navigation always counts the whole lake, whatever the filters. A new motor needs about 5 matches of history before DRIFT and STEP can fire; until then it shows as "building baseline". GROUP DEVICE groups the queue by motor, with a robot's brownouts under its battery.

**Triage** records what the team did about an anomaly:
- **INVESTIGATE**: someone is looking into it.
- **MARK RESOLVED**: fixed. Write what you did.
- **DISMISS AS EXPECTED**: not a problem, for example after a gearbox change. Write why. You can also restart the motor's baseline from that match.
- **REOPEN**: undo either of the last two.

Triage is saved in the lake and kept in the backup. It only works in a browser on the laptop running `serve`. Anyone viewing over the network can look but can't change anything.

### Filters

The bar under the header filters by season, robot, event, and unit. A filter set in one view stays set in the others, and it's in the address, so a copied link keeps it. CLEAR FILTERS clears them everywhere. History's own filters (metric type, phase, subsystem, slot) come after the divider and affect History only. Replay ignores the unit filter, because its match list doesn't record which units played each match.

On a wide screen each column scrolls on its own, and the ◂ / ▸ control on a side rail shrinks it to a strip. This laptop remembers that per view.

### Setup: checks and configuration

The **Setup** tab shows what `flashpoint doctor` checks, and your whole configuration, and lets you change it without editing files.

- **Checks:** one line per check, marked ✓, `!` (warning) or ✕ (problem). Known problems have a button: **Create configuration** (as `flashpoint init`), **Download owlet**, **Set AdvantageScope path**, **Install** or **Start the acquire service** (Linux and Windows). The checks run again after each fix. Anything else shows the command to run.
- **Configuration:** your team number and robot addresses, each robot's slots, buses and signals, the report and anomaly thresholds, and the units list, each with the file it comes from. `DEFAULT` marks a file that ships with Flashpoint; saving it writes your own copy. Each section has its own **SAVE**:
  - a save is checked with the same rules as loading the file, and refused with the same message if something is wrong (two slots with the same CAN id, a signal under a root the season doesn't have);
  - a save is refused if someone changed the file by hand since you opened it: reload and redo the edit;
  - comments and the order of everything you didn't change are kept, and the previous version goes to `.backups/` in the config folder (the last 20 of each file);
  - after a save, the view says what to run: `flashpoint derive` after a robot change, `flashpoint detect` after an anomaly threshold, `flashpoint report` after a report threshold. Saving never changes the lake.
- **Draft robot:** pick an imported log and press **DRAFT FROM THIS LOG**. Every CAN device on its hoots becomes a slot, with its serial number if the log has a CAN inventory, and bus names if the robot code logs bus status ([robot-side logging](robot-side.md)). Give each slot an id, subsystem and role, then **SAVE ROBOT**. A log with no hoots can't be drafted from.

Fixes and saves only run on the computer running `flashpoint serve`. Opened from another computer, or with `--host 0.0.0.0`, Setup is read-only and shows commands instead of buttons.

### Showing other people on the pit network

```sh
flashpoint serve --host 0.0.0.0
```

Other devices on the same network can then open `http://<this laptop's IP>:8000/`. There's no password, so only do this on a network you trust. Over the network, the app is read-only.

## Take Replay on a USB stick

```sh
flashpoint report --static /Volumes/USB/flashpoint
```

This writes a folder that opens by double-clicking `index.html`, on any computer with no install and no network. Only Replay is included; History and Anomalies need `serve`. Add `--no-raw` to leave the raw logs out if the stick is small.

## Describe a robot

Flashpoint needs to know which motor is which. Each robot has a file in the `robots/` folder of your config folder, such as Ignite's [`2026-comp.toml`](../../config/robots/2026-comp.toml). The quickest start is **Draft robot** in the [Setup view](#setup-checks-and-configuration); you can also edit the file by hand. Each motor is a slot:

```toml
[[slot]]
id = "drive-fl"
bus = "canivore"          # "rio" or "canivore"
model = "TalonFX"
can_id = 11
subsystem = "drivetrain"
role = "front-left drive"
gear_ratio = 6.746031746031747
```

The file also names:
- the **battery proxy**: a low-current device on the main power bus, like the gyro, whose voltage stands in for the battery;
- **sibling groups**: motors that do the same job and are compared with each other;
- **swap dates** for older logs that have no CAN inventory.

Flashpoint picks the robot file from the robot code's project name (`project = "Robot-2026"`) and the log's year.

### NetworkTables signals

To bring a NetworkTables value (subsystem state, PhotonVision) into Flashpoint, add a `[[signal]]` table to the robot file. `root` names an entry in the season file, `seasons/2026.toml` in your config folder, and `entry` is the rest of the name. The season file that ships with Flashpoint has the roots every WPILib robot shares; add your robot code's own root as `robot` (Ignite's is in [`config/seasons/2026.toml`](../../config/seasons/2026.toml)):

```toml
[[signal]]
id = "intake-at-extension-setpoint"
root = "robot"                      # NT:Robot/m_robotContainer/
entry = "Intake/At Extension Setpoint"
subsystem = "intake"
metric = "at_setpoint"
```

Numbers are stored as numbers, and true/false as 1/0. A declared entry that a log doesn't have, or that isn't a number, is listed as a warning when `derive` runs; `flashpoint doctor` shows how many signals each robot declares.

After editing a robot file, run:

```sh
flashpoint derive --all
```

You don't need to re-ingest.

### Thresholds and the motor list

You probably won't need to change these, but they're all team-editable (in the Setup view or by hand, in your config folder) and checked when Flashpoint loads them. A file missing from your config folder uses the default that ships with Flashpoint:

| File | Controls |
|---|---|
| `report.toml` ([default](../../src/flashpoint/defaults/report.toml)) | Replay marker and History limits: temperatures, brownout and sag voltages, CAN gap lengths. Also an optional AdvantageScope path |
| `anomaly.toml` ([default](../../src/flashpoint/defaults/anomaly.toml)) | Anomaly thresholds. After editing, run `flashpoint detect` |
| `anomaly-causes.toml` ([default](../../src/flashpoint/defaults/anomaly-causes.toml)) | The "likely causes" text on each anomaly |
| `failure-labels.toml` (Ignite's: [config/](../../config/failure-labels.toml)) | Real failures, used by `flashpoint backtest` to check detection. No default |
| `units/units-seed.csv` ([how to fill it in](../../config/units/README.md)) | The list of every physical motor the team owns, by serial number, with its label |

## Check the setup: `flashpoint doctor`

Run `flashpoint doctor` whenever something looks wrong. It changes nothing.

| Section | What to look for |
|---|---|
| `lake` | The folder you expect, and how many files are at each stage. "N file(s) from an older pipeline version" means a Flashpoint update needs `flashpoint rebuild` |
| `config` | The config folder in use. "missing" means run `flashpoint init` (or set `FLASHPOINT_CONFIG` for a checkout's `config/`) |
| `defaults` | Settings files that come from the defaults shipped with Flashpoint, because your config folder has no copy |
| `seasons`, `robots` | Each season's roots and each robot. An empty robot list means no robot file was added yet |
| `checks` | One line per setup check. `PROBLEM` lines say what's wrong and the fix under them; `doctor` then exits `1`. See the [CLI reference](cli-reference.md#flashpoint-doctor) |
| `owlet cache` | Where owlet is kept. The owlet check says whether each version your logs need is downloaded; if not, you can't convert hoots offline |
| `acquire` | The last automatic import and backup ([automatic import](auto-import.md#check-the-status)) |
| `AdvantageScope` | Which install "Open in AdvantageScope" uses |

## Event-day checklist

**The week before:**
- [ ] Update Flashpoint ([Updating](#updating)) while you have internet, and run `flashpoint doctor`.
- [ ] Make sure this season's owlet is downloaded: **Download owlet** in Setup, or ingest a recent hoot.
- [ ] Confirm the robot in Setup matches the robot: CAN IDs, swaps since last event.
- [ ] If you use [automatic import](auto-import.md), run `flashpoint acquire --dry-run` with the robot on.

**At the event:**
- [ ] After each match, pull logs: automatic import, or copy them and run `flashpoint ingest`.
- [ ] `flashpoint serve` and check Replay for FAULT markers, and Anomalies for anything new.
- [ ] Triage what you looked at, so the queue stays meaningful.

**After the event:**
- [ ] Back up the lake ([backup](auto-import.md#backup-and-restore)).
- [ ] Add any real failures to `failure-labels.toml` in your config folder.
