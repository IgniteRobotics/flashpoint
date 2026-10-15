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
- The [GitHub CLI](https://cli.github.com/) (`gh`). The repository is private, so you need to be in the IgniteRobotics GitHub organization. Ask a mentor if you aren't.

Then, once:

```sh
gh auth login
gh repo clone IgniteRobotics/flashpoint
cd flashpoint
git switch rewrite
pipx install --python python3.12 .
```

`--python` picks which Python pipx uses. Name any 3.11–3.13 you have installed. On Windows, use `py -3.12` to find it, or give the full path.

Last, tell Flashpoint where the robot configuration is. A pipx install can't find the checkout's `config/` folder on its own. Add this to your shell profile (`~/.zshrc` or `~/.bashrc`), using the folder you cloned into:

```sh
export FLASHPOINT_CONFIG="$HOME/flashpoint/config"
```

On Windows, set it once with `setx FLASHPOINT_CONFIG "%USERPROFILE%\flashpoint\config"`, then open a new terminal.

Check it worked:

```sh
flashpoint doctor
```

Under `robots` you should see `2025-comp` and `2026-comp`. If nothing is listed there, `FLASHPOINT_CONFIG` isn't set in this terminal.

### Updating

```sh
cd flashpoint
git pull
pipx install --force --python python3.12 .
```

### Installing without GitHub access

A mentor can copy the `flashpoint` folder to a USB stick, and you can install from there: `pipx install --python python3.12 /Volumes/USB/flashpoint` (or `E:\flashpoint` on Windows). Then set `FLASHPOINT_CONFIG` to a copy of its `config/` folder that you keep on the laptop.

## Before an event: get ready offline

Hoot files are converted with CTRE's `owlet` tool. Flashpoint downloads the right owlet version the first time it sees a hoot of that format, so **ingest one recent hoot while you still have internet**:

```sh
flashpoint ingest path/to/a/recent.hoot
flashpoint doctor
```

`doctor` lists each owlet version under `owlet cache`. The one for this season's hoots must say `cached`. Older seasons can say `owlet-not-cached` until you need them.

## Where your data lives: the lake

Flashpoint keeps everything in one folder, the **lake**, at `~/flashpoint-lake` by default. To use another folder, such as a bigger drive:
- put `--lake PATH` on any command, or
- set `FLASHPOINT_LAKE` in your shell profile, the same way as `FLASHPOINT_CONFIG`.

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

Flashpoint needs to know which motor is which. Each robot has a file in [`config/robots/`](../../config/robots/), such as [`2026-comp.toml`](../../config/robots/2026-comp.toml). Each motor is a slot:

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

To bring a NetworkTables value (subsystem state, PhotonVision) into Flashpoint, add a `[[signal]]` table to the robot file. `root` names an entry in the season file, [`config/seasons/2026.toml`](../../config/seasons/2026.toml), and `entry` is the rest of the name:

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

You probably won't need to change these, but they're all team-editable and checked when Flashpoint loads them:

| File | Controls |
|---|---|
| [`config/report.toml`](../../config/report.toml) | Replay marker and History limits: temperatures, brownout and sag voltages, CAN gap lengths. Also an optional AdvantageScope path |
| [`config/anomaly.toml`](../../config/anomaly.toml) | Anomaly thresholds. After editing, run `flashpoint detect` |
| [`config/anomaly-causes.toml`](../../config/anomaly-causes.toml) | The "likely causes" text on each anomaly |
| [`config/failure-labels.toml`](../../config/failure-labels.toml) | Real failures, used by `flashpoint backtest` to check detection |
| [`config/units/`](../../config/units/README.md) | The list of every physical motor the team owns, by serial number, with its label |

## Check the setup: `flashpoint doctor`

Run `flashpoint doctor` whenever something looks wrong. It changes nothing.

| Section | What to look for |
|---|---|
| `lake` | The folder you expect, and how many files are at each stage. "N file(s) from an older pipeline version" means a Flashpoint update needs `flashpoint rebuild` |
| `seasons`, `robots` | Each configured season and robot. `INVALID` means a config file has a mistake, and the message says where. An empty list means `FLASHPOINT_CONFIG` isn't set |
| `owlet cache` | `cached` for this season's version, or you can't convert hoots offline |
| `acquire` | The last automatic import and backup ([automatic import](auto-import.md#check-the-status)) |
| `AdvantageScope` | Which install "Open in AdvantageScope" uses |

## Event-day checklist

**The week before:**
- [ ] `git pull` and reinstall ([Updating](#updating)), while you have internet.
- [ ] Ingest a recent hoot so this season's owlet is cached, and check with `flashpoint doctor`.
- [ ] Confirm the robot file in `config/robots/` matches the robot: CAN IDs, swaps since last event.
- [ ] If you use [automatic import](auto-import.md), run `flashpoint acquire --dry-run` with the robot on.

**At the event:**
- [ ] After each match, pull logs: automatic import, or copy them and run `flashpoint ingest`.
- [ ] `flashpoint serve` and check Replay for FAULT markers, and Anomalies for anything new.
- [ ] Triage what you looked at, so the queue stays meaningful.

**After the event:**
- [ ] Back up the lake ([backup](auto-import.md#backup-and-restore)).
- [ ] Add any real failures to `config/failure-labels.toml`.
