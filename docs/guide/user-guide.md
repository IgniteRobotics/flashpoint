# User guide

For students and mentors who run Flashpoint: at the pit, after an event, or on the team laptop. You don't need to know Python. If you want to change the code, read the [developer guide](under-the-hood/developer-guide.md) instead.

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

You need a computer running one of:
- macOS 13 or later;
- Linux with glibc 2.35 or later (Ubuntu 22.04 or later);
- Windows on an x86-64 PC (not Windows on ARM).

Flashpoint runs on Python 3.11 to 3.14 (not 3.15 yet). You don't have to install that Python yourself: [uv](https://docs.astral.sh/uv/) downloads one for Flashpoint if your computer doesn't have a suitable one.

1. Install uv, once. On macOS, `brew install uv`. On Windows, `winget install --id=astral-sh.uv -e`. On Linux (or any of them), use [uv's installer](https://docs.astral.sh/uv/getting-started/installation/). Open a new terminal afterwards.
2. Install Flashpoint and create your configuration:

   ```sh
   uv tool install --python 3.13 flashpoint
   flashpoint init --team 1234
   ```
3. Import one match's logs, then look at them:

   ```sh
   flashpoint ingest path/to/one-match/
   flashpoint serve
   ```

If `flashpoint` isn't found after the install, run `uv tool update-shell` and open a new terminal.

**With pipx instead.** If you already use [pipx](https://pipx.pypa.io/), it works too, but it needs a Python 3.11 to 3.14 already installed:
- macOS and Linux: `pipx install --python python3.13 flashpoint` (any of `python3.11` to `python3.14` that you have).
- Windows: in the python.org installer, tick "py launcher". Then `py -3.13 -m pip install --user pipx`, and `py -3.13 -m pipx install flashpoint`.

`flashpoint init` creates your **config folder** (`~/.config/flashpoint`, or `%APPDATA%\flashpoint` on Windows) with your team number, your lake folder, your team name and colours (`theme.toml`), and copies of the default settings. Anything you leave out (`--team`, `--lake`, `--team-name`, `--palette`) it asks for in a terminal; leave the name blank for none. See [`flashpoint init`](reference/cli.md#flashpoint-init) for the options. It never overwrites a file, so running it again is safe. The robot addresses Flashpoint tries come from the team number.

**Your first import drafts your robot.** When the lake has logs from a season that has no robot file yet, `ingest` drafts one from them (`robots/2026-comp.toml`, from the match with the most CAN devices) and says so. Replay shows that match's data right away. Open **Setup > Robots** and fill in each slot's subsystem and role; keep the drafted slot ids, which already name each device's model and CAN id. Logs with hoots but no wpilog, or an import with `--no-robot-draft`, draft nothing: use **Setup > Draft robot** for those ([Setup](reference/views/setup.md)).

`flashpoint doctor` lists the setup checks. Right after `init`, the robot check is a warning: the lake has no logs from this season yet. Once you import this season's logs it is a problem only while no robot is configured. A drafted robot whose slots have no subsystem yet is a warning, not a problem.

### Updating

See [Update Flashpoint](how-to/update-flashpoint.md).

### Installing without internet

See [Install without internet](how-to/install-offline.md).

### Installing from a checkout

For Ignite's own laptops, or to try changes that aren't released yet, install from the repository:

```sh
git clone https://github.com/IgniteRobotics/flashpoint.git
cd flashpoint
uv tool install --python 3.13 .
```

Ignite's configuration (robots, units, failure labels) is the checkout's `config/` folder. Point Flashpoint at it in your shell profile (`~/.zshrc` or `~/.bashrc`), or copy it to `~/.config/flashpoint`:

```sh
export FLASHPOINT_CONFIG="$HOME/flashpoint/config"
```

On Windows, set it once with `setx FLASHPOINT_CONFIG "%USERPROFILE%\flashpoint\config"`, then open a new terminal. To update a checkout install: `git pull`, then `uv tool install --force --python 3.13 .`.

## Before an event: get ready offline

Hoot files are converted with CTRE's `owlet` tool. Flashpoint downloads the right owlet version the first time it sees a hoot of that format, so **ingest one recent hoot while you still have internet**:

```sh
flashpoint ingest path/to/a/recent.hoot
flashpoint doctor
```

`doctor` checks owlet for each hoot format in your lake: each must say `downloaded, verified`. With no hoots yet, it checks the newest owlet this version of Flashpoint knows. In the Setup view, **Download owlet** does the same without a hoot.

That newest owlet may be last season's. If CTRE changes the hoot format for a new season, only a Flashpoint update can read the new hoots, and `doctor` can't tell until it sees one. So ingest a hoot from **this season's** robot code before an event (see [Starting a new season](#starting-a-new-season)).

## Where your data lives: the lake

Flashpoint keeps everything in one folder, the **lake**, at `~/flashpoint-lake` by default. To use another folder, such as a bigger drive, use one of these. When more than one is set, the first in this list wins:
1. `--lake PATH` on any command;
2. `FLASHPOINT_LAKE` in your shell profile;
3. `lake` in `acquire.toml` in your config folder (`flashpoint init --lake PATH` does this).

A leftover `FLASHPOINT_LAKE` hides what `acquire.toml` says. `flashpoint doctor` shows the lake in use.

The lake keeps an untouched copy of every log, so you can delete the copies you imported from. Don't edit files inside the lake by hand. [How it works](under-the-hood/how-it-works.md#the-lake) explains what each folder holds.

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
  FAILED      GACMP_Q7_rio_2026-04-09_21-51-06.hoot: failed (owlet-download-failed), will retry
13 ingested, 0 skipped, 1 failed, 1 quarantined, 0 quarantined earlier in 13.1s
```

- **Ingested** files are in the lake.
- **WARNING** means the file was read but something was off. `incomplete-read` means the end of the file is missing, usually because the robot lost power while writing it. The rest of the data is still used.
- **QUARANTINED** means the file is bad: it's empty, corrupt, or in a format Flashpoint can't read. It's kept in the lake but not used, and later runs don't try it again. They list it as `quarantined earlier`.
- **FAILED** means the laptop failed, not the file. Usually there was no internet to download owlet, the tool that reads `.hoot` files. Run `flashpoint ingest` again once you're online, or let automatic import retry it. If owlet crashes on the same file 3 times, the file is quarantined.
- To retry quarantined files, for example after updating Flashpoint, run `flashpoint rebuild --quarantined`. It works from the lake's own copies, so you don't need the original logs.
- **"declared signal(s) not written"** lines mean the robot config expects a NetworkTables entry that the log doesn't have, which is normal for older code. It only matters if you were looking for that signal.
- **"session(s) with hoots but no wpilog"** means some hoots had no `.wpilog` from the same time to align to. Their motors get no history: no Replay charts, usage or match features. Motor history needs both files, the hoots from Phoenix 6 and a wpilog from WPILib data logging (`DataLogManager.start()` in `robotInit()`). One such session among many is usually the hoot logger restarting mid-event, and is safe to ignore. When every session of a season is like this, `doctor`'s "Hoots without a wpilog" check warns too: the robot code isn't writing wpilogs.

Ingest then works out matches, aligns the logs, runs anomaly detection, and updates Replay, all on its own.

### How matches are identified

Replay lists competition matches only. Flashpoint knows a log is from a match when:
1. the wpilog holds FMS data (`FMSInfo`), which the robot gets from the field at an event; or
2. the wpilog's file name has the match in it, the way the Driver Station names match logs: `FRC_yyyyMMdd_HHmmss_EVENT_Q7.wpilog` (`P` for practice, `Q` for qualification, `E` for elimination matches).

Logs from the shop, the practice field or the pit have neither, so they get no Replay entry. They still count in History: every motor's usage, temperatures and anomalies include them. When Replay is empty but the lake has logs, it says how many sessions it found that aren't matches.

To have logs pulled from the robot and USB sticks without doing this by hand, set up [automatic import](how-to/auto-import.md).

## Look at the data: `flashpoint serve`

```sh
flashpoint serve
```

Open <http://127.0.0.1:8000/> in a browser. Stop the server with Ctrl-C. The app has these views.

Each view has its own reference page, with every control, marker and badge:

| View | For |
|---|---|
| [Replay](reference/views/replay.md) | One match: tracks, markers, a readout of every motor, and the raw logs |
| [History](reference/views/history.md) | Every motor over time, one point per match |
| [Anomalies](reference/views/anomalies.md) (beta) | What detection flagged, with evidence, likely causes and triage |
| [Setup](reference/views/setup.md) | The `doctor` checks, your whole configuration, and Draft robot |
| [Live CAN](reference/views/live-can.md) (experimental) | CAN dropouts while you shake-test or drive-test |

### Showing other people on the pit network

See [Show the app on the pit network](how-to/share-on-the-pit-network.md). There's no password, and the app is read-only over the network.

## Take Replay on a USB stick

See [Take Replay on a USB stick](how-to/replay-on-a-usb-stick.md).

## Team name and colours

The app header shows your team name and number, and the app uses your team's colours. Both live in `theme.toml`: see [Set the team name and colours](how-to/team-name-and-colours.md) and the [`theme.toml` reference](reference/config/theme.md).

## Describe a robot

Flashpoint needs to know which motor is which. Each robot has a file in the `robots/` folder of your config folder. The quickest start is **Draft robot** in the [Setup view](reference/views/setup.md). Every key, NetworkTables signals, sibling groups, the battery proxy, swap dates and which robot file a log uses are in the [robot file reference](reference/config/robots.md).

The other settings files (`report.toml`, `anomaly.toml`, `anomaly-causes.toml`, `failure-labels.toml`, `seasons/`, `units/units-seed.csv`) each have a page under [Reference: config files](reference/config/robots.md). A file missing from your config folder uses the default that ships with Flashpoint.

## Starting a new season

See [Start a new season](how-to/start-a-new-season.md).

## Check the setup: `flashpoint doctor`

Run `flashpoint doctor` whenever something looks wrong. It changes nothing.

| Section | What to look for |
|---|---|
| `lake` | The folder you expect, and how many files are at each stage. "N file(s) from an older pipeline version" means a Flashpoint update needs `flashpoint rebuild` |
| `config` | The config folder in use. "missing" means run `flashpoint init` (or set `FLASHPOINT_CONFIG` for a checkout's `config/`) |
| `defaults` | Settings files that come from the defaults shipped with Flashpoint, because your config folder has no copy |
| `seasons`, `robots` | Each season's roots and each robot. An empty robot list means no robot file was added yet |
| `checks` | One line per setup check. `PROBLEM` lines say what's wrong and the fix under them; `doctor` then exits `1`. See the [CLI reference](reference/cli.md#flashpoint-doctor) |
| `owlet cache` | Where owlet is kept. The owlet check says whether each version your logs need is downloaded; if not, you can't convert hoots offline |
| `acquire` | The last automatic import and backup ([automatic import](how-to/auto-import.md#check-the-status)) |
| `AdvantageScope` | Which install "Open in AdvantageScope" uses |

## Event-day checklist

**The week before:**
- [ ] First event of a season? Do [Starting a new season](#starting-a-new-season) first.
- [ ] Update Flashpoint ([Updating](#updating)) while you have internet, and run `flashpoint doctor`.
- [ ] Make sure this season's owlet is downloaded: **Download owlet** in Setup, or ingest a recent hoot.
- [ ] Confirm the robot in Setup matches the robot: CAN IDs, swaps since last event.
- [ ] If you use [automatic import](how-to/auto-import.md), run `flashpoint acquire --dry-run` with the robot on.

**At the event:**
- [ ] After each match, pull logs: automatic import, or copy them and run `flashpoint ingest`.
- [ ] `flashpoint serve` and check Replay for FAULT markers, and Anomalies for anything new.
- [ ] Triage what you looked at, so the queue stays meaningful.

**After the event:**
- [ ] Back up the lake ([backup](how-to/back-up-the-lake.md)).
- [ ] Add any real failures to `failure-labels.toml` in your config folder.
