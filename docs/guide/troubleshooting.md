# Troubleshooting

Fixes for the problems people hit in their first 30 minutes, by the message or symptom you see. Where Flashpoint prints the message, it's quoted exactly; `<...>` stands for the part that changes. For a bad key in a settings file, see that file's "Common errors" under [Reference: config files](reference/config/acquire.md). Still stuck? Ask in [Q&A discussions](https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a), or see the [FAQ](faq.md).

## Install and first run

### `flashpoint` isn't found after installing

**What you see:** `flashpoint: command not found` (macOS, Linux), or PowerShell saying `flashpoint` isn't recognized as the name of a cmdlet (Windows).

**Why:** `uv tool install` puts the command in uv's tool folder, which isn't on your `PATH` yet.

**Fix:** run `uv tool update-shell`, then open a new terminal window. If you installed with pipx, run `pipx ensurepath` instead. Check with `flashpoint --version`.

### Python is refused

**What you see:** `pipx install flashpoint` fails with pip saying the package requires a different Python, or `flashpoint doctor` shows a `PROBLEM` on Python:

**Message:** `Python <version> isn't supported (3.11 to 3.14)`

**Why:** Flashpoint runs on Python 3.11 to 3.14, and that install used another one.

**Fix:** install with uv, which downloads a supported Python for Flashpoint and leaves your own alone: `uv tool install --python 3.13 flashpoint` ([Install](install/index.md)). With pipx, point it at a supported Python: `pipx install --python python3.13 flashpoint`.

### A proxy or firewall blocks the owlet download

**What you see:** the first hoot fails to import. Ingest prints, with `owlet-download-failed` as the reason:

**Message:** `FAILED      <file>: failed (<reason>), will retry`

and `flashpoint doctor` shows the owlet as not downloaded:

**Message:** `owlet <version>: <state> (needed for the first hoot import; needs internet)`

**Why:** Flashpoint converts hoots with CTRE's `owlet` tool, which it downloads from CTRE the first time it meets a hoot version. A school or event network can block that download.

**Fix:** any of these:
- set your proxy for the command, for example `HTTPS_PROXY=http://proxy.example:8080 flashpoint ingest <folder>` (PowerShell: `$env:HTTPS_PROXY = "http://proxy.example:8080"` first). Flashpoint uses the standard proxy variables;
- ingest one hoot on another network (home, a phone hotspot) first: owlet stays cached in `~/.cache/flashpoint/` (or `FLASHPOINT_CACHE`), so it's needed only once per hoot version;
- [install offline](how-to/install-offline.md), which brings owlet along.

The failed files are retried on the next `ingest`, so run it again once owlet can download.

### Windows paths and quoting

**What you see:** a command can't find a folder whose name has a space in it, a `FLASHPOINT_CONFIG` setting seems ignored, or a `report.toml` path turns into nonsense.

**Why:** PowerShell splits unquoted paths at spaces, sets environment variables differently from macOS and Linux, and TOML treats a backslash in double quotes as an escape.

**Fix:**
- Quote paths with spaces: `flashpoint ingest "D:\Robot Logs\GACMP"`, `--lake "C:\Users\Pit Laptop\flashpoint-lake"`.
- Set a variable with `$env:`, then run the command on its own line: `$env:FLASHPOINT_CONFIG = "$PWD\flashpoint-sample\config"`. (`set NAME=value` is the old `cmd.exe` form, and the `NAME=value command` form is macOS and Linux only.)
- In TOML files, write Windows paths in single quotes, so backslashes stay literal: `path = 'C:\Users\Public\wpilib\2026\advantagescope\AdvantageScope (WPILib).exe'`.

### The sample's checksum doesn't match

**What you see:** the Quickstart's checksum line for `flashpoint-sample-1.zip` prints `FAILED` (macOS and Linux) or `False` (Windows).

**Why:** the download was cut short or damaged.

**Fix:** delete `flashpoint-sample-1.zip` and download it again with the [Quickstart's](quickstart.md#try-it-with-sample-logs) command, then check it again. If it still fails, report it with the [bug form](https://github.com/IgniteRobotics/flashpoint/issues/new/choose).

## `doctor` problems

`flashpoint doctor` (and the Setup view) prints one line per check. Each `PROBLEM` line below is followed in the output by a `fix:` line with the command to run.

### No configuration

**Message:** `no configuration in <folder>: Flashpoint doesn't know your team or robot`

**Why:** there's no `acquire.toml` in the config folder, so Flashpoint has no team number and maps no robot.

**Fix:** `flashpoint init --team <number>`, or **Create configuration** in the Setup view.

### No team number

**Message:** `no team number: no configuration yet`

**Message:** `no team number: acquire can't find your robot`

**Why:** without a team number, robot pull can't work out the robot's addresses.

**Fix:** `flashpoint init --team <number>`, or set `team` in [`acquire.toml`](reference/config/acquire.md).

### A settings file doesn't load

**Message:** `<file> doesn't load (see above)`

or a line naming the file and its first bad key, such as `robots/2026-comp.toml: slot.0.gearing: Extra inputs are not permitted`.

**Why:** a typo, a key from another file, or a value of the wrong type. Flashpoint refuses unknown keys so typos don't pass silently.

**Fix:** edit the file the `fix:` line names. Each file's reference page lists its keys and its common errors: [`acquire.toml`](reference/config/acquire.md#common-errors), [robot files](reference/config/robots.md#common-errors), [`theme.toml`](reference/config/theme.md#common-errors), [`report.toml`](reference/config/report.md#common-errors), [`anomaly.toml`](reference/config/anomaly.md#common-errors), [season files](reference/config/seasons.md#common-errors).

### No robot for this season

**Message:** `no robot configured for season <year>, and the lake has <year> logs: their motors aren't matched to slots`

**Why:** the lake has logs from this season but no robot file says which motor is which.

**Fix:** use **Draft robot** in the Setup view, or let the next `flashpoint ingest` draft one ([Set up a robot file](how-to/set-up-a-robot-file.md)). Then run `flashpoint derive`.

### The lake folder can't be written

**Message:** `can't write to <folder>`

**Message:** `can't create <folder>: <parent> isn't writable`

**Fix:** point `lake` in [`acquire.toml`](reference/config/acquire.md) (or `--lake`) at a folder you own, such as `~/flashpoint-lake`.

### Hoots this version can't read

**Message:** `the lake has C<number> hoots, which this Flashpoint can't read`

**Why:** a newer Phoenix version writes a hoot format (its "compliancy") this Flashpoint has no owlet for. This happens after a new season's Phoenix release.

**Fix:** update Flashpoint ([Update Flashpoint](how-to/update-flashpoint.md)), then run `flashpoint rebuild --quarantined` to read the hoots that were set aside.

### owlet isn't downloaded, or its checksum doesn't match

**Message:** `owlet <version>: <state>`

where the state is `not downloaded` or `checksum doesn't match`. It's a `PROBLEM` when the lake already has hoots that need it.

**Fix:** `flashpoint ingest` of any hoot downloads it again (with internet). If the checksum keeps failing, delete `~/.cache/flashpoint/owlet/` and ingest again. Behind a proxy, see [above](#a-proxy-or-firewall-blocks-the-owlet-download).

**Message:** `owlet <version> has no build for <platform>`

**Why:** CTRE doesn't publish owlet for this computer's operating system and processor (for example 32-bit Windows).

**Fix:** import hoots on a supported computer ([support matrix](support-matrix.md#platforms)).

### The backup is overdue

**Message:** `<what> not backed up for <time> (<last backup>)`

**Message:** `rclone isn't installed, so no backup can run; <status>`

**Why:** logs or configuration have waited longer than `backup.overdue_h` hours for a backup, or rclone, which does the copying, isn't installed.

**Fix:** install rclone if the message says so, then `flashpoint backup` once you're online. To quiet it over a long event with no internet, raise `overdue_h` ([Back up the lake](how-to/back-up-the-lake.md)).

### AdvantageScope path not found

**Message:** `configured AdvantageScope path not found: <path>`

**Fix:** correct `path` under `[advantagescope]` in [`report.toml`](reference/config/report.md#advantagescope), or clear it to use the WPILib install. The Setup view's **Set AdvantageScope path** button does it for you.

## Logs and views

### Replay says "No Replay data"

Replay shows **No Replay data** with one of three explanations.

**Message:** `The lake holds no imported logs yet: <lake>`

**Why:** nothing has been imported into the lake the app is reading. Often the logs went into a different lake: `ingest` and `serve` each use `--lake`, else `FLASHPOINT_LAKE`, else `lake` in `acquire.toml`, else `~/flashpoint-lake`.

**Fix:** import with the command the view shows, or run `serve` with the same `--lake` you imported into. If it also says files were quarantined, see [A file was quarantined](#a-file-was-quarantined).

**Message:** `Replay lists only competition matches, identified from FMS data in the log or a match-style log file name.`

**Why:** every log in the lake is practice, pit or home testing, not a competition match. See [Every log is a non-match session](#every-log-is-a-non-match-session).

**Message:** `<count> waiting for Replay data. On the laptop that holds the lake, run:`

**Why:** the matches are imported, but their Replay data hasn't been built. The automatic import builds it; a plain `ingest` builds it too, unless it was run with `--no-derive`.

**Fix:** `flashpoint report`. For one match the view names, `flashpoint report --match <key>`.

### Ingest says derive failed

**What you see:** `ingest` or `rebuild` lists the imported files, then an error from derive, then:

**Message:** `derive failed: Replay data was not built. Fix the error above, then run: flashpoint derive --report`

**Why:** the files were imported, but the derive that runs after the import stopped with an error, so no Replay data was built for them. The error printed just above says why; a broken robot file shows as `robot configuration error`. The command exits 1 even when every file imported. Running `ingest` again doesn't help: it skips files it already has, so derive doesn't run again.

**Fix:** fix the error derive printed (for a robot file, see [A settings file doesn't load](#a-settings-file-doesnt-load)), then run `flashpoint derive --report`.

### Every log is a non-match session

**What you see:** Replay lists no matches, and History shows the logs' motor usage.

**Why:** Flashpoint names a match from the FMS info in the log, then from the file name (`FRC_yyyyMMdd_HHmmss_<event>_<match>.wpilog`), then from The Blue Alliance. A log from practice, the pit or home has none of those, so it's a non-match session. That's expected away from an event. At an event, the Driver Station gets FMS info only when connected to the field.

**Fix:** nothing, for practice logs: History still counts their usage. If they really are matches, keep the original file names (WPILib names them with the event and match once the FMS connects), and make sure robot code [records NetworkTables to the DataLog](robot-side/datalog.md), where the FMS info arrives.

### A match says "no aligned samples"

**What you see:** the match list shows **NO ALIGNED SAMPLES**, and the match opens with the reason in place of its tracks:

**Message:** `No aligned samples: there is no wpilog to align its hoots to.`

**Fix:** robot code needs to write a wpilog as well as hoots: [turn on the WPILib DataLog](robot-side/datalog.md). The match's raw hoots are still listed for download.

**Message:** `No aligned samples: no robot is configured for robot code <project> in season <year>, so its devices aren't matched to slots. Add one with Draft robot in the Setup view.`

**Fix:** see [The robot shows as "unknown"](#the-robot-shows-as-unknown).

**Message:** `No aligned samples: no device data could be aligned to the wpilog.`

**Why:** the hoots and the wpilog share no signal or enable edge Flashpoint can line up ([how alignment works](robot-side/datalog.md#what-flashpoint-reads-from-it)).

**Fix:** open the match in AdvantageScope from the same view to look at the raw logs, and report it with the [bug form](https://github.com/IgniteRobotics/flashpoint/issues/new/choose) if both logs look complete.

### The robot shows as "unknown"

**What you see:** matches show robot "unknown", and their motors aren't mapped to slots.

**Why:** no robot file matches the log. Either the log's season has no robot file, or it has several (a practice and a competition bot) and none has the `project` the robot code logged in its [build info](robot-side/build-info.md).

**Fix:** use **Draft robot** in the Setup view on one of the logs ([Set up a robot file](how-to/set-up-a-robot-file.md)), or set `project` in the right [robot file](reference/config/robots.md#which-robot-file-a-log-uses) to the log's project name. Then run `flashpoint derive`.

### Replay shows "Low alignment confidence"

**Message:** `▲ Low alignment confidence (<method>): hoot and wpilog times may disagree. Check events against AdvantageScope.`

**Why:** the hoots were lined up by the robot-enable edges or the wall clock, not by a signal written to both logs, so times can be off by up to half a second.

**Fix:** treat marker times as approximate. Robot code that writes one signal to both logs (CTRE's swerve code does) gets exact alignment.

### No temperature

**What you see:** Replay's header names the buses without temperature:

**Message:** `Temperature not logged on <buses>: the hoots are not Pro-licensed, and temperature needs a Phoenix Pro-licensed device on that bus.`

and those buses' temperature cells read `not logged (needs Phoenix Pro)`. A gyro or CANcoder reads `temperature not tracked`, Pro or not. When no bus has temperature and every hoot was Pro-licensed, the header says instead:

**Message:** `Temperature not logged. <reason>`

with the reason:

**Message:** `No temperature was logged for this match.`

**Why:** a hoot exports motor temperature only when that bus's log has a Phoenix Pro-licensed device. The second message means the hoots had Pro but no temperature signal was recorded.

**Fix:** see [Phoenix Pro and temperature](robot-side/pro-and-temperature.md). Everything except temperature works without Pro. `flashpoint doctor` shows how many of your hoots are Pro-licensed.

### A file was quarantined

**What you see:** ingest lists the file as quarantined, with a reason such as `empty-file`:

**Message:** `QUARANTINED <file>: <reason>`

**Why:** the file itself is bad, so Flashpoint sets it aside instead of retrying it. The reason says how:

| Reason | Means |
|---|---|
| `empty-file` | The file is 0 bytes: the robot lost power before writing anything |
| `invalid-header` | Not a wpilog or hoot, or damaged at the start |
| `too-old` | A wpilog version older than Flashpoint reads |
| `unsupported-version` | A wpilog version newer than Flashpoint reads |
| `unsupported-compliancy:<n>` | A hoot format this Flashpoint has no owlet for: [update](how-to/update-flashpoint.md) |
| `no-signals-for-profile` | The hoot has none of the signals Flashpoint reads |

A file whose conversion keeps failing (`owlet-failed`, `owlet-timeout`) is quarantined after its third try. Quarantined files are listed by `flashpoint doctor`; they're never shown in the app.

**Fix:** usually none: an empty or damaged file has nothing to read. For `unsupported-compliancy`, update Flashpoint and run `flashpoint rebuild --quarantined`, which tries every quarantined file again. A later `ingest` skips the file, saying it was quarantined earlier.

### A file was imported as incomplete

**What you see:** ingest imports the file with a warning, `short-coverage` or `incomplete-read`; when you open the match in AdvantageScope from Replay, the hoot is listed with an **INCOMPLETE** badge:

**Message:** `WARNING     <file>: <warning>`

**Why:**
- `short-coverage`: the hoot covers much less time than the other hoots of its session, usually because it was cut short.
- `incomplete-read`: owlet read what it could and stopped early, because the file's end is missing. That's what a robot power-off mid-write leaves behind.

**Fix:** none needed: Flashpoint keeps what was read. The missing stretch has no data, and Replay shows it as not logged. If a hoot that should be complete keeps doing this, check the robot's [disk space](robot-side/disk-space.md).
