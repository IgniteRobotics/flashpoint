# CLI reference

Every Flashpoint command is a subcommand of `flashpoint`. This page covers what each one is for, every option, and an example. The option tables are generated from the CLI itself (`python tools/gen-cli-reference.py`), so they match `flashpoint <command> --help`.

In a source checkout, put `poetry run` in front of each command (`poetry run flashpoint doctor`).

## Settings every command shares

| Setting | Default | Change it with |
|---|---|---|
| Lake (where logs and results are stored) | `lake` in `acquire.toml`, else `~/flashpoint-lake` | `--lake PATH` on any command, or `FLASHPOINT_LAKE` |
| Config folder (team, robots, seasons, thresholds) | `~/.config/flashpoint/` (Windows: `%APPDATA%\flashpoint`) | `FLASHPOINT_CONFIG` |
| Download cache (owlet) | `~/.cache/flashpoint/` | `FLASHPOINT_CACHE` |

A settings file missing from the config folder falls back to the default that ships with Flashpoint (thresholds, anomaly settings, season roots, the app's colours). `doctor` lists which files are defaults.

Exit codes: `0` success, `1` failed (for `ingest` and `rebuild`: a file failed or was quarantined in this run), `2` usage or configuration error, `3` another Flashpoint process holds the lock, `130` interrupted with Ctrl-C.

## `flashpoint init`

Create your config folder: `acquire.toml` with your team number and lake folder, `theme.toml` with the name and colours the app shows, plus copies of the default settings files to edit. Without `--team`, `--lake`, `--team-name` or `--palette` it asks for them (in a terminal only; leave the name blank for none). It never overwrites a file: run it again and it lists what already exists and changes nothing. The robot addresses acquire tries come from the team number. Of the season files Flashpoint ships, it copies the current season's and earlier ones; a later season is read from Flashpoint itself until you copy it.

It prints each file it created, then the next step: `next: import a match's logs with flashpoint ingest <folder> (it drafts your robot from them), then run flashpoint serve`.

<!-- generated:cli:init -->
| Option | What it does | Default |
|---|---|---|
| `--team NUMBER` | your FRC team number, 1 to 99999 (asked if omitted) | |
| `--lake LAKE` | lake folder for your logs (asked if omitted; ~/flashpoint-lake) | |
| `--team-name NAME` | name shown in the app header, at most 40 characters (asked if omitted) | |
| `--palette PRESET` | app colours: amber, green, blue, red or white (asked if omitted; amber) | |
<!-- /generated:cli:init -->

With no team name, the header shows your team number from `--team` ("telemetry // 254"); with neither, it reads just "telemetry". For your own colours instead of a preset, use the Setup view's Appearance section or edit `theme.toml`.

```sh
flashpoint init --team 254 --team-name "The Cheesy Poofs" --palette blue --lake ~/frc-lake
```

## `flashpoint ingest`

Copy `.wpilog` and `.hoot` files into the lake and read them. You can pass files, folders (searched recursively), or both. Files already in the lake are skipped, so re-running is safe. When at least one file was ingested, `derive` (and so `detect`) runs afterward on its own, and then the Replay data is rebuilt for the matches that changed (the same build as `flashpoint report`).

<!-- generated:cli:ingest -->
| Option | What it does | Default |
|---|---|---|
| `PATH` | .wpilog and .hoot files, or folders (searched recursively) | |
| `--profile {health,all}` | hoot signals: health (what Flashpoint uses; default) or all (about 3x larger) | `health` |
| `--jobs N` | hoot files converted in parallel (default: half your CPU cores) | |
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
| `--no-derive` | skip the automatic derive, and so the Replay build too | |
| `--no-report` | skip the automatic Replay build after derive | |
| `--no-robot-draft` | don't draft a robot file for a season that has none (see Setup > Draft robot) | |
<!-- /generated:cli:ingest -->

```sh
flashpoint ingest ~/Downloads/GACMP/
```

Files that need attention are listed before the summary:

| Line | Meaning |
|---|---|
| `QUARANTINED name: reason` | The file itself is bad (empty, corrupt, an unknown format). It stays in the lake, unused, and later runs don't try it again |
| `FAILED name: failed (reason), will retry` | The machine failed, not the file: owlet couldn't be downloaded (no internet), crashed, or timed out. The next `ingest`, `rebuild`, or acquire cycle tries it again. A file that crashes owlet or times out 3 times is quarantined |
| `SKIPPED name: quarantined earlier (reason)` | The file was quarantined in an earlier run. Retry it with `flashpoint rebuild --quarantined` |
| `WARNING name: ...` | The file was ingested, but something in it looked off (the warnings are listed) |

Then one line counts files ingested, skipped, failed, quarantined, and quarantined earlier. When files were quarantined earlier, a hint to run `flashpoint rebuild --quarantined` follows it. The exit code is `1` when a file failed or was quarantined in this run. Files quarantined earlier don't change it.

If another Replay build is running (the acquire watch or `serve` at start), `ingest` says so; run `flashpoint report` after it finishes. A failed Replay build prints its error. Neither changes the exit code.

## `flashpoint rebuild`

Reprocess, from its raw copy, every file that an older version of Flashpoint processed, and every file that `failed` (a machine error such as no internet for the owlet download). `restore` marks every file this way. Use it when `doctor` says "N file(s) from an older pipeline version", and after a `restore`. When it reprocessed anything, `derive --all` runs afterward, then the Replay build. With nothing to do, it prints `0 ingested` and stops. You never need the original logs.

<!-- generated:cli:rebuild -->
| Option | What it does | Default |
|---|---|---|
| `--profile {health,all}` | hoot signals: health (what Flashpoint uses; default) or all (about 3x larger) | `health` |
| `--quarantined` | reprocess only quarantined files, from their raw copies, with a fresh 3 tries | |
| `--no-report` | skip the automatic Replay build after derive | |
| `--no-robot-draft` | don't draft a robot file for a season that has none (see Setup > Draft robot) | |
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:rebuild -->

A file whose raw copy is missing is listed as `MISSING RAW` and left as it is.

```sh
flashpoint rebuild
flashpoint rebuild --quarantined
```

## `flashpoint derive`

Build the match-level data from what `ingest` read. That means grouping each wpilog with its hoots, aligning their clocks, mapping devices to robot slots and physical units, framing auto and teleop, and computing per-motor match features. Only sessions whose inputs changed are rebuilt. Detection runs at the end.

**The first derive drafts your robot.** When the lake holds logs from a season that has no robot file in your config folder, `derive` first drafts one per robot-code project, from the match with the most CAN devices, and saves it as `robots/<season>-comp.toml` (a second project gets `-practice`). It prints `drafted robot 2026-comp from 2026gacmp_qm7 (22 devices) into ...`, and the file's first line says it was drafted. Name each slot's subsystem and role in Setup > Robots, and keep the drafted slot ids. A season that already has any robot file is never touched. Hoot-only logs have no robot-code project, so they draft nothing: use Setup > Draft robot for them. With no configuration (`flashpoint init`) nothing is drafted, and a read-only config folder is named in a warning. Since `ingest`, `rebuild` and the acquire cycle run `derive`, they draft too.

<!-- generated:cli:derive -->
| Option | What it does | Default |
|---|---|---|
| `--all` | rebuild every session, not only changed ones (after editing a robot file) | |
| `--no-robot-draft` | don't draft a robot file for a season that has none (see Setup > Draft robot) | |
| `--report` | then build Replay data for the matches that changed, as ingest does | |
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:derive -->

```sh
flashpoint derive --all
```

## `flashpoint detect`

Recompute anomalies. `derive` already does this, so you only need it after editing `anomaly.toml` in your config folder ([the default](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/defaults/anomaly.toml)); no re-derive is needed. It skips the work when nothing changed.

<!-- generated:cli:detect -->
| Option | What it does | Default |
|---|---|---|
| `--all` | run even when nothing changed | |
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:detect -->

```sh
flashpoint detect
```

## `flashpoint backtest`

Score anomaly detection against the known failures in `failure-labels.toml` in your config folder (Ignite's is [config/failure-labels.toml](https://github.com/IgniteRobotics/flashpoint/blob/main/config/failure-labels.toml)): which ones were caught, how many matches early, and the false-positive rate. Writes a Markdown report and its data to `<lake>/report/backtest/`.

<!-- generated:cli:backtest -->
| Option | What it does | Default |
|---|---|---|
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:backtest -->

```sh
flashpoint backtest
```

## `flashpoint report`

Build the Replay data for each match, about 1 MB per match. Matches whose inputs haven't changed are skipped. `serve` runs this for you when it starts, and `acquire` runs it after each cycle that derives something new, so you only need it on its own for a static export.

Only one build runs on a lake at a time. While another build holds `<lake>/meta/report.lock`, `report` exits with code `3` and writes nothing; run it again when that build ends. `serve` started during a build skips its own build and serves the existing data.

<!-- generated:cli:report -->
| Option | What it does | Default |
|---|---|---|
| `--event EVENT` | only this event, e.g. 2026gacmp (repeatable) | |
| `--match KEY` | only this match key, e.g. 2026gacmp_qm7 (repeatable) | |
| `--all` | rebuild even unchanged matches | |
| `--static OUT` | also write a Replay folder that opens from index.html with no server | |
| `--no-raw` | with --static: leave the raw logs out (much smaller) | |
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:report -->

```sh
flashpoint report --static /Volumes/USB/flashpoint
```

## `flashpoint serve`

Run the Flashpoint app in your browser: Replay, History, Anomalies, Setup and Live CAN. Builds any missing Replay data first.

<!-- generated:cli:serve -->
| Option | What it does | Default |
|---|---|---|
| `--host HOST` | bind address (default 127.0.0.1); 0.0.0.0 shares the app, read-only, on the pit network | `127.0.0.1` |
| `--port PORT` | port (default 8000; 0 = any free port) | `8000` |
| `--robot-host ADDRESS` | robot address for the Live CAN view (repeatable; default: the acquire hosts) | |
| `--no-build` | start without building Replay data first | |
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:serve -->

`--host 0.0.0.0` shares the app on the pit network. There's no password, so use it only on a network you trust. Nothing can be changed from the app while it runs this way, not even on this laptop: no triage, no Setup saves or fixes, no Appearance changes, no AdvantageScope launch. The Live CAN view connects to the robot only while it's open ([Live CAN health](../how-to/live-can-health.md)).

```sh
flashpoint serve
# then open http://127.0.0.1:8000/
```

Stop it with Ctrl-C.

## `flashpoint acquire`

Pull new logs from the roboRIO (over SFTP) and from USB drives plugged into this machine, verify each copy, then ingest and derive them. It never deletes or changes anything on the robot or the drive. On the robot, each file's checksum runs at the lowest CPU priority (`nice -n 19`) so robot code keeps the processor. Settings come from `acquire.toml`; see [Automatic import](../how-to/auto-import.md).

Pulling from the robot and running acquire as a service (`--watch`) are **beta, tested on one robot**; `--help` says so. Importing from USB drives isn't labelled.

<!-- generated:cli:acquire -->
| Option | What it does | Default |
|---|---|---|
| `--watch` | repeat every poll_s until stopped; this is also how it runs as a service (beta, tested on one robot) | |
| `--host H [H ...]` | robot addresses to try instead of the configured ones | |
| `--include-active` | also pull logs still being written (tagged incomplete) | |
| `--dry-run` | list what would be copied and change nothing (not with --watch) | |
| `--no-usb` | don't scan removable drives | |
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:acquire -->

```sh
flashpoint acquire --dry-run
flashpoint acquire --watch
```

## `flashpoint backup`

Back up the raw logs, the metadata and your configuration files to the rclone remote set as `backup.remote` in `acquire.toml`, right now. A running `acquire --watch` already does this on its own schedule, and this command exits with code `3` while one is running. See [Automatic import](../how-to/auto-import.md#backup-and-restore).

<!-- generated:cli:backup -->
| Option | What it does | Default |
|---|---|---|
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:backup -->

```sh
flashpoint backup
```

## `flashpoint restore`

Copy a backup into a new, empty lake. Then run `rebuild` on that lake to regenerate everything else. The backed-up configuration goes into the config folder only if it has no configuration files; otherwise it goes to `restored-config/` in the lake, and the config folder is left as it is. See [Restore drill](../how-to/auto-import.md#restore-drill).

<!-- generated:cli:restore -->
| Option | What it does | Default |
|---|---|---|
| `--force` | restore into a lake that already has data (avoid; use a new lake) | |
| `--remote REMOTE` | rclone remote:path to restore from (default: backup.remote) | |
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:restore -->

Restore into a new lake rather than using `--force`; a restore never overwrites configuration.

```sh
flashpoint restore --lake ~/flashpoint-lake-restored
flashpoint rebuild --lake ~/flashpoint-lake-restored
```

## `flashpoint doctor`

Check the setup. It shows:
- the Flashpoint and Python versions;
- the config folder, and which settings files are the bundled defaults;
- where the lake is, and how many files it holds at each stage;
- files that need a `rebuild`;
- the seasons and robots configured;
- the last acquire and backup;
- which AdvantageScope install was found.

Then it runs the setup checks, one line each, marked `ok`, `WARN`, `PROBLEM` or `n/a`, with the fix under each warning or problem:

| Check | A problem when |
|---|---|
| Configuration | the config folder doesn't exist (fix: `flashpoint init`) |
| Each settings file (`acquire.toml`, `report.toml`, `anomaly.toml`, `anomaly-causes.toml`, `theme.toml`, seasons, robots, `failure-labels.toml`) | it doesn't load; the message names the file and the bad key |
| Team | there's no team number and no `hosts` in `acquire.toml`, so acquire can't find the robot |
| Robot for this season | no robot file is for the current season |
| Lake | the lake folder can't be written (not created yet is only a warning) |
| Backup | logs or configuration have waited longer than `backup.overdue_h` (default 24 hours) for a backup (a failed last attempt, or no backup configured, is a warning). See [When a backup is overdue](../how-to/auto-import.md#when-a-backup-is-overdue) |
| owlet, per hoot format in the lake | the matching owlet isn't downloaded, or its checksum doesn't match |
| AdvantageScope | the configured path is wrong (not found at all is a warning) |
| Acquire service | never a problem: a warning when it isn't installed or running (Linux and Windows), `n/a` on macOS |
| Python | the version isn't 3.11 to 3.14 |

It changes nothing and downloads nothing. It exits `1` when any check is a problem, else `0`. Run it first whenever something looks wrong.

<!-- generated:cli:doctor -->
| Option | What it does | Default |
|---|---|---|
| `--lake LAKE` | lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake) | |
<!-- /generated:cli:doctor -->

```sh
flashpoint doctor
```
