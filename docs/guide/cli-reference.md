# CLI reference

Every Flashpoint command is a subcommand of `flashpoint`. This page covers what each one is for, the options you'll use, and an example. For the full option list, run `flashpoint <command> --help`; it is always current.

In a source checkout, put `poetry run` in front of each command (`poetry run flashpoint doctor`).

## Settings every command shares

| Setting | Default | Change it with |
|---|---|---|
| Lake (where logs and results are stored) | `~/flashpoint-lake` | `--lake PATH` on any command, or `FLASHPOINT_LAKE` |
| Config folder (robots, seasons, thresholds) | the checkout's `config/`, else `~/.config/flashpoint/` | `FLASHPOINT_CONFIG` |
| Download cache (owlet) | `~/.cache/flashpoint/` | `FLASHPOINT_CACHE` |

Exit codes: `0` success, `1` failed (or a file was quarantined), `2` usage or configuration error, `3` another Flashpoint process holds the lock, `130` interrupted with Ctrl-C.

## `flashpoint ingest`

Copy `.wpilog` and `.hoot` files into the lake and read them. You can pass files, folders (searched recursively), or both. Files already in the lake are skipped, so re-running is safe. When at least one file was ingested, `derive` (and so `detect`) runs afterward on its own.

| Option | What it does |
|---|---|
| `--profile all` | Export every hoot signal instead of the health set (about 3× larger). Default `health` |
| `--jobs N` | Hoot files converted in parallel. Default: half your CPU cores |
| `--no-derive` | Skip the automatic `derive` |

```sh
flashpoint ingest ~/Downloads/GACMP/
```

The last line counts files ingested, skipped, and quarantined. A quarantined file is one Flashpoint couldn't read (corrupt or truncated); it is listed with the reason, and the exit code is `1`.

## `flashpoint rebuild`

Reprocess, from its raw copy, every file that an older version of Flashpoint processed. `restore` marks every file this way. Use it when `doctor` says "N file(s) from an older pipeline version", and after a `restore`. When it reprocessed anything, `derive --all` runs afterward. With nothing to do, it prints `0 ingested` and stops.

```sh
flashpoint rebuild
```

## `flashpoint derive`

Build the match-level data from what `ingest` read. That means grouping each wpilog with its hoots, aligning their clocks, mapping devices to robot slots and physical units, framing auto and teleop, and computing per-motor match features. Only sessions whose inputs changed are rebuilt. Detection runs at the end.

| Option | What it does |
|---|---|
| `--all` | Rebuild every session. Use it after editing a robot config in `config/robots/` |

```sh
flashpoint derive --all
```

## `flashpoint detect`

Recompute anomalies. `derive` already does this, so you only need it after editing [`config/anomaly.toml`](../../config/anomaly.toml); no re-derive is needed. It skips the work when nothing changed.

| Option | What it does |
|---|---|
| `--all` | Recompute even when nothing changed |

```sh
flashpoint detect
```

## `flashpoint backtest`

Score anomaly detection against the known failures in [`config/failure-labels.toml`](../../config/failure-labels.toml): which ones were caught, how many matches early, and the false-positive rate. Writes a Markdown report and its data to `<lake>/report/backtest/`.

```sh
flashpoint backtest
```

## `flashpoint report`

Build the Replay data for each match, about 1 MB per match. Matches whose inputs haven't changed are skipped. `serve` runs this for you when it starts, and `acquire` runs it after each cycle that derives something new, so you only need it on its own for a static export.

Only one build runs on a lake at a time. While another build holds `<lake>/meta/report.lock`, `report` exits with code `3` and writes nothing; run it again when that build ends. `serve` started during a build skips its own build and serves the existing data.

| Option | What it does |
|---|---|
| `--event KEY` | Only this event, e.g. `2026gacmp` |
| `--match KEY` | Only this match, e.g. `2026gacmp_qm7` |
| `--all` | Rebuild matches even when unchanged |
| `--static DIR` | Also write a Replay folder that opens from `index.html` with no server, for a USB stick |
| `--no-raw` | With `--static`: leave the raw logs out (much smaller) |

```sh
flashpoint report --static /Volumes/USB/flashpoint
```

## `flashpoint serve`

Run the Flashpoint app in your browser: Replay, History, and Anomalies. Builds any missing Replay data first.

| Option | What it does |
|---|---|
| `--host 0.0.0.0` | Share on the pit network. Read-only and no password, so only on a network you trust. Default `127.0.0.1` (this machine only) |
| `--port N` | Default `8000` |
| `--no-build` | Start without building Replay data first |

```sh
flashpoint serve
# then open http://127.0.0.1:8000/
```

Stop it with Ctrl-C.

## `flashpoint acquire`

Pull new logs from the roboRIO (over SFTP) and from USB drives plugged into this machine, verify each copy, then ingest and derive them. It never deletes or changes anything on the robot or the drive. Settings come from `acquire.toml`; see [Automatic import](auto-import.md).

| Option | What it does |
|---|---|
| `--dry-run` | List what would be copied and change nothing. Can't be combined with `--watch` |
| `--watch` | Keep going, one cycle every `poll_s` seconds, until Ctrl-C |
| `--host ADDR [ADDR ...]` | Try these robot addresses instead of the configured ones |
| `--no-usb` | Don't scan removable drives |
| `--include-active` | Also pull logs still being written (tagged as incomplete) |

```sh
flashpoint acquire --dry-run
flashpoint acquire --watch
```

## `flashpoint backup`

Back up the raw logs and metadata to the rclone remote set as `backup.remote` in `acquire.toml`, right now. A running `acquire --watch` already does this on its own schedule, and this command exits with code `3` while one is running. See [Automatic import](auto-import.md#backup-and-restore).

```sh
flashpoint backup
```

## `flashpoint restore`

Copy a backup into a new, empty lake. Then run `rebuild` on that lake to regenerate everything else.

| Option | What it does |
|---|---|
| `--remote NAME:PATH` | Restore from here instead of `backup.remote` |
| `--force` | Restore into a lake that already has data. Avoid this; restore into a new lake |

```sh
flashpoint restore --lake ~/flashpoint-lake-restored
flashpoint rebuild --lake ~/flashpoint-lake-restored
```

## `flashpoint doctor`

Check the setup. It reports:
- the Flashpoint and Python versions;
- where the lake is, and how many files it holds at each stage;
- files that need a `rebuild`;
- the seasons and robots configured, and whether those files are valid;
- which owlet versions are cached;
- the last acquire and backup;
- which AdvantageScope install was found.

It changes nothing. Run it first whenever something looks wrong.

```sh
flashpoint doctor
```
