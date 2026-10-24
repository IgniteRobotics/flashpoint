# Automatic import

`flashpoint acquire --watch` pulls new logs into Flashpoint without anyone copying files. Leave it running on the pit laptop, or on a team computer the robot connects to:
- When the robot is powered and on the same network, it pulls the new logs.
- When someone plugs a USB stick into the laptop, it copies the logs off that too.

Each new log is ingested, derived, checked for anomalies, and built into Replay within a cycle or two.

This page assumes Flashpoint is installed and `flashpoint doctor` works ([User guide: Install](user-guide.md#install)).

## What it does

Every cycle (default every 30 seconds) the watch does the following, in order:

1. **Robot:** it tries each configured robot address until one answers, then lists the `.wpilog` and `.hoot` files in the log folders on the roboRIO (internal storage and its USB stick). New files are copied over SFTP into the lake's `inbox/`. Each copy is checked against the size and SHA-256 hash of the file on the robot.
2. **USB drives:** it looks for removable drives plugged into this computer, finds `.wpilog` and `.hoot` files up to four folders deep, and copies the new ones with the same checks. When a drive is finished, the log says `volume <name> (<path>) is safe to remove`.
3. **Ingest:** everything in `inbox/` is ingested, then derived, then run through detection. This includes files you drop in `inbox/` by hand.
4. **Replay:** after a successful derive, it builds the Replay data for the new or changed matches only (the same build as `flashpoint report`). A running `flashpoint serve` lists a new match in Replay after you reload the page; you don't need to restart it. A build that fails, or that finds another build already running, is tried again next cycle, also after the watch restarts.
5. **Backup:** if a backup is configured and due, it runs.

**It never changes anything on the robot or the stick.** It doesn't delete, move, or rename anything there, and it has no option to.

A file is pulled once. The pull history lives in the lake, so restarting the watch, or plugging the same stick in again, copies only what's new.

Files still being written are skipped. On the robot, the newest `.wpilog` in each folder and the newest hoot session are still open while the robot code runs. The watch leaves them and pulls them on a later cycle, after the robot code restarts or the robot is power-cycled. Files that change while being listed are also left for the next cycle. To take open files anyway, run `flashpoint acquire --include-active` once. Those copies are tagged as incomplete reads.

WPILib and Phoenix start deleting old logs when the robot's disk gets close to full. When a log folder on the robot has less than `low_space_mb` free (default 100 MB), the watch logs a warning, and `doctor` shows it. Pull the logs and clear space on the robot by hand before it starts deleting them.

Importing from cloud folders (Google Drive, Dropbox, network shares) isn't supported. Download those files and use `flashpoint ingest`, or drop them in `<lake>/inbox/`.

## Configure it: `acquire.toml`

`flashpoint init` writes `acquire.toml` in the config folder (`~/.config/flashpoint/`, `%APPDATA%\flashpoint` on Windows, or the `FLASHPOINT_CONFIG` folder) with your team number and lake. The robot addresses come from the team number. Without a team number or `hosts`, acquire refuses to start and names `flashpoint init`. Unknown keys are an error, so typos are caught. Every key:

```toml
team = 6829
lake = "/home/pit/flashpoint-lake"
hosts = []
user = "lvuser"
password = ""
roots = ["/home/lvuser/logs", "/u/logs"]
poll_s = 30
settle_s = 5
low_space_mb = 100
removable_media = true

[backup]
remote = "gdrive:flashpoint"
interval_min = 15
```

| Key | Default | What it does |
|---|---|---|
| `team` | none | Your team number. Acquire tries `10.TE.AM.2` (team 6829: `10.68.29.2`, team 254: `10.2.54.2`), then `roborio-TEAM-frc.local`, then the USB tether `172.22.11.2` |
| `lake` | `~/flashpoint-lake` | The lake folder for every command. `--lake` and `FLASHPOINT_LAKE` override it |
| `hosts` | `[]` (use the team's addresses) | Robot addresses to try instead, in order |
| `user` | `lvuser` | SSH user on the roboRIO |
| `password` | `""` (none) | SSH password, if the rio has one |
| `roots` | `/home/lvuser/logs`, `/u/logs` | Log folders on the robot: internal storage, and the robot's USB stick |
| `poll_s` | `30` | Seconds between cycles in `--watch` |
| `settle_s` | `5` | Seconds to wait and re-check a file's size before copying it, so half-written files are skipped |
| `low_space_mb` | `100` | Warn when a robot log folder has less than this much free space |
| `removable_media` | `true` | Scan USB drives plugged into this computer. `--no-usb` turns it off for one run |
| `backup.remote` | none (no backup) | rclone remote and path to back up to, e.g. `gdrive:flashpoint` |
| `backup.interval_min` | `15` | Minutes between backups in `--watch` |

To try another robot address for one run, use `--host ADDR`; it replaces `hosts`.

The first time the watch connects to a robot, it records the robot's SSH key in `~/.cache/flashpoint/known-robots.json`. If the key changes later (the rio was reimaged), it logs a warning and accepts the new one.

## Try it once first

With the robot on and connected:

```sh
flashpoint acquire --dry-run
```

It lists what would be copied and changes nothing:

```
robot 10.68.29.2: ok, 2 to copy
  10.68.29.2  /home/lvuser/logs/FRC_20260409_215113_GACMP_Q7.wpilog  20563712 bytes
  ...
dry run: 2 file(s), 41127424 bytes would be copied; nothing changed
```

`robot: unreachable` means no address answered (see [Troubleshooting](#troubleshooting)). Then run one real cycle with `flashpoint acquire`, and check that `flashpoint doctor` shows it.

## Run it in the background

### Linux (systemd user service)

Flashpoint ships a unit file, [`src/flashpoint/deploy/systemd/flashpoint-acquire.service`](../../src/flashpoint/deploy/systemd/flashpoint-acquire.service). It expects `flashpoint` at `~/.local/bin/flashpoint`, which is where pipx puts it. Install it:

```sh
mkdir -p ~/.config/systemd/user
cp src/flashpoint/deploy/systemd/flashpoint-acquire.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now flashpoint-acquire
journalctl --user -u flashpoint-acquire -f      # watch its log
```

The service reads environment variables from `~/.config/flashpoint/env`, one `KEY=value` per line. Put `FLASHPOINT_CONFIG` (and `FLASHPOINT_LAKE`, if you moved the lake) there; your shell profile isn't read by services.

```sh
FLASHPOINT_CONFIG=/home/pit/flashpoint/config
```

On a computer with no one logged in, keep the service running after logout:

```sh
loginctl enable-linger "$USER"
```

For USB drives on a computer with no desktop, the watch only sees drives that are mounted. Install `udisks2` and `udiskie`, and run udiskie as a user service (`systemctl --user enable --now udiskie`), so drives mount when plugged in.

### Windows (scheduled task)

Flashpoint ships a task definition, [`src/flashpoint/deploy/windows/flashpoint-acquire.xml`](../../src/flashpoint/deploy/windows/flashpoint-acquire.xml). It runs `%USERPROFILE%\.local\bin\flashpoint.exe acquire --watch` at logon, restarts it up to 3 times if it fails, and never runs two copies at once. Install it from the checkout folder:

```bat
schtasks /Create /XML src\flashpoint\deploy\windows\flashpoint-acquire.xml /TN flashpoint-acquire
schtasks /Run /TN flashpoint-acquire
```

The task uses your user environment, so the `setx FLASHPOINT_CONFIG ...` from install applies. To remove it: `schtasks /Delete /TN flashpoint-acquire /F`.

Ending the task (`schtasks /End`, or End in Task Scheduler) kills it immediately. A copy cut off mid-file leaves a `.part` file in the inbox, which is never ingested and is cleaned up on the next start.

### macOS

There's no service definition for macOS yet. Run the watch in a terminal window and leave it open:

```sh
flashpoint acquire --watch
```

To keep it running after you close the terminal, use `tmux` (`brew install tmux`): run `tmux`, start the watch, then press Ctrl-B then D to detach. Run `tmux attach` to get back to it. Stop it with Ctrl-C either way.

## Backup and restore

The watch can copy the lake's raw logs, its metadata and your configuration to any [rclone](https://rclone.org/) remote, such as Google Drive. Install rclone, then create a remote once:

```sh
rclone config            # make a remote, for example "gdrive"
```

Set `backup.remote = "gdrive:flashpoint"` in `acquire.toml`. Flashpoint never reads your rclone credentials.

- The watch backs up every `interval_min` minutes. `flashpoint backup` runs one now; it exits with code 3 while a watch is running, because the watch does it itself.
- Raw logs are copied once and never overwritten. Metadata is a consistent snapshot in `meta/latest` on the remote.
- Each metadata backup moves the files it replaces into `meta/history/<UTC time>/` on the remote, so a backup from a wrong or empty lake can't destroy the last good one. Nothing deletes old history folders; remove them by hand when you want the space.
- A large first backup goes up over several cycles; `doctor` shows the backup as still owed until it's done. Within the watch, each backup gives up on a dead connection within about a minute and stops after 4 minutes.
- Your configuration goes to `config/latest` on the remote: the TOML files at the top of the config folder and in `seasons/` and `robots/`, and the CSV files in `units/`. Nothing else in that folder is copied: not the service's `env` file, not the Setup view's `.backups/`, not notes you left there. Files you never changed from the defaults aren't in the folder, so they aren't copied either; they come back with Flashpoint.
- Saving a robot or a threshold makes a backup due even on a day with no new matches, so the watch copies it within `interval_min` minutes.
- Each configuration backup moves the files it replaces or deletes into `config/history/<UTC time>/`. To get an older robot file back, copy it from there with rclone, for example `rclone copy gdrive:flashpoint/config/history/20261023T180000Z/robots/2026-comp.toml ~/.config/flashpoint/robots/`.

Bronze, silver, and gold aren't backed up, because they can be rebuilt from raw.

### Restore drill

Practice this before you need it. Always restore into a new lake folder and an empty config folder, never over the ones you're using. Pointing `FLASHPOINT_CONFIG` at a new folder makes the drill act like a new laptop:

```sh
export FLASHPOINT_CONFIG=~/restore-drill-config
flashpoint restore --remote gdrive:flashpoint --lake ~/restore-drill
flashpoint rebuild --lake ~/restore-drill
flashpoint doctor  --lake ~/restore-drill
unset FLASHPOINT_CONFIG
```

On Windows, use `set FLASHPOINT_CONFIG=%USERPROFILE%\restore-drill-config` in that terminal instead of `export`.

- `restore` copies the raw logs and metadata, including anomaly triage. It refuses a lake that already has data unless you pass `--force`.
- It copies your configuration into the config folder **only if that folder has no configuration files yet**. If it has any, nothing in it is changed: the backed-up copy goes to `restored-config/` inside the restored lake, and `restore` says so. Compare and copy what you need by hand. `--force` never changes this.
- `--remote name:path` restores from somewhere other than `backup.remote`. The drill needs it, because the new config folder has no `acquire.toml` yet.
- A backup made before configuration was included still restores; `restore` says it held no configuration.
- To restore an older metadata snapshot, first copy that `meta/history/<time>/` folder over `meta/latest` on the remote with rclone.

## Check the status

```sh
flashpoint doctor
```

The `acquire` section shows:
- the last cycle;
- each source with its result (`ok`, `unreachable`, `error`) and what it copied;
- low-space warnings;
- files that failed to copy, with the reason;
- whether derive is pending or failed;
- whether a Replay build is owed (`Replay build pending`) or failed (`Replay build error: ...`, with the end of its output);
- the last backup.

On Linux, `journalctl --user -u flashpoint-acquire` has the full log. Each cycle ends with a line like:

```
cycle done: robot 10.68.29.2 ok, volume ROBOT-USB ok; transfers {'verified': 3}; inbox {'success': 3}
```

`verified` copies passed the hash check, and `size-verified` copies were checked by size only. `retry` copies failed and will be tried again next cycle.

## Troubleshooting

| You see | What it means | What to do |
|---|---|---|
| `robot: unreachable` in the dry run, or `unreachable` in `doctor` | None of `hosts` answered. That's normal when the robot is off, and the watch keeps trying | Check that the robot is on and this computer is on its network (radio or USB tether). Try `flashpoint acquire --dry-run --host 10.68.29.2`. Check `team` in `acquire.toml` |
| `robot <host>: cannot open a session: ...` | The robot answered but SSH login failed | Check `user` and `password` in `acquire.toml` |
| A USB stick is plugged in but nothing is copied | The drive isn't seen as removable, isn't mounted, or has no logs within four folders of its top | Make sure the drive shows up in Finder, File Explorer, or `lsblk`. On a Linux computer with no desktop, set up udiskie. Check that `removable_media` isn't `false` |
| `removable volume detection failed, skipping USB this cycle` | The operating system's drive listing failed | It retries next cycle. If it repeats, run the watch in a terminal to see the full error |
| The newest log on the robot isn't pulled | It's still open, and the watch skips open files | Restart the robot code or power-cycle the robot, or run `flashpoint acquire --include-active` once |
| `robot <host>: low space on <root>: N MB free` | The robot is close to deleting old logs on its own | Pull the logs, then free space on the rio |
| `robot <host> has no sha256sum; files are accepted on size` | The rio image is missing a tool, so copies are checked by size only | Safe to ignore; size checks still catch truncated copies |
| `another acquire is running on this lake (pid ... on ... since ...)` | Another `acquire`, or a `backup`, is using this lake | Stop the other one, or let it finish. If that process has already exited, just run again: the lock is released automatically, so there is never a stale lock to delete |
| A `.part` file in `inbox/` | A copy was cut off: the watch was killed, or a stick was pulled mid-copy | Nothing. `.part` files are never ingested and are removed on the next start |
| `ingest exited with N and changed nothing; inbox kept` | Ingest failed on something in the inbox | Run `flashpoint ingest <lake>/inbox` by hand to see the error |
| `Replay build error: ...` in `doctor` | The Replay build after derive failed; the watch tries again every cycle | Run `flashpoint report` by hand to see the full error. Usually a configuration error in `config/robots/` or `report.toml` |
| `a Replay build is already running on this lake` from `flashpoint report` | The watch (or another `report`) is building Replay data right now | Run it again once that build ends; there is never a stale lock to delete |
| `failed ...` lines in `doctor` | A file failed its copy check three times, and the watch has stopped retrying it | Copy it by hand and run `flashpoint ingest` on it; if that fails too, the file on the robot is damaged |
