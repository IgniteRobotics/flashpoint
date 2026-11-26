# Import logs from the robot automatically

**Beta: beta, tested on one robot.** Pulling logs from the robot, and running the watch as a service, have been checked on one team's roboRIO so far. Importing from USB sticks isn't affected. If a pull misbehaves on your robot, please [open an issue](https://github.com/IgniteRobotics/flashpoint/issues).

`flashpoint acquire --watch` pulls new logs into Flashpoint without anyone copying files. Leave it running on the pit laptop, or on a team computer the robot connects to:

- When the robot is powered and on the same network, it pulls the new logs.
- When someone plugs a USB stick into the laptop, it copies the logs off that too.

Each new log is ingested, derived, checked for anomalies, and built into Replay within a cycle or two.

## Before you start

- Flashpoint installed, and a configuration with your team number:

    ```sh
    uv tool install --python 3.13 flashpoint
    flashpoint init --team <your team number>
    ```

    The [Install](../install/index.md) page has the steps for each computer. The robot addresses Flashpoint tries come from the team number.
- The robot on, and this computer on its network: the radio, Ethernet, or the USB tether.
- Robot code that writes its logs to `/home/lvuser/logs` or the robot's USB stick ([Signal logging](../robot-side/signal-logging.md)).
- To run it as a background service: Linux or Windows. On macOS, you leave it running in a terminal.

## Steps

1. Check what it would copy, with the robot on. This changes nothing:

    ```sh
    flashpoint acquire --dry-run
    ```

    ```text
    robot 10.68.29.2: ok, 2 to copy
      10.68.29.2  /home/lvuser/logs/FRC_20260409_215113_GACMP_Q7.wpilog  20563712 bytes
      ...
    dry run: 2 file(s), 41127424 bytes would be copied; nothing changed
    ```

    `robot: unreachable` means no address answered (see [Troubleshooting](#troubleshooting)). To try another address for one run, add `--host <address>`.

2. Run one real cycle. It copies the new logs, checks each copy, and ingests them:

    ```sh
    flashpoint acquire
    ```

3. Leave it running:

    ```sh
    flashpoint acquire --watch
    ```

    Stop it with Ctrl-C. To have it start on its own, [run it in the background](#run-it-in-the-background).

4. Set up a backup, so the logs it pulls don't exist only on this laptop ([Back up the lake](back-up-the-lake.md)). The watch then backs up on its own.

## Check it worked

Run `flashpoint doctor`. Its `acquire` section shows the last cycle and each source with its result (`ok`, `unreachable` or `error`) and what it copied. The pulled matches are in Replay after you reload the page. [Check the status](#check-the-status) lists everything the section can show.

Each cycle of the watch ends with a log line like this:

```text
cycle done: robot 10.68.29.2 ok, volume ROBOT-USB ok; transfers 3 verified; inbox 3 success
```

If a source is `unreachable` or a file failed to copy, see the [table below](#troubleshooting). For files that fail to import, see [Troubleshooting](../troubleshooting.md#a-file-was-quarantined).

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

WPILib and Phoenix start deleting old logs when the robot's disk gets close to full. When a log folder on the robot has less than `low_space_mb` free (default 100 MB), the watch logs a warning, and `doctor` shows it. Pull the logs and clear space on the robot by hand before it starts deleting them ([Disk space](../robot-side/disk-space.md)).

Importing from cloud folders (Google Drive, Dropbox, network shares) isn't supported. Download those files and use `flashpoint ingest` ([Import logs from a USB stick](import-from-usb.md)), or drop them in `<lake>/inbox/`.

## Configure it: `acquire.toml`

`flashpoint init` writes `acquire.toml` in the config folder (`~/.config/flashpoint/`, `%APPDATA%\flashpoint` on Windows, or the `FLASHPOINT_CONFIG` folder) with your team number and lake. Without a team number or `hosts`, acquire refuses to start and names `flashpoint init`. Unknown keys are an error, so typos are caught. Every key, its default and what it does is in the [`acquire.toml` reference](../reference/config/acquire.md).

The watch reads `acquire.toml` once, when it starts. After you change it, here or in the Setup view's **Team and connection** section, restart the watch: `systemctl --user restart flashpoint-acquire` on Linux, `schtasks /End` then `schtasks /Run /TN flashpoint-acquire` on Windows, or Ctrl-C and start it again.

The first time the watch connects to a robot, it records the robot's SSH key in `~/.cache/flashpoint/known-robots.json`. If the key changes later (the rio was reimaged), it logs a warning and accepts the new one.

## Run it in the background

**Beta: beta, tested on one robot.** Running acquire as a service has been checked on one team's setup so far.

The easiest way is the Setup view: its acquire service check has an **Install** button on Linux and Windows. To do it by hand, clone the repository to get the service files, then run the commands below from that folder:

```sh
git clone https://github.com/IgniteRobotics/flashpoint.git
cd flashpoint
```

### Linux (systemd user service)

Flashpoint ships a unit file, [`src/flashpoint/deploy/systemd/flashpoint-acquire.service`](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/deploy/systemd/flashpoint-acquire.service). It expects `flashpoint` at `~/.local/bin/flashpoint`, which is where `uv tool install` and pipx put it.

1. Install and start it:

    ```sh
    mkdir -p ~/.config/systemd/user
    cp src/flashpoint/deploy/systemd/flashpoint-acquire.service ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable --now flashpoint-acquire
    journalctl --user -u flashpoint-acquire -f      # watch its log
    ```

2. If you set `FLASHPOINT_CONFIG` or `FLASHPOINT_LAKE` in your shell profile, put them in `~/.config/flashpoint/env` too, one `KEY=value` per line. Services don't read your shell profile:

    ```text
    FLASHPOINT_CONFIG=/home/pit/flashpoint/config
    ```

3. On a computer with no one logged in, keep the service running after logout:

    ```sh
    loginctl enable-linger "$USER"
    ```

4. On a computer with no desktop, the watch only sees USB drives that are mounted. Install `udisks2` and `udiskie`, and run udiskie as a user service (`systemctl --user enable --now udiskie`), so drives mount when plugged in.

### Windows (scheduled task)

Flashpoint ships a task definition, [`src/flashpoint/deploy/windows/flashpoint-acquire.xml`](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/deploy/windows/flashpoint-acquire.xml). It runs `%USERPROFILE%\.local\bin\flashpoint.exe acquire --watch` at logon, restarts it up to 3 times if it fails, and never runs two copies at once. From the cloned folder:

```powershell
schtasks /Create /XML src\flashpoint\deploy\windows\flashpoint-acquire.xml /TN flashpoint-acquire
schtasks /Run /TN flashpoint-acquire
```

The task uses your user environment, so it sees `FLASHPOINT_CONFIG` if you set it with `setx` (only needed for a checkout install; `flashpoint init` doesn't need it). To remove it: `schtasks /Delete /TN flashpoint-acquire /F`.

Ending the task (`schtasks /End`, or End in Task Scheduler) kills it immediately. A copy cut off mid-file leaves a `.part` file in the inbox, which is never ingested and is cleaned up on the next start.

### macOS

There's no service definition for macOS yet. Run the watch in a terminal window and leave it open:

```sh
flashpoint acquire --watch
```

To keep it running after you close the terminal, use `tmux` (`brew install tmux`): run `tmux`, start the watch, then press Ctrl-B then D to detach. Run `tmux attach` to get back to it. Stop it with Ctrl-C either way.

## Backup and restore

The watch also backs up the lake's raw logs, its metadata and your configuration to an [rclone](https://rclone.org/) remote, such as Google Drive, every `interval_min` minutes. Set `backup.remote` in `acquire.toml` to turn it on. [Back up the lake](back-up-the-lake.md) covers setting it up, what a backup holds, [when a backup is overdue](back-up-the-lake.md#when-a-backup-is-overdue), and the [restore drill](back-up-the-lake.md#restore-drill).

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

On Linux, `journalctl --user -u flashpoint-acquire` has the full log. `verified` copies in the `cycle done` line passed the hash check, and `size-verified` copies were checked by size only. `retry` copies failed and will be tried again next cycle.

## Troubleshooting

These are the problems specific to pulling logs. For problems importing the files once they're copied, see [Troubleshooting](../troubleshooting.md).

| You see | What it means | What to do |
|---|---|---|
| `robot: unreachable` in the dry run, or `unreachable` in `doctor` | None of `hosts` answered. That's normal when the robot is off, and the watch keeps trying | Check that the robot is on and this computer is on its network (radio or USB tether). Try `flashpoint acquire --dry-run --host 10.68.29.2`. Check `team` in `acquire.toml` |
| `robot <host>: cannot open a session: ...` | The robot answered but SSH login failed | Check `user` and `password` in `acquire.toml` |
| A USB stick is plugged in but nothing is copied | The drive isn't seen as removable, isn't mounted, or has no logs within four folders of its top | Make sure the drive shows up in Finder, File Explorer, or `lsblk`. On a Linux computer with no desktop, set up udiskie. Check that `removable_media` isn't `false` |
| `removable volume detection failed`, followed by the error | The operating system's drive listing failed | It retries next cycle. If it repeats, run the watch in a terminal to see the full error |
| The newest log on the robot isn't pulled | It's still open, and the watch skips open files | Restart the robot code or power-cycle the robot, or run `flashpoint acquire --include-active` once |
| `robot <host>: low space on <root>: N MB free` | The robot is close to deleting old logs on its own | Pull the logs, then free space on the rio |
| `robot <host> has no sha256sum; files are accepted on size` | The rio image is missing a tool, so copies are checked by size only | Safe to ignore; size checks still catch truncated copies |
| `another acquire is running on this lake (pid ... on ... since ...)` | Another `acquire`, or a `backup`, is using this lake | Stop the other one, or let it finish. If that process has already exited, just run again: the lock is released automatically, so there is never a stale lock to delete |
| A `.part` file in `inbox/` | A copy was cut off because the watch was killed, or the computer lost power, partway through. (A stick pulled mid-copy removes its own `.part` and counts as a failed try.) | Nothing. `.part` files are never ingested and are removed on the next start |
| `ingest exited with N and changed nothing; inbox kept` | Ingest failed on something in the inbox | Run `flashpoint ingest <lake>/inbox` by hand to see the error |
| `Replay build error: ...` in `doctor` | The Replay build after derive failed; the watch tries again every cycle | Run `flashpoint report` by hand to see the full error. Usually a mistake in a robot file (`robots/` in your config folder) or in `report.toml`. `flashpoint doctor` shows where your config folder is |
| `a Replay build is already running on this lake` from `flashpoint report` | The watch (or another `report`) is building Replay data right now | Run it again once that build ends; there is never a stale lock to delete |
| `failed ...` lines in `doctor` | A file failed its copy check three times, and the watch has stopped retrying it | Copy it by hand and run `flashpoint ingest` on it; if that fails too, the file on the robot is damaged |
