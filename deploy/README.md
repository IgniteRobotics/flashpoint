# Deploying the acquisition service

`flashpoint acquire --watch` pulls new logs from the roboRIO and from removable
volumes into the lake, and optionally backs them up with rclone. Run it as a
per-user service; it needs no display server.

## Install

```sh
pipx install flashpoint      # or: pipx install /path/to/flashpoint-checkout
```

pipx puts the executable at `~/.local/bin/flashpoint` (Linux, macOS) and
`%USERPROFILE%\.local\bin\flashpoint.exe` (Windows). Both service definitions
below point there; edit them if `pipx environment --value PIPX_BIN_DIR` differs.

## Configuration: `acquire.toml`

Read from `<config_root>/acquire.toml`. The config root is `$FLASHPOINT_CONFIG`
if set, else the repo's `config/` directory (source checkouts), else
`~/.config/flashpoint/`. A pipx install uses `~/.config/flashpoint/acquire.toml`.
Unknown keys are rejected. The lake is `$FLASHPOINT_LAKE` or `~/flashpoint-lake`.

```toml
hosts = ["10.68.29.2", "roborio-6829-frc.local", "172.22.11.2"]
user = "lvuser"
password = ""                          # optional
roots = ["/home/lvuser/logs", "/u/logs"]
poll_s = 30
settle_s = 5
low_space_mb = 100
removable_media = true
[backup]
remote = "gdrive:flashpoint"           # absent = no backup
interval_min = 15
```

Try it once in the foreground before installing a service:
`flashpoint acquire --dry-run` lists what would be copied and changes nothing.

## Linux (systemd user unit)

```sh
mkdir -p ~/.config/systemd/user
cp deploy/systemd/flashpoint-acquire.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now flashpoint-acquire
journalctl --user -u flashpoint-acquire -f
```

Environment variables (`FLASHPOINT_LAKE`, `FLASHPOINT_CONFIG`) go in
`~/.config/flashpoint/env`, one `KEY=value` per line; the file is optional.

On a headless box, keep the user manager running without a login session:

```sh
loginctl enable-linger "$USER"
```

Headless USB: the watch only sees volumes that are mounted. Install `udisks2`
and run `udiskie` (a user service, for example `systemctl --user enable --now udiskie`)
so drives are mounted automatically when plugged in. Use `--no-usb` or
`removable_media = false` to turn the USB scan off.

## Windows (Task Scheduler)

```bat
schtasks /Create /XML deploy\windows\flashpoint-acquire.xml /TN flashpoint-acquire
schtasks /Run /TN flashpoint-acquire
```

The task starts at logon, restarts on failure (3 times, 1 minute apart), has no
time limit, and ignores a second start while one is running. Remove it with
`schtasks /Delete /TN flashpoint-acquire /F`.

## macOS

No service definition yet. Run it in a terminal (or tmux) while you need it:

```sh
flashpoint acquire --watch
```

## Backup (rclone)

Install rclone, then create a remote once; Flashpoint never reads rclone
credentials.

```sh
rclone config                 # create a remote, for example "gdrive"
```

Set `backup.remote = "gdrive:flashpoint"` in `acquire.toml`. The watch backs up
every `interval_min` minutes; `flashpoint backup` runs one now. Raw logs are
copied append-only (`--immutable`); metadata is a consistent snapshot.

### Restore drill

Practice this before you need it. Restore into a scratch lake, never over a live one:

```sh
flashpoint restore --lake /tmp/restore-drill
flashpoint rebuild --lake /tmp/restore-drill
flashpoint doctor  --lake /tmp/restore-drill
```

`restore` copies raw files and metadata and refuses a lake that already has a
ledger (`--force` overrides). `--remote name:path` restores from somewhere other
than `backup.remote`. `rebuild` regenerates bronze, silver and gold from raw.

## Status

```sh
flashpoint doctor
```

reports the environment, lake health, and the acquisition and backup status.
