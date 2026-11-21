# Back up the lake

The lake is the only copy of your imported logs and your anomaly triage. Back it up off the laptop, along with your configuration, and practise a restore before you need one.

Flashpoint backs up with [rclone](https://rclone.org/) to any remote rclone supports, such as Google Drive, a NAS or another disk. Flashpoint never reads your rclone credentials.

## Before you start

- Flashpoint installed and configured, with at least one log imported. On an empty lake, `flashpoint backup` stops with `no ledger at <lake>/meta/flashpoint.sqlite; nothing to back up`.
- rclone installed: follow [rclone's install page](https://rclone.org/install/).
- Somewhere to back up to, and internet (or a network) to reach it.

## Steps

1. Make an rclone remote, once. `rclone config` asks a few questions; give the remote a short name, such as `gdrive`:

    ```sh
    rclone config
    ```

2. Open `acquire.toml` in your config folder (`~/.config/flashpoint/` on macOS and Linux, `%APPDATA%\flashpoint` on Windows) and add the remote, with a folder on it:

    ```toml
    [backup]
    remote = "gdrive:flashpoint"
    ```

    `interval_min` and `overdue_h` go in the same table. They're in the [`acquire.toml` reference](../reference/config/acquire.md).

3. Run one backup now:

    ```sh
    flashpoint backup
    ```

    It prints what it backed up:

    ```text
    backed up raw, metadata and configuration to gdrive:flashpoint
    ```

    A large first backup can take a while. If [automatic import](auto-import.md) is running, this command exits with code `3` and says `a running watch backs up on its own`: the watch does it instead.

4. Leave the backups to the watch. `flashpoint acquire --watch` backs up every `interval_min` minutes (default 15) when something changed, such as a new log or a saved robot file or threshold. Without the watch, run `flashpoint backup` after each import.

5. Run the [restore drill](#restore-drill) once, so you know the backup works.

## Check it worked

`flashpoint doctor` (or the **Backup** check in the Setup view) says when the last backup succeeded:

```text
  ok       Backup                       last backup 3 min ago
```

The remote has three folders, `raw`, `meta` and `config`:

```sh
rclone lsd gdrive:flashpoint
```

If the check is a warning or a problem, see [The backup is overdue](../troubleshooting.md#the-backup-is-overdue).

## What a backup holds

- **Raw logs**, copied once and never overwritten.
- **Metadata**, including anomaly triage: a consistent snapshot in `meta/latest` on the remote. Each backup moves the files it replaces into `meta/history/<UTC time>/`, so a backup from a wrong or empty lake can't destroy the last good one. Nothing deletes old history folders; remove them by hand when you want the space.
- **Your configuration**, in `config/latest`: the TOML files at the top of the config folder and in `seasons/` and `robots/`, and the CSV files in `units/`. Nothing else in that folder is copied: not the service's `env` file, not the Setup view's `.backups/`, not notes you left there. `flashpoint init` puts copies of the default files in the folder, so they're backed up too, even if you never changed them. Each backup moves the files it replaces or deletes into `config/history/<UTC time>/`.

Bronze, silver, gold and the Replay data aren't backed up: they're rebuilt from raw.

Within the watch, each backup gives up on a dead connection within about a minute and stops after 4 minutes. A large first backup goes up over several cycles, and `doctor` shows the backup as still owed until it's done.

To get an older robot file back, copy it from the history with rclone:

```sh
rclone copy gdrive:flashpoint/config/history/20261023T180000Z/robots/2026-comp.toml ~/.config/flashpoint/robots/
```

## When a backup is overdue

The **Backup** check, in `flashpoint doctor` and the Setup view, watches for data that exists only on this laptop:

- A log is **waiting** when the lake received it after the last successful backup. Your configuration is waiting when it changed since then. This works the same whether logs came in through the watch or `flashpoint ingest`.
- **Problem:** something has waited longer than `backup.overdue_h` hours (default 24). The message says how many logs, whether configuration changed, for how long, and the last error, for example `2 logs and configuration changes not backed up for 30 h (last backup 2 days ago)`. The **Setup** tab shows a **!** in your warning colour on every view until a backup succeeds. `doctor` exits `1`.
- **Warning:** the last backup failed, but nothing has waited that long yet. Also when `backup.remote` is set but rclone isn't installed: `rclone isn't installed, so no backup can run; <status>`.
- **ok:** says when the last backup succeeded. With no `backup.remote` set, it's ok and marked `(optional)`, and says how many logs exist only on this laptop.

The check only reads what's recorded on this laptop; it never contacts the remote. At an event without internet, it turns into a problem after a day. That's accurate: nothing from the event is backed up yet. Once you're back online, the watch catches up on its own, or run `flashpoint backup`. To keep it quiet over a long event, raise `overdue_h` (for example `72`). Before going back to a Flashpoint version older than this one, remove the `overdue_h` line: older versions refuse unknown keys.

## Restore drill

Practise this before you need it. Always restore into a new lake folder and an empty config folder, never over the ones you're using. Pointing `FLASHPOINT_CONFIG` at a new folder makes the drill act like a new laptop.

1. Restore into a scratch lake and a scratch config folder, then rebuild:

    === "macOS and Linux"

        ```sh
        export FLASHPOINT_CONFIG=~/restore-drill-config
        flashpoint restore --remote gdrive:flashpoint --lake ~/restore-drill
        flashpoint rebuild --lake ~/restore-drill
        ```

    === "Windows (PowerShell)"

        ```powershell
        $env:FLASHPOINT_CONFIG = "$HOME\restore-drill-config"
        flashpoint restore --remote gdrive:flashpoint --lake "$HOME\restore-drill"
        flashpoint rebuild --lake "$HOME\restore-drill"
        ```

    `--remote` is needed because the new config folder has no `acquire.toml` yet. `restore` prints:

    ```text
    restored 47 file(s) from gdrive:flashpoint; bronze, silver and gold must be rebuilt
    restored the configuration into /home/pit/restore-drill-config
    run: flashpoint rebuild
    ```

    The rebuild converts every hoot again, so it takes a while, and needs owlet (internet, if this laptop has never downloaded it).

2. Check the restored lake:

    === "macOS and Linux"

        ```sh
        flashpoint doctor --lake ~/restore-drill
        ```

    === "Windows (PowerShell)"

        ```powershell
        flashpoint doctor --lake "$HOME\restore-drill"
        ```

    The `lake` section counts the files, and the robot and owlet checks are ok. To look at it, run `flashpoint serve --lake ~/restore-drill` and check that Replay lists your matches and Anomalies keeps your triage.

3. Go back to your own configuration, then delete the two scratch folders:

    === "macOS and Linux"

        ```sh
        unset FLASHPOINT_CONFIG
        ```

    === "Windows (PowerShell)"

        ```powershell
        Remove-Item Env:FLASHPOINT_CONFIG
        ```

What `restore` does and doesn't do:

- It copies the raw logs and metadata, including anomaly triage. It refuses a lake that already has data (`<lake>/meta/flashpoint.sqlite already exists; restore into a new lake, or pass --force`). Use a new lake rather than `--force`.
- It copies your configuration into the config folder **only if that folder has no configuration files yet**. If it has any, nothing in it is changed: the backed-up copy goes to `restored-config/` inside the restored lake, and `restore` says `kept the configuration in <config folder>; the backed-up copy is in <lake>/restored-config`. Compare and copy what you need by hand. `--force` never changes this.
- A backup made before configuration was included still restores; `restore` says `the backup held no configuration`.
- To restore an older metadata snapshot, first copy that `meta/history/<time>/` folder over `meta/latest` on the remote with rclone.

The full options are in the [CLI reference](../reference/cli.md#flashpoint-restore). To move to a new laptop for real, see [Move Flashpoint to another laptop](move-to-another-laptop.md).
