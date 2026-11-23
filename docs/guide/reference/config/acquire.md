# acquire.toml

`acquire.toml` holds your team number, the lake folder, and how [automatic import](../../how-to/auto-import.md) reaches the robot and backs up. It lives in your config folder: `~/.config/flashpoint/` on macOS and Linux, `%APPDATA%\flashpoint` on Windows, or the folder `FLASHPOINT_CONFIG` names. `flashpoint init` writes it. Its presence is what makes the folder count as a configuration: without it, `ingest` and `derive` warn and map no robot.

Unknown keys are an error, so typos are caught. Every key, with its default:

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
overdue_h = 24
```

| Key | Default | What it does |
|---|---|---|
| `team` | none | Your team number, 1 to 99999. Robot pull tries `10.TE.AM.2` (team 6829: `10.68.29.2`, team 254: `10.2.54.2`), then `roborio-TEAM-frc.local`, then the USB tether `172.22.11.2` |
| `lake` | `~/flashpoint-lake` | The lake folder for every command. `--lake` and `FLASHPOINT_LAKE` override it |
| `hosts` | `[]` (use the team's addresses) | Robot addresses to try instead, in order. `--host ADDR` replaces it for one run |
| `user` | `lvuser` | SSH user on the roboRIO |
| `password` | `""` (none) | SSH password, if the rio has one |
| `roots` | `/home/lvuser/logs`, `/u/logs` | Log folders on the robot, searched with their subfolders: internal storage, and the robot's USB stick |
| `poll_s` | `30` | Seconds between cycles in `--watch` |
| `settle_s` | `5` | Seconds to wait and re-check a file's size before copying it, so half-written files are skipped |
| `low_space_mb` | `100` | Warn when a robot log folder has less free space than this ([Disk space](../../robot-side/disk-space.md)) |
| `removable_media` | `true` | Scan USB drives plugged into this computer. `--no-usb` turns it off for one run |
| `backup` | none | The `[backup]` table. Without it, nothing is backed up |
| `remote` | none (no backup) | In `[backup]`: the rclone remote and path, such as `gdrive:flashpoint` ([Back up the lake](../../how-to/back-up-the-lake.md)) |
| `interval_min` | `15` | In `[backup]`: minutes between backups in `--watch` |
| `overdue_h` | `24` | In `[backup]`: hours logs or configuration may wait for a backup before the backup check reports a problem. At least 1 |

The watch reads `acquire.toml` once, when it starts. After changing it, restart the watch ([Import logs from the robot automatically](../../how-to/auto-import.md)).

## Common errors

`flashpoint doctor` checks the file and names the key. These are its messages:

- A misspelt key (here `tem` for `team`):
  ```text
  acquire.toml: tem: Extra inputs are not permitted
  ```
- A number in quotes:
  ```text
  acquire.toml: team: Input should be a valid integer
  ```
- A value out of range:
  ```text
  acquire.toml: backup.overdue_h: Input should be greater than or equal to 1
  ```
- A key given twice:
  ```text
  acquire.toml: Cannot overwrite a value (at line 3, column 12)
  ```
