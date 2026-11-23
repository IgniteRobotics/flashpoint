# Move Flashpoint to another laptop

Move the lake and your configuration to a new laptop without losing any history or triage. The usual way is to restore the new laptop from your backup. If you have no backup, copy the two folders instead.

## Before you start

- A new laptop that Flashpoint runs on: macOS 13 or later, Linux with glibc 2.35 or later, or Windows on an x86-64 PC ([Install](../install/index.md)).
- Internet on the new laptop: for the install, for the backup, and to download owlet again for the hoots.
- Either a backup that's up to date ([Back up the lake](back-up-the-lake.md)), or the old laptop at hand to copy from.
- Time: the rebuild after a restore converts every hoot again.

## Steps

1. On the old laptop, stop anything that writes to the lake: the acquire watch or its service, and `flashpoint serve`. On Linux, `systemctl --user stop flashpoint-acquire`; on Windows, `schtasks /End /TN flashpoint-acquire`.

2. On the old laptop, make sure the backup has everything. Run `flashpoint backup`, then `flashpoint doctor`: the **Backup** check should be ok, with nothing waiting:

    ```text
      ok       Backup                       last backup 0 min ago
    ```

3. On the new laptop, install uv and Flashpoint, following the [Install](../install/index.md) page for that computer:

    ```sh
    uv tool install --python 3.13 flashpoint
    ```

    **Don't run `flashpoint init` yet.** `restore` brings your configuration back only into an empty config folder.

4. Install rclone on the new laptop ([rclone's install page](https://rclone.org/install/)) and make the same remote with `rclone config`. Give it the same name as on the old laptop, such as `gdrive`.

5. Restore into a new lake:

    === "macOS and Linux"

        ```sh
        flashpoint restore --remote gdrive:flashpoint --lake ~/flashpoint-lake
        ```

    === "Windows (PowerShell)"

        ```powershell
        flashpoint restore --remote gdrive:flashpoint --lake "$HOME\flashpoint-lake"
        ```

    It prints how many files it restored, and `restored the configuration into <config folder>`.

6. Re-point the lake. The restored `acquire.toml`, in the config folder (`~/.config/flashpoint/` on macOS and Linux, `%APPDATA%\flashpoint` on Windows), still names the old laptop's lake folder in `lake`. Change it to the new one:

    ```toml
    lake = "/Users/pit/flashpoint-lake"
    ```

    On Windows, write the path in single quotes so the backslashes stay as they are: `lake = 'C:\Users\Pit\flashpoint-lake'`.

7. Rebuild everything from the raw logs. This downloads owlet the first time it meets a hoot:

    ```sh
    flashpoint rebuild
    ```

8. If the old laptop ran automatic import, set it up here ([Run it in the background](auto-import.md#run-it-in-the-background)), and remove it from the old laptop. Two laptops backing up to the same remote replace each other's copy of the lake's metadata.

### Or copy the folders

With no backup, copy the folders by hand, after step 1:

1. Copy the whole lake folder (`~/flashpoint-lake` by default) and the whole config folder from the old laptop to the new one, on a USB drive or over the network.
2. Install Flashpoint on the new laptop (step 3 above), without running `flashpoint init`.
3. Put the config folder where Flashpoint looks: `~/.config/flashpoint/` on macOS and Linux, `%APPDATA%\flashpoint` on Windows.
4. Re-point `lake` in `acquire.toml` to where you put the lake (step 6 above).
5. Run `flashpoint doctor`. If its `lake` section says some files are from an older pipeline version (the new laptop has a newer Flashpoint), run `flashpoint rebuild`.

The owlet cache (`~/.cache/flashpoint/`) doesn't need copying; Flashpoint downloads it again.

## Check it worked

Run `flashpoint doctor` on the new laptop:

- `lake` names the new folder, and counts the files at each stage, the same number as on the old laptop.
- `config` names the config folder, and the robot check names your robot.
- The owlet check for each hoot format says `owlet <version> downloaded, verified`.
- After a restore, the **Backup** check is ok: the restore counts as a backup, since everything restored is on the remote.

Then run `flashpoint serve`: Replay lists the same matches, and Anomalies keeps your triage.

If Replay is empty, the app is probably reading a different lake: see [Replay says "No Replay data"](../troubleshooting.md#replay-says-no-replay-data). For Windows paths, see [Windows paths and quoting](../troubleshooting.md#windows-paths-and-quoting).
