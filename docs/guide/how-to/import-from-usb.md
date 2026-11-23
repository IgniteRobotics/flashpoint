# Import logs from a USB stick

Copy the robot's logs off its USB stick, or off the roboRIO itself, and import them into the lake. To have this happen on its own, see [Import logs from the robot automatically](auto-import.md).

## Before you start

- Flashpoint installed, and a configuration made with `flashpoint init` ([Install](../install/index.md)).
- Internet the first time you import a hoot, so Flashpoint can download owlet, CTRE's hoot converter. After that, importing works offline ([Before an event: get ready offline](../user-guide.md#before-an-event-get-ready-offline)).
- Robot code that writes its logs where you'll look. `DataLogManager.start()` writes the wpilog to a `logs` folder on the robot's USB stick when one is plugged in, and to `/home/lvuser/logs` otherwise ([DataLog](../robot-side/datalog.md)). [Signal logging](../robot-side/signal-logging.md) puts the hoots next to it.

## Steps

1. Turn the robot off, then take the USB stick out of the roboRIO and plug it into the laptop.

    No stick, or the logs are on the rio's own storage? Copy them over SFTP instead, with the robot on and the laptop on its network. The user is `lvuser`, with no password unless your rio has one. For team 6829 the address is `10.68.29.2` (`10.TE.AM.2`):

    ```sh
    scp -r lvuser@10.68.29.2:/home/lvuser/logs ~/robot-logs
    ```

    An SFTP app such as WinSCP or FileZilla works too. Then go to step 3.

2. Copy the stick's `logs` folder to the laptop in Finder or File Explorer, for example into a folder called `robot-logs` in your home folder. Then the stick can go back in the robot. Each robot power-on writes its own files; copy them all. Files already in the lake are skipped later, so copying old ones again does no harm.

3. Import the folder:

    === "macOS and Linux"

        ```sh
        flashpoint ingest ~/robot-logs
        ```

    === "Windows (PowerShell)"

        ```powershell
        flashpoint ingest "$HOME\robot-logs"
        ```

    You can pass files or folders, and folders are searched all the way down. Quote a path with a space in it.

4. Read the summary at the end. Files that need attention are listed first, then one line counts them:

    ```text
      WARNING     GACMP_E10_rio_2026-04-11_19-29-15.hoot: incomplete-read
      QUARANTINED GADAL_Q11_..._2026-03-06_17-52-51.hoot: empty-file
      FAILED      GACMP_Q7_rio_2026-04-09_21-51-06.hoot: failed (owlet-download-failed), will retry
    13 ingested, 0 skipped, 1 failed, 1 quarantined, 0 quarantined earlier in 13.1s
    ```

    - `WARNING`: the file was read, but something was off. `incomplete-read` means its end is missing, usually because the robot lost power while writing it. The rest is used.
    - `QUARANTINED`: the file is bad (empty, damaged, or a format Flashpoint can't read). It's kept in the lake but not used, and later imports don't try it again.
    - `FAILED`: the laptop failed, not the file, usually because owlet couldn't be downloaded. Run the same command again once you're online.

    The exit code is `1` when a file failed or was quarantined in this run.

5. Wait for the rest. After the import, `ingest` works out the matches, maps the motors to your robot file, runs anomaly detection and builds the Replay data, each with its own line:

    ```text
    derived 4 of 4 session(s)
    detected 0 anomalies (0 HIGH, 0 MED, 0 LOW); 22 unit(s) building baseline; scanned through 2026gacmp_qm7
    built 4, unchanged 0, removed 0; 4 match(es) listed in 2.1s
    ```

    The first import of a season also drafts your robot file ([Set up a robot file](set-up-a-robot-file.md)).

6. Look at the matches:

    ```sh
    flashpoint serve
    ```

    Open <http://127.0.0.1:8000/>. Stop the app with Ctrl-C.

You can delete the copies on the laptop afterwards: the lake keeps an untouched copy of every log.

## Check it worked

The summary's first count is the number of files ingested. **Replay** lists each competition match by its key, such as `2026gacmp_qm7`. Replay lists matches only: a log is a match when it holds FMS data from the field, or its file name has the match in it, the way the Driver Station names match logs (`FRC_yyyyMMdd_HHmmss_EVENT_Q7.wpilog`). Logs from the shop, the practice field or the pit get no Replay entry, but **History** counts their motor usage ([How matches are identified](../user-guide.md#how-matches-are-identified)).

`report` prints a line when some sessions had no match key:

```text
  3 session(s) without a match key (no Replay entry; their usage still counts in History)
```

If Replay is empty, see [Replay says "No Replay data"](../troubleshooting.md#replay-says-no-replay-data) and [Every log is a non-match session](../troubleshooting.md#every-log-is-a-non-match-session). For lines in the summary, see [A file was quarantined](../troubleshooting.md#a-file-was-quarantined), [A file was imported as incomplete](../troubleshooting.md#a-file-was-imported-as-incomplete), and, for `owlet-download-failed`, [A proxy or firewall blocks the owlet download](../troubleshooting.md#a-proxy-or-firewall-blocks-the-owlet-download).
