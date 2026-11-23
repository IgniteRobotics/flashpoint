# Share logs privately for a bug report

Logs from other teams' robots are the most useful thing you can give us: every team logs a little differently, and Flashpoint only gets better at reading them by seeing them. This page says which files to send and how to send them so they stay private.

**Never attach `.wpilog` or `.hoot` files to a public issue or discussion.** They hold your team's match data: every NetworkTables value robot code logs, Driver Station inputs, match data, and every CTRE device's serial number ([what a log contains](../privacy.md#what-a-log-and-a-lake-contain)). Treat logs like your robot code.

## Before you start

- You know which match or log shows the problem.
- You can share a folder from Google Drive, Dropbox or a similar service with one person.
- Ask your mentor before you send team logs outside the team.

## Send the logs

1. Open a bug report with the [bug form](https://github.com/IgniteRobotics/flashpoint/issues/new/choose). Fill it in as usual, including `flashpoint --version` and the `flashpoint doctor` output. Name the log files and say what's in them, but don't attach them. Say that you have logs to share.
2. Wait for a maintainer to reply on the issue with how to send them, usually a Google Drive or Dropbox link shared only with them.
3. Collect the files for the match, the wpilog and every hoot from the same time:
    - **From Flashpoint:** in **REPLAY**, pick the match. Under **OPEN IN ADVANTAGESCOPE** in the **DETAILS** rail there is a download link for each raw log: **⇩ wpilog**, and **⇩ hoot ·** with the bus. Download each one. They're saved as `<match key>__<wpilog or bus>__<original name>`. **HISTORY** has the same links under **LOGS** for each of a unit's recent matches.
    - **From where you copied them:** the original files from the robot or the USB stick work just as well. Use these for a practice or pit log, which Replay doesn't list, and for a file that failed to import or was quarantined.
4. Put the files in one folder and upload it through the private route the maintainer gave you. Share it with that person only, not with "anyone with the link".
5. Reply on the issue that the logs are shared. Don't post the link or the files there.

Logs shared with us are used only to test Flashpoint, and are deleted when you ask. We don't publish them or pass them on.

## Check it worked

- The issue page has no `.wpilog`, `.hoot` or `.zip` attachments, and no link to the shared folder.
- The shared folder lists the wpilog and each hoot of the match, with the match key at the start of each name if you downloaded them from Replay.
- The maintainer confirms on the issue that they have them.

If a file wouldn't import, include its `ingest` line in the issue, such as `QUARANTINED <file>: <reason>`. [A file was quarantined](../troubleshooting.md#a-file-was-quarantined) explains the reasons. The [Privacy and network](../privacy.md) page lists everything a log and the lake hold.
