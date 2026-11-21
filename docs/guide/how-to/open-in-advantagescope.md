# Open a match in AdvantageScope

Replay's **OPEN IN ADVANTAGESCOPE** opens a match's wpilog in AdvantageScope, for a frame-by-frame look at everything robot code logged. Flashpoint finds the match's logs in the lake, puts them in a folder under readable names, and starts AdvantageScope on the wpilog. You add the hoots from that folder yourself.

It works only on the laptop running `flashpoint serve`, with the app opened on that laptop.

## Before you start

- AdvantageScope is installed on the laptop that runs `serve`. The WPILib installer includes it. Flashpoint looks for it in this order:
    1. the `path` under `[advantagescope]` in [`report.toml`](../reference/config/report.md#advantagescope), when it's set;
    2. the newest WPILib year: `~/wpilib/<year>/advantagescope/` on macOS and Linux, `C:\Users\Public\wpilib\<year>\advantagescope\` on Windows;
    3. a standalone install: `/Applications/AdvantageScope.app` on macOS, `%LOCALAPPDATA%\Programs\AdvantageScope\AdvantageScope.exe` on Windows, or `advantagescope` on the `PATH` on Linux.
- `serve` runs the usual way, without `--host 0.0.0.0`. Shared on the network, nothing can be launched, not even on the laptop itself.
- The match has a wpilog. A match with hoots only has nothing to open: download its hoots instead.

## Open the match

1. Check that Flashpoint finds AdvantageScope:

    ```sh
    flashpoint doctor
    ```

    The `AdvantageScope:` line names the program and how it was found, such as `(WPILib 2026)`. If it says `AdvantageScope: not found`, go to [Set the path](#set-the-path) first.

2. Start the app and open it on this laptop, at <http://127.0.0.1:8000/>:

    ```sh
    flashpoint serve
    ```

3. In **REPLAY**, pick the match.
4. In the **DETAILS** rail on the right, press **OPEN IN ADVANTAGESCOPE**. AdvantageScope opens with the match's wpilog. On macOS, if it's already running, it opens a new window.
5. To add the hoots, use **File › Insert log** in AdvantageScope and pick them from the folder the panel shows.

The folder is under your computer's temporary folder, in `flashpoint-as/<match key>/`. Each file is named `<match key>__<wpilog or bus>__<original name>`. The lake's own copies are never changed. `serve` removes folders older than a day when it starts.

## Set the path

If AdvantageScope is somewhere Flashpoint doesn't look, tell it where:

1. In the app, open **SETUP**, then **CHECKS**. The AdvantageScope line says "not found: install WPILib, or set its path, to open logs from Replay".
2. Press **Set AdvantageScope path**, and enter the full path to the program, such as `/Applications/AdvantageScope.app` on macOS or `C:\Users\Public\wpilib\2026\advantagescope\AdvantageScope (WPILib).exe` on Windows. Flashpoint checks the path exists, then writes it to `report.toml`.
3. Stop `serve` (Ctrl-C) and start it again. `serve` looks for AdvantageScope when it starts, so the Replay button uses the new path only after a restart.

Or edit `report.toml` in your config folder yourself. On Windows, use single quotes so the backslashes stay as they are:

```toml
[advantagescope]
path = 'C:\Users\Public\wpilib\2026\advantagescope\AdvantageScope (WPILib).exe'
```

Clear `path` to go back to the WPILib or standalone install.

## Check it worked

After step 4 the panel says:

```text
Opened <match key>__wpilog__<file>.wpilog in AdvantageScope.
To add the hoots, use File › Insert log in AdvantageScope and pick them from:
<folder>
```

followed by the hoot files. A hoot whose end is missing (the robot lost power while writing it) is marked **INCOMPLETE**. AdvantageScope shows the wpilog's fields in its sidebar.

If the panel says "AdvantageScope did not open:" and a reason, or shows "Open in AdvantageScope is only available on the machine running Flashpoint" in place of the button, the reason says what to fix: open the app on the laptop running `serve`, at `127.0.0.1`, and start `serve` without `--host`. If `doctor` says `configured AdvantageScope path not found: <path>`, see [AdvantageScope path not found](../troubleshooting.md#advantagescope-path-not-found).

A static export on a USB stick has no button, only the downloads ([Take Replay on a USB stick](replay-on-a-usb-stick.md)).
