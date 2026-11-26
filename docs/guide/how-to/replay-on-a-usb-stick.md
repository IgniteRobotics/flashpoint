# Take Replay on a USB stick

`flashpoint report --static` writes Replay into a folder that opens from `index.html`, on any computer, with no install and no network. Use it to hand matches to a drive coach, a mentor at home, or a team without Flashpoint.

Only Replay is in the export. History, Anomalies, Setup and Live CAN need `flashpoint serve`.

## Before you start

- The lake has at least one imported match. With no logs imported, `report` stops with ``no derived sessions in <lake>; run `flashpoint ingest` first``.
- A USB stick with room for it. Replay data is about 1 MB per match, and the raw logs are added on top: the wpilog and every hoot of each match. For a small stick, leave the raw logs out with `--no-raw`.
- Set the team name and colours first if you want them on the stick ([Set the team name and colours](team-name-and-colours.md)). The export uses them as they are when you write it.

## Write the export

1. Plug in the stick. Pick a folder on it that is new or empty, such as `flashpoint`. Flashpoint refuses a folder that has other files in it, unless it's an earlier export, which it replaces.
2. Run `report` with `--static` and that folder.

    macOS:

    ```sh
    flashpoint report --static /Volumes/<stick>/flashpoint
    ```

    Linux (the stick is usually under `/media/` and your user name):

    ```sh
    flashpoint report --static /media/<you>/<stick>/flashpoint
    ```

    Windows (use the stick's drive letter):

    ```powershell
    flashpoint report --static E:\flashpoint
    ```

    To leave the raw logs out, add `--no-raw`:

    ```sh
    flashpoint report --static /Volumes/<stick>/flashpoint --no-raw
    ```

3. Wait for the `exported` line (below), then eject the stick the usual way before you unplug it.
4. On the other computer, open the stick's `flashpoint` folder and double-click `index.html`. It opens in the default browser.

`report` first builds Replay data for any match that needs it, then exports. The export holds every match that has Replay data, not only new ones; `--match` and `--event` limit the build, not the export. To update the stick after new matches or a change of colours, run the same command again. It replaces the folder's contents.

## What works on the stick

| Works | Doesn't |
|---|---|
| The match list, filters, tracks, markers, zoom, the readout and **COMPARE** | Zooming past the stored resolution: the window line reads "bucket resolution (_n_ ms)" |
| Downloading each match's wpilog and hoots, unless you used `--no-raw` | **OPEN IN ADVANTAGESCOPE**: there's no button, only the downloads |
| Anomaly badges on the tracks, as they were when you exported. Each says when its snapshot was taken | Opening an anomaly: the Anomalies view isn't in the export |
| Your team name and colours, as they were when you exported | History, Anomalies, Setup and Live CAN |

With `--no-raw`, the downloads are replaced by "Raw logs not included in this export. Their lake hashes:" and a hash per log.

The export holds no folder path from the laptop that made it, so it is safe to hand out.

## Check it worked

`report` ends with two lines like these:

```text
built 2, unchanged 10, removed 0; 12 match(es) listed in 3.1s
exported 12 match(es) to /Volumes/<stick>/flashpoint (36 raw file(s)); open /Volumes/<stick>/flashpoint/index.html
```

With `--no-raw` the second line says `(0 raw file(s), not included)`. If it says `export failed: <folder> is not empty and is not a previous Flashpoint export`, pick a new folder on the stick.

Open `index.html`. The header shows a **STATIC EXPORT** pill on the right, and **REPLAY** is the only tab. Pick a match: its tracks and markers show, as they do in `serve`.

If Replay says **No Replay data**, or a match says "This export does not include it.", see [Replay says "No Replay data"](../troubleshooting.md#replay-says-no-replay-data). The [Replay view reference](../reference/views/replay.md) describes every part of the screen.
