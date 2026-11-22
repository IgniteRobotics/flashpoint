# Quickstart

From nothing to your own match in Replay: install, then three commands.

## Your own logs

You need a folder with one match's logs from your robot: the `.wpilog` from the roboRIO's USB stick or `/home/lvuser/logs`, and the `.hoot` files from CTRE signal logging. Then:

```sh
uv tool install --python 3.13 flashpoint
flashpoint init --team <your team number>
flashpoint ingest <your logs folder>
flashpoint serve
```

Open <http://127.0.0.1:8000/> and pick the match in **Replay**.

- The install brings its own Python. If you don't have uv yet, or `flashpoint` isn't found afterwards, follow the [Install](install/index.md) page for your computer.
- `init` asks for anything you leave out: your lake folder (where logs and results are stored), your team name and the app's colours.
- The first `ingest` drafts your robot from the logs, so Replay shows motors straight away. Name each motor in **Setup > Robots** when you have a minute.
- The first hoot downloads CTRE's converter, so do this once with internet before you rely on it at an event.

Stop the app with Ctrl-C.

## Try it with sample logs

No logs yet? Try Flashpoint on one real match from team 6829's 2026 season: qualification match 7 at the Georgia district championship, a full match with the robot enabled throughout. The bundle is 55.4 MB, with SHA-256 `20ecfd9e3e4c8cdef14c26e80d75735cab048759e52565636a99987f79a8f96c`.

It uses its own configuration and its own lake inside the bundle's folder, so your configuration and lake aren't touched. Install Flashpoint first ([Install](install/index.md)), then, in an empty folder:

=== "macOS and Linux"

    ```sh
    curl -LO https://github.com/IgniteRobotics/flashpoint/releases/download/samples-v1/flashpoint-sample-1.zip
    echo "20ecfd9e3e4c8cdef14c26e80d75735cab048759e52565636a99987f79a8f96c  flashpoint-sample-1.zip" | shasum -a 256 -c -
    unzip -q flashpoint-sample-1.zip -d flashpoint-sample
    cd flashpoint-sample
    FLASHPOINT_CONFIG="$PWD/config" flashpoint ingest --lake "$PWD/lake" logs/
    FLASHPOINT_CONFIG="$PWD/config" flashpoint serve --lake "$PWD/lake"
    ```

=== "Windows (PowerShell)"

    ```powershell
    Invoke-WebRequest https://github.com/IgniteRobotics/flashpoint/releases/download/samples-v1/flashpoint-sample-1.zip -OutFile flashpoint-sample-1.zip
    (Get-FileHash flashpoint-sample-1.zip -Algorithm SHA256).Hash -eq "20ecfd9e3e4c8cdef14c26e80d75735cab048759e52565636a99987f79a8f96c"
    Expand-Archive flashpoint-sample-1.zip -DestinationPath flashpoint-sample
    cd flashpoint-sample
    $env:FLASHPOINT_CONFIG = "$PWD\config"
    flashpoint ingest --lake "$PWD\lake" logs
    flashpoint serve --lake "$PWD\lake"
    ```

    The checksum line must print `True`. `FLASHPOINT_CONFIG` stays set in this PowerShell window, so close it when you're done; a new window uses your own configuration again.

The checksum line must print `OK` (on Windows, `True`). If it doesn't, the download was cut short: see [The sample's checksum doesn't match](troubleshooting.md#the-samples-checksum-doesnt-match).

Then open <http://127.0.0.1:8000/> and pick **2026gacmp_qm7** in Replay. Anomalies has two findings from this match to look at. Stop the app with Ctrl-C.

!!! note "Expected: a note about 10 declared signals"
    `ingest` prints one line about 10 `declared signal(s) not written`, naming CAN bus status signals such as `can-rio-utilization`. The sample's robot file is Ignite's, which declares the [CAN bus status](robot-side/bus-status.md) entries its robot code logs now; this match is from before that logger was added. Nothing is wrong with the sample.

To remove the sample, delete the `flashpoint-sample` folder.

Prefer to look around without installing anything? The [hosted demo](demo.md) runs Replay on sample matches in your browser.

## Next

- Get the most out of your logs: [Robot side](robot-side/index.md) says what to log in robot code, and what each piece adds.
- Set Flashpoint up for your team, start to finish: [team setup tutorial](tutorials/team-setup.md).
