# Flashpoint

FRC robot telemetry: import WPILib .wpilog and CTRE .hoot logs, and track every motor's health across your robot's life. Runs offline on a pit laptop.

![From the match list to one match in Replay: tracks, markers and the readout](https://igniterobotics.github.io/flashpoint/latest/assets/media/flow-ingest-to-replay.gif)

| Replay | History | Anomalies (beta) |
|---|---|---|
| ![Replay view: one match's motor currents, temperatures and battery voltage on a shared timeline](https://igniterobotics.github.io/flashpoint/latest/assets/media/replay.png) | ![History view: one motor's temperature and current across every match of the season](https://igniterobotics.github.io/flashpoint/latest/assets/media/history.png) | ![Anomalies view: flagged motors with their evidence and triage state](https://igniterobotics.github.io/flashpoint/latest/assets/media/anomalies.png) |

Point Flashpoint at the logs your robot already writes, and a local web app shows:
- **Replay**: one match on a timeline, with markers for heat, brownouts, stalls and CAN dropouts.
- **History**: each motor across every match, event and season. It follows the physical motor by serial number through swaps, when the robot logs its CAN inventory.
- **Anomalies (beta)**: what looks wrong, with the evidence, for the team to triage.
- **Live CAN (experimental)**: which device drops out or reboots while you shake-test the robot.

For a deep dive into one log, it opens the match in AdvantageScope. It needs no account and sends no telemetry, and it's free and open source.

## Works with / needs

- **Robot:** CTRE Phoenix 6 devices with signal logging on (`.hoot` files), and the WPILib DataLog (`.wpilog`). Motor temperature needs Phoenix Pro on the device. REV and Phoenix 5 devices aren't supported yet.
- **Laptop:** macOS, Linux or Windows. The first `.hoot` needs internet once, to download CTRE's converter.
- **What's tested:** the [support matrix](https://igniterobotics.github.io/flashpoint/latest/support-matrix/) lists each device, log setup and platform, and how it was tested.

## Quickstart

```sh
uv tool install --python 3.13 flashpoint
flashpoint init --team <your team number>
flashpoint ingest <your logs folder>
flashpoint serve
```

Then open <http://127.0.0.1:8000/> and pick a match in Replay. The install needs [uv](https://docs.astral.sh/uv/), which brings its own Python; [Install](https://igniterobotics.github.io/flashpoint/latest/install/) has the steps for each computer, pipx, and installing without internet.

## Try it

- **No install:** the [hosted demo](https://igniterobotics.github.io/flashpoint/latest/demo/) is Replay in your browser, on real matches from team 6829's robots.
- **On your laptop:** the [sample logs](https://igniterobotics.github.io/flashpoint/latest/quickstart/#try-it-with-sample-logs) are one full match to ingest and serve, with two anomalies (beta) to look at.

## Documentation

The docs are at <https://igniterobotics.github.io/flashpoint/>:
- [Why Flashpoint](https://igniterobotics.github.io/flashpoint/latest/why/): what it does for a team, and what it doesn't do yet.
- [Quickstart](https://igniterobotics.github.io/flashpoint/latest/quickstart/) and [your first event](https://igniterobotics.github.io/flashpoint/latest/tutorials/first-event/).
- [Robot side](https://igniterobotics.github.io/flashpoint/latest/robot-side/): what to log in robot code, with Java samples.
- [Support matrix and known limits](https://igniterobotics.github.io/flashpoint/latest/support-matrix/).
- [Troubleshooting](https://igniterobotics.github.io/flashpoint/latest/troubleshooting/), when something in the first 30 minutes doesn't work.
- [CLI reference](https://igniterobotics.github.io/flashpoint/latest/reference/cli/) and [how it works](https://igniterobotics.github.io/flashpoint/latest/under-the-hood/how-it-works/).

**Getting help:** ask questions in [Q&A discussions](https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a), and report bugs with the [issue forms](https://github.com/IgniteRobotics/flashpoint/issues/new/choose). Don't attach match logs to a public issue.

## Project

Flashpoint is built by FRC team 6829, Ignite Robotics.
- **Contributing:** the [developer guide](https://igniterobotics.github.io/flashpoint/latest/under-the-hood/developer-guide/) covers building from source and running the tests; [contributing](https://igniterobotics.github.io/flashpoint/latest/contributing/) covers how changes are made.
- **Specs and changes:** [`openspec/`](https://github.com/IgniteRobotics/flashpoint/tree/main/openspec/). Current behavior is in `openspec/specs/`; work in progress is in `openspec/changes/`.
- **Decisions:** [`docs/adr/`](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/adr/README.md).
- **Legacy:** the pre-rewrite code is kept at the `legacy-2025` tag.

## License

BSD 3-Clause. See [LICENSE](https://github.com/IgniteRobotics/flashpoint/blob/main/LICENSE).
