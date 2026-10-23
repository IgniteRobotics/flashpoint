# Flashpoint Robot Telemetry Platform

> ## flashpoint
> *also flash point.*
> ### noun
> 1: Also Physical Chemistry. the lowest temperature at which a liquid in a specified apparatus will give off sufficient vapor to ignite momentarily on application of a flame.
>
> 2: a critical point or stage at which something or someone suddenly causes or creates some significant action:

Flashpoint is Ignite Robotics' robot telemetry platform. It reads the logs an FRC robot records, WPILib DataLogs (`.wpilog`) and CTRE Phoenix 6 signal logs (`.hoot`), and tracks the health of every motor across the robot's whole life:
- temperature, current, energy, and stall time;
- brownouts;
- CAN dropouts and reboots.

It follows each physical motor by its serial number through swaps and rebuilds. It flags anomalies before they cost a match, with the goal of predicting failures before they happen. Everything runs offline on a pit laptop.

You see the results in a local web app:
- **Replay**: one match, with markers for heat, brownouts, stalls, and CAN faults;
- **History**: every motor across matches, events, and seasons;
- **Anomalies**: what Flashpoint flagged, with evidence and triage.

For a deep dive into a single log, it opens AdvantageScope.

## Quickstart

Needs Python 3.11–3.13 and [pipx](https://pipx.pypa.io/).

> Flashpoint isn't on PyPI yet, so `pipx install flashpoint` doesn't work today. Until it is, [install from a checkout](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/user-guide.md#installing-from-a-checkout).

```sh
pipx install flashpoint
flashpoint init --team <your team number>
flashpoint ingest path/to/logs/
flashpoint serve                   # open http://127.0.0.1:8000/
```

Then open **Setup** in the app: it checks the install, and drafts your robot's configuration from a log. The [user guide](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/user-guide.md#install) has the details, including installing without internet.

**From source** (needs [Poetry](https://python-poetry.org/) 2; see the [developer guide](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/developer-guide.md)):

```sh
git clone https://github.com/IgniteRobotics/flashpoint.git
cd flashpoint
poetry install
export FLASHPOINT_CONFIG=config    # the repo's config/ is Ignite's configuration
poetry run flashpoint doctor
```

## Documentation

- [User guide](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/user-guide.md): install, import logs, and use Replay, History, and Anomalies.
- [Automatic import](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/auto-import.md): pull logs from the robot and USB sticks on their own, and back up the lake.
- [Robot-side logging](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/robot-side.md): Java samples for the CAN inventory and bus-status loggers.
- [CLI reference](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/cli-reference.md): every `flashpoint` command.
- [How it works](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/how-it-works.md): the pipeline, the lake, matches, motors, and detection.
- [Developer guide](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/guide/developer-guide.md): build, test, CI, and how changes are made.

## Project

- **Code:** the `flashpoint` package in `src/flashpoint/`.
- **Specs and changes:** [`openspec/`](https://github.com/IgniteRobotics/flashpoint/tree/main/openspec/). Current behavior is in `openspec/specs/`; work in progress is in `openspec/changes/`.
- **Decisions:** [`docs/adr/`](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/adr/README.md). **Research and roadmap:** [`docs/rewrite/`](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/rewrite/README.md).
- **Legacy:** the pre-rewrite code is kept at the `legacy-2025` tag.

## License

BSD 3-Clause. See [LICENSE](https://github.com/IgniteRobotics/flashpoint/blob/main/LICENSE).
