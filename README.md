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

Needs Python 3.11–3.13, [Poetry](https://python-poetry.org/), the [GitHub CLI](https://cli.github.com/) (logged in with `gh auth login`), and access to the IgniteRobotics organization.

```sh
gh repo clone IgniteRobotics/flashpoint -- -b rewrite
cd flashpoint
poetry install
poetry run flashpoint ingest path/to/logs/
poetry run flashpoint serve        # open http://127.0.0.1:8000/
```

To install it as a regular command on a pit laptop, see the [user guide](docs/guide/user-guide.md#install).

## Documentation

- [User guide](docs/guide/user-guide.md): install, import logs, and use Replay, History, and Anomalies.
- [Automatic import](docs/guide/auto-import.md): pull logs from the robot and USB sticks on their own, and back up the lake.
- [CLI reference](docs/guide/cli-reference.md): every `flashpoint` command.
- [How it works](docs/guide/how-it-works.md): the pipeline, the lake, matches, motors, and detection.
- [Developer guide](docs/guide/developer-guide.md): build, test, CI, and how changes are made.

## Project

- **Code:** the `flashpoint` package in `src/flashpoint/`.
- **Specs and changes:** [`openspec/`](openspec/). Current behavior is in `openspec/specs/`; work in progress is in `openspec/changes/`.
- **Decisions:** [`docs/adr/`](docs/adr/README.md). **Research and roadmap:** [`docs/rewrite/`](docs/rewrite/README.md).
- **Legacy:** the pre-rewrite code is kept at the `legacy-2025` tag.
