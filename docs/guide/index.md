# Flashpoint

FRC robot telemetry: import WPILib .wpilog and CTRE .hoot logs, and track every motor's health across your robot's life. Runs offline on a pit laptop.

**Replay** puts one match on a timeline, with markers for heat, brownouts, stalls and CAN dropouts.

![Replay view: one match's motor currents, temperatures and battery voltage on a shared timeline](assets/media/replay.png)

**History** follows each motor across every match, event and season. When the robot logs its CAN inventory, it tracks the physical motor by serial number through swaps.

![History view: one motor's temperature and current across every match of the season](assets/media/history.png)

**Anomalies (beta)** lists what looks wrong, with the evidence, for the team to triage.

![Anomalies view: flagged motors with their evidence and triage state](assets/media/anomalies.png)

[Why Flashpoint](why.md) says what it does for a team, and what it doesn't do yet.

**[Open the hosted demo](demo.md)**: Replay in your browser, on real matches from team 6829's robots, with nothing to install.

## Start here

<div class="grid cards" markdown>

-   **[Quickstart](quickstart.md)**

    ---

    Install, then three commands to get your own match into Replay. Or try it on sample logs first.

-   **[Install](install/index.md)**

    ---

    Step by step for macOS, Linux and Windows, including installing without internet.

-   **[Your first event](tutorials/first-event.md)**

    ---

    One match from logs to Replay, History and an anomaly (beta), the way you'd do it at an event.

-   **[Robot side](robot-side/index.md)**

    ---

    What to log in robot code so Flashpoint can do more, with Java samples that compile.

</div>

## Elsewhere in the repository

- [`deploy/`](https://github.com/IgniteRobotics/flashpoint/blob/main/deploy/README.md): how to install the automatic-import service by hand (the Setup view can do it for you).
- [`notebooks/`](https://github.com/IgniteRobotics/flashpoint/blob/main/notebooks/README.md): marimo notebooks for questions the app doesn't answer yet.
- [`examples/robot-java/`](https://github.com/IgniteRobotics/flashpoint/tree/main/examples/robot-java): the robot-side samples, as a small robot project that compiles.
- [`docs/adr/`](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/adr/README.md): architecture decisions.
- [`docs/rewrite/`](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/rewrite/README.md): the research and roadmap behind the rewrite.
