# Flashpoint documentation

| Read | If you want to |
|---|---|
| [Setting up Flashpoint for your team](tutorials/team-setup.md) | Start here if your team is new to Flashpoint: install, configure, robot code, logs, backups and branding, in order |
| [User guide](user-guide.md) | Install Flashpoint, import logs, and use Replay, History, Anomalies, Setup and Live CAN |
| [Robot-side logging](robot-side/index.md) | Add the CAN inventory and bus-status loggers to robot code (Java samples) |
| [Live CAN health](how-to/live-can-health.md) | Watch CAN dropouts, reboots and faults live during shake and drive tests, and add the publisher to robot code |
| [Automatic import](how-to/auto-import.md) | Have logs pulled from the robot and USB sticks on their own, and back up the lake |
| [CLI reference](reference/cli.md) | Look up a `flashpoint` command and its options |
| [How it works](under-the-hood/how-it-works.md) | Understand the pipeline, the lake, matches, motors, and detection, and query the data yourself |
| [Developer guide](under-the-hood/developer-guide.md) | Build from source, run the tests, and make a change |

Elsewhere in the repository:
- [`deploy/`](https://github.com/IgniteRobotics/flashpoint/blob/main/deploy/README.md): how to install the automatic-import service by hand (the Setup view can do it for you).
- [`notebooks/`](https://github.com/IgniteRobotics/flashpoint/blob/main/notebooks/README.md): marimo notebooks for questions the app doesn't answer yet.
- [`examples/robot-java/`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/CanHealthPublisher.java): the suggested live CAN health publisher for robot code.
- [`docs/adr/`](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/adr/README.md): architecture decisions.
- [`docs/rewrite/`](https://github.com/IgniteRobotics/flashpoint/blob/main/docs/rewrite/README.md): the research and roadmap behind the rewrite.
