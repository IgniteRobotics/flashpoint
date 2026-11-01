# Flashpoint documentation

| Read | If you want to |
|---|---|
| [Setting up Flashpoint for your team](team-setup.md) | Start here if your team is new to Flashpoint: install, configure, robot code, logs, backups and branding, in order |
| [User guide](user-guide.md) | Install Flashpoint, import logs, and use Replay, History, Anomalies, Setup and Live CAN |
| [Robot-side logging](robot-side.md) | Add the CAN inventory and bus-status loggers to robot code (Java samples) |
| [Live CAN health](live-can-health.md) | Watch CAN dropouts, reboots and faults live during shake and drive tests, and add the publisher to robot code |
| [Automatic import](auto-import.md) | Have logs pulled from the robot and USB sticks on their own, and back up the lake |
| [CLI reference](cli-reference.md) | Look up a `flashpoint` command and its options |
| [How it works](how-it-works.md) | Understand the pipeline, the lake, matches, motors, and detection, and query the data yourself |
| [Developer guide](developer-guide.md) | Build from source, run the tests, and make a change |

Elsewhere in the repository:
- [`deploy/`](../../deploy/README.md): how to install the automatic-import service by hand (the Setup view can do it for you).
- [`notebooks/`](../../notebooks/README.md): marimo notebooks for questions the app doesn't answer yet.
- [`examples/robot-java/`](../../examples/robot-java/CanHealthPublisher.java): the suggested live CAN health publisher for robot code.
- [`docs/adr/`](../adr/README.md): architecture decisions.
- [`docs/rewrite/`](../rewrite/README.md): the research and roadmap behind the rewrite.
