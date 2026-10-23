# Flashpoint documentation

| Read | If you want to |
|---|---|
| [User guide](user-guide.md) | Install Flashpoint, import logs, and use Replay, History, and Anomalies. Start here |
| [Robot-side logging](robot-side.md) | Add the CAN inventory and bus-status loggers to robot code (Java samples) |
| [Automatic import](auto-import.md) | Have logs pulled from the robot and USB sticks on their own, and back up the lake |
| [CLI reference](cli-reference.md) | Look up a `flashpoint` command and its options |
| [How it works](how-it-works.md) | Understand the pipeline, the lake, matches, motors, and detection, and query the data yourself |
| [Developer guide](developer-guide.md) | Build from source, run the tests, and make a change |

Elsewhere in the repository:
- [`deploy/`](../../deploy/README.md): how to install the automatic-import service by hand (the Setup view can do it for you).
- [`notebooks/`](../../notebooks/README.md): marimo notebooks for questions the app doesn't answer yet.
- [`docs/adr/`](../adr/README.md): architecture decisions.
- [`docs/rewrite/`](../rewrite/README.md): the research and roadmap behind the rewrite.
