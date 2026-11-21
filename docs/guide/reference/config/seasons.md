# Season files (seasons/*.toml)

A season file names the entry prefixes (**roots**) that a season's logs use, so a robot file's [`[[signal]]`](robots.md#signal-a-value-from-the-log) tables can say `root = "robot"` instead of the whole name. Flashpoint ships one per season ([2026](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/defaults/seasons/2026.toml)) with the roots every WPILib robot shares. To add your robot code's own root, copy it to `seasons/2026.toml` in your config folder and set `robot`. Ignite's is [`config/seasons/2026.toml`](https://github.com/IgniteRobotics/flashpoint/blob/main/config/seasons/2026.toml):

```toml
season = 2026

[roots]
robot = "NT:Robot/m_robotContainer/"
photonvision = "NT:/photonvision/"
camera_publisher = "NT:/CameraPublisher/"
preferences = "NT:/Preferences/"
fms = "NT:/FMSInfo/"
flashpoint = "/Flashpoint/"
```

Your file replaces the bundled one whole, so keep every root in it.

| Key | Required | What it is |
|---|---|---|
| `season` | yes | The year. It must match the file name |
| `roots` | yes | The `[roots]` table |
| `robot` | no | In `[roots]`: your robot code's NetworkTables root. Until it's set, robot-code signals are reported as missing |
| `photonvision` | yes | In `[roots]`: PhotonVision's NetworkTables root |
| `camera_publisher` | yes | In `[roots]`: the camera server's root |
| `preferences` | yes | In `[roots]`: WPILib Preferences |
| `fms` | yes | In `[roots]`: the FMS info the Driver Station publishes, which gives the match |
| `flashpoint` | no | In `[roots]`: where robot code writes Flashpoint's own wpilog entries (`/Flashpoint/`): the CAN inventory and bus status |

NetworkTables entries appear in the wpilog as `NT:` followed by the topic, which is why most roots start that way. [Start a new season](../../how-to/start-a-new-season.md) covers making next year's file.

## Common errors

From `flashpoint doctor`:

- A file that leaves out bundled roots (here everything but `robot`):
  ```text
  seasons/2026.toml: roots.photonvision: Field required; roots.camera_publisher: Field required; roots.preferences: Field required; roots.fms: Field required
  ```
- A robot file whose signal uses a root this file doesn't have: see [robot file errors](robots.md#common-errors).
