# Robot files (robots/*.toml)

A robot file tells Flashpoint which motor is which: every CAN device on the robot (a **slot**), what the robot code calls its buses, the NetworkTables signals to bring in, which slots to compare with each other, and which device stands in for the battery. Each robot has one file in the `robots/` folder of your config folder, such as Ignite's [`2026-comp.toml`](https://github.com/IgniteRobotics/flashpoint/blob/main/config/robots/2026-comp.toml).

The quickest start is **Draft robot** in the Setup view, which writes one from a log ([Set up a robot file](../../how-to/set-up-a-robot-file.md)). Your first import usually drafts one for you.

## A whole file

Most slots are left out here; the sibling group names three drive slots that aren't shown.

```toml
robot = "2026-comp"        # keep it the same as the file name: robots/2026-comp.toml
season = 2026
project = "Robot-2026"     # the robot code's project name, from build info
battery_proxy = "gyro"     # optional: a slot id

[[slot]]
id = "drive-fl"
bus = "canivore"
model = "TalonFX"
can_id = 11
subsystem = "drivetrain"
role = "front-left drive"
gear_ratio = 6.746031746031747
swaps = [2026-03-02]       # optional: dates this motor was replaced, for logs with no CAN inventory

[[slot]]
id = "gyro"
bus = "canivore"
model = "Pigeon2"
can_id = 1
subsystem = "drivetrain"
role = "gyro"

[[bus]]                    # optional: the robot code's name for a bus, tied to its hoot bus
name = "DriveTrain"
hoot = "canivore"

[[signal]]                 # optional: a NetworkTables or wpilog value to bring in
id = "intake-at-extension-setpoint"
root = "robot"
entry = "Intake/At Extension Setpoint"
subsystem = "intake"
metric = "at_setpoint"

[[siblings]]               # optional: slots that do the same job, compared with each other
name = "drive"
slots = ["drive-fl", "drive-fr", "drive-bl", "drive-br"]
```

## Top level

| Key | Required | What it does |
|---|---|---|
| `robot` | yes | The robot's name. Keep it the same as the file name |
| `season` | yes | The season (calendar year) this file is for |
| `project` | yes | The robot code's project name, as its [build info](../../robot-side/build-info.md) logs it. Picks this file when a season has more than one robot |
| `battery_proxy` | no | A slot id whose supply voltage stands in for the battery (below) |
| `slot` | yes | The `[[slot]]` tables, one per CAN device |
| `bus` | no | The `[[bus]]` tables |
| `signal` | no | The `[[signal]]` tables |
| `siblings` | no | The `[[siblings]]` tables |

## `[[slot]]`: one CAN device

| Key | Required | What it does |
|---|---|---|
| `id` | yes | A short name, unique in the file, such as `drive-fl` |
| `bus` | yes | `rio`, `canivore` (any CANivore), or a CANivore's 32-character hex id when the robot has more than one |
| `model` | yes | The device model as the hoot names it, such as `TalonFX`, `Pigeon2` or `CANcoder` |
| `can_id` | yes | Its CAN id, 0 to 62 |
| `subsystem` | yes | The subsystem it belongs to, such as `drivetrain` or `intake`. Filters and History group by it |
| `role` | yes | What it does, in words: `front-left drive` |
| `gear_ratio` | no | Motor turns per mechanism turn, for mechanism-speed readouts |
| `swaps` | no | Dates (`2026-03-02`, unquoted) the device in this slot was replaced. For logs with no [CAN inventory](../../robot-side/can-inventory.md) only, each date starts a new history (a new legacy unit). With an inventory, Flashpoint follows serial numbers and ignores them |

## `[[bus]]`: the robot code's bus names

| Key | What it does |
|---|---|
| `name` | The bus name your robot code uses, such as `DriveTrain` |
| `hoot` | The hoot bus it is: `rio`, `canivore`, or a CANivore's hex id |

[CAN bus status](../../robot-side/bus-status.md) and [Live CAN](../../how-to/live-can-health.md) name buses the way robot code does. This table ties those names to the hoot's. Bus names must be unique.

## `[[signal]]`: a value from the log

A signal brings a NetworkTables value (subsystem state, PhotonVision) or a wpilog entry into Flashpoint.

| Key | What it does |
|---|---|
| `id` | A short name, unique in the file |
| `root` | A root from the [season file](seasons.md), such as `robot`, `photonvision` or `flashpoint` |
| `entry` | The rest of the entry name after the root |
| `subsystem` | The subsystem it belongs to |
| `component` | Optional: the part of the subsystem, such as a bus name for CAN bus status |
| `metric` | What it measures, in `snake_case`, such as `at_setpoint` |

Numbers are stored as numbers, and true/false as 1/0. A declared entry that a log doesn't have, or that isn't a number, is listed as a warning when `derive` runs. No two signals may map the same root and entry. [CAN bus status](../../robot-side/bus-status.md) shows the five signals per bus that CAN health needs.

## `[[siblings]]`: slots compared with each other

| Key | What it does |
|---|---|
| `name` | The group's name, unique in the file |
| `slots` | Two or more slot ids. A slot can be in only one group |

The SIBLING anomaly method compares each slot with the median of its group, so put motors that do the same job together: the four drive motors, or a shooter's two flywheels.

## The battery proxy

`battery_proxy` names a low-current device on the main power bus, such as the gyro, whose supply voltage stands in for the battery: Replay's battery track, brownout markers and brownout findings use it. A motor is a bad choice, because its own wiring drop under load reads as a brownout. With no proxy, Flashpoint uses the median supply voltage across devices. See [Battery voltage and the PDH](../../robot-side/battery-and-pdh.md).

## Which robot file a log uses

A log's season is the calendar year it was recorded in. Flashpoint takes the robot files for that season. If there's only one, that's the one. If there are several (a practice bot and a comp bot), it picks the one whose `project` matches the project name the robot code wrote into the log. With no match, the log's matches show robot "unknown" and their motors stay unmapped. Logs from December testing of next season's robot count as this season, so give that robot a file for this season too, or wait until January.

After editing a robot file, run `flashpoint derive --all`. You don't need to re-ingest.

## Common errors

`flashpoint doctor` checks every robot file and names the file and key:

- A key the file doesn't take (here `gearing` for `gear_ratio`, in the first slot):
  ```text
  robots/2026-comp.toml: slot.0.gearing: Extra inputs are not permitted
  ```
- A bus that isn't `rio`, `canivore` or a hex id (bus names are lower case):
  ```text
  robots/2026-comp.toml: slot.0.bus: bus must be 'rio', 'canivore', or a 32-hex CANivore id
  ```
- A battery proxy that isn't a slot id:
  ```text
  robots/2026-comp.toml: battery proxy 'pigeon' is not one of the slots
  ```
- A signal whose root the season file doesn't define (add the root to `seasons/2026.toml`):
  ```text
  robots/2026-comp.toml: 2026-comp.toml: signal 'a' names root 'robot', which season 2026 does not define
  ```
