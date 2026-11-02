# Live CAN health

The **Live CAN** view shows, while you work on the robot, which CTRE device dropped out, rebooted or faulted, and whether it was one device or the whole bus. Use it to find loose CAN and power connectors:

- **Shake test** (robot disabled): wiggle each harness, one at a time, and watch for a dropout on the device you're touching.
- **Drive test** (robot enabled): drive hard, and see which devices drop or reboot under load.

The same view serves both: every event is tagged ENABLED or DISABLED, so there's no mode to switch. A 40 ms dropout is invisible to the eye and to a 2 Hz dashboard, but it shows here, because robot code counts dropouts every loop and the view only shows what the count did.

Live CAN reads the robot, not the lake. It records nothing: when you're done, nothing has changed in the lake or the configuration. For the exact picture after a match (frame by frame, from the hoot logs), use CAN health in Replay and History.

Live CAN needs a small addition to robot code ([below](#adding-it-to-robot-code)). Until robot code publishes it, the view says the robot "is not publishing live CAN health".

## Using it

1. Start the app as usual with `flashpoint serve`, on a laptop that can reach the robot (on its Wi-Fi, by USB, or tethered).
2. Open the **LIVE CAN** tab. It connects to the robot at the addresses `flashpoint acquire` uses (from your team number, or `hosts` in `acquire.toml`). To use another address: `flashpoint serve --robot-host 10.68.29.2`.
3. The header reads **CONNECTED**, the robot's project, and the robot file it matched. Each bus shows its utilisation and error counts, then a card per device, by slot.

Each device card shows one state:

| State | Meaning |
|---|---|
| OK | Heard, nothing new |
| DROPPED | Had a dropout in the last 5 seconds |
| REBOOTED | Reset in the last 5 seconds |
| FAULTED | A fault bit is set in its latest update |
| DISCONNECTED | Robot code says it isn't heard at all |
| MISSING | In your robot file, but robot code doesn't report it: check it's wired, powered, and registered in robot code |
| UNMAPPED | Reported, but no slot in your robot file matches its bus, model and CAN id |

The card also shows the device's dropout and reset totals since Flashpoint connected to the robot, and its lowest supply voltage in the latest update.

The **event log**, newest first, has one line per event: the time, ENABLED or DISABLED, what happened, the slot and device, and the bus. Bus-wide dropouts are one line naming every device involved.

### Marks

Type what you're about to do in **MARK** ("now wiggling: FL module") and press Enter or **SET**. Every event after that carries the mark until you change it or press **CLEAR**. Marks and the event log stay in your browser only: reloading the page clears them, and other people watching don't see them. Device totals come from the server: everyone watching sees the same totals, and they survive a reload. They start again from zero when nobody has had the view open for about 30 seconds.

### Isolated or bus-wide

When several devices on one bus drop out in the same update, the cause is usually the bus (a CAN wire, the CANivore's USB, a terminator), not each device. Flashpoint applies the same rule as CAN health: a dropout is **bus-wide** when at least half of the bus's other reporting devices also dropped in that update. It's shown as one bus event and isn't counted against any device. Otherwise it's **isolated**, charged to that device.

Updates arrive about every 100 ms, so a dropout that crosses two updates can show as two isolated dropouts instead of one bus-wide one. CAN health's after-match view has the exact timing.

### When robot code restarts

Totals count from when robot code started. When they go down, Flashpoint takes it as a robot code restart: it logs one RESTART event and starts counting again, without reporting the drop as dropouts or reboots.

## The contract

Robot code publishes these NetworkTables topics, all under `/Flashpoint/Live/CANHealth/`. `<bus>` is the bus name your robot code uses (`rio`, or a CANivore's name such as `DriveTrain`); `<Model>-<id>` is the hoot device name (such as `TalonFX-11`), so it reads the same as Replay and your robot file. Totals count from when robot code started; "since the last update" values cover the time since robot code last published.

| Topic | Type | Meaning |
|---|---|---|
| `Version` | integer | Contract version: `1` |
| `Project` | string | The robot code's project name, as in the wpilog metadata (such as `Robot-2026`). Picks the robot file: the one with this `project`, latest season first |
| `Enabled` | boolean | Whether the robot is enabled |
| `<bus>/<Model>-<id>/Connected` | boolean | Whether the device is heard right now |
| `<bus>/<Model>-<id>/Dropouts` | integer | Robot loops (a running total) in which the device's watched status signal was older than 2.5 times its update period |
| `<bus>/<Model>-<id>/MaxAgeMs` | double | The oldest that signal was in any loop since the last update, in milliseconds |
| `<bus>/<Model>-<id>/Resets` | integer | Device resets robot code has seen (a running total) |
| `<bus>/<Model>-<id>/Faults` | integer | Faults seen at any point since the last update, as bits: 0 under-voltage, 1 boot during enable, 2 hardware, 3 bridge brownout, 4 bad magnet |
| `<bus>/<Model>-<id>/MinSupplyV` | double | The lowest supply voltage since the last update |
| `<bus>/Bus/Utilization` | double | Fraction of the bus in use, 0 to 1 |
| `<bus>/Bus/BusOffCount` | integer | Times the bus went off (a running count) |
| `<bus>/Bus/TxFullCount` | integer | Times the transmit buffer was full (a running count) |
| `<bus>/Bus/ReceiveErrorCount` | integer | Receive error counter |
| `<bus>/Bus/TransmitErrorCount` | integer | Transmit error counter |

A topic with the wrong type, or a negative count, is ignored and noted on its device. The rest keeps working. If `Version` isn't `1`, the view shows nothing but a message naming both versions: update Flashpoint or the robot code so they match.

**Keep these topics where they are.** Don't move them under `/Flashpoint/CANBus/` to sit next to the bus-status logger. When robot code logs NetworkTables to its wpilog, these topics are written as `NT:/Flashpoint/Live/...`, which Flashpoint's `flashpoint` season root (`/Flashpoint/`) doesn't match, so they never mix with the DataLog entries under `/Flashpoint/CANBus/` that CAN health reads.

## Adding it to robot code

[`examples/robot-java/CanHealthPublisher.java`](../../examples/robot-java/CanHealthPublisher.java) is a suggestion, not a library: copy it into your robot project and adapt it. It's written for Phoenix 6 2026 and WPILib 2026, and CI doesn't compile it, so check it against your Phoenix 6 version.

```java
private final CanHealthPublisher canHealth = new CanHealthPublisher("Robot-2026");

public void robotInit() {
  canHealth.add("DriveTrain", frontLeftDrive, frontLeftDrive.getPosition());
  canHealth.add("DriveTrain", frontLeftEncoder, frontLeftEncoder.getPosition());
  canHealth.add("DriveTrain", pigeon, pigeon.getYaw());
  canHealth.add("rio", intakeRoller, intakeRoller.getVelocity());
  // ... every TalonFX, CANcoder and Pigeon 2
  canHealth.addBus("rio");
  canHealth.addBus("DriveTrain");
}

public void robotPeriodic() {
  canHealth.periodic();
}
```

- For each device, pass the signal to watch. Pick one updated at **50 Hz or faster** (position or velocity on a motor, position on a CANcoder, yaw on a Pigeon 2): a dropout counts when that signal is older than 2.5 of its own update periods, so a slow signal only catches long dropouts.
- The sample has `add` for TalonFX, CANcoder and Pigeon 2 only. For another CTRE device (a CANdle, CANrange or TalonFXS), add an `add` overload in your copy, or its slot in your robot file shows MISSING.
- `periodic()` refreshes every watched signal once per loop with one `BaseStatusSignal.refreshAll` call per bus, keeps the totals, and publishes every 5 loops (100 ms, NetworkTables' own rate). Bus status is read every half second.
- Use the project name your wpilog metadata uses ([build info](robot-side.md#build-info)), so the view finds your robot file.

**Check the loop time before a competition.** The sample should cost well under 1 ms per loop for 30 devices, but measure it: watch the loop-time overrun warnings in the Driver Station, or log `Timer.getFPGATimestamp()` around `canHealth.periodic()`. If it costs too much, watch fewer signals or publish less often.

## Without a robot: the fake robot

Flashpoint has a fake robot that publishes the contract for Ignite's 2026 robot, for trying the view or working on it. Run it from a Flashpoint checkout ([developer guide](developer-guide.md)); a pipx install can't run it with plain `python`:

```sh
poetry run python -m flashpoint.live.fake_robot --scenario shake
flashpoint serve --robot-host 127.0.0.1
```

Scenarios: `shake` (disabled, one device at a time drops out), `drive` (enabled, bus-wide drops, reboots and bus errors), `restart` (totals climb, then robot code restarts). `--period` sets the seconds between updates. Stop it with Ctrl-C.

## Troubleshooting

| The view says | Check |
|---|---|
| CONNECTING or NOT CONNECTED, and stays there | The laptop can reach the robot (`ping 10.TE.AM.2`), robot code is running, and the address is right (`--robot-host`) |
| No robot address | Run `flashpoint init --team <number>`, or start with `--robot-host`. If you already set a team, `acquire.toml` may not load: run `flashpoint doctor` to see why |
| NOT PUBLISHING | Robot code doesn't publish under `/Flashpoint/Live/CANHealth/`: add the publisher, or check it runs |
| UNSUPPORTED VERSION | Flashpoint and the robot code use different contract versions |
| Every device UNMAPPED, and "no robot configuration has project …" | `project` in your robot file must equal the published `Project` |
| A device UNMAPPED | Its bus name isn't in the robot file's **BUSES** table, or its model or CAN id differ from the slot's |
