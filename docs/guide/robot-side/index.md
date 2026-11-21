# Robot side: what to log

Flashpoint reads the logs your robot already writes. Most of what it can tell you depends on what robot code logs. The list below is ranked: start at the top, and stop wherever your team runs out of time. The first two rows are what most WPILib and Phoenix 6 robots already do.

The samples are **Java only** (WPILib and Phoenix 6). C++ and Python teams can log the same entries; the entry names and formats on each page are what Flashpoint reads.

| # | Add | What Flashpoint gains | Without it | Sample |
|---|---|---|---|---|
| 1 | [Phoenix 6 signal logging](signal-logging.md) (hoots), and a check for a hoot after each match | Per-motor current, voltage, velocity and CAN dropouts: Replay's motor tracks, History, and most anomaly findings | No CTRE motor data at all | [`SignalLoggerSetup.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/SignalLoggerSetup.java) |
| 2 | [The WPILib DataLog](datalog.md), with Driver Station data | Match identity (event and match number), match phases, and the clock that lines hoots up with the match. Hoots need it: a session with hoots but no wpilog gets no motor history | Hoot-only sessions; ingest warns, and motors get no history | [`SignalLoggerSetup.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/SignalLoggerSetup.java) |
| 3 | [Status-signal rates](signal-rates.md) of 50 Hz for current, voltage, velocity and temperature, set before `optimizeBusUtilization()` | Current spikes and stalls are visible, and timed to 20 ms | After the optimisation, signals you didn't set drop to 4 Hz; Replay notes which slots log current slower than 20 Hz | [`SignalRates.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/SignalRates.java) |
| 4 | [Phoenix Pro](pro-and-temperature.md) on the motors | Motor temperature in the hoot: thermal tracks, temperature limits and thermal findings | No temperature; everything else still works | (a license, not code) |
| 5 | [Build info](build-info.md) | The robot code's project name, which picks the robot file when a season has two robots (practice and competition) | With two robots in a season, matches go to robot "unknown" | [`BuildInfo.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/BuildInfo.java) |
| 6 | [The CAN inventory](can-inventory.md) | Each motor's serial number, so its history follows the physical motor through swaps | History is kept per slot (CAN id), split at the swap dates in your robot file | [`CanInventoryLogger.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/CanInventoryLogger.java) |
| 7 | [CAN bus status](bus-status.md) | Bus utilisation and error counts per bus, per match phase, in CAN health | Those columns read "not logged" | [`CanBusStatusLogger.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/CanBusStatusLogger.java) |
| 8 | [Battery voltage and the PDH](battery-and-pdh.md) | Today, nothing: Flashpoint uses a battery proxy. Entry names on that page are planned, not read yet | Nothing lost today; declare a proxy device in your robot file | none yet |
| 9 | [Free disk space](disk-space.md) on the roboRIO and its USB stick | Logs keep being written all event | A full disk stops hoots, and WPILib deletes old wpilogs to make room | (a pit check, not code) |

To watch CAN dropouts live during shake and drive tests, robot code also publishes running totals over NetworkTables: see [Live CAN health](../how-to/live-can-health.md).

## The sample project

[`examples/robot-java/`](https://github.com/IgniteRobotics/flashpoint/tree/main/examples/robot-java) is a minimal WPILib robot project with every sample in `src/main/java/frc/robot/flashpoint/`. The samples are suggestions, not a library: copy the files you want into your robot project (same package, or change the `package` line) and adapt them. Each file says which WPILib and Phoenix 6 versions it was tested with, on a line at the top. Check them against your versions before an event.

[`Robot.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/Robot.java) wires all of them in one place:

```java
  @Override
  public void robotInit() {
    SignalLoggerSetup.start();
    BuildInfo.log();

    SignalRates.apply(frontLeftDrive, intakeRoller);

    CanInventoryLogger.start();
    canBusStatus = new CanBusStatusLogger("rio", "DriveTrain");

    canHealth.add("DriveTrain", frontLeftDrive, frontLeftDrive.getPosition());
    canHealth.add("DriveTrain", frontLeftEncoder, frontLeftEncoder.getPosition());
    canHealth.add("DriveTrain", pigeon, pigeon.getYaw());
    canHealth.add("rio", intakeRoller, intakeRoller.getVelocity());
    canHealth.addBus("rio");
    canHealth.addBus("DriveTrain");
  }
```

Order matters in two places: `SignalLoggerSetup.start()` goes first, because everything after it writes to the log it starts; and `SignalRates.apply(...)` goes after you configure your motors.

To check that the project compiles (JDK 17; the first run needs network access, or the WPILib installer's `~/wpilib/2026/maven`):

```sh
cd examples/robot-java
./gradlew compileJava
```

## What Flashpoint doesn't read yet

Some fields the samples log are kept but not used yet:

- the `firmware` field in the CAN inventory ([#87](https://github.com/IgniteRobotics/flashpoint/issues/87));
- fault signals in the hoots ([#86](https://github.com/IgniteRobotics/flashpoint/issues/86));
- battery voltage and total current, whatever their entry names ([#6](https://github.com/IgniteRobotics/flashpoint/issues/6)).

## AdvantageKit

AdvantageKit logs are **not tested on real logs** yet. See [AdvantageKit](advantagekit.md) and the [support matrix](../support-matrix.md) for what works today.
