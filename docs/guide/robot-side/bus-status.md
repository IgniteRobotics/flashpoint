# CAN bus status

CAN bus status gives CAN health each bus's utilisation and error counts, per match phase.

Phoenix 6 reports each bus's health. [`CanBusStatusLogger.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/CanBusStatusLogger.java) writes five values per bus, every half second, under `/Flashpoint/CANBus/<name>/`, where `<name>` is the bus name your robot code uses (`rio`, or the CANivore's name such as `DriveTrain`):

| Entry | Type | Meaning |
|---|---|---|
| `Utilization` | double | Fraction of the bus in use, 0 to 1 |
| `BusOffCount` | integer | Times the bus went off (a running count) |
| `TxFullCount` | integer | Times the transmit buffer was full (a running count) |
| `ReceiveErrorCount` | integer | Receive error counter |
| `TransmitErrorCount` | integer | Transmit error counter |

```java
/*
 * A suggestion, not a library: copy it into your robot project and adapt it.
 * Tested with WPILib 2026.2.1, Phoenix 6 26.3.0
 * Docs: https://igniterobotics.github.io/flashpoint/latest/robot-side/bus-status/
 */
package frc.robot.flashpoint;

import com.ctre.phoenix6.CANBus;
import com.ctre.phoenix6.CANBus.CANBusStatus;
import edu.wpi.first.util.datalog.DataLog;
import edu.wpi.first.util.datalog.DoubleLogEntry;
import edu.wpi.first.util.datalog.IntegerLogEntry;
import edu.wpi.first.wpilibj.DataLogManager;
import edu.wpi.first.wpilibj.Notifier;
import java.util.List;

/** Logs CAN bus status to /Flashpoint/CANBus/<name>/ every half second. */
public final class CanBusStatusLogger {
  private record Bus(
      CANBus bus,
      DoubleLogEntry utilization,
      IntegerLogEntry busOff,
      IntegerLogEntry txFull,
      IntegerLogEntry rec,
      IntegerLogEntry tec) {}

  private final List<Bus> buses;
  private final Notifier notifier = new Notifier(this::log);

  /** names: the buses as your robot code names them, e.g. "rio" and "DriveTrain". */
  public CanBusStatusLogger(String... names) {
    DataLog log = DataLogManager.getLog();
    buses = java.util.Arrays.stream(names)
        .map(name -> {
          String prefix = "/Flashpoint/CANBus/" + name + "/";
          return new Bus(
              new CANBus(name),
              new DoubleLogEntry(log, prefix + "Utilization"),
              new IntegerLogEntry(log, prefix + "BusOffCount"),
              new IntegerLogEntry(log, prefix + "TxFullCount"),
              new IntegerLogEntry(log, prefix + "ReceiveErrorCount"),
              new IntegerLogEntry(log, prefix + "TransmitErrorCount"));
        })
        .toList();
    notifier.setName("CanBusStatusLogger");
    notifier.startPeriodic(0.5);
  }

  private void log() {
    for (Bus b : buses) {
      CANBusStatus status = b.bus().getStatus();
      if (!status.Status.isOK()) {
        continue; // skip a failed read rather than logging zeros
      }
      b.utilization().append(status.BusUtilization);
      b.busOff().append(status.BusOffCount);
      b.txFull().append(status.TxFullCount);
      b.rec().append(status.REC);
      b.tec().append(status.TEC);
    }
  }
}
```

Create it once in `robotInit()`, for example `new CanBusStatusLogger("rio", "DriveTrain");`, and keep a reference so the `Notifier` isn't garbage collected.

Then tell Flashpoint about the bus names in your robot file. In the Setup view, the robot's **BUSES** table ties each name to its hoot bus (`rio`, or `canivore`). Then add five `[[signal]]` tables per bus (see the [robot file reference](../reference/config/robots.md)). **Draft robot** doesn't add them for you. Each one uses `root = "flashpoint"`, `entry = "CANBus/<name>/<Entry>"`, `subsystem = "can"`, `component = "<name>"` (the same name as in BUSES), and one `metric` from `utilization`, `bus_off_count`, `tx_full_count`, `receive_error_count` and `transmit_error_count`. CAN health finds them only by those `subsystem`, `component` and `metric` values. Ignite's [`config/robots/2026-comp.toml`](https://github.com/IgniteRobotics/flashpoint/blob/main/config/robots/2026-comp.toml) has a full set for `rio` and `DriveTrain`:

```toml
[[signal]]
id = "can-rio-utilization"
root = "flashpoint"
entry = "CANBus/rio/Utilization"
subsystem = "can"
component = "rio"
metric = "utilization"
```

A bus whose status isn't logged reads "not logged" in CAN health, never zero.
