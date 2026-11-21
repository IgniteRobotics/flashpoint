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
