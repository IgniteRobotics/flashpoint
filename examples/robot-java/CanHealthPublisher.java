/*
 * A SUGGESTION, NOT A LIBRARY. Copy this into your robot project and adapt it.
 *
 * Publishes Flashpoint's live CAN health contract, version 1, over NetworkTables under
 * /Flashpoint/Live/CANHealth/ (see docs/guide/live-can-health.md for the full table).
 * Written for Phoenix 6 2026 and WPILib 2026. CI does not compile it (that needs Gradle and
 * WPILib), so check it against your Phoenix 6 version before an event, and check your loop
 * timing with it running.
 *
 * Robot code counts; Flashpoint only compares totals between updates. A dropout is a loop in
 * which a device's watched status signal is older than 2.5 times its update period: watch a
 * signal updated at 50 Hz or faster (position or velocity on motors, position on encoders, yaw
 * on a Pigeon 2).
 *
 * Usage, in Robot.java:
 *
 *   private final CanHealthPublisher canHealth = new CanHealthPublisher("Robot-2026");
 *
 *   public void robotInit() {
 *     canHealth.add("DriveTrain", drivetrain.getModule(0).getDriveMotor(), ...);  // and so on
 *     canHealth.addBus("rio");
 *     canHealth.addBus("DriveTrain");
 *   }
 *
 *   public void robotPeriodic() {
 *     canHealth.periodic();
 *   }
 *
 * Don't move these topics under /Flashpoint/CANBus/: the wpilog copy of NetworkTables topics is
 * "NT:/Flashpoint/Live/...", which Flashpoint's season root /Flashpoint/ must not match.
 */
package frc.robot.flashpoint;

import com.ctre.phoenix6.BaseStatusSignal;
import com.ctre.phoenix6.CANBus;
import com.ctre.phoenix6.CANBus.CANBusStatus;
import com.ctre.phoenix6.StatusSignal;
import com.ctre.phoenix6.hardware.CANcoder;
import com.ctre.phoenix6.hardware.ParentDevice;
import com.ctre.phoenix6.hardware.Pigeon2;
import com.ctre.phoenix6.hardware.TalonFX;
import edu.wpi.first.networktables.BooleanPublisher;
import edu.wpi.first.networktables.DoublePublisher;
import edu.wpi.first.networktables.IntegerPublisher;
import edu.wpi.first.networktables.NetworkTableInstance;
import edu.wpi.first.wpilibj.DriverStation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Publishes per-device CAN health totals for Flashpoint's Live CAN view. */
public final class CanHealthPublisher {
  private static final String ROOT = "/Flashpoint/Live/CANHealth/";
  private static final long CONTRACT_VERSION = 1;
  private static final double GAP_FACTOR = 2.5; // same as Flashpoint's can-health gap rule
  private static final int PUBLISH_EVERY_LOOPS = 5; // 100 ms at 50 Hz, NetworkTables' own rate
  private static final int BUS_STATUS_EVERY_LOOPS = 25; // 0.5 s: getStatus() talks to the driver

  // Fault bits, as Flashpoint reads them.
  private static final int UNDER_VOLTAGE = 1 << 0;
  private static final int BOOT_DURING_ENABLE = 1 << 1;
  private static final int HARDWARE = 1 << 2;
  private static final int BRIDGE_BROWNOUT = 1 << 3;
  private static final int BAD_MAGNET = 1 << 4;

  /** One CTRE device and the signals read for it. */
  private static final class Device {
    final ParentDevice device;
    final BaseStatusSignal watched;
    final BaseStatusSignal supply;
    final Map<StatusSignal<Boolean>, Integer> faults; // fault signal -> its bit
    final BooleanPublisher connected;
    final IntegerPublisher dropoutsPub;
    final DoublePublisher maxAgeMs;
    final IntegerPublisher resetsPub;
    final IntegerPublisher faultsPub;
    final DoublePublisher minSupplyV;

    long dropouts; // running totals since robot code started
    long resets;
    double worstAgeMs; // since the last publish
    int faultBits;
    double lowestSupply = Double.MAX_VALUE;

    Device(
        NetworkTableInstance nt,
        String bus,
        String model,
        ParentDevice device,
        BaseStatusSignal watched,
        BaseStatusSignal supply,
        Map<StatusSignal<Boolean>, Integer> faults) {
      this.device = device;
      this.watched = watched;
      this.supply = supply;
      this.faults = faults;
      String prefix = ROOT + bus + "/" + model + "-" + device.getDeviceID() + "/";
      connected = nt.getBooleanTopic(prefix + "Connected").publish();
      dropoutsPub = nt.getIntegerTopic(prefix + "Dropouts").publish();
      maxAgeMs = nt.getDoubleTopic(prefix + "MaxAgeMs").publish();
      resetsPub = nt.getIntegerTopic(prefix + "Resets").publish();
      faultsPub = nt.getIntegerTopic(prefix + "Faults").publish();
      minSupplyV = nt.getDoubleTopic(prefix + "MinSupplyV").publish();
    }

    /** Called every loop, after the bus's signals were refreshed. */
    void check() {
      double ageMs = watched.getTimestamp().getLatency() * 1000.0;
      double hz = watched.getAppliedUpdateFrequency();
      if (hz > 0 && ageMs > GAP_FACTOR * 1000.0 / hz) {
        dropouts++;
      }
      worstAgeMs = Math.max(worstAgeMs, ageMs);
      if (device.hasResetOccurred()) { // latched by Phoenix; reading it clears it
        resets++;
      }
      if (supply.getStatus().isOK()) {
        lowestSupply = Math.min(lowestSupply, supply.getValueAsDouble());
      }
      for (Map.Entry<StatusSignal<Boolean>, Integer> fault : faults.entrySet()) {
        if (Boolean.TRUE.equals(fault.getKey().getValue())) {
          faultBits |= fault.getValue();
        }
      }
    }

    /** Publishes, then starts the next "since the last publish" window. */
    void publish() {
      connected.set(device.isConnected());
      dropoutsPub.set(dropouts);
      maxAgeMs.set(worstAgeMs);
      resetsPub.set(resets);
      faultsPub.set(faultBits);
      minSupplyV.set(lowestSupply == Double.MAX_VALUE ? 0.0 : lowestSupply);
      worstAgeMs = 0;
      faultBits = 0;
      lowestSupply = Double.MAX_VALUE;
    }
  }

  /** One bus's status topics. */
  private record Bus(
      CANBus bus,
      DoublePublisher utilization,
      IntegerPublisher busOff,
      IntegerPublisher txFull,
      IntegerPublisher rec,
      IntegerPublisher tec) {}

  private final NetworkTableInstance nt = NetworkTableInstance.getDefault();
  private final Map<String, List<Device>> devicesByBus = new LinkedHashMap<>();
  private final Map<String, BaseStatusSignal[]> signalsByBus = new LinkedHashMap<>();
  private final List<Bus> buses = new ArrayList<>();
  private final BooleanPublisher enabled;
  private int loop;

  /** project: the robot code's project name, as in the wpilog metadata ("Robot-2026"). */
  public CanHealthPublisher(String project) {
    nt.getIntegerTopic(ROOT + "Version").publish().set(CONTRACT_VERSION);
    nt.getStringTopic(ROOT + "Project").publish().set(project);
    enabled = nt.getBooleanTopic(ROOT + "Enabled").publish();
  }

  /** A Talon FX on `bus` (the name your robot code uses: "rio" or the CANivore's name). */
  public void add(String bus, TalonFX motor, BaseStatusSignal watched) {
    Map<StatusSignal<Boolean>, Integer> faults = new LinkedHashMap<>();
    faults.put(motor.getFault_Undervoltage(), UNDER_VOLTAGE);
    faults.put(motor.getFault_BootDuringEnable(), BOOT_DURING_ENABLE);
    faults.put(motor.getFault_Hardware(), HARDWARE);
    faults.put(motor.getFault_BridgeBrownout(), BRIDGE_BROWNOUT);
    add(bus, "TalonFX", motor, watched, motor.getSupplyVoltage(), faults);
  }

  /** A CANcoder; watch its position. */
  public void add(String bus, CANcoder encoder, BaseStatusSignal watched) {
    Map<StatusSignal<Boolean>, Integer> faults = new LinkedHashMap<>();
    faults.put(encoder.getFault_Undervoltage(), UNDER_VOLTAGE);
    faults.put(encoder.getFault_BootDuringEnable(), BOOT_DURING_ENABLE);
    faults.put(encoder.getFault_Hardware(), HARDWARE);
    faults.put(encoder.getFault_BadMagnet(), BAD_MAGNET);
    add(bus, "CANcoder", encoder, watched, encoder.getSupplyVoltage(), faults);
  }

  /** A Pigeon 2; watch its yaw. */
  public void add(String bus, Pigeon2 gyro, BaseStatusSignal watched) {
    Map<StatusSignal<Boolean>, Integer> faults = new LinkedHashMap<>();
    faults.put(gyro.getFault_Undervoltage(), UNDER_VOLTAGE);
    faults.put(gyro.getFault_BootDuringEnable(), BOOT_DURING_ENABLE);
    faults.put(gyro.getFault_Hardware(), HARDWARE);
    add(bus, "Pigeon2", gyro, watched, gyro.getSupplyVoltage(), faults);
  }

  private void add(
      String bus,
      String model,
      ParentDevice device,
      BaseStatusSignal watched,
      BaseStatusSignal supply,
      Map<StatusSignal<Boolean>, Integer> faults) {
    List<Device> list = devicesByBus.computeIfAbsent(bus, b -> new ArrayList<>());
    list.add(new Device(nt, bus, model, device, watched, supply, faults));
    List<BaseStatusSignal> signals = new ArrayList<>();
    for (Device d : list) {
      signals.add(d.watched);
      signals.add(d.supply);
      signals.addAll(d.faults.keySet());
    }
    signalsByBus.put(bus, signals.toArray(new BaseStatusSignal[0]));
  }

  /** Publishes `name`'s bus status under <name>/Bus/ (the same five values as the wpilog logger). */
  public void addBus(String name) {
    String prefix = ROOT + name + "/Bus/";
    buses.add(new Bus(
        new CANBus(name),
        nt.getDoubleTopic(prefix + "Utilization").publish(),
        nt.getIntegerTopic(prefix + "BusOffCount").publish(),
        nt.getIntegerTopic(prefix + "TxFullCount").publish(),
        nt.getIntegerTopic(prefix + "ReceiveErrorCount").publish(),
        nt.getIntegerTopic(prefix + "TransmitErrorCount").publish()));
  }

  /** Call from robotPeriodic(), once per loop. */
  public void periodic() {
    for (BaseStatusSignal[] signals : signalsByBus.values()) {
      BaseStatusSignal.refreshAll(signals); // one call per bus, every loop
    }
    for (List<Device> list : devicesByBus.values()) {
      for (Device d : list) {
        d.check();
      }
    }
    loop++;
    if (loop % PUBLISH_EVERY_LOOPS == 0) {
      enabled.set(DriverStation.isEnabled());
      for (List<Device> list : devicesByBus.values()) {
        for (Device d : list) {
          d.publish();
        }
      }
    }
    if (loop % BUS_STATUS_EVERY_LOOPS == 0) {
      for (Bus b : buses) {
        CANBusStatus status = b.bus().getStatus();
        if (!status.Status.isOK()) {
          continue; // skip a failed read rather than publishing zeros
        }
        b.utilization().set(status.BusUtilization);
        b.busOff().set(status.BusOffCount);
        b.txFull().set(status.TxFullCount);
        b.rec().set(status.REC);
        b.tec().set(status.TEC);
      }
    }
  }
}
