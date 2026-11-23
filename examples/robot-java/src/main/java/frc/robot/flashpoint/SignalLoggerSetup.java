/*
 * A suggestion, not a library: copy it into your robot project and adapt it.
 * Tested with WPILib 2026.2.1, Phoenix 6 26.3.0
 * Docs: https://igniterobotics.github.io/flashpoint/latest/robot-side/signal-logging/
 *
 * Checked 2026-11-21 against the WPILib DataLog page and CTRE's Phoenix 6 signal logging page:
 * - DataLogManager writes to a "logs" folder on the USB stick if one is plugged in, otherwise
 *   to /home/lvuser/logs, and records NetworkTables changes.
 * - DriverStation.startDataLog adds DS control and joystick data (DataLogManager alone doesn't).
 * - Phoenix 6 signal logging starts on its own on a roboRIO 2 (or a roboRIO 1 with a USB stick)
 *   once the robot is enabled or the Driver Station connects. CTRE's page doesn't name the
 *   default folder, so this sets it: one hoot per CAN bus, in a subfolder named for the time the
 *   robot program started.
 */
package frc.robot.flashpoint;

import com.ctre.phoenix6.SignalLogger;
import edu.wpi.first.wpilibj.DataLogManager;
import edu.wpi.first.wpilibj.DriverStation;
import edu.wpi.first.wpilibj.RobotBase;

/** Starts the WPILib DataLog and points Phoenix 6 signal logging at the same folder. */
public final class SignalLoggerSetup {
  private SignalLoggerSetup() {}

  /** Call first thing in robotInit(), before anything else logs. */
  public static void start() {
    DataLogManager.start();
    DriverStation.startDataLog(DataLogManager.getLog());
    if (RobotBase.isReal()) {
      // Hoots land next to the wpilog: on the USB stick when there is one, otherwise
      // /home/lvuser/logs. Flashpoint's robot pull and USB import look in both.
      SignalLogger.setPath(DataLogManager.getLogDir());
    }
  }
}
