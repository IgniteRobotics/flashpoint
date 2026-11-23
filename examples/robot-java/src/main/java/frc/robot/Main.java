// Tested with WPILib 2026.2.1, Phoenix 6 26.3.0
package frc.robot;

import edu.wpi.first.wpilibj.RobotBase;

/** WPILib's entry point. Don't add anything here; the samples are wired in {@link Robot}. */
public final class Main {
  private Main() {}

  public static void main(String... args) {
    RobotBase.startRobot(Robot::new);
  }
}
