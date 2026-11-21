// Tested with WPILib 2026.2.1, Phoenix 6 26.3.0
package frc.robot;

import com.ctre.phoenix6.CANBus;
import com.ctre.phoenix6.hardware.CANcoder;
import com.ctre.phoenix6.hardware.Pigeon2;
import com.ctre.phoenix6.hardware.TalonFX;
import edu.wpi.first.wpilibj.TimedRobot;
import frc.robot.flashpoint.BuildInfo;
import frc.robot.flashpoint.CanBusStatusLogger;
import frc.robot.flashpoint.CanHealthPublisher;
import frc.robot.flashpoint.CanInventoryLogger;
import frc.robot.flashpoint.SignalLoggerSetup;
import frc.robot.flashpoint.SignalRates;

/** Every Flashpoint robot-side sample, wired in one place. Your devices go where these are. */
public class Robot extends TimedRobot {
  private final CANBus driveTrain = new CANBus("DriveTrain");
  private final TalonFX frontLeftDrive = new TalonFX(11, driveTrain);
  private final CANcoder frontLeftEncoder = new CANcoder(21, driveTrain);
  private final Pigeon2 pigeon = new Pigeon2(30, driveTrain);
  private final TalonFX intakeRoller = new TalonFX(5);

  private final CanHealthPublisher canHealth = new CanHealthPublisher("Robot-2026");
  private CanBusStatusLogger canBusStatus;

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

  @Override
  public void robotPeriodic() {
    canHealth.periodic();
  }
}
