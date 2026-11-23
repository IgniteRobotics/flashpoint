/*
 * A suggestion, not a library: copy it into your robot project and adapt it.
 * Tested with WPILib 2026.2.1, Phoenix 6 26.3.0
 * Docs: https://igniterobotics.github.io/flashpoint/latest/robot-side/signal-rates/
 *
 * Checked 2026-11-21 against CTRE's Phoenix 6 status signals page: optimizeBusUtilization()
 * disables or slows down (to 4 Hz by default) every status signal that wasn't given an update
 * frequency, so set the rates Flashpoint needs first.
 */
package frc.robot.flashpoint;

import com.ctre.phoenix6.BaseStatusSignal;
import com.ctre.phoenix6.hardware.ParentDevice;
import com.ctre.phoenix6.hardware.TalonFX;

/** Keeps the motor signals Flashpoint reads at 50 Hz, then lets Phoenix trim the rest. */
public final class SignalRates {
  private SignalRates() {}

  /** Call once from robotInit(), after configuring the motors. */
  public static void apply(TalonFX... motors) {
    for (TalonFX motor : motors) {
      BaseStatusSignal.setUpdateFrequencyForAll(
          50,
          motor.getStatorCurrent(),
          motor.getSupplyCurrent(),
          motor.getSupplyVoltage(),
          motor.getMotorVoltage(),
          motor.getVelocity(),
          motor.getDeviceTemp());
    }
    ParentDevice.optimizeBusUtilizationForAll(motors);
  }
}
