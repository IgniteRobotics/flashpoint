# Signal rates

How often a status signal updates is how often Flashpoint sees it. At 4 Hz a current sample comes every 250 ms, so a 100 ms stall spike can fall between two samples and never show up. Flashpoint wants motor current, voltage, velocity and temperature at **50 Hz** (every 20 ms, one sample per robot loop).

## The optimisation caveat

Checked 2026-11-21 against CTRE's [status signals page](https://v6.docs.ctr-electronics.com/en/stable/docs/api-reference/api-usage/status-signals.html): `optimizeBusUtilization()` disables or slows down every status signal that wasn't given an update frequency. Slowed signals run at 4 Hz by default, which keeps them in the hoot, but only just. So set the rates you need first, then optimise.

This is what happened on Ignite's 2026 robot: the roboRIO-bus Talon FXs logged current, temperature and velocity at 4 Hz, while the CANivore's devices logged at about 250 Hz ([#7](https://github.com/IgniteRobotics/flashpoint/issues/7)).

[`SignalRates.java`](https://github.com/IgniteRobotics/flashpoint/blob/main/examples/robot-java/src/main/java/frc/robot/flashpoint/SignalRates.java) sets the signals Flashpoint reads to 50 Hz on each motor, then optimises:

```java
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
```

Call it once in `robotInit()`, after configuring the motors. Signals that share a status frame update at the highest rate any of them asks for. If you use followers, CTRE's page says the leader must keep `DutyCycle`, `MotorVoltage` and `TorqueCurrent` enabled: add them to the list for leaders.

## The roboRIO bus budget

The roboRIO's own CAN bus is classic CAN at 1 Mbit/s, and a CANivore's is CAN FD, so the same rates cost the roboRIO bus much more. CTRE warns that above 90 % utilisation the bus behaves unpredictably. With [CAN bus status](bus-status.md) logged, CAN health shows each bus's utilisation per match phase: check the roboRIO bus after raising rates, and if it's high, drop the rate of signals Flashpoint doesn't read (position on motors that don't need it, for example) before dropping current.

## How Replay shows a slow signal

When a slot logs current slower than 20 Hz, Replay's header says so, for example:

```text
2 slot(s) log current slower than 20 Hz (indexer, intake); spikes between their samples are not visible.
```

Stall and brownout times on those slots are rounded to their sample period. Raise their rates, and the note goes away for the next match.
