# Phoenix Pro and temperature

Motor temperature is only in a hoot when the log has a Phoenix Pro-licensed device. Without Pro, Flashpoint gets everything else, but no temperature.

## What a free hoot has

Checked 2026-11-21 against the "Free Signals" list on CTRE's [signal logging page](https://v6.docs.ctr-electronics.com/en/stable/docs/api-reference/api-usage/signal-logging.html): "Any log that contains a pro-licensed device will export all signals." Without one, only the free list exports. For a Talon FX that's `SupplyCurrent`, `StatorCurrent`, `MotorVoltage`, `Position` and `Velocity` (plus `SupplyVoltage` for every device) and some fault flags, but not `DeviceTemp`. Only the over-temperature fault flags are free.

So a free hoot has every motor signal Flashpoint uses except temperature.

## What you get without Pro

- **Works:** Replay's current, voltage and velocity tracks, CAN dropouts, stall and brownout detection, History for everything except temperature, and the anomaly findings that don't depend on temperature.
- **Missing:** the temperature track and temperature limits in Replay, temperature in History, and thermal findings. Replay says why in place of the track: "Temperature not logged. The hoots are not Pro-licensed, so they carry no temperature."
- History's temperature health reads NO DATA for now. [#75](https://github.com/IgniteRobotics/flashpoint/issues/75) changes that to "not logged (needs Phoenix Pro)", and adds a Pro count to `flashpoint doctor`.

A Pigeon 2 or CANcoder has no temperature signal at all, Pro or not.

Flashpoint reads motor temperature only from hoots. If thermal tracking matters to your team, Pro on the motors you worry about is the way to get it.
