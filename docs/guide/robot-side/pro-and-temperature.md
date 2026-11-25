# Phoenix Pro and temperature

Motor temperature is only in a hoot when the log has a Phoenix Pro-licensed device. Without Pro, Flashpoint gets everything else, but no temperature.

## What a free hoot has

Checked 2026-11-21 against the "Free Signals" list on CTRE's [signal logging page](https://v6.docs.ctr-electronics.com/en/stable/docs/api-reference/api-usage/signal-logging.html): "Any log that contains a pro-licensed device will export all signals." Without one, only the free list exports. For a Talon FX that's `SupplyCurrent`, `StatorCurrent`, `MotorVoltage`, `Position` and `Velocity` (plus `SupplyVoltage` for every device) and some fault flags, but not `DeviceTemp`. Only the over-temperature fault flags are free.

So a free hoot has every motor signal Flashpoint uses except temperature. A Pigeon 2 or CANcoder has no temperature Flashpoint tracks at all, Pro or not.

## Why one Pro device is enough for a bus

CTRE licenses the export per log: "Any log that contains a pro-licensed device will export all signals." Each CAN bus writes its own hoot, so one Pro-licensed device on a bus unlocks that bus's whole hoot, temperature included, for every device on it. A bus with no Pro device gets the free list only. A robot can have both: a Pro CANivore with temperature, and a free rio bus without it. Flashpoint decides per bus.

## What you get without Pro

- **Works:** Replay's current, voltage and velocity tracks, CAN dropouts, stall and brownout detection, History for everything except temperature, and the anomaly findings that don't depend on temperature.
- **Missing:** the temperature track and temperature limits in Replay, temperature in History, and thermal findings. Detection never raises a finding because temperature is missing; it skips it, and its summary counts those rows as "not logged (needs Phoenix Pro)".
- Replay's header names each bus without temperature and says it needs a Phoenix Pro-licensed device. That bus's temperature cells read "not logged (needs Phoenix Pro)".
- `flashpoint doctor` and the Setup view's checks report the Phoenix Pro coverage: how many hoots are Pro-licensed, and how many aren't on each bus. It's information, never a warning, because the only fix is buying Pro.

## N/A and NEEDS PRO in History

When a unit has no temperature value in range, History's health reads one of three things instead of OK, WARN or HOT:

| Health | Meaning |
|---|---|
| **NEEDS PRO** | The device reports temperature, but every hoot that carried it was not Pro-licensed. Its latest temperature reads "not logged (needs Phoenix Pro)". |
| **N/A** | Flashpoint tracks no temperature for this kind of device: a CANcoder has no temperature signal, and a Pigeon 2's own temperature isn't tracked. Reads "temperature not tracked", Pro or not. |
| **NO DATA** | The device should report temperature, its hoots were Pro-licensed, and there's still no value: check the log. |

All three rank below OK, and none counts toward the "units warn/hot" tile. An open anomaly still raises any of them to WARN or HOT.

Flashpoint reads motor temperature only from hoots. If thermal tracking matters to your team, Pro on the motors you worry about is the way to get it.
