## MODIFIED Requirements

### Requirement: Rule-based event markers
The system SHALL place markers on the Replay timeline from fixed, configurable rules, each with a level (WARN or FAULT), a source slot, `power`, or a bus, a time, and a one-line message:
- temperature reaching the warning limit (default 65 °C, WARN) or the fault limit (default 75 °C, FAULT);
- the battery proxy dropping below the brownout threshold (default 6.75 V, FAULT) or a sag threshold (default 8.0 V, WARN);
- a stall interval as defined by motor physics (WARN);
- a device reboot event during the match (FAULT, on the device's slot), as defined by CAN health;
- an isolated frame gap of at least the dropout threshold during the match (default 0.25 s, WARN, on the device's slot), as defined by CAN health;
- a bus-wide frame gap of at least the bus-silence threshold during the match (default 1 s, WARN, on the bus).

Markers SHALL be read from the lake's derived layers, never computed from raw logs. Bus-wide gaps shorter than the bus-silence threshold SHALL NOT produce markers. A device with no configured slot SHALL NOT get a reboot or dropout marker. Selecting a marker SHALL move the cursor to its time and show its details. Markers SHALL NOT claim a cause or a fix.

#### Scenario: Temperature marker
- **WHEN** a synthetic slot's temperature reaches 66 °C at T+108 s
- **THEN** a WARN marker for that slot sits at T+108 s with the message "Temperature 66 °C"

#### Scenario: Brownout marker
- **WHEN** every device's supply voltage drops to 6.5 V for 0.2 s at T+97 s
- **THEN** a FAULT marker for `power` sits at T+97 s

#### Scenario: Reboot marker
- **WHEN** a synthetic match has a reboot event on the intake roller slot at T+128 s while enabled, with the reset count going from 0 to 1
- **THEN** a FAULT marker for that slot sits at T+128 s with the message "Device rebooted (reset count 0 → 1)"

#### Scenario: Long dropout marker
- **WHEN** a synthetic match has an isolated frame gap of 820 ms on the intake roller slot starting at T+127 s
- **THEN** a WARN marker for that slot sits at T+127 s with the message "No CAN frames for 0.82 s"

#### Scenario: Logger gaps stay quiet
- **WHEN** the Q7 Replay data is built
- **THEN** no marker comes from a bus-wide frame gap, and no reboot or dropout marker is shown

#### Scenario: Whole bus silent
- **WHEN** every device on a synthetic rio hoot is silent for 1.4 s at T+60 s
- **THEN** a WARN marker for the rio bus sits at T+60 s with the message "Whole bus silent for 1.4 s", and no slot on that bus gets a dropout marker for that time

#### Scenario: Quiet match
- **WHEN** no rule fires for a match
- **THEN** the timeline shows no markers, and the match list shows zero counts
