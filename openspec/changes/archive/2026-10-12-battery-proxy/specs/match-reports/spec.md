## MODIFIED Requirements

### Requirement: Replay view
The Replay view SHALL show:
- a **match list** grouped by event: each entry shows the match label, the robot, its marker counts by level, and a mark when alignment confidence is low;
- a **header** with the match key, robot, event, start time, duration, alignment confidence, and source log names;
- **tracks** over match time, with auto and teleop bands. By default these are the battery proxy (as defined under rule-based event markers), total supply current, the hottest slot's temperature, and the highest-current slot's current. The user MAY swap any track for any slot and metric. Each track shows its value at the cursor, its range, and a reference line where a limit is known;
- a **scrub cursor**, set by dragging or with the keyboard;
- a **readout** listing every slot's current, temperature, and status at the cursor. With no cursor placed, it lists each slot's match maximum, hottest first.

Missing data SHALL be shown as "not logged", never as zero. For a match without temperature (non-Pro hoots), the header SHALL say why.

#### Scenario: Hottest motor in two clicks
- **WHEN** a user opens the app and picks `2026gacmp_qm7` from the match list
- **THEN** the readout lists Q7's slots by maximum temperature, hottest first, with slots over the limit marked

#### Scenario: Scrub
- **WHEN** the user moves the cursor to T+90 s
- **THEN** every track shows its value at T+90 s, and the readout lists each slot's current and temperature at that time

#### Scenario: Temperature not available
- **WHEN** a match's hoots are not Pro-licensed
- **THEN** temperature tracks and readout cells read "not logged", and the header says the hoots carry no temperature

### Requirement: Rule-based event markers
The system SHALL place markers on the Replay timeline from fixed, configurable rules, each with a level (WARN or FAULT), a source slot, `power`, or a bus, a time, and a one-line message:
- temperature reaching the warning limit (default 65 °C, WARN) or the fault limit (default 75 °C, FAULT);
- the battery proxy dropping below the brownout threshold (default 6.75 V, held at least 0.02 s, FAULT) or a sag threshold (default 8.0 V, WARN);
- a stall interval as defined by motor physics (WARN);
- a device reboot event during the match (FAULT, on the device's slot), as defined by CAN health;
- an isolated frame gap of at least the dropout threshold during the match (default 0.25 s, WARN, on the device's slot), as defined by CAN health;
- a bus-wide frame gap of at least the bus-silence threshold during the match (default 1 s, WARN, on the bus).

The battery proxy SHALL be the supply voltage of the robot's declared battery-proxy device. When the robot declares none, or that device has no supply voltage in the match, the proxy SHALL be the median supply voltage across the devices logged at each moment. A power marker's message SHALL name the proxy it used. A sample SHALL count as below a threshold only until the next sample from the same device, and never for longer than a gap limit (default 0.1 s), so a logging gap cannot stretch a dip into a brownout. A single motor's supply dip SHALL NOT produce a power marker unless the proxy itself crosses the threshold.

Markers SHALL be read from the lake's derived layers, never computed from raw logs. Bus-wide gaps shorter than the bus-silence threshold SHALL NOT produce markers. A device with no configured slot SHALL NOT get a reboot or dropout marker. Selecting a marker SHALL move the cursor to its time and show its details. Markers SHALL NOT claim a cause or a fix.

#### Scenario: Temperature marker
- **WHEN** a synthetic slot's temperature reaches 66 °C at T+108 s
- **THEN** a WARN marker for that slot sits at T+108 s with the message "Temperature 66 °C"

#### Scenario: Brownout marker
- **WHEN** every device's supply voltage, including the declared proxy's, drops to 6.5 V for 0.2 s at T+97 s
- **THEN** a FAULT marker for `power` sits at T+97 s, and its message names the gyro as the proxy

#### Scenario: Motor dip is not a brownout
- **WHEN** one motor's supply voltage drops to 6.0 V for 1 s while the declared proxy stays at 11 V
- **THEN** no power marker is placed

#### Scenario: Median fallback
- **WHEN** a robot declares no battery proxy, and 3 of 5 devices drop to 6.5 V for 0.2 s while 2 stay at 11 V
- **THEN** a FAULT marker for `power` sits at that time, and its message names the median of devices as the proxy

#### Scenario: Logging gap does not stretch a dip
- **WHEN** the proxy logs one sample at 6.5 V followed by no sample for 0.8 s, then 11 V
- **THEN** the brownout marker for that dip lasts 0.1 s (the gap limit), not 0.8 s

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

### Requirement: Anomaly badges in Replay
Replay SHALL show anomalies that affect the matches it lists:
- each match in the match list with an open anomaly flagged at that match SHALL show the highest severity glyph among them;
- each track whose slot has an open anomaly flagged at the shown match SHALL show a badge with the anomaly's method and title;
- a `power` anomaly SHALL badge the battery track.

The badges SHALL use the same sensitivity setting as the Anomalies view. In served mode, the badges SHALL come from the current anomalies and triage, without rebuilding Replay data, and selecting a badge SHALL open that anomaly in the Anomalies view. In static mode, the export SHALL carry a snapshot of the open anomalies for its matches, labelled with the time it was taken. Static badges SHALL be read-only and SHALL NOT link to the Anomalies view. Building Replay data SHALL NOT depend on anomalies, so a detection run never rebuilds match data.

#### Scenario: Served badge
- **WHEN** the app is served on a corpus lake built from `2026-gacmp-q7` with a test configuration that sets the brownout threshold to 8.5 V and the sag threshold to 9.0 V (above Q7's lowest battery proxy voltage), and Replay opens Q7
- **THEN** Q7 in the match list shows the HIGH glyph, the battery track shows the brownout badge, and selecting it opens that anomaly in the Anomalies view

#### Scenario: Resolved anomaly drops its badge
- **WHEN** the Q7 brownout anomaly is resolved, and Replay is refreshed
- **THEN** Q7 shows no anomaly glyph, and the Replay data files are unchanged

#### Scenario: Static snapshot
- **WHEN** a static export of Q7 is made and opened from disk with networking disabled
- **THEN** the brownout badge shows with the snapshot time, and it does not link anywhere

#### Scenario: Detection does not rebuild Replay
- **WHEN** detection runs again after a configuration change, and the report command runs
- **THEN** no match data file is rewritten
