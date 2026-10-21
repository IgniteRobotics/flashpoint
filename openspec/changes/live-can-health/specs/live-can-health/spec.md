## Purpose

Show the pit crew, while they shake-test or drive-test the robot, which CTRE device dropped out, rebooted or faulted, and whether the problem was one device or the whole bus, from running totals the robot code publishes over NetworkTables.

## ADDED Requirements

### Requirement: Robot-side contract
Flashpoint SHALL accept live CAN health from robot code that publishes the following NetworkTables topics, all under `/Flashpoint/Live/CANHealth/`:

- `Version` (integer): the contract version, `1` for this change;
- `Project` (string): the robot code's project name, as in the wpilog metadata (such as `Robot-2026`);
- `Enabled` (boolean): whether the robot is enabled;
- for each CTRE device, under `<bus>/<Model>-<id>/`, where `<bus>` is the bus name the robot code uses (such as `rio` or `DriveTrain`) and `<Model>-<id>` matches the hoot device name (such as `TalonFX-11`):
  - `Connected` (boolean): whether the device is currently heard;
  - `Dropouts` (integer): the number of robot loops, since the robot code started, in which the device's watched status signal was older than 2.5 times its update period;
  - `MaxAgeMs` (number): the oldest that signal was in any loop since the previous update;
  - `Resets` (integer): the number of device resets the robot code has seen since it started;
  - `Faults` (integer bit set): faults seen at any point since the previous update: bit 0 under-voltage, bit 1 boot during enable, bit 2 hardware, bit 3 bridge brownout, bit 4 bad magnet;
  - `MinSupplyV` (number): the lowest supply voltage since the previous update;
- for each bus, under `<bus>/Bus/`: `Utilization` (number, 0–1), `BusOffCount`, `TxFullCount`, `ReceiveErrorCount` and `TransmitErrorCount` (integers).

The guide SHALL document every topic in this list, with its type and meaning, and a suggested Java sample SHALL publish all of them.

#### Scenario: Contract accepted
- **WHEN** the fake robot publishes contract version 1 with three devices on `DriveTrain` and two on `rio`
- **THEN** the Live CAN view shows five devices under two buses, and the connection reads connected

#### Scenario: Unknown contract version
- **WHEN** the fake robot publishes `Version` 2
- **THEN** the view shows a message naming the published and supported versions, shows no devices, and records no events

#### Scenario: Guide matches the contract
- **WHEN** the documentation checks run
- **THEN** every topic the service reads appears in the guide's contract table, and every topic in the table is read by the service

### Requirement: Connection to the robot
`flashpoint serve` SHALL open one NetworkTables connection to the robot while at least one Live CAN view is open, shared by every open view, and SHALL close it within 30 seconds after the last one closes. It SHALL try the robot addresses from the `acquire` configuration unless the serve command names an address. The view SHALL show the connection state: connecting, connected, connected but robot code not publishing the contract, or contract version not supported. A missing or unreachable robot SHALL NOT stop or slow the other views.

#### Scenario: No robot
- **WHEN** a Live CAN view is opened and no robot is reachable
- **THEN** the view reads connecting and keeps retrying, and Replay and History still respond normally

#### Scenario: Shared connection
- **WHEN** two browsers open the Live CAN view at the same time
- **THEN** the server holds one robot connection, and both views show the same devices and events

#### Scenario: Robot code without the publisher
- **WHEN** the robot is reachable but publishes nothing under `/Flashpoint/Live/CANHealth/`
- **THEN** the view reads that the robot code is not publishing live CAN health, and links to the guide

### Requirement: Devices mapped to slots
Each reported device SHALL be mapped to a slot of the robot configuration whose `project` matches the published `Project`, choosing the latest season when several match. A device's bus SHALL be matched through the configuration's bus list, which ties each robot-code bus name to a hoot bus. A reported device with no slot SHALL be shown as unmapped. A configured slot that the robot does not report SHALL be shown as missing. When no robot configuration matches the project, every device SHALL be shown unmapped and the view SHALL say which project name it could not find.

#### Scenario: Mapped device
- **WHEN** the fake robot publishes `Project` `Robot-2026` and device `DriveTrain/TalonFX-11`
- **THEN** the view shows it as slot `drive-fl` (front-left drive) on the CANivore

#### Scenario: Missing slot
- **WHEN** the fake robot reports every 2026 slot except `steer-fl`
- **THEN** `steer-fl` is shown as missing

#### Scenario: Unknown project
- **WHEN** the fake robot publishes `Project` `Robot-2099`
- **THEN** every device is shown unmapped, and the view names `Robot-2099` as not found

### Requirement: Live events
The service SHALL compare each device's and bus's values with its previous update and record an event when:
- **dropout:** a device's `Dropouts` rose, carrying how many it rose by and the device's `MaxAgeMs`;
- **disconnected / reconnected:** a device's `Connected` changed;
- **reboot:** a device's `Resets` rose;
- **fault:** a fault bit was set in a device's `Faults` that was clear in its previous update;
- **bus errors:** a bus's `BusOffCount`, `TxFullCount`, `ReceiveErrorCount` or `TransmitErrorCount` rose.

The first update from a device or bus SHALL be its baseline and SHALL NOT record an event. Each event SHALL carry the time the server received the update, the bus, the device and slot (when it has one), and whether the robot was enabled according to `Enabled` in that update. Events SHALL be shown within 1 second of the update reaching the server.

#### Scenario: Shake-test dropout while disabled
- **WHEN** the fake robot, disabled, raises `DriveTrain/TalonFX-11` `Dropouts` from 0 to 2 with `MaxAgeMs` 48
- **THEN** one dropout event is shown for `drive-fl`, counting 2, with 48 ms, marked disabled

#### Scenario: Drive-test reboot while enabled
- **WHEN** the fake robot, enabled, raises `rio/TalonFX-2` `Resets` from 0 to 1 and sets its boot-during-enable bit
- **THEN** a reboot event and a boot-during-enable fault event are shown for that device, both marked enabled

#### Scenario: Baseline is not an event
- **WHEN** the view connects to a fake robot whose devices already report `Dropouts` 40 and `Resets` 1
- **THEN** no event is recorded until a value rises

### Requirement: Isolated and bus-wide dropouts
A dropout SHALL be classified by the other devices reporting on the same bus in the same update: **bus-wide** when at least half of them also had a dropout in that update, **isolated** otherwise. A bus-wide dropout SHALL be shown as one bus event naming the devices involved, and SHALL NOT be charged to any device. Updates SHALL be grouped by the update in which the server received them, never by matching exact timestamps.

#### Scenario: One device drops
- **WHEN** one of eight `DriveTrain` devices has a dropout in an update and the other seven do not
- **THEN** one isolated dropout is shown for that device, and the bus has no bus-wide event

#### Scenario: Whole bus drops
- **WHEN** five of eight `DriveTrain` devices have a dropout in the same update
- **THEN** one bus-wide event is shown for `DriveTrain`, naming the five devices, and none of them is shown with an isolated dropout

### Requirement: Robot code restarts and bad values
When a device's or bus's running total (`Dropouts`, `Resets`, or a bus count) is lower than in its previous update, the service SHALL treat it as a robot code restart: it SHALL record one restart event and take the new values as a fresh baseline, with no dropout, reboot or bus event from that comparison. A topic with the wrong type, or a negative count, SHALL be ignored, and the device SHALL be shown with a note naming the topic, without stopping the service or affecting other devices.

#### Scenario: Robot code restarts
- **WHEN** the fake robot's totals reset to 0 after reading `Dropouts` 12 and `Resets` 1
- **THEN** one restart event is shown, and no dropout or reboot event comes from the drop to 0

#### Scenario: Wrong type
- **WHEN** the fake robot publishes `rio/TalonFX-8/Dropouts` as a string
- **THEN** that device shows a note naming `Dropouts`, and the other devices keep updating

### Requirement: Live CAN view
The app SHALL have a Live CAN view, registered with the shared shell and present only in served mode. It SHALL show:
- the connection state, and whether the robot is enabled;
- for each bus, its utilisation and error counts, and a grid of its devices by slot, each shown as ok, dropped (a dropout in the last 5 seconds), rebooted, faulted, disconnected, missing or unmapped, with its dropout and reset totals since the server's robot connection opened, and its last `MinSupplyV`;
- an event log, newest first;
- a mark control: a short text label ("now wiggling: FL module") attached to every event that follows until it is changed or cleared.

Marks and the event log SHALL exist only in the browser that made them: they SHALL be lost when that view is reloaded, and SHALL NOT be shared with other viewers.

#### Scenario: Marked shake test
- **WHEN** a user sets the mark "FL module", and the fake robot then raises `drive-fl`'s `Dropouts`
- **THEN** the dropout event in the log carries the mark "FL module"

#### Scenario: Static export
- **WHEN** a static export is opened from disk
- **THEN** the Live CAN view is not in the navigation

#### Scenario: Reload drops marks
- **WHEN** a user sets a mark, events are recorded, and the page is reloaded
- **THEN** the event log is empty and no mark is set, while the device totals still show

### Requirement: Nothing written to the lake
The live service SHALL NOT write to the lake, the ledger or configuration.

#### Scenario: Live session leaves the lake unchanged
- **WHEN** a Live CAN view runs against the fake robot through dropouts, reboots, faults and a restart, and is then closed
- **THEN** every file under the lake root is byte-for-byte unchanged
