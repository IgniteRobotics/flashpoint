## MODIFIED Requirements

### Requirement: Live CAN view
The app SHALL have a Live CAN view, registered with the shared shell and present only in served mode. It SHALL show:
- the connection state, and whether the robot is enabled;
- for each bus, its utilisation and error counts, and a grid of its devices by slot, each shown as ok, dropped (a dropout in the last 5 seconds), rebooted, faulted, disconnected, missing or unmapped, with its dropout and reset totals since the server's robot connection opened, and its last `MinSupplyV`;
- for each bus, its own event log, newest first, under that bus's devices. Each event SHALL appear only in its bus's log; a robot code restart, which has no bus, SHALL appear in every bus's log;
- a mark control: a short text label ("now wiggling: FL module") attached to every event that follows, on every bus, until it is changed or cleared.

Marks and the event logs SHALL exist only in the browser that made them: they SHALL be lost when that view is reloaded, and SHALL NOT be shared with other viewers.

#### Scenario: Marked shake test
- **WHEN** a user sets the mark "FL module", and the fake robot then raises `drive-fl`'s `Dropouts`
- **THEN** the dropout event in the `DriveTrain` log carries the mark "FL module"

#### Scenario: Static export
- **WHEN** a static export is opened from disk
- **THEN** the Live CAN view is not in the navigation

#### Scenario: Reload drops marks
- **WHEN** a user sets a mark, events are recorded, and the page is reloaded
- **THEN** every bus's event log is empty and no mark is set, while the device totals still show

#### Scenario: Events stay in their bus's log
- **WHEN** the fake robot raises `DriveTrain/TalonFX-11` `Dropouts` and, in the same update, `rio/TalonFX-2` `Resets`
- **THEN** the dropout appears only in the `DriveTrain` log, and the reboot only in the `rio` log

#### Scenario: Restart shown on every bus
- **WHEN** the fake robot's totals reset to 0 (a robot code restart)
- **THEN** one restart event appears in the `DriveTrain` log and one in the `rio` log
