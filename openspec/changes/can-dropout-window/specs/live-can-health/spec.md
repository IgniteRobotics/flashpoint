## MODIFIED Requirements

### Requirement: Live events
The service SHALL compare each device's and bus's values with its previous update and record an event when:
- **dropout:** a device's `Dropouts` rose, carrying how many it rose by and the device's `MaxAgeMs`. When the robot configuration sets a dropout window for the device's bus (see `robot-config`), a rise whose `MaxAgeMs` is under the window SHALL NOT be an event, SHALL NOT count in the device's totals, and SHALL NOT count towards a bus-wide dropout. A rise without a valid `MaxAgeMs` SHALL be kept;
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

#### Scenario: Jitter under the bus window
- **WHEN** the robot configuration gives `DriveTrain` a 35 ms dropout window, and the fake robot raises `DriveTrain/TalonFX-11` `Dropouts` by 1 with `MaxAgeMs` 15
- **THEN** no dropout event is recorded, and `drive-fl`'s dropout total does not change

#### Scenario: Dropout over the bus window
- **WHEN** the same robot raises `DriveTrain/TalonFX-11` `Dropouts` by 1 with `MaxAgeMs` 48
- **THEN** one dropout event is shown for `drive-fl`, with 48 ms

#### Scenario: Bus without a window
- **WHEN** the fake robot raises `rio/TalonFX-2` `Dropouts` by 1 with `MaxAgeMs` 15, and `rio` has no dropout window
- **THEN** one dropout event is shown for that device
