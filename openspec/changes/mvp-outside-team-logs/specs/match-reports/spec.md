## ADDED Requirements

### Requirement: Temperature on non-Pro hoots
Replay SHALL decide the reason for missing temperature per bus, from each hoot's Pro flag. On a bus whose hoots are not Pro-licensed, the temperature track and readout cell of every slot that reports temperature SHALL read "not logged (needs Phoenix Pro)". A slot whose device model has no temperature that Flashpoint tracks (such as a CANcoder or a Pigeon 2) SHALL read "temperature not tracked". Neither SHALL be drawn as zero or counted as hottest. The header SHALL name each bus without temperature, and say that temperature needs a Phoenix Pro-licensed device on that bus. When every bus has temperature, the header SHALL say nothing about it. With no cursor placed, the readout SHALL rank slots with temperature first, hottest first, then the rest by current.

#### Scenario: Whole match non-Pro
- **WHEN** a synthetic match's rio and CANivore hoots are both not Pro-licensed
- **THEN** every motor slot's temperature reads "not logged (needs Phoenix Pro)", the default hottest-slot track shows the same text and no line, and the header names both buses

#### Scenario: Mixed Pro and non-Pro buses
- **WHEN** a synthetic match has a Pro-licensed CANivore hoot with temperature and a non-Pro rio hoot without it
- **THEN** CANivore slots show their temperatures, rio motor slots read "not logged (needs Phoenix Pro)", the hottest-slot track picks the hottest CANivore slot, and the header names only the rio bus

#### Scenario: Sensors without temperature on a Pro match
- **WHEN** corpus `2026-gacmp-q7` is opened in Replay
- **THEN** the gyro and CANcoder readout cells read "temperature not tracked", and the header says nothing about missing temperature

#### Scenario: Non-Pro build succeeds
- **WHEN** Replay data is built for a synthetic lake whose hoots carry no temperature
- **THEN** the build succeeds within its budget, and no temperature marker is produced
