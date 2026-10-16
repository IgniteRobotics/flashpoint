## Purpose

Find failing CAN and power connections from the hoots already in the lake: device reboots, frame dropouts that one device suffers while its bus siblings keep talking, and bus-wide gaps that come from the logger, counted per match for each slot and unit so a connector that is getting worse shows up before it costs a match.

## ADDED Requirements

### Requirement: Reboot events
For every session with hoots, derive SHALL record a reboot event for a device when either:
- its reset count increases between two consecutive samples in one hoot; or
- its boot-during-enable fault turns from off to on.

A reset-count increase and a boot-during-enable rise on the same device within 2 seconds of each other SHALL be one event. The first reset-count sample of a hoot SHALL be a baseline, never an event. Each event SHALL carry the session, bus, device, slot and unit (when mapped), its time on the hoot's clock, lake time (when the hoot is aligned), match time (when the session is framed and match-keyed), whether the robot was enabled, and the reset count before and after (when logged). When a hoot has no reset-count signal (Phoenix 2025 hoots), reboots SHALL be detected from the boot-during-enable fault alone, and the session's CAN-health rows SHALL say the reset count was not logged.

#### Scenario: Reboot while enabled
- **WHEN** a synthetic rio hoot has TalonFX 2's reset count go from 0 to 1 at hoot time 344.96 s, and its boot-during-enable fault turn on at 345.16 s, while the robot is enabled
- **THEN** exactly one reboot event is recorded for that device, marked enabled, with reset count 0 before and 1 after

#### Scenario: Baseline is not a reboot
- **WHEN** a hoot's first reset-count sample for a device is 3 and it never changes
- **THEN** no reboot event is recorded for that device

#### Scenario: No reboots in the corpus
- **WHEN** corpus `2026-gacmp-q7` is derived
- **THEN** no reboot event is recorded

#### Scenario: Hoot without reset count
- **WHEN** a synthetic 2025-style hoot with no reset-count signal, in a framed, match-keyed session, has a device's boot-during-enable fault turn on while enabled
- **THEN** one reboot event is recorded for that device with no reset counts, and its CAN-health rows say the reset count was not logged

#### Scenario: 2025 hoots in the corpus
- **WHEN** corpus `2025-gacmp-q19` (2025 hoots with no wpilog) is derived
- **THEN** no reboot event is recorded, and derive succeeds

### Requirement: Frame gaps measured per device
For every device in a session's hoots, derive SHALL measure the device's frame stream against its own nominal period: the median interval between its frames while the robot is enabled. An interval of at least 2.5 times the nominal period SHALL be a frame gap, and the frames it misses SHALL be its length divided by the nominal period, rounded, minus one. Only gaps that start while the robot is enabled SHALL count. Frames in the first 5 seconds of each hoot SHALL be ignored. A device whose frames stop before an enabled window ends SHALL have a gap running to the earlier of the window end and the last frame of any device in that hoot; the end of a hoot SHALL never count as a gap. Devices with no configured slot SHALL be measured, so they count as siblings, but SHALL get no CAN-health row.

#### Scenario: Gap against the device's own period
- **WHEN** a synthetic device with an 8 ms frame period goes silent for 40 ms while enabled
- **THEN** one frame gap of 40 ms is recorded for it, missing 4 frames

#### Scenario: Normal jitter is not a gap
- **WHEN** a synthetic device with an 8 ms period has intervals of up to 19 ms
- **THEN** no frame gap is recorded for it

#### Scenario: Disabled time ignored
- **WHEN** a device is silent for 2 s while the robot is disabled
- **THEN** no frame gap is recorded for that silence

#### Scenario: Device stops before the window ends
- **WHEN** a synthetic device's frames stop 3 s before teleop ends while the other devices on its bus keep sending until the end
- **THEN** a frame gap of about 3 s is recorded for it, ending at the teleop end

#### Scenario: Truncated hoot
- **WHEN** corpus `2026-gacmp-e10-truncated` (a hoot read as `incomplete-read`) is derived
- **THEN** no device on that hoot gets a gap running past the hoot's last frame, and derive succeeds

### Requirement: Isolated and bus-wide gaps
Each frame gap SHALL be classified by its siblings, the other devices measured on the same hoot:
- **bus-wide** when at least half of the siblings have a frame gap overlapping it in time;
- **isolated** otherwise.

Overlap SHALL be decided by comparing time intervals, never by equal timestamps. Bus-wide gaps SHALL be counted for the bus as logger gaps and SHALL NOT be charged to any device. Isolated gaps SHALL be charged to their device.

#### Scenario: Logger gap on the CANivore
- **WHEN** corpus `2026-gacmp-q7` is derived
- **THEN** its CANivore bus has logger gaps recorded, every CANcoder on it has no isolated gap, and no CANivore device has more than 5 isolated gaps or one longer than 50 ms

#### Scenario: One device drops out
- **WHEN** a synthetic hoot has ten devices, and one of them is silent for 820 ms while enabled while the other nine keep their periods
- **THEN** that device has one isolated gap of 820 ms, and the bus has no logger gap for that time

#### Scenario: Overlapping, not equal
- **WHEN** six of ten synthetic devices are silent over windows that overlap the same 60 ms but start up to 5 ms apart
- **THEN** each of those gaps is bus-wide, and one logger gap is counted for the bus

### Requirement: Per-match CAN health in gold
For every framed session with a match key and hoots, derive SHALL write one CAN-health row per (match, phase in {auto, teleop, match}, slot, unit) for each mapped device on its hoots, holding:
- reboot events (all, and while enabled);
- isolated gap count and frames missed per enabled minute;
- the longest isolated gap;
- whether the reset count was logged;
- the robot id, bus, alignment confidence, and source log ids.

It SHALL also write one row per (match, phase, bus) holding the logger gap count, the total and longest logger gap, the number of devices measured, and the bus status summary (see "Bus status per match"). A unit swapped mid-session SHALL be charged only with the events and gaps from its own time in the slot. A slot whose bus has no hoot in the session SHALL get no row, never a row of zeros.

#### Scenario: Q7 rows
- **WHEN** corpus `2026-gacmp-q7` is derived with the 2026 robot configuration
- **THEN** every mapped device on its hoots has a `match`, `auto`, and `teleop` CAN-health row with zero reboots, and the rio and CANivore buses each have a bus row

#### Scenario: Mid-session swap
- **WHEN** a synthetic session swaps unit `ctre:A` for `ctre:B` in a slot, and the slot has an isolated gap after the swap
- **THEN** the gap is counted on `ctre:B`'s row and not on `ctre:A`'s

#### Scenario: Session with no hoot on a bus
- **WHEN** corpus `2026-gacmp-p2` (a wpilog with no hoots) is derived
- **THEN** it has no CAN-health rows, and derive succeeds

#### Scenario: Session without a match key
- **WHEN** a synthetic session with an aligned hoot and a reboot has no match key
- **THEN** it has no CAN-health rows, and its reboot event is still recorded with lake time and no match time

#### Scenario: Unaligned hoot
- **WHEN** a session's hoot could not be aligned to a wpilog (for example the hoot-only corpus `2025-gacmp-q19`)
- **THEN** its events carry hoot time and no lake time, it has no CAN-health rows, and derive succeeds

### Requirement: Bus status per match
When a session's wpilog carries the bus-status signals (see `nt-signals`), the bus row SHALL summarise them over each phase: the maximum and mean utilisation, the increase in bus-off and TX-full counts, and the maximum receive and transmit error counts. A bus SHALL be matched to its status signals through the robot configuration, which names each bus as the robot code does (such as `DriveTrain`) and gives its hoot bus (`rio`, `canivore`, or the CANivore's 32-hex id). Where a session has no bus-status signals, those columns SHALL be empty and read "not logged", never zero.

#### Scenario: Bus status logged
- **WHEN** a synthetic session's wpilog logs `DriveTrain` utilisation from 0.40 to 0.62 and its transmit error count from 0 to 12 during teleop, and the robot configuration maps `DriveTrain` to `canivore`
- **THEN** the CANivore bus row for teleop holds a maximum utilisation of 0.62 and a maximum transmit error count of 12

#### Scenario: Older robot code
- **WHEN** corpus `2026-gacmp-q7` (recorded before bus status was logged) is derived
- **THEN** its bus rows have empty bus-status columns, and derive succeeds

### Requirement: CAN health is rebuildable and bounded
CAN-health results SHALL be derived only from bronze, the ledger, and configuration, and SHALL be rebuilt whenever a session is re-derived. The step SHALL add no more than 3 seconds and 150 MB of peak memory to deriving one match on the reference laptop.

#### Scenario: Q7 budget
- **WHEN** corpus `2026-gacmp-q7` is derived from an existing bronze layer
- **THEN** the CAN-health step takes under 3 s, and derive's peak memory rises by under 150 MB

#### Scenario: Deterministic output
- **WHEN** corpus `2026-gacmp-q7` is derived twice with `--force`
- **THEN** its CAN-health rows and reboot events are identical between the runs
