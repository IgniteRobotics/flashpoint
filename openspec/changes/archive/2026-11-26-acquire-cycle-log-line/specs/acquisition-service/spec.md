## ADDED Requirements

### Requirement: Cycle summary log line
At the end of every cycle, acquire SHALL log one line at info level that starts with `cycle done`, followed by ` (stopped)` when the cycle was stopped, then `: `, the sources, `; transfers `, the transfer counts, `; inbox `, and the inbox counts. Each source SHALL be written as its name and outcome, joined by `, `, or `no sources` when there were none. Transfer and inbox counts SHALL each be written as `<count> <status>` pairs joined by `, `, in a fixed order that does not depend on the order files finished. No transfers SHALL be written as `none` and an empty inbox as `empty`. The line SHALL NOT contain brackets, braces, or quote characters around the counts or statuses.

#### Scenario: Robot pull of three logs
- **WHEN** a cycle pulls 3 logs from robot `10.68.29.2`, all hash-verified, and ingests all 3
- **THEN** the line reads `cycle done: robot 10.68.29.2 ok; transfers 3 verified; inbox 3 success`

#### Scenario: Mixed results
- **WHEN** a cycle pulls 2 hash-verified logs, has 1 copy fail and set for retry, ingests 1 file, and quarantines 1 file
- **THEN** the transfers part reads `2 verified, 1 retry` and the inbox part reads `1 success, 1 quarantined`

#### Scenario: Same results, same line
- **WHEN** two cycles have the same transfer and inbox results, with the files finishing in a different order
- **THEN** both cycles log the same line

#### Scenario: Nothing to do
- **WHEN** a cycle sees no sources, transfers nothing, and finds the inbox empty
- **THEN** the line reads `cycle done: no sources; transfers none; inbox empty`

#### Scenario: Stopped cycle
- **WHEN** a cycle is stopped partway through
- **THEN** the line starts with `cycle done (stopped): `
