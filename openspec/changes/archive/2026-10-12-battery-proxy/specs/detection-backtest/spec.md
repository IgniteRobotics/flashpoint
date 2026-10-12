## MODIFIED Requirements

### Requirement: Failure labels file
The team SHALL record known failures and repairs in one labels file. Each label SHALL give:
- a robot, and either a unit (serial or legacy unit id) or a slot;
- the match key at which it was seen, or a date;
- a kind: failure, degradation, or repair;
- a free-text note.

It MAY also give the metric it is expected to show in. The file SHALL be validated on load. Unknown keys, a label with neither unit nor slot, an unknown robot or slot, and a malformed match key or date SHALL be rejected with a message naming the label. The file SHALL ship seeded with the known cases:
- Johnson Q105: the intake roller leader rebooted after an 820 ms dropout (failure), and the intake-extension follower's isolated CAN gaps rose (degradation).

GACMP Q7 SHALL NOT be seeded as a brownout: no brownout was seen there, and its motors' supply dips are wiring drops.

#### Scenario: Seeded labels load
- **WHEN** the shipped labels file is loaded with the shipped robot configurations
- **THEN** it yields at least the two seeded labels, each resolved to a robot and slot

#### Scenario: Bad label
- **WHEN** a label names slot `drive-xx`, which robot `2026-comp` does not have
- **THEN** loading fails with an error naming the label and `drive-xx`
