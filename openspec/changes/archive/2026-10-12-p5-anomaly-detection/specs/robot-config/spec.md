## ADDED Requirements

### Requirement: Sibling groups
A robot configuration SHALL allow optional sibling groups. Each group has a name and a list of two or more of that robot's slot ids, for slots that do the same job (for example, the four drive motors). A slot SHALL belong to at most one group. Validation SHALL reject the following with a message naming the file and the group:
- an unknown slot id;
- a group with fewer than two slots;
- a slot in more than one group;
- a duplicate group name.

The shipped 2026 competition robot configuration SHALL declare at least the drive motors and the steer motors as groups.

#### Scenario: 2026 groups
- **WHEN** the 2026 competition robot configuration is loaded
- **THEN** it has a `drive` group of `drive-fl`, `drive-fr`, `drive-bl`, `drive-br` and a `steer` group of the four steer slots

#### Scenario: Unknown slot in a group
- **WHEN** a sibling group lists `drive-xx`
- **THEN** loading fails with an error naming the group and `drive-xx`

#### Scenario: No groups declared
- **WHEN** a robot configuration declares no sibling groups
- **THEN** it loads, and sibling comparison produces nothing for that robot
