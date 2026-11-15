## ADDED Requirements

### Requirement: Live CAN is marked experimental
Until its pit check passes on a real robot, the Live CAN view SHALL show an "Experimental" label next to its title, with one line saying it needs robot code that publishes CAN health and can report false dropouts on fast buses. The label SHALL link to the Live CAN guide by its published web address, which the app gets from the server (see team-setup, "Documentation links"). The Live CAN guide SHALL open with the same status. The label SHALL show whether or not a robot is connected, and when the robot publishes nothing it SHALL stay next to the message that the robot code is not publishing live CAN health, rather than replace it.

#### Scenario: Label in the view
- **WHEN** the Live CAN view is opened in served mode against the fake robot
- **THEN** the title carries an "Experimental" label, its line names the robot code requirement and possible false dropouts on fast buses, and its link is a full web address to the Live CAN guide

#### Scenario: Robot publishes nothing
- **WHEN** the Live CAN view is opened while the fake robot is connected but publishes no CAN health entries
- **THEN** the "Experimental" label and the existing "not publishing live CAN health" message both show

#### Scenario: Guide states the status
- **WHEN** the Live CAN guide is opened
- **THEN** its first paragraph says Live CAN is experimental, needs robot code, and can report false dropouts on fast buses
