## MODIFIED Requirements

### Requirement: Competitions in the evidence
The detail panel SHALL name the competition the evidence comes from, taken from each evidence match's event. When the evidence spans more than one competition, the panel SHALL list them in match order, each with its number of matches, and the evidence chart SHALL draw a labelled boundary line between the last match of one competition and the first match of the next. The chart SHALL also name the first competition at its start. A competition's name SHALL be shown as text. When the evidence spans more than one year, every name SHALL carry its year (`2025 GACMP`). When a boundary's label would overlap the label before it, the chart SHALL leave that label off and still draw its line; the panel SHALL still name every competition.

#### Scenario: One competition
- **WHEN** every evidence match of the selected anomaly is at `2026gadal`
- **THEN** the panel reads "COMPETITION · GADAL", and the evidence chart names GADAL at its start with no boundary line

#### Scenario: Evidence spans competitions
- **WHEN** the selected anomaly's evidence is 3 matches at `2026gadal` then 2 at `2026gacmp`
- **THEN** the panel reads "COMPETITIONS · GADAL · 3 MATCHES → GACMP · 2 MATCHES", and the evidence chart names GADAL at its start and has one boundary line, labelled GACMP, between the third and fourth matches
