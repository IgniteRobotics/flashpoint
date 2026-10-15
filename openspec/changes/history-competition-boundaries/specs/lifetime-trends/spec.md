## ADDED Requirements

### Requirement: Competition boundaries in History
The History trend chart SHALL show where each competition starts, taken from each plotted match's event in match order. A competition is a run of consecutive matches with the same event; the same event appearing again after another one SHALL start a new run. The chart SHALL name the first competition at its start with no line, and SHALL draw a labelled boundary line, named for the new competition, between the last match of one competition and the first match of the next. Above the chart, History SHALL list the competitions in match order: "COMPETITION · <name>" when there is one, or "COMPETITIONS · " followed by each name and its number of matches, joined by "→", when there are more.

A competition's name SHALL be its event code in capitals without the year (`2026gacmp` reads `GACMP`). When the plotted matches span more than one year, every name SHALL carry its year (`2025 GACMP`). Names SHALL be shown as text.

When a boundary's label would overlap the label before it, the chart SHALL leave that label off and still draw its line; the list above the chart SHALL still name every competition. The boundaries SHALL follow the plotted matches: they SHALL change with the filters, the metric, and per-unit or per-slot lines exactly as the x axis does. With no matches in range, History SHALL show neither boundaries nor the competitions list.

#### Scenario: Season spans two competitions
- **WHEN** History shows the synthetic season (30 matches at `2026gadal`, then 30 at `2026gacmp`) with no event filter
- **THEN** the list reads "COMPETITIONS · GADAL · 30 MATCHES → GACMP · 30 MATCHES", and the chart names GADAL at its start and has one boundary line, labelled GACMP, between the 30th and 31st matches

#### Scenario: One competition
- **WHEN** the event filter is `2026gacmp`
- **THEN** the list reads "COMPETITION · GACMP", and the chart names GACMP at its start with no boundary line

#### Scenario: Boundaries follow the filters
- **WHEN** the event filter is cleared after being `2026gacmp`
- **THEN** the GADAL start label and the GACMP boundary line return, without reloading the page

#### Scenario: Spans more than one year
- **WHEN** the plotted matches are 2 at `2025gaalb` then 3 at `2026gadal`
- **THEN** the list reads "COMPETITIONS · 2025 GAALB · 2 MATCHES → 2026 GADAL · 3 MATCHES", and the boundary line is labelled `2026 GADAL`

#### Scenario: Crowded labels
- **WHEN** two boundaries fall so close together on the chart that their labels would overlap
- **THEN** both lines are drawn, only the earlier of the two labels is drawn, and the list above the chart names both competitions

#### Scenario: No matches in range
- **WHEN** the filters leave no match in range
- **THEN** History shows its "No matches in this range." notice and no competitions list
