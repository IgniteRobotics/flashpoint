# View: History

History shows how each motor trends across every match in the lake: one point per match for a metric such as peak temperature, supply current or CAN gaps, with a line per physical motor (a **unit**, followed by serial number) or per position on the robot (a **slot**). Use it to spot a motor that runs hotter or draws more than it used to, to see where a unit has been, and to jump to the matches that stand out. It needs `flashpoint serve`; a static export doesn't have it.

![History view: one motor's temperature and current across every match of the season](../../assets/media/history.png)

## Layout

| Part | What it holds |
|---|---|
| Filter bar | The shared filters, then History's own after **HISTORY ONLY** |
| Summary tiles | **UNITS TRACKED**, **MATCHES**, **POWERED-ON H**, **UNITS WARN/HOT** |
| Main column | The metric buttons, **PER UNIT** / **PER SLOT**, the chart, and the units table |
| **UNIT** rail (right) | The selected unit's totals, lifeline and matches |

The ▸ button at the top of the rail shrinks it to a strip labelled **UNIT**; press the strip to bring it back. Your browser remembers this for History.

History lists motors: devices that log stator current. Usage totals (**POWERED-ON H** and the rail's totals) also count sessions that aren't competition matches, such as shop and practice-field logs.

## Filters

**SEASON**, **ROBOT**, **EVENT** and the **UNIT** chip are the filters every view shares ([the header and filter bar](replay.md#the-header-and-filter-bar)). Selecting a unit in the table or on the chart sets the unit filter, so it follows you to Replay and Anomalies.

After **HISTORY ONLY** come four filters that affect History only. History keeps them when you leave and come back through its tab.

| Filter | Choices |
|---|---|
| **TYPE** | All types, Qualification, Practice, Elimination, Semifinal, Final (the types the lake has) |
| **PHASE** | MATCH (the whole match, the default), AUTO, TELEOP |
| **SUBSYSTEM** | All subsystems, or one from the robot files |
| **SLOT** | All slots, or one slot id |

**CLEAR FILTERS** clears History's own filters too. Every filter runs as a query on the laptop running `serve`; nothing is filtered in the browser.

## Summary tiles

The tiles cover the filtered range:

| Tile | What it counts |
|---|---|
| **UNITS TRACKED** | Units with at least one match |
| **MATCHES** | Matches |
| **POWERED-ON H** | Hours the units were powered, over every session in range (the phase filter doesn't apply) |
| **UNITS WARN/HOT** | Units whose health (below) is WARN or HOT |

## Metric and lines

The metric buttons pick what the chart and table show:

| Button | Per match | Unit |
|---|---|---|
| **PEAK TEMPERATURE** | The highest temperature | °C |
| **LOWEST SUPPLY VOLTAGE** | The lowest supply voltage | V |
| **SUPPLY ENERGY** | Energy drawn from the battery | Wh |
| **STALL TIME** | Time stalled | s |
| **SUPPLY CURRENT P95** | The 95th percentile of supply current | A |
| **REBOOTS** | Device reboots | count |
| **ISOLATED CAN GAPS** | CAN frame gaps on this device alone | count |
| **CAN FRAMES MISSED PER ENABLED MINUTE** | Missed CAN frames | /min |
| **LONGEST ISOLATED CAN GAP** | The longest gap on this device alone | ms |

The CAN metrics come from the CAN frames in the hoots.

**PER UNIT** draws one line per unit, following its serial number across slots and robots. **PER SLOT** draws one line per slot, named _robot_/_slot_, whichever unit was in it. A motor swapped into a slot shows as a new line per unit and the same line per slot.

The heading names the metric, its unit, and the reference value when there is one.

## Chart

- One point per match, in time order. The axis labels are the match part of the key (`qm7`).
- The line above the chart names the competitions: "COMPETITION · GACMP", or "COMPETITIONS · GADAL · 12 MATCHES → GACMP · 15 MATCHES" when there's more than one. A year is added to the names when the range spans more than one season.
- A dashed vertical line marks the start of each later competition, labelled with its event. A label that would overlap the one before it is left off; the line above the chart still names every competition.
- A dashed horizontal **REF** line shows the reference value: `temp_warn_c` for peak temperature and `voltage_danger_v` for lowest supply voltage ([report.toml](../config/report.md)). The other metrics have none.
- A match is shaded when its alignment confidence isn't high.
- With a unit selected (per unit), its line is drawn bright and thicker, with a warning-coloured dot on its low-alignment matches and a larger dot (**◆** in the legend) where it changed slot.
- A gap in a line is a match the unit wasn't installed for.

Point at the chart to see the match key and time. When the pointer is near a line, that line lights up and the readout adds the unit or slot, its value, its slot, the alignment, and "slot changed" when it moved. With no line near, the readout uses the selected unit's point.

Pressing the chart:

| Press on | Per unit | Per slot |
|---|---|---|
| Another unit's line | Selects that unit | |
| The selected unit's point, or near no other line | Opens that match in Replay, with that slot's tracks | |
| A slot's line | | Opens that match in Replay, with that slot's tracks |
| Anywhere else | Updates the readout only | Updates the readout only |

The legend under the chart names the selected unit ("select a unit to emphasise it" when there's none), the other lines, the reference, the low-alignment shading and the slot-change mark, when each is on the chart.

## Units table

The heading reads **UNITS · _n_ IN RANGE · _metric_**. There's one row per unit in range:

| Column | What it shows |
|---|---|
| **UNIT · SLOT** | The slot's role (or slot id), then the slot. Press it to select the unit; press it again to clear the selection |
| **SERIAL** | The serial number, or the unit id for a legacy unit |
| **IN SERVICE** | First and last day it was seen in range |
| **MATCHES** | Matches in range |
| **LATEST** | The metric in its latest match |
| **CHANGE** | Latest minus first, with a sign |
| **TREND** | A sparkline of the metric per match. Every row shares one scale, from zero unless a value is negative; hover for the unit's range and the shared scale |
| **HEALTH** | A status word, and an anomaly badge when it has open anomalies |

### Health

| Status | When |
|---|---|
| OK | The unit's highest temperature in range is below `temp_warn_c` |
| WARN | At or above `temp_warn_c` |
| HOT | At or above `temp_fault_c` |
| NO DATA | No temperature in range |

Open anomalies at normal sensitivity raise it: a HIGH anomaly makes the unit HOT, a MED or LOW one makes it at least WARN. The badge beside it, such as **2 HIGH**, is the count and worst severity of those anomalies, and opens them in Anomalies.

Health reads temperature only, so a motor with no temperature, such as one on a hoot without Phoenix Pro, shows NO DATA ([Phoenix Pro and temperature](../../robot-side/pro-and-temperature.md)). The limits are in [report.toml](../config/report.md).

## UNIT rail

With no unit selected: "Select a unit in the table to see its odometry and lifeline."

For a selected unit:

- its model and serial ("SN _serial_", or "legacy id"), its current role or slot, and its health. Here health is from temperature alone, over every match, so it can differ from the table's;
- totals, filtered by season and robot only: **MATCHES IN SERVICE**, **POWERED-ON** (h), **SUPPLY ENERGY** (Wh), **THERMAL CYCLES**, **STALL** (s), **MAX TEMPERATURE** (°C);
- "Sessions without aligned samples: _n_ (not counted above)";
- **LIFELINE**, newest first: **First seen**, **Moved**, **Replaced by** _unit_, **Last seen**, each with the date, the match key (or "practice"), the robot and slot, the bus and CAN id, and whether it came from a CAN inventory or a legacy epoch ([CAN inventory](../../robot-side/can-inventory.md));
- **MATCHES**: its 12 most recent matches in range, each with **REPLAY _match key_**, which opens it in Replay with that slot's tracks, and **LOGS**, which lists that match's raw logs below with download links and **OPEN IN ADVANTAGESCOPE**. If the match has no Replay data yet, it says "Replay data for this match is not built: run `flashpoint report --match <key>`";
- **LINK TO THIS UNIT**, an address that opens History with this unit selected.

A unit id that isn't in the lake (from an old link) shows "Not found", "No unit _id_ in this lake." and the **CLOSEST** ids, as links.

## Empty and error states

| State | What History shows |
|---|---|
| No match features in the lake | **No matches ingested**: "The lake at _lake_ has no match features yet." and "Ingest logs with `flashpoint ingest <folder of .wpilog and .hoot files>`, then reload." |
| The filters leave no match | "No matches in this range.", and which filter value no match in the lake has |
| The server can't answer | **History unavailable** and the error |

## Shared on the network

History changes nothing, so it works the same from another computer (`flashpoint serve --host 0.0.0.0`), except that the **OPEN IN ADVANTAGESCOPE** under **LOGS** says "Open in AdvantageScope is only available on the machine running Flashpoint. Use the downloads below." With `--host 0.0.0.0` that is true on the laptop itself too. Selecting a unit only changes your own page address. See [`flashpoint serve`](../cli.md#flashpoint-serve).
