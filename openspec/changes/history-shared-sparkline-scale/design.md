## Context

`/api/devices` already returns every unit in range with its `spark` list (the selected metric per match, `null` where the unit has a row but no value; a match where it was not installed has no entry at all), ordered by time. The device table draws each with `FP.spark(values)`, which scales to that list's own min and max plus 10 % padding (`shell.js`). The table renders every row the server returns, so the browser holds every value the shared scale needs. See proposal.md for why.

## Goals / Non-Goals

**Goals:** one scale per render of the table, computed from exactly the rows shown; a description per cell that a screen reader and a hover both get.

**Non-Goals:** reusing the shared scale on the main chart (it already has one y-axis); persisting or configuring the scale.

## Decisions

1. **Compute the scale in the browser, not the query service.** The rows are already in the response, the scale is a pure function of them, and it changes with nothing but the response. A server field (`spark_range`) would duplicate data the client has and widen an API the spec does not need. Alternative considered: DuckDB `min`/`max` over the same filter; rejected for the reason above.

2. **`FP.spark(values, { range: [lo, hi] })`.** With a range, the helper scales to it and does not pad below `lo`, so a zero baseline sits on the cell's bottom edge; it keeps a small pad above `hi` so the top value is not clipped by the stroke. Without a range it behaves as today (other callers are unchanged). When `lo == hi` (for example every value 0) the line is drawn flat at the bottom.

3. **Zero baseline rule.** `lo = min(0, lowest value)`, `hi = highest value`, over every non-null value of every row. That gives "from zero when nothing is negative" and "lowest to highest otherwise" from one expression.

4. **Description.** The cell's `title` and the canvas's accessible label read, for example, `This unit 0–3 · shared scale 0–42 count`, numbers formatted with `FP.fmt` like the LATEST column; a unit with one value reads `This unit 4 · shared scale 0–42 count`. The canvas carries `data-scale="lo hi"` so browser tests can assert the scale without reading pixels; the "bottom tenth" scenario is checked once with `getImageData`.

No D1–D11 decision is touched (this is view rendering only), and no full logs are read: the table works on the gold features already queried, so the memory and time budgets of `/api/devices` are unchanged.

## Risks / Trade-offs

- [One extreme unit flattens every other row] → intended: the shared scale is the comparison; the unit's own range stays in its description, and selecting the unit draws its full detail on the main chart.
- [A metric with a large constant offset (for example a temperature that sits at 40–45 °C) loses visible wiggle against a zero baseline] → accepted for comparability; the description gives the exact range. Revisit only if the pit check says temperature rows become unreadable.
