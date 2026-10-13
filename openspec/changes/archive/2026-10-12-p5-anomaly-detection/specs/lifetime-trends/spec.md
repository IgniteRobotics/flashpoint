## MODIFIED Requirements

### Requirement: Read-only local query service
In served mode, the app SHALL answer History queries from the lake through a local query service. The service SHALL run with no network access. It SHALL be read-only toward logs and derived layers: no request changes raw files, bronze, silver, gold, or the ledger's records of files and logs. The only writes it SHALL accept are triage changes, as guarded and stored by anomaly review. Query parameters SHALL be treated as data, never as query text.

#### Scenario: Offline and read-only
- **WHEN** the app is served from a corpus lake with networking disabled, and every History query is exercised
- **THEN** each query answers, and the lake files are byte-identical afterwards

#### Scenario: Hostile parameter
- **WHEN** a History query is sent with the event filter `x' OR 1=1 --`
- **THEN** it returns no rows and no error, and the lake is unchanged

#### Scenario: Empty lake
- **WHEN** History is opened on a lake with no gold rows
- **THEN** it shows a "no matches ingested" message naming the lake path, and does not error

#### Scenario: Triage is the only write
- **WHEN** every History and Anomalies query is exercised and one anomaly is set to investigating
- **THEN** raw, bronze, silver, and gold files are byte-identical afterwards, and the only new metadata record is that triage change

### Requirement: Device table and summary tiles
Below the chart, History SHALL list every unit in range with:
- its current slot (role) and serial;
- its in-service span;
- the selected metric's latest value and change over the range;
- a small trend line;
- a health status.

Health SHALL be the worse of two parts:
- temperature limits: OK, WARN (a match maximum at or above the warning limit in range), or HOT (at or above the fault limit);
- the unit's open anomalies at normal sensitivity: a MED or LOW anomaly counts as WARN, and a HIGH anomaly counts as HOT.

In served mode, a unit with open anomalies SHALL show their count and highest severity in its row, linking to the Anomalies view filtered to that unit. Above the chart, tiles SHALL show units tracked, matches in range, total powered-on hours in range, and units with a WARN or HOT status.

#### Scenario: Health from temperature
- **WHEN** a unit's match maximum temperature reached 66 °C once in the range
- **THEN** its health reads WARN, and the "units warn/hot" tile counts it

#### Scenario: Health from an anomaly
- **WHEN** a unit stays under every temperature limit but has an open HIGH SIBLING anomaly
- **THEN** its health reads HOT, its row shows 1 HIGH anomaly linking to the Anomalies view, and the "units warn/hot" tile counts it

#### Scenario: Closed anomalies do not count
- **WHEN** a unit's only anomaly is dismissed
- **THEN** its health is computed from temperature alone
