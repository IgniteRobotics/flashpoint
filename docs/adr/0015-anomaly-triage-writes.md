# 0015. Anomaly triage writes

- Status: Proposed
- Date: 2026-10-11
- Amends: [0013](0013-views-one-local-app.md) (the read-only app) and [0014](0014-guarded-advantagescope-launch.md) (its one write route)
- Deciders: Josh
- Refs: openspec change `p5-anomaly-detection` (design.md D7, D8), D10, canvas board "Anomalies"

## Context
P5 detects anomalies and shows them in an Anomalies view. Findings only help if someone acts on them, and the canvas board closes the loop in the app: investigate, resolve with what was found, dismiss as expected, reopen. Until now the app's only state-changing route was the AdvantageScope launch (ADR-0014), and it writes nothing. The lake stays read-only toward logs and derived layers.

Triage is people's work. Detection must recompute anomalies from gold and configuration at any time, so triage can't live in the anomalies table. A rebuild, a re-derive, or a pipeline version change must never lose it.

## Decision
The local app accepts **one kind of write, a triage change**, through `POST /api/anomalies/<id>/triage`:
- **The same guard as the launch:** loopback bind, loopback client, `Host` and `Origin` naming this machine, a JSON body, and the `X-Flashpoint` header (value `triage`), which together force a CORS preflight the server never answers. The body is at most 4 KB. Shared on the network, or opened as a static export, the view is read-only and offers no actions.
- **Append-only storage in the ledger:** a new `anomaly_triage` table in `meta/flashpoint.sqlite`. Each change is a new row: time, previous state, new state, outcome text, the restart flag, and copies of the subject, metric, and onset match. Triggers refuse UPDATE and DELETE. The current state is the latest row per anomaly id. The ledger backup carries the table, and the restore drill checks it. Rebuild, derive, detect, and version changes never touch it.
- **A fixed transition table:** new → investigating or dismissed; investigating → resolved or dismissed; resolved or dismissed → new (reopen). Resolving and dismissing need an outcome of 1–500 characters. The client never sends a path, a query, or a state the table does not allow.
- **One follow-on process:** a dismissal with "restart baseline" starts `flashpoint detect` for this lake, detached, through the same spawn helper as the launch. Detection takes its own `meta/detect.lock`. When the lock is held, the acquire cycle runs detect later, because it checks for a pending restart every cycle.

Logs, bronze, silver, gold, and the ledger's records of files and logs stay read-only to the app. The only new metadata record a triage request creates is its own row.

## Consequences
- Triage survives every rebuild. Anomaly ids are stable, a hash of the subject, method, metric, and onset match, so the same anomaly comes back resolved.
- If configuration changes an anomaly's onset, its id changes, and the old triage shows in its tab as "no longer detected". Students re-triage the new id.
- The server and the acquire watch both write the ledger. WAL mode and the busy timeout already handle that, and triage inserts are single-row transactions.
- The server docstring and ADR-0013's "read-only" statement now carry this exception.

## Alternatives considered
- **A separate `triage.sqlite`:** backup and restore already carry the ledger image, and a second database is a second thing to lose.
- **A mutable state column:** loses the history the spec requires, and concurrent writers could overwrite each other.
- **Triage in a team tool (Notion, a spreadsheet) instead of the app:** findings and their outcomes would drift apart, and "seen this before" couldn't show past outcomes.
- **Writable from the pit network:** anyone on the network could close findings. Deferred until the app has accounts.
