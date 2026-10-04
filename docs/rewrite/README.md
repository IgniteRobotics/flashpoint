# Flashpoint Rewrite Research

*Generated: 2026-10-04 | Research mode: general (fallback) | Scope: all branches + external ecosystem*

Flashpoint ingests FRC robot logs (WPILib `.wpilog` and CTRE Phoenix 6 `.hoot`). Its goal is to detect anomalies and eventually predict failures over a robot's life (IgniteRobotics, 2026). This folder collects the research behind a ground-up rewrite.

## TL;DR

- **Main does not run as written.**
  - Both Docker images fail to build.
  - `setup-db.py` and `ingest_dir.sh` call a module that was renamed away.
  - Hoot conversion only works on Windows.
  - The only path that plausibly runs is the system-log ingest.

  See [03-pitfalls-and-performance.md](03-pitfalls-and-performance.md).
- **There are two competing architectures, both off main:**
  - The team's `development` branch: puller → importer → SQLite → Streamlit, in Docker.
  - Josh's `feature/power-tracking`: a tested `utils/` package of hoot → pandas → PDF or static Plotly site.

  The second has better engineering discipline. The first is closer to the stated "life of the robot" goal. Neither scales: one uses wide outer-joins of sparse signals, the other writes about 27 MB of JSON per match. See [02-branch-survey.md](02-branch-survey.md).
- **Recommendation:**
  - Rebuild around a **canonical long-format Parquet store queried with DuckDB/Polars**.
  - Read logs with the official readers: RobotPy `wpiutil.log` for wpilog, and a version-aware **owlet registry** for hoot, as AdvantageScope does.
  - Take match identity from in-log FMSInfo and enrich it with The Blue Alliance.
  - Track **physical units by CTRE serial number**, not CAN ID. Every robot logs a CAN inventory at boot (team decision, [99](99-ctre-device-serial-numbers.md)), so wear history follows a motor through swaps and between robots.
  - Don't rebuild AdvantageScope. Link to it.

  See [05-target-architecture.md](05-target-architecture.md) and [06-roadmap.md](06-roadmap.md).
- **Temperature risk, retired in P0:** without a Pro-licensed device, CTRE's free hoot export excludes continuous `DeviceTemp` (CTR Electronics, n.d.-a). **P0 finding (2026-10-04):** `owlet --check-pro` reports the team's 2025 and 2026 hoots as **Pro-licensed**, so thermal trending from hoot works ([ADR-0005](../adr/0005-temperature-source.md)).

## Documents

| # | Doc | What's in it |
|---|---|---|
| 01 | [Current capabilities](01-current-capabilities.md) | Every feature that exists on any branch, with data flow and schema diagrams |
| 02 | [Branch survey](02-branch-survey.md) | All 9 branches, project timeline, alternate approaches, what to salvage |
| 03 | [Pitfalls & performance](03-pitfalls-and-performance.md) | 57 cited defects: correctness, performance, ops, and process |
| 04 | [Ecosystem & tools](04-ecosystem-and-tools.md) | Existing FRC tools, data stack options, anomaly-detection libraries, match metadata APIs |
| 05 | [Target architecture](05-target-architecture.md) | Proposed rewrite design, data model, and decisions with trade-offs |
| 06 | [Roadmap](06-roadmap.md) | Phased plan, exit criteria, and what to reuse from today's code |
| 99 | [CTRE device serial numbers](99-ctre-device-serial-numbers.md) | Team decision: log CTRE serials on all robots; basis for unit identity (05 §4a) |
| — | [References](references.md) | Full APA 7 reference list |

## How code is cited

Repository code is cited as IgniteRobotics (2026), with an inline locator `branch:path:line`. For example, `main:ingest_library.py:137` or `origin/feature/power-tracking:utils/hoot_loader.py:86`. Line numbers come from the remote branch tips as of 2026-10-04:
- `main` at `70be731`
- `origin/development` at `16c8d88`
- `origin/feature/power-tracking` at `af97804`

## Audit

```
Queries sent: 22 (10 web search, 4 gh repo search, 1 gh code search, 7 direct doc fetch)
Sources received: 52 fetch attempts (46 succeeded, 6 failed)
Sources cited: 43 external + 1 repository
Failures: 6 fetches (moved/403 pages; substitutes used). 3-consecutive-failure stop: no
Per-source tier: see references.md (primary = spec/source/official docs; secondary = vendor blog/benchmark; tertiary = repo listings)
Routing decision: fallback (no specialist matched; codebase + tooling research)
Sub-questions:
  1. What does Flashpoint do today, on every branch?
  2. What alternate approaches have been tried, and what did they teach?
  3. What is broken, slow, or risky?
  4. What existing tools/specs/libraries can be leveraged instead of built?
  5. What should the rewrite look like, and in what order?
Not covered: QuestDB, Panel, Evidence, Prophet/statsforecast (no sources fetched).
```

Material marked `[Background — not from search]` is engineering judgment from the author, not a fetched source.
