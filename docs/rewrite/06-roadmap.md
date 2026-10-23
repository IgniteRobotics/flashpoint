# 06 — Rewrite Roadmap

Phases are ordered by dependency, not by calendar. Each phase ends with something usable at a competition. Sizes are relative (S/M/L), because team availability drives the dates.

**Status and tracking live on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).** Each phase below links its `roadmap` issue, which holds its status, what shipped, and what's left. Bugs and follow-ups hang off it as sub-issues. This doc keeps the scope, dependencies, migration plan, quality gates and risks.

## 1. Overview

```mermaid
flowchart LR
    P0[P0 Stabilize<br>& decide] --> P1[P1 Core readers<br>& lake]
    P1 --> P2[P2 Semantics<br>& physics]
    P2 --> P3[P3 Acquire<br>& automate]
    P2 --> P4[P4 Views]
    P3 --> P5[P5 Anomaly<br>detection]
    P2 --> P2b[NT signal<br>mapping]
    P2b --> P5
    P4 --> P5
    P4 --> P4b[Live & Pit views]
    P3 --> P4b
    P3 --> P4c[Import status view]
    P4 --> P4c
    P4 --> P4e[Live CAN health]
    P4e --> P4b
    P3 --> P4f[Live import updates]
    P4 --> P4f
    P4f --> P4c
    P3 --> P4g[Team setup]
    P4 --> P4g
    P4g --> P4h[Team branding]
    P4 --> P4d[Replay UX]
    P5 --> P6[P6 Prediction<br>& fleet]
```

## 2. Phases

### P0 — Stabilize & decide (S)
> **Tracking:** [#16](https://github.com/IgniteRobotics/flashpoint/issues/16) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

- **Repo:**
  - Tag the current `main` as `legacy-2025`.
  - Archive the branches under `archive/*`: `development`, `power-tracking`, `static-site`, `dataviz`, `docker`.
  - Delete `hoot-support`.
  - Protect `main` and require PRs plus CI.
- **Decisions:** make the calls D1–D11 in [05 §6](05-target-architecture.md#6-key-decisions). Record each as a short ADR in `docs/adr/`.
- **Golden corpus:** pick about 6 real logs and commit them through LFS or a fetch script.
  - one qual wpilog, plus its rio and CANivore hoots
  - one practice log
  - one non-FMS log
  - one log with a corrupt tail
  - one 2025 log and one 2026 log (owlet versions)
- **Robot-side CAN inventory logger (all robots):** implement the contract in [05 §4a](05-target-architecture.md#4a-device-identity-slot-vs-unit) and [99](99-ctre-device-serial-numbers.md).
  - First open `http://<rio>:1250/?action=getdevices` against the current Phoenix 6 robot to confirm the field names. The only documented example is from Phoenix 5 (CTR Electronics, n.d.-h).
  - Ship it before the next practice session, because every log recorded without it becomes a legacy-identity log.
  - Back-fill a `units` seed: walk each robot and spare shelf with Tuner X, record each serial against its slot, and label the motors physically.
- **Pro check:** run `owlet --check-pro` (Mechanical Advantage, 2026) on the corpus. ✅ Done 2026-10-04: all hoots are Pro-licensed (ADR-0005). `--compliancy` was also found to be the owlet selection key (ADR-0004).
- **Exit:** ADRs merged, corpus available, CI skeleton green, `/Flashpoint/CANInventory` present in a fresh log from every robot.

### P1 — Core readers & lake (L)
> **Tracking:** [flashpoint-private#67](https://github.com/IgniteRobotics/flashpoint-private/issues/67) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

- **wpilog reader** on `robotpy-wpiutil` (RobotPy, n.d.). Stream it into Arrow record batches and write bronze Parquet. Handle struct-typed entries (WPILib Developers, 2023).
- **owlet registry:**
  - `(phoenix_api_version, os, arch) → binary`, with checksums and lazy download.
  - Pick the version by probing the hoot. Fail loudly on version mismatch (CTR Electronics, n.d.-c).
  - Build a multi-arch container (fixes the x86-only Linux binaries).
- **Ledger:** content hash with `received` → `bronze` → `silver` → `gold` → `success` states plus `pipeline_version`, and quarantine on error. This ports the idea from `legacy-2025:ingest_system_log.py`.
- **Metadata extraction:** FMSInfo (WPILib Developers, 2026a), git metadata, and `systemTime` anchoring (WPILib, n.d.-a).
- **CLI:** `flashpoint ingest PATH...` and `flashpoint doctor` (environment, owlet, and Pro checks).
- **Exit:**
  - The golden corpus ingests byte-for-byte reproducibly.
  - Re-ingesting is a no-op.
  - Ingesting a full qual match **(wpilog + 2 hoots) takes under 30 s and under 1 GB RAM** on a pit laptop. Benchmark it in CI.

### P2 — Semantics & physics (M)
> **Tracking:** [flashpoint-private#68](https://github.com/IgniteRobotics/flashpoint-private/issues/68) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

- **Config:**
  - Season TOML and robot TOML, validated with pydantic.
  - Generated from today's `datamaps/*.csv` and `utils/motors.toml` by a one-off migration script. Fix the `Pheonix6` typo during migration.
- **Match identity resolver:** FMSInfo, then the filename regex (one implementation, covering `P|Q|E` per WPILib Developers, 2026b), then an optional TBA key lookup (The Blue Alliance, n.d.).
- **Session grouping:** pair the wpilog and hoots by time overlap.
- **Device identity:** add `identity/inventory.py`.
  - Map the CANInventory entry to `DEVICE_OBSERVATION`, `UNIT`, and `SLOT`, detect mid-session swaps, and assign legacy epochs for old logs.
  - `doctor` reports unmapped serials and slots with no observation.
- **Match phases:** auto, teleop, and disabled, taken from DS enable and autonomous flags. Fixes #13.
- **Physics:** port `analyzer.py` from power-tracking, along with its tests, onto Polars:
  - Use time-weighted means.
  - Don't zero-fill before the first sample.
  - Keep velocity signed.
  - Fixes #23, #26, #27.
- **DCMotor residuals:** expected current from motor constants (WPILib, n.d.-c) and the configured gear ratio.
- **Gold:** the `match_slot_features` table, keyed by match, slot, and unit.
- **Exit:** power-tracking's numbers reproduce within tolerance on the corpus. The gold table answers "max temp of every drive motor at GADAL" and "lifetime Wh for serial X across all robots" in under 1 s in DuckDB.

### NetworkTables signal mapping (S–M, follow-up to P2)
> **Tracking:** [flashpoint-private#69](https://github.com/IgniteRobotics/flashpoint-private/issues/69) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

P2 mapped CAN devices (slots and units) into robot config, and deliberately left out signals that only exist in NetworkTables: subsystem telemetry the robot code publishes, and vision. They are still described only by legacy files, which is the last thing blocking `retire-legacy-code` stage 2b-ii.
- **Season config:** add `config/seasons/<year>.toml` (planned in `05-target-architecture.md`, not built yet) with the NT prefixes from `log_configs/config2025.json`: metrics, preferences, FMS, PhotonVision, and CameraPublisher.
- **Robot config:** NT entries get the same labels as CAN slots (subsystem, assembly, subassembly, component, metric), from `datamaps/2025/metrics_map.csv` (28 rows). Cameras and their metrics come from `datamaps/2025/vision_map.csv` (36 rows).
- **Migration:** extend `tools/migrate-legacy-config.py`, which reads from the `legacy-2025` tag. **2024 is out of scope** (decided 2026-10-06): there is no 2024 robot config, and the 2024 maps and log config stay recoverable from the tag.
- **Derive:** label NT signals in silver alongside slot signals, so Replay tracks, History, and P5 rules can use subsystem telemetry (for example the 2025 corraler motor current and voltage).
- **Exit:** every row of the two 2025 maps and every prefix in `config2025.json` is covered by config, verified by a test against the tag. `retire-legacy-code` 3b can then delete `datamaps/` and `log_configs/`.

### P3 — Acquire & automate (S)
> **Tracking:** [#17](https://github.com/IgniteRobotics/flashpoint/issues/17) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

- **SFTP puller** (paramiko or asyncssh, [Background]):
  - copies to the inbox, verifies size and hash, and **never deletes on the robot by default**;
  - is aware of rotation: warn when robot free space is under 100 MB, since WPILib and Phoenix prune at 50 MB (WPILib Developers, 2026b; CTR Electronics, n.d.-a).
- **Removable media:** USB sticks (macOS, Linux, Windows) are a pull source next to the robot, read-only and hash-verified. Manual inbox drops still work.
- **Watcher:** `flashpoint acquire --watch` cycles robot, volumes, ingest, derive, the Replay build and backup.
- **Packaging:** a systemd user unit with a correct `ExecStart` (#7) and a Windows scheduled task. Windows is supported. The container is deferred out of P3.
- **Backup:** `rclone` copy of `lake/raw` (immutable) and a `lake/meta` snapshot [Background]. Replaces `drive-backup.py`.
- **Exit:** plug in the robot, and within 5 min its matches show up in the views, unattended.

### P4 — Views (M)
> **Tracking:** [flashpoint-private#71](https://github.com/IgniteRobotics/flashpoint-private/issues/71) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

- **Per-match static site:**
  - Rebuild the power-tracking SPA on silver and gold.
  - Payload ≤ 2 MB per match with min/max envelopes.
  - Escape all values (#56).
  - Ship a `serve` command.
  - Add an "Open in AdvantageScope" download link for the raw wpilog (Mechanical Advantage, n.d.-a).
- **Lifetime app** (marimo, per marimo, n.d.):
  - per-slot **and per-unit** trend lines across matches, events, and robots;
  - a unit page with odometry (hours, Wh, thermal cycles, stall-seconds) and the slot history the serial has occupied;
  - filters push down to DuckDB SQL instead of loading the whole table (#35).
- **Optional:** Grafana with the DuckDB plugin on the pit server (MotherDuck, n.d.).
- **Exit:** a drive coach can answer "is FL-drive running hotter than last event?" in two clicks, offline.

### AdvantageScope launch (S, follow-up to P4)
> **Tracking:** [flashpoint-private#72](https://github.com/IgniteRobotics/flashpoint-private/issues/72) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

### Live & Pit views (M, follow-up to P4)
> **Tracking:** [#18](https://github.com/IgniteRobotics/flashpoint/issues/18) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

The canvas's **Live** and **Pit** boards read the robot, not the lake, so P4 left them out. They become one change after P4 and P3:
- an NT4 client (or a small relay so one robot connection serves many viewers) feeding Live at about 2 Hz;
- the Pit board's checks and GO / DECIDE / NO-GO verdict;
- both as views in the P4 shell: one file in `web/static/views/` and one `FP.view` call each, no restyling.
- **Needs first:** data sources for pit spin-test current, fault counts, firmware events, and batteries (none are logged today), and a decision on whether views may write (notes, swaps, pit tasks); P4 views are read-only.

### Live CAN health (M, follow-up to P4 and CAN health)
> **Tracking:** [#22](https://github.com/IgniteRobotics/flashpoint/issues/22) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3). **OpenSpec:** `live-can-health`.

Split out of Live & Pit views so the pit crew gets live feedback during shake tests (disabled) and drive tests (enabled):
- **Robot side:** robot code publishes running totals per CTRE device over NetworkTables under `/Flashpoint/Live/CANHealth/` (dropouts, worst signal age, resets, fault bits, lowest supply voltage), plus bus status and `Enabled`. Totals, so a 40 ms dropout still counts over a link that updates about every 100 ms. This repo carries the contract, a guide and a suggested Java sample; the robot change is a Robot-2026 PR.
- **Laptop side:** one NetworkTables connection in `serve`, events from what changed between updates, the `can-health` sibling rule (isolated vs bus-wide), and a Live CAN view with marks.
- **Live only:** nothing written to the lake. The Pit board can use its results later.

### Live import updates (M, follow-up to P3 and P4)
> **Tracking:** [#23](https://github.com/IgniteRobotics/flashpoint/issues/23) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

During an event, logs arrive every few minutes through the `acquire` watch, but the app only shows them after a restart or `flashpoint report` (#65), and import problems stay in `doctor` output and log lines.
- **Views update as imports happen:** `serve` notices lake changes made by other processes and pushes them to open browsers. Replay's match list gains new matches, an open match reloads in place, History and Anomalies refresh. Fixes [#15](https://github.com/IgniteRobotics/flashpoint/issues/15).
- **Import activity in the shell:** a header indicator (idle / pulling / importing / building Replay) and a feed of pulled, imported, warned and quarantined files.
- **Problems as they happen,** linked to their match: import problems (quarantine, `incomplete-read`, `short-coverage`, truncation, failed pulls, low robot space) and data problems (missing bus hoot, missing declared signals, unmapped devices, no CAN inventory, suspect clock alignment, no match key).
- **One problem list:** this change defines it; the Import status view builds its per-event table on it. Read-only; retries stay on the CLI ([#14](https://github.com/IgniteRobotics/flashpoint/issues/14)).

### Import status view (S–M, follow-up to P3 and P4)
> **Tracking:** [#19](https://github.com/IgniteRobotics/flashpoint/issues/19) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

A view in the P4 shell that answers three questions about the lake: **are we missing logs, did they all import clean, and do the clocks line up?** Most of the data is already in the ledger. Little of it is visible today outside `flashpoint doctor` and SQL.
- **Missing logs:**
  - per session: expected buses (from the robot config) against the hoots actually grouped in `session_hoots`, so a session with a wpilog but no CANivore hoot stands out;
  - per event: gaps in the match sequence (Q1…Qn from FMS info or filenames). The Blue Alliance schedule fills in the expected list when online, which stays optional;
  - files seen by `acquire` (`pulls`) that never reached `success`.
- **Clean import:**
  - `files.stage`, `reason` and `warnings` (quarantined, `incomplete-read`, `short-coverage`);
  - `logs.truncated_bytes` and `orphan_records`, `hoot_logs.read_status` and `integrity`;
  - `unmapped_devices` and logs with no CAN inventory (legacy epochs);
  - **new:** record when owlet exports of a hoot differed in size ([#4](https://github.com/IgniteRobotics/flashpoint/issues/4) tail loss). Today that is only a log line, so it can't be counted.
- **Timing:**
  - per hoot: `session_hoots.offset_us`, `method`, `confidence`, `spread_us`, `bus_agreement_us`, with low-confidence or disagreeing alignments flagged;
  - per wpilog: `anchor_source` and `utc_offset_us`, flagging logs anchored by fallback instead of `systemTime`;
  - framing: matches whose `match_phases` came from a fallback source.
- **Shape:** a table per event, one row per match, a status light per check, and drill-through to Replay and AdvantageScope. It is read-only, like the other P4 views. Re-ingest or re-derive stays on the CLI.
- **Exit:** after an event, a student can see in one screen which matches are missing logs, which imported with warnings, and which have suspect clock alignment, without SQL.

### Team setup (L, follow-up to P3 and P4)
> **Tracking:** [#24](https://github.com/IgniteRobotics/flashpoint/issues/24) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3). **OpenSpec:** `team-setup`.

Make Flashpoint usable by any FRC team, not just 6829 with access to a private repo:
- **Public install:** the repo goes public and Flashpoint is published to PyPI, so `pipx install flashpoint` works as ADR-0009 assumed. Bundled defaults ship in the package; Ignite's robots and units stay in the repo's `config/`, out of the wheel.
- **Configuration home:** a per-user folder created by `flashpoint init` (team number, lake folder). The fallback to a checkout's `config/` goes away (fixes [flashpoint-private#54](https://github.com/IgniteRobotics/flashpoint-private/issues/54)). The team number drives the robot addresses.
- **Setup view:** the whole configuration and every setup check (`doctor` uses the same checks), editing of team, robots and slots, thresholds and units with the loading rules and backups, drafting a robot from a log, and guarded fix actions (create config, owlet, AdvantageScope, acquire service). Read-only when shared on the network.
- **Needs first:** a human review of what going public exposes (corpus match logs, CAN ids and serials).
- **Status (2026-10-21):** implemented in two PRs into `rewrite`: packaging, config home, `init` and setup checks ([flashpoint-private#88](https://github.com/IgniteRobotics/flashpoint-private/pull/88)), then the Setup view, editor, fix actions and robot draft (ADR-0017). Still with a human: going public, claiming the PyPI name and the first release (`v0.2.0`), and the new-team walkthrough.

### Team branding (S–M, follow-up to Team setup)
> **Tracking:** [#25](https://github.com/IgniteRobotics/flashpoint/issues/25) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3). **OpenSpec:** `team-branding`.

Each team sees its own name and colours instead of Ignite's:
- **Team name** in the header and page title, set in `theme.toml`, by `flashpoint init`, or in the Setup view.
- **Palette:** presets (`amber`, today's look, stays the default) or a custom primary, background and warning colour, with every shade derived from them.
- **Readable by rule:** text keeps 4.5:1 contrast on every surface and warnings stay distinct from the primary colour; a failing palette is refused with a suggested fix.
- Applies to every view, the charts and static exports. Colour tokens are renamed from amber to roles (`--accent*`, `--warn`).

### Replay UX (S, follow-up to P4)
> **Tracking:** [flashpoint-private#73](https://github.com/IgniteRobotics/flashpoint-private/issues/73) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

Two improvements to the Replay board:
- **Match list filters:** filter the left match list by season (year), robot, and competition (event). The filters combine, live in the URL like the rest of Replay's state, and default to the most recent event. The static export filters the same way over the matches it contains.
- **Timeline zoom:** zoom in and out on the Replay timeline (buttons, scroll or pinch, and drag to select a range), with every track and marker following the same window. Envelopes are about 1000 buckets per match (about 0.15 s each for a full match), so:
  - zooming within that resolution works everywhere, including the static export;
  - finer detail needs `serve` to fetch higher-resolution envelopes for the visible window from silver. Static exports stop at bucket resolution and say so;
  - single-sample detail stays in AdvantageScope (D8: link, don't rebuild).
- **Exit:** a drive coach finds a match by year, robot, and event in two clicks, and can zoom to a two-second brownout without leaving Replay.

### CAN health (S–M, precursor to P5)
> **Tracking:** [flashpoint-private#74](https://github.com/IgniteRobotics/flashpoint-private/issues/74) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

- **Change:** `can-health`. It needs Robot-2026#108 on the robot for bus status.
- **What it adds:**
  - reboot events, from `ResetCount` rises and `Fault_BootDuringEnable`;
  - frame gaps, measured against each device's own median frame interval while enabled. A gap that most of the device's bus siblings share is a logger gap (the CANivore shows dozens per match), never charged to the device;
  - per-match counts per unit and slot in `gold/can_health`, logger gaps and bus status in `gold/can_bus_health`, and events in `gold/can_events`.
- **Views:**
  - Replay: reboot (FAULT), isolated dropout ≥ 0.25 s (WARN), and whole-bus silence ≥ 1 s (WARN). These replace the old silver sample-gap marker;
  - History: reboots, isolated gaps, missed frames per enabled minute, and the longest gap, per unit.
- **Labelled case:** Johnson Q105.
  - The intake roller leader (rio TalonFX 2) rebooted after an 820 ms dropout.
  - The intake-extension follower (rio TalonFX 14) had 42 isolated gaps, against 7, 20 and 1 in Q15, Q53 and Q76.
- **Deferred:** chain-order localisation, to the 2027 robot.

### P5 — Anomaly detection (L)
> **Tracking:** [#20](https://github.com/IgniteRobotics/flashpoint/issues/20) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

- **Tier 1, rules:**
  - temperature thresholds (port 55 °C avg / 65 °C max);
  - stall seconds (high current at near-zero velocity);
  - CAN dropout counts: done from the hoots by **CAN health** (above), with no DS log reader. Brownout counts still need the DS log reader (LigerBots, 2026);
  - current residual P95 above a threshold.
- **Tier 2, baselines:**
  - robust z-score (median/MAD) of each gold feature against that **unit's** own history. A swapped-in unit starts its own baseline automatically;
  - also compared against the sibling **slots** (the 4 drive motors should look alike).
- **Tier 3, models:**
  - IsolationForest (scikit-learn developers, n.d.) or PyOD detectors (Zhao et al., n.d.) on gold feature vectors;
  - River HalfSpaceTrees for online scoring (River developers, n.d.);
  - STUMPY discords on silver current traces to catch odd shapes (STUMPY developers, n.d.).
- **Output:** an `anomalies` table, badges in the views, and an optional Slack or Discord webhook.
- **Exit:**
  - Backtest on the 2025–2026 logs flags at least one known real failure (team pit notes are the ground truth).
  - False-positive rate is low enough that students don't learn to ignore it.

### P6 — Prediction & fleet (L, stretch)
> **Tracking:** [#21](https://github.com/IgniteRobotics/flashpoint/issues/21) on the [Flashpoint project](https://github.com/orgs/IgniteRobotics/projects/3).

- Label failures from pit notes and maintenance records to make a `maintenance_events` table keyed by **serial number**. Unit identity makes these labels reliable.
- Model time to failure or degradation slopes per component (thermal rise rate, residual drift). Literature shows measurable current and speed shifts before faults (Jin et al., 2014).
- Optionally pool data across robots or teams. That needs the robot config to be portable.

## 3. Migration of existing assets

| Asset | Action | Phase |
|---|---|---|
| Raw logs on Drive / `telemetry/` | Bulk `flashpoint ingest` into `lake/raw` (dedup by hash) | P1 |
| `db/robot.db`, GRITS.db | **Don't migrate.** Rebuild from raw. Keep as a read-only archive | P1 |
| `datamaps/*.csv`, `log_configs/*.json`, `utils/motors.toml` | Script → `config/seasons/*.toml`, `config/robots/*.toml` | P2 (device maps); NT signal mapping (NT maps, log configs) |
| power-tracking `analyzer.py` + tests | Port to Polars; keep the test cases as golden expectations | P2 |
| power-tracking SPA + specs | Reference only; the canvas sets the UX (Replay + History, ADR-0013) | P4 |
| `development` match regex | Becomes the single filename-fallback parser | P2 |
| Known motor swaps (pit notes, memory) | Declare as `[[slots.swaps]]` dates in robot TOML → legacy unit epochs | P2 |
| Excalidraw diagrams | Replace with the mermaid diagrams in these docs | P0 |

## 4. Branching

```mermaid
gitGraph
    commit id: "legacy main"
    commit id: "P0 (PR 9)"
    branch rewrite
    branch feature/p1-core-ingest-lake
    commit id: "P1"
    checkout rewrite
    merge feature/p1-core-ingest-lake
    branch feature/p2-semantics-and-physics
    commit id: "P2"
    checkout rewrite
    merge feature/p2-semantics-and-physics
    commit id: "P3, P4, legacy stages 2a–2d"
    checkout main
    merge rewrite id: "one-time replacement"
```

- `rewrite` was the integration branch, cut from `main` after P0 (2026-10-04). Every phase and every legacy-removal stage was a `feature/*` branch from `rewrite`, merged back through a PR with CI.
- **Done 2026-10-22:** `rewrite` merged into `main`, replacing the legacy code, and was retired. The install steps and the developer guide's branching section were updated in the same PR.
- From here on, `main` is the only long-lived branch: each change is a `feature/<name>` branch from `main`, merged back through a PR with CI.

## 5. Quality gates (every phase)

- **CI:** `pytest --strict-markers`, coverage on `src/flashpoint/{readers,lake,physics}` ≥ 85%, ruff, mypy `--strict`.
- **Golden-corpus E2E** on every PR. Add a perf budget check from P1 onward.
- **Container builds:** amd64 and arm64.
- **Docs:** the ADR log is updated when a decision changes.

## 6. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| ~~Logs are not Pro, so no hoot temperature~~ | Retired | — | P0 confirmed the logs are Pro-licensed (ADR-0005). `doctor` flags any log that isn't |
| owlet / Phoenix format changes mid-season | Medium | High | Registry plus `doctor`. Pin the robot's Phoenix version during events |
| Logs rotate out before they're pulled | Medium | High | P3 free-space alerts. A USB drive on the robot |
| Student turnover leaves the code unmaintained | High | Medium | Small core, typed, tested, ADRs, marimo notebooks as the on-ramp |
| Diagnostics server not reachable or schema changes, so no inventory | Medium | High | Normalize on the robot (contract schema v1), never crash, log an error payload. `doctor` alerts on logs with no inventory. Fall back to legacy epochs |
| Too few failures to learn from | High | Medium | Lean on Tier 1 and 2 (physics and baselines). ML is optional |
| Scope creep into rebuilding AdvantageScope | Medium | Medium | D8: link, don't rebuild |

## 7. Odds

**[Background]** My honest estimate is that P0–P4 delivers a working tool by the 2027 events. Doing the first five phases before anomaly detection beats trying to bolt ML onto today's pipeline. Today's storage and joins return wrong numbers (#10–#27), so a model trained on them would learn the bugs.
