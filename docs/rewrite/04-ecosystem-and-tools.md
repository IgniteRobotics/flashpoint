# 04 — Ecosystem: Existing Tools to Use

The rule for the rewrite: **only build what is Flashpoint-specific**, meaning lifetime trends across matches and anomaly detection. Parsing, single-log viewing, and match metadata already exist.

## 1. Log formats and official readers

### WPILib DataLog (`.wpilog`)
- **Spec.** The format is "a simple binary logging format designed for high speed logging of timestamped data values" (WPILib Developers, 2022).
  - All values are little-endian.
  - It has three control record types: Start, Finish, and Set metadata.
  - "There is no timestamp ordering requirement for records."
  - A Kaitai Struct definition (`wpilog.ksy`) ships alongside the spec.
- **Struct and protobuf typed entries.** WPILib's packed-struct spec defines an embedded schema to "enable dynamic decoding" (WPILib Developers, 2023). Today's `csv_converter.py` silently drops these.
- **Readers.**
  - "WPILib provides a `DataLogReader` class for Java, C++, and Python" (WPILib, n.d.-b).
  - In Python, RobotPy's `wpiutil.log.DataLogReader` (RobotPy, n.d.) is a binding over the C++ reader. **Use it instead of the vendored pure-Python `datalog.py`.**
  - `robotpy-wpiutil` is already in `main:requirements.txt` but has never been used.
- **DataLogTool.** It "integrates a SFTP client for downloading data log files" and exports CSV in list or table style (WPILib, n.d.-b).

### CTRE Phoenix 6 hoot (`.hoot`)
- **Capture.** Status signals are captured "automatically with their timestamps from CAN", and "Logging is not affected by the timing of the main robot loop or Java GC". There is one file per CAN bus (CTR Electronics, n.d.-a).
- **Conversion.** owlet is "a command-line utility to convert CTR Electronics hoot (.hoot) files into other logging file formats". Usage: `owlet -f wpilog in.hoot out.wpilog` (CTR Electronics, n.d.-b). It can also export MCAP (CTR Electronics, n.d.-a).
- **Licensing.**
  - Without a Pro-licensed device in the log, only a fixed free subset exports.
  - For Talon FX that subset is SupplyCurrent, StatorCurrent, MotorVoltage, Position, Velocity, and fault booleans.
  - It does **not** include continuous DeviceTemp (CTR Electronics, n.d.-a).
  - **P0 finding (2026-10-04):** the team's 2025 and 2026 hoots are Pro-licensed, so everything exports (ADR-0005).
- **Prior art for version handling.** AdvantageScope's `owletInterface.ts`:
  - picks "an owlet executable capable of opening the Hoot log",
  - spawns `owlet <hoot> <wpilog> -f wpilog`,
  - uses `owlet --check-pro` (Mechanical Advantage, 2026).
  - **P0 finding (2026-10-04):** `owlet --compliancy` reports the hoot format version (2025: 13, 2026: 19). That is the key for choosing an owlet version (ADR-0004).

  **Copy this design.**
- **Hoot Replay** needs a Pro-licensed device (CTR Electronics, n.d.-d). It isn't relevant to analytics.

### REV and Driver Station logs (not supported today)
- **REV `.revlog`.** REV's 2026 Status Logger is "the official solution to logging CAN status frames for REV devices". Its logs "must first be converted to a .wpilog", either through AdvantageScope or the `revlog-converter` npm package (REV Robotics, n.d.). URCL is the older community option (Mechanical Advantage, n.d.-b).
- **Driver Station `.dslog`/`.dsevents`.** These carry brownouts, packet loss, and battery voltage, all strong failure signals. Python parser: `ligerbots/dslogparser` (LigerBots, 2026).

## 2. Viewers: don't rebuild AdvantageScope

| Tool | What it does | Role in Flashpoint |
|---|---|---|
| **AdvantageScope** (Mechanical Advantage, n.d.-a) | Opens WPILOG, DS logs, Hoot, REVLOG, CSV, RLOG. Graphs, 3D field, CSV export | **Deep dive into a single log.** Flashpoint should give a download or open link, not launch it on the server (see `origin/development:viz.py:88-94`) |
| **AdvantageKit** (Littleton Robotics, n.d.) | "a logging, telemetry, and replay framework" that writes wpilog | Optional robot-side upgrade; deterministic replay |
| **DataLogTool** (WPILib, n.d.-b) | SFTP download and CSV export | Manual fallback for retrieval |
| Glass / Shuffleboard / Elastic | Live dashboards | Out of scope (live, not historical) |

**The gap Flashpoint fills.** Every tool above looks at **one log at a time**. Nothing in the ecosystem tracks *motor TalonFX-11 across 80 matches and 3 events*. That is the product. A GitHub search turned up no maintained Python wpilog → Parquet/Polars pipeline (search listing only, not verified in depth). The closest community projects are `jonahsnider/wpilog-parser` (TypeScript) and `TripleHelixProgramming/wpilog-mcp`.

## 3. Data stack options

### Engine

| Option | Evidence | Fit |
|---|---|---|
| **pandas** (today) | 365.71 s at PDS-H SF-10, against Polars streaming at 3.89 s and DuckDB at 5.87 s. The benchmark is published by the Polars vendor (Polars, 2025) | Too slow for wide time-series joins and object-dtype strings |
| **Polars** | Lazy and streaming. `join_asof` matches "on nearest key rather than equal keys", with a tolerance such as `"1ms"` (Polars, n.d.) | ✅ Transform layer |
| **DuckDB** | Queries pandas or Polars frames "as if they are regular tables" (DuckDB Foundation, n.d.-a). On Parquet "only the columns required for the query are read", with zonemap skipping, globs, and hive partitioning (DuckDB Foundation, n.d.-b). ASOF join (DuckDB Foundation, n.d.-c) | ✅ Query/serving layer, embedded, no server |

### Storage

| Option | Evidence | Fit |
|---|---|---|
| **SQLite** (today) | — | ❌ Row store and TEXT columns. Wrong tool for billions of samples |
| **Parquet (hive-partitioned)** | InfluxDB 3 is built on Arrow, DataFusion, and Parquet. "Parquet actually offered 5x better compression than more specialized time series file formats" (Lamb, 2024) | ✅ **Recommended.** Plain files: copyable to a laptop, back up with any sync tool |
| TimescaleDB | Hypertables "automatically partition your time-series data by time" (Tiger Data, n.d.) | ⚠️ Needs a Postgres server. Worth it only if Flashpoint becomes a multi-team hosted service |
| InfluxDB / QuestDB | Not evaluated (no sources fetched) | — |

### Presentation

| Option | Evidence | Fit |
|---|---|---|
| Streamlit + PyGWalker (today) | `cache_resource` is "shared across all users, sessions, and reruns" (Streamlit, n.d.). That is the root cause of the filter bugs | ⚠️ Fine for exploration, fragile for apps |
| **marimo** | "stored as pure Python (Git-friendly), executable as a script, and deployable as an app", with native SQL (marimo, n.d.) | ✅ Analyst notebooks and apps over DuckDB, reviewable in PRs |
| **Grafana + DuckDB plugin** | "lets Grafana query local DuckDB files". The plugin is unsigned and needs glibc Linux (MotherDuck, n.d.) | ✅ Lifetime dashboards and alerting, if a server is available |
| **Static site** (power-tracking) | Already built. Offline Plotly | ✅ Per-match reports at events. Shrink the payload (see [05](05-target-architecture.md)) |

## 4. Anomaly detection and predictive maintenance

| Library | What it gives you | Use for |
|---|---|---|
| **WPILib `DCMotor`** | Can "Calculate current drawn by motor with given speed and input voltage". Ships constants for Kraken X60/X44, Falcon 500, NEO, Vortex, and Minion (WPILib, n.d.-c) | **Physics baseline.** Residual = measured current − model current at (V, ω). Explainable to students |
| **PyOD** | "the most comprehensive Python library for anomaly detection", with 61 detectors behind one `fit`/`decision_function` API (Zhao et al., n.d.) | Batch scoring of per-match feature vectors |
| **scikit-learn IsolationForest** | "isolates observations by randomly selecting a feature and then randomly selecting a split value" (scikit-learn developers, n.d.) | First multivariate model; no labels needed |
| **River HalfSpaceTrees** | "an online variant of isolation forests" (River developers, n.d.) | Incremental scoring as each new match lands |
| **STUMPY** | Matrix profile for "Anomaly/novelty (discord) discovery" (STUMPY developers, n.d.) | Odd *shapes* within a match (a stalled intake, a binding elevator) |
| **tsfresh** | "automatically calculates a large number of time series characteristics" (tsfresh developers, n.d.) | Feature discovery offline. Too heavy to run per match |

**Domain evidence.** In a BLDC fan bearing-fault study, "there were about 9% changes in current and rotary speed of fan when the BLDC motor behaved unusual" (Jin et al., 2014). Current and speed residuals are measurable early signals.

**[Background — not from search]**
- Classic motor-current signature analysis needs kHz-rate current spectra. FRC CAN status frames run at roughly 50–250 Hz, so expect **trend and residual** methods to beat spectral fault detection.
- Start with rules and robust z-scores against each motor's own history, before any ML. With roughly 50–150 matches per season, there isn't much data.

## 5. Match metadata enrichment

- **In-log first.** WPILib publishes `/FMSInfo` topics (EventName, MatchNumber, ReplayNumber, MatchType, IsRedAlliance, StationNumber, FMSControlData) (WPILib Developers, 2026a). NetworkTables describes these as "Information about the currently running match that comes from the Driver Station and the Field Management System" (WPILib, 2020).
- **The Blue Alliance API v3.**
  - Requires the `X-TBA-Auth-Key` header.
  - Match keys look like `yyyy[EVENT_CODE]_[COMP_LEVEL]m[MATCH_NUMBER]` with levels `pm, qm, ef, qf, sf, f`. Practice (`pm`) was added in 3.27.0.
  - Supports ETag caching (The Blue Alliance, n.d.).

  Use it for canonical match keys, alliance partners, scores, and match start times.
- **FRC Events API.** Official, needs a token. "may not be used for commercial purposes" (FIRST, n.d.).

## 6. Retrieval

- Logs go to "a USB flash drive in a folder called logs if one is attached, or to /home/lvuser/logs otherwise" (WPILib, n.d.-a). Hoot files go to a path such as `/media/sda1/ctre-logs/` (CTR Electronics, n.d.-a).
- DataLogTool uses SFTP as `lvuser` with a blank password, and "will not be able to connect wirelessly if the radio firewall is enabled" (WPILib, n.d.-b).
- On startup, `FRC_TBD_*` logs are deleted. `FRC_*` logs are pruned oldest-first under 50 MB free (WPILib Developers, 2026b).
- **[Background]** Use a Python SFTP client (paramiko or asyncssh) instead of shelling out to `sshpass scp`. Copy logs, verify size and hash, and never delete on the robot by default; rely on robot-side rotation. Replace `drive-backup.py` with `rclone sync` of the Parquet and raw folders.
