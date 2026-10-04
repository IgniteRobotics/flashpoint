# 01 — Current Capabilities

This is an inventory of everything Flashpoint can do, or tries to do, across all branches. "Works?" reflects code reading plus test runs on exported branch snapshots (IgniteRobotics, 2026).

## 1. Capability matrix

| Area | Capability | Where | Works? |
|---|---|---|---|
| **Parsing** | Pure-Python WPILib DataLog reader. A vendored copy of WPILib's `printlog/datalog.py` example (WPILib, n.d.-b) | `main:datalog.py` | ✅ but slow |
| | wpilog → long CSV (`entry,type,value,timestamp`, quote char `\|`) → gzip | `main:csv_converter.py` | ✅ |
| | hoot → wpilog through CTRE `owlet` (CTR Electronics, n.d.-b) | `main:ingest_library.py:50` | ⚠️ Windows `.exe` only |
| | Per-OS and per-season owlet binaries (2024/2025/2026 × mac/linux/win) | `origin/development:ingest_library.py:54-62` | ⚠️ pinned to 2026, Linux build is x86-64 only |
| | hoot → wide DataFrame with a SHA-256 keyed CSV cache, TalonFX signals only | `origin/feature/power-tracking:utils/hoot_loader.py` | ✅ tested (mocked) |
| **Match framing** | Trim to the first `DS:enabled=True` and last `False` | `main:ingest_library.py:62-94` | ⚠️ the span includes the auto→teleop gap |
| | Trim by motor-voltage activity above 0.5 V | `origin/feature/power-tracking:utils/trimmer.py` | ✅ |
| | Parse match ID from the filename (`[A-Z]{4,5}[0-9]?_[EQP]\d\d?`) | `origin/development:docker-services/importer/main.py:8` | ⚠️ two regexes disagree |
| | Parse metadata (git SHA, build date, branch, dirty) and FMSInfo (event, match, replay, type, alliance, station) | `main:ingest_library.py:148-184` | ✅ |
| **Semantic mapping** | CSV datamaps: NT entry → subsystem/assembly/subassembly/component/metric, per season | `main:datamaps/**` | ⚠️ typo drops climber signals |
| | Per-season NT prefix config (`log_configs/configYYYY.json`) | `main:log_configs/` | ✅ |
| | TOML CAN-ID → display name, with per-competition overrides | `origin/feature/power-tracking:utils/motors.toml` | ✅ |
| **Storage** | SQLite: `file_metadata`, `log_metadata`, `device_data_raw`/`_telemetry`/`_stats`, `vision_*`, `preferences` | `main:ingest_library.py:210-380` | ⚠️ no indexes, everything TEXT, not idempotent |
| | Content-hash import ledger (`file_metadata.success`) | `main:ingest_system_log.py:8-40` | ✅ system/device ingest only |
| | Upsert writes (`sql_upsert`) | `origin/development:ingest_library.py:403` | ❌ wrong DB path and key |
| **Metrics** | Per-component min/max/avg/σ of voltage, current, velocity, position, temperature per match | `main:ingest_library.py:396-544` | ⚠️ outer-join blow-up |
| | Vision latency stats and has-target | `main:ingest_library.py:546-595` | ⚠️ CameraPublisher never selected |
| | Motor power (V×I_stator), supply power, energy (Wh), P95 peaks, W/RPS, temperature flags (>55 °C avg / >65 °C max) | `origin/feature/power-tracking:utils/analyzer.py` | ✅ tested |
| | Electrical summaries (abs min/max/avg) | `main:summary_metrics.py` | ❌ targets a deleted table |
| **Visualization** | Streamlit + PyGWalker explorer over `device_stats` | `main:viz.py` | ✅ |
| | Cascading Year/Event/Match filters, refresh, AdvantageScope launcher | `origin/development:viz.py` | ⚠️ needs refresh; launches on the server |
| | PDF report (matplotlib): tables, 1 s heatmaps, total power, energy, multi-match comparison | `origin/feature/power-tracking:utils/reporter.py` | ✅ |
| | Static site: per-match JSON, manifest, Plotly SPA (Stats / Heatmaps / Total Power / W/RPS), 2-match compare, offline Plotly | `origin/feature/power-tracking:utils/site_builder.py`, `templates/index.html` | ✅ needs HTTP serving |
| **Acquisition** | Ping the roboRIO, then scp logs (sshpass) and optionally delete them on the robot | `main:docker-services/ingest/src/main.py` | ❌ grep/date/delete bugs |
| | Importer: sort files into `telemetry/<MATCH>/`, ingest in a loop | `origin/development:docker-services/importer/main.py` | ⚠️ sleeps 2.8 h |
| | Copy logs and the DB to a Google Drive mount | `main:drive-backup.py` | ⚠️ filter no-op |
| **Ops** | docker-compose (ingest+dataviz → puller+importer+dataviz) | `main:docker-compose.yml`, `origin/development` | ❌ on main |
| | Poetry, pytest (111 tests), design specs and plans | `origin/feature/power-tracking` | ✅ |

**What does not exist anywhere:** anomaly detection, failure prediction, cross-match or lifetime trends beyond PyGWalker drag-and-drop, robot identity (which physical robot or chassis), physical device identity (motors are tracked only by CAN ID, so swaps are invisible; see [05 §4a](05-target-architecture.md#4a-device-identity-slot-vs-unit)), alerting, auth, REV `.revlog` or Driver Station log support, pose/state tables (designed in `docs/data_arch.excalidraw` but never built).

## 2. Main-branch data flow (as designed)

```mermaid
flowchart LR
    RIO[(roboRIO<br>/home/lvuser/logs)] -->|sshpass scp| PULL[ingest container<br>main.py watchdog]
    PULL -->|"python3 ingest_dir.sh ❌"| DIR[ingest_dir.sh]
    DIR -->|"ingest_file.py ❌ missing"| X((broken))

    subgraph Manual paths
      W[.wpilog] --> SYS[ingest_system_log.py]
      H[.hoot] --> DEV[ingest_device_log.py]
      W & H --> MATCH[ingest_match_logs.py]
    end

    SYS & DEV & MATCH --> LIB[ingest_library.py]
    LIB -->|owlet.exe| WPI[.wpilog]
    LIB --> CSV[csv_converter.py<br>→ .csv → .gz]
    CSV --> PD[pandas<br>split / trim / map / pivot]
    MAP[datamaps/*.csv<br>log_configs/*.json] --> PD
    PD -->|to_sql append| DB[(SQLite<br>db/robot.db)]
    DEV -->|separate DB| HDB[(db/hoot.db)]
    DB --> VIZ[viz.py<br>Streamlit + PyGWalker]
```

## 3. Main-branch schema

```mermaid
erDiagram
    file_metadata {
        TEXT filename PK
        INTEGER import_timestamp
        TEXT file_hash
        BOOLEAN success
    }
    log_metadata {
        TEXT filename PK
        TEXT event
        TEXT match_id
        TEXT match_type
        TEXT replay_num
        TEXT commit_hash
        TEXT git_branch
    }
    device_data_raw {
        TEXT filename
        TEXT event_year
        TEXT event
        TEXT match_id
        TEXT entry
        TEXT value
        REAL timestamp
        REAL match_time
        TEXT subsystem
        TEXT component
        TEXT metric
        REAL numeric_value
    }
    device_telemetry {
        TEXT event
        TEXT match_id
        REAL match_time
        TEXT subsystem
        TEXT component
        REAL voltage
        REAL current
        REAL velocity
        REAL position
        REAL temperature
    }
    device_stats {
        TEXT event
        TEXT match_id
        TEXT subsystem
        TEXT component
        REAL avg_current
        REAL max_temperature
    }
    vision_data_raw ||--o{ vision_telemetry : "pivot"
    vision_telemetry ||--o{ vision_stats : "groupby"
    file_metadata ||--|| log_metadata : "filename"
    log_metadata ||--o{ device_data_raw : "event+match (no FK)"
    device_data_raw ||--o{ device_telemetry : "outer-merge pivot"
    device_telemetry ||--o{ device_stats : "groupby"
```

There are no foreign keys or indexes, and every raw row repeats about 8 TEXT key columns (`main:ingest_library.py:249-268`).

## 4. Intended (never completed) data architecture

Taken from `main:docs/data_arch.excalidraw`. Each domain follows a three-step **raw → processed → stats** pattern:

```mermaid
flowchart LR
    WL[wpilog files] --> LM[log_metadata] --> MI[match_info]
    WL --> DR[device_data_raw] --> DT[device_telemetry] --> DS[device_stats]
    WL --> SR[state_data_raw] --> ST[state_telemetry]
    WL --> PR[preference_data_raw] --> PS[preference_stats]
    WL --> POR[pose_data_raw] --> POT[pose_telemetry] --> POS[pose_stats]
    WL --> VR[vision_data_raw] --> VT[vision_telemetry] --> VS[vision_stats]
    classDef missing stroke-dasharray: 5 5
    class MI,SR,ST,PS,POR,POT,POS missing
```

Dashed nodes were never built. The pattern is sound and maps onto a medallion (bronze/silver/gold) layout in the target design. See [05](05-target-architecture.md).

## 5. power-tracking data flow

```mermaid
flowchart TD
    CLI["python -m utils<br>FILE... | --hoot-dir"] --> FM[find_matches<br>group by match id]
    FM --> CH{sha256 cache hit?}
    CH -- no --> OW[owlet-2026-os<br>hoot → wpilog]
    OW --> DLR[datalog.DataLogReader]
    DLR --> PIV[pivot TalonFX doubles<br>dict-of-dicts → wide DF]
    PIV --> CSV[(cache/hash.csv)]
    CH -- yes --> CSV
    CSV --> MRG[outer merge on Timestamp<br>rio + canivore]
    MRG --> TR[trim_to_match]
    TR --> AN[normalize abs → build_match<br>power, energy, totals]
    TOML[motors.toml] --> NM[names.resolve]
    AN --> NM
    NM --> PDF[reporter → matplotlib PdfPages]
    NM --> SB[site_builder<br>decimate ~50 Hz → data/id.json]
    SB --> MAN[manifest.json]
    SB --> SPA[index.html + plotly.min.js]
```

## 6. Signals actually used

| Source | Signals | Notes |
|---|---|---|
| wpilog `NT:Robot/m_robotContainer/*` | Per-subsystem current, voltage, temperature, position, beam breaks (`datamaps/2025/metrics_map.csv`) | Team-defined telemetry; names change every season |
| wpilog `NT:/FMSInfo/*` | EventName, MatchNumber, ReplayNumber, MatchType, IsRedAlliance, StationNumber | Published by WPILib DriverStation (WPILib Developers, 2026a) |
| wpilog `MetaData*` | Project name, build date, commit hash, git branch, dirty flag | From a build-time version file |
| wpilog `NT:/photonvision/*` | latencyMillis, hasTarget, plus about 10 unused per camera | |
| wpilog `NT:/Preferences/*` | All tunables | Stored as text |
| wpilog `DS:enabled` | Enable edge | Used for match framing |
| hoot `Phoenix6/TalonFX-N/*` | SupplyVoltage, SupplyCurrent, StatorCurrent, MotorVoltage, Velocity, Position, DeviceTemp | Without Pro, DeviceTemp **is not** exportable (CTR Electronics, n.d.-a). The team's logs are Pro-licensed (P0 check, ADR-0005) |
