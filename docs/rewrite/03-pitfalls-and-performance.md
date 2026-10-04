# 03 — Pitfalls & Performance Issues

Severity: 🔴 data loss/corruption or won't run · 🟠 wrong results · 🟡 performance/scale · ⚪ hygiene. Code citations are IgniteRobotics (2026), written `branch:file:line`.

## 1. Won't run (main)

| # | Sev | Issue | Locator |
|---|---|---|---|
| 1 | 🔴 | `from ingest_file import setup_db`, but the module was renamed. The ingest Docker build runs this at build time, so the build fails | `legacy-2025:setup-db.py:1`, `legacy-2025:docker-services/ingest/Dockerfile:21` |
| 2 | 🔴 | `ingest_dir.sh` calls the missing `ingest_file.py` | `legacy-2025:ingest_dir.sh:3` |
| 3 | 🔴 | The puller runs a shell script with `python3` | `legacy-2025:docker-services/ingest/src/main.py:121` |
| 4 | 🔴 | The dataviz Dockerfile installs a `requirements.txt` that doesn't exist, and `src/main.py` is a dangling symlink to `/home/ubuntu/...` | `legacy-2025:docker-services/dataviz/Dockerfile:10` |
| 5 | 🔴 | The only owlet is a Windows PE32+ `.exe`, so hoot conversion fails on Linux and macOS. `subprocess.run` has no `check=True`, so the failure is silent until the CSV is missing | `legacy-2025:ingest_library.py:50` |
| 6 | 🔴 | The bash `for` loop is missing its `done` | `legacy-2025:ingest_dir_scripts/ingest_device_dir.sh:1-7` |
| 7 | 🔴 | `ExecStart=export … && docker compose up $DIR`. systemd doesn't run a shell, and `compose up` doesn't take a directory | `legacy-2025:docker-services/ingest/flashpoint-ingest.service:6` |
| 8 | 🔴 | `summary_metrics.py` reads a `metrics` table in `db/metrics.db`, but neither exists any more | `legacy-2025:summary_metrics.py:56,67` |
| 9 | 🟠 | Three inbox directories never connect: the puller writes `/app/telemetry`, `manage_imports` reads `./imported_files`, and the dir scripts read `data/*` | various |

## 2. Wrong results (silent)

| # | Sev | Issue | Locator |
|---|---|---|---|
| 10 | 🟠 | `str.startswith(photon_prefix, camerapub_prefix)` passes the second prefix as pandas' `na` parameter, not as an alternative prefix. Per the docs, `na` is the "Object shown if element tested is not a string" (pandas, n.d.). CameraPublisher entries are never selected | `legacy-2025:ingest_library.py:137` |
| 11 | 🟠 | Datamap typo `Pheonix6/TalonFX-15/...`, so climber position and temperature never map | `legacy-2025:datamaps/rio_devices_map.csv:15-16` |
| 12 | 🟠 | `try/except KeyError` around a boolean `.loc` mask. That never raises, so the `has_*_data` flags are always True and the "no data" branches are dead | `legacy-2025:ingest_library.py:409-433, 558-566` |
| 13 | 🟠 | Match framing takes the first enable and the last disable, so it includes the auto→teleop disabled gap. If the log has no `True` row, `min()` returns NaN and `astype` crashes. The code catches `KeyError`, which is the wrong exception | `legacy-2025:ingest_library.py:62-78` |
| 14 | 🟠 | Joining V, I, T, vel, and pos with outer merges on **exact float `match_time`**. Different CAN signals are almost never sampled at the same instant, so rows become a sparse union full of NaNs. Joins on NaN `assembly`/`subassembly` keys add more errors | `legacy-2025:ingest_library.py:456-535` |
| 15 | 🟠 | Year, config, and vision map are hard-coded to `2025` in match ingest. `development` passes `2026`, but `datamaps/2026` doesn't exist | `legacy-2025:ingest_match_logs.py:35,68,86-123`; `origin/development:ingest_dir.py:54` |
| 16 | 🟠 | `row['value'].split(': ')` crashes if a value contains a second `": "` | `legacy-2025:ingest_library.py:155` |
| 17 | 🟠 | `log_metadata.filename` is a PRIMARY KEY written with `append`, so re-importing raises IntegrityError partway through. With no transaction around the whole file, that leaves a partial import | `legacy-2025:ingest_library.py:232-245, 384-386` |
| 18 | 🟠 | `ingest_match_logs` never checks the hash ledger, so re-running it duplicates every raw row | `legacy-2025:ingest_match_logs.py:22-24` |
| 19 | 🟠 | The upsert engine points at the relative `sqlite:///robot.db`, which is lost when the container is recreated. It is keyed on `filename` for tables that don't have that column | `origin/development:ingest_library.py:403-404` |
| 20 | 🟠 | `manage_imports` takes the wpilog match ID from `split("_")[-1]` but the hoot match ID from `split("_")[1]`. A hoot with no matching wpilog raises KeyError | `legacy-2025:manage_imports.py:39-46` |
| 21 | 🟠 | Two match regexes disagree: the importer accepts `P` and a digit suffix, `ingest_dir` doesn't. A substring test makes `Q1` match `Q12` | `origin/development:ingest_dir.py:14,36` vs `docker-services/importer/main.py:8` |
| 22 | 🟠 | `astype(int)` on match_id or replay_num crashes for practice and non-FMS logs | `origin/development:ingest_library.py:199` |
| 23 | 🟠 | Before a signal's first sample, `fillna(0.0)` pulls averages and P95 down, temperature most of all. Means are weighted by rows, not by time | `origin/feature/power-tracking:utils/analyzer.py:48-60`, `reporter.py:96-110` |
| 24 | 🟠 | The cache key is the file hash only, and ignores the signal pattern. Old caches silently lack newer columns (Velocity, DeviceTemp) | `origin/feature/power-tracking:utils/hoot_loader.py:152` |
| 25 | 🟠 | Decimating with `arr[::step]` aliases and drops current spikes, which are the events you care about most | `origin/feature/power-tracking:utils/site_builder.py:36-37,64-65` |
| 26 | 🟠 | `abs()` is applied to Velocity as well, which loses direction | `origin/feature/power-tracking:utils/analyzer.py:15-17` |
| 27 | 🟠 | The TOTAL row includes unnamed motors while only named rows are displayed, and supply totals need **every** motor to have supply data | `origin/feature/power-tracking:utils/analyzer.py:85-96` |

## 3. Performance & scale

| # | Sev | Issue | Locator |
|---|---|---|---|
| 30 | 🟡 | **Pure-Python per-record parser**: `DataLogIterator.__next__` builds a Python object for every record. WPILib also ships compiled `DataLogReader` bindings for Python (RobotPy, n.d.; WPILib, n.d.-b) | `legacy-2025:datalog.py:181-217` |
| 31 | 🟡 | **CSV string round-trip**: binary log → text CSV → full gzip rewrite → `pd.read_csv` with every value as an object string → `pd.to_numeric`. Arrays are stored as the text `array('d', [...])` | `legacy-2025:csv_converter.py:88-131`, `ingest_library.py:58, 188-198` |
| 32 | 🟡 | Every raw sample is stored with about 8 repeated TEXT key columns, no indexes, and no partitioning, so the DB grows at about (#signals × rate × match length × row width) | `legacy-2025:ingest_library.py:249-268` |
| 33 | 🟡 | `df.to_sql(append)` with default settings is row-oriented inserts into SQLite | `legacy-2025:ingest_library.py:385` |
| 34 | 🟡 | `iterrows()` over metadata and FMS frames | `legacy-2025:ingest_library.py:153,160` |
| 35 | 🟡 | `viz.py` runs `SELECT *` and filters in pandas on every refresh. `os.walk(".")` over the repo on every rerun | `origin/development:viz.py:31,76` |
| 36 | 🟡 | **Sparse pivot**: in a synthetic test (18 motors × 6 signals × 100 Hz × 150 s) the result was 270k rows × 109 columns, about 235 MB in RAM, a 60 MB cache CSV, and about 7 s. Real logs cover the whole power-on period, not just the match | `origin/feature/power-tracking:utils/hoot_loader.py:86-113` |
| 37 | 🟡 | **About 27 MB of JSON per match** against a 3–5 MB spec target: full float precision, 10 arrays per motor, and energy arrays the site never charts. `update_manifest` re-parses every JSON on every run | `origin/feature/power-tracking:utils/site_builder.py:36-106` |
| 38 | 🟡 | Rio and CANivore frames are merged on exact float timestamps, which roughly doubles the row count | `origin/feature/power-tracking:utils/hoot_loader.py:57-64` |
| 39 | 🟡 | The SPA uses O(n·w) JS smoothing and never calls `Plotly.purge`, so memory leaks when switching tabs | `origin/feature/power-tracking:utils/templates/index.html:547-561` |
| 40 | 🟡 | The importer's `time.sleep(10000)` is commented "10s" but is 2.8 h. Every pass re-ingests the whole tree | `origin/development:docker-services/importer/main.py:35` |
| 41 | 🟡 | Docker builds took about 10 minutes (`COPY . .` before `pip install`). Partly fixed in `ae5c139`; dataviz is still unfixed | commit `ae5c139` |

**Why long format and as-of joins fix #14, #36, and #38.** Phoenix 6 captures "all status signals … automatically with their timestamps from CAN" (CTR Electronics, n.d.-a), so each signal has its own clock. WPILib states "There is no timestamp ordering requirement for records" (WPILib Developers, 2022). Exact-key joins are therefore wrong by construction. DuckDB's ASOF join exists because "Time series data is not always perfectly aligned" (DuckDB Foundation, n.d.-c), and Polars `join_asof` matches "on nearest key rather than equal keys" with a configurable tolerance (Polars, n.d.).

## 4. Operations & security

| # | Sev | Issue | Locator |
|---|---|---|---|
| 50 | 🔴 | The puller **deletes logs on the robot** after an scp whose success is judged by grepping stderr. A partial copy can lose the original. The remove command also lacks `sshpass` | `legacy-2025:docker-services/ingest/src/main.py:90-116` |
| 51 | 🔴 | **Log rotation race.** WPILib deletes `FRC_` logs oldest-first when free space drops below 50 MB (WPILib Developers, 2026b; WPILib, n.d.-a), and Phoenix deletes old hoots at 50 MB free (CTR Electronics, n.d.-a). If the puller is down for an event weekend, data is gone | design |
| 52 | 🟠 | The puller's date filter does integer math on YYYYMMDD (`20251001 - 1 = 20251000`). The `ls \| find \| grep *DATE` pipeline is sent to the remote shell and doesn't do what was intended | `legacy-2025:docker-services/ingest/src/main.py:42-53` |
| 53 | 🟠 | `StrictHostKeyChecking=no` and `UserKnownHostsFile=/dev/null`. Acceptable on a robot LAN; don't copy it elsewhere | `legacy-2025:docker-services/ingest/src/main.py:32-33` |
| 54 | 🟠 | `os.system("mv …")` with unquoted filenames (injection, and breaks on spaces). AdvantageScope is launched with `os.system` using paths built from strings | `origin/development:docker-services/importer/main.py:25`, `viz.py:88-94` |
| 55 | 🟠 | `drive-backup.py`: the `.replace()` result is discarded, `moveErr` reads stdout, and timestamps aren't zero-padded (`2025105` is ambiguous) | `legacy-2025:drive-backup.py:27,54,63-64` |
| 56 | 🟠 | The SPA puts match IDs and names into `innerHTML`/`onclick` unescaped | `origin/feature/power-tracking:utils/templates/index.html:224,289,347` |
| 57 | 🟠 | `sys.exit` inside library code: one bad hoot aborts a `--hoot-dir` batch **after** `--clean` has already wiped the site data | `origin/feature/power-tracking:utils/hoot_loader.py:120,138`, `cli.py:100-104` |
| 58 | 🟠 | PyGWalker `spec_io_mode="rw"`: every user writes the same `gw_config.json`, and that file is committed | `legacy-2025:viz.py:23` |
| 59 | 🟠 | The SPA calls `fetch('manifest.json')`, so it needs HTTP even though the spec says "no server required" | `origin/feature/power-tracking:utils/templates/index.html:209,244` |

## 5. Process & repo hygiene

| # | Sev | Issue |
|---|---|---|
| 60 | 🔴 | No CI and no tests on main, ever. The rename-vs-PR conflict (#1, #2) would have been caught by a single smoke test |
| 61 | 🔴 | Merges were reverted as an undo button, and main was pushed directly ([02 §5](02-branch-survey.md#5-mergerevert-churn-on-main-oct-2125-2025)) |
| 62 | 🟠 | About 150 commits sit on long-lived unmerged branches, and two teams went in different directions |
| 63 | 🟠 | `requirements.txt` is a 99-line `pip freeze` of a dev machine (black, ipython, casadi, duckdb, pyntcore), and all of it is unused by ingest |
| 64 | 🟠 | Python versions are split: 3.12.3 (dataviz), 3.14.1 (importer/puller), 3.12.8 (power-tracking). Specs say 3.11. The f-string at `legacy-2025:docker-services/ingest/src/main.py:94` needs 3.12+ |
| 65 | ⚪ | About 20 MB of owlet binaries committed and growing every season. `.claude/`, `.superpowers/*.pid`, and `.DS_Store` were committed |
| 66 | ⚪ | Bumping the season means hand-editing `config20XX.json`, `datamaps/20XX/`, the owlet name, and hard-coded years in about 6 places |
| 67 | ⚪ | The excalidraw diagrams name `ingest_file.py` and `metrics.db`; the docs have drifted from the code |

## 6. Domain pitfalls the rewrite must design for

- **Hoot licensing.**
  - "Any log that contains a pro-licensed device will export all signals. Otherwise, [only a listed subset] … can be exported for free" (CTR Electronics, n.d.-a). Continuous `DeviceTemp` is not on the free Talon FX list.
  - **P0 finding (2026-10-04):** `owlet --check-pro` (as used by Mechanical Advantage, 2026) reports the team's 2025 and 2026 hoots as Pro-licensed, so all signals export. The ingest records `pro_licensed` per log in case that ever changes (ADR-0005).
- **owlet versioning.**
  - Tuner X documents an "API Version Mismatch" error that "may happen if your hoot file was generated using an old version of Phoenix" (CTR Electronics, n.d.-c).
  - Pick the owlet version from the log, not from the host OS or the folder name.
  - **P0 finding (2026-10-04):** `owlet --compliancy` prints the hoot's format version: 19 for 2026 logs and 13 for 2025 logs. owlet 26.1.0 reads that version for older hoots but refuses to convert them. Select by compliancy ([ADR-0004](../adr/0004-owlet-registry-by-compliancy.md)).
  - **P0 finding (2026-10-04):** owlet converts a truncated hoot **without error** (corpus `2026-gacmp-e10-truncated`), so ingest must detect corrupt tails itself, for example from signal timestamp coverage.
- **Timestamps.** wpilog timestamps are "64-bit integer microseconds. The zero time is not specified" (WPILib Developers, 2022). Anchor them to wall-clock time using the `systemTime` entry.
- **Match identity.**
  - Filenames are `FRC_yyyyMMdd_HHmmss_{event}_{P|Q|E}{n}.wpilog` (WPILib Developers, 2026b). `E{n}` doesn't map directly to TBA's `sf`/`f` + set-number keys (The Blue Alliance, n.d.).
  - ReplayNumber isn't in the filename at all.
  - Use the in-log FMSInfo as the source of truth.
- **Device identity.** Phoenix 6 `device_hash` is "unique for this device's hardware type and ID … not unique across networks" (CTR Electronics, n.d.-g), and hoot signals are keyed by CAN ID. A swapped motor that keeps its CAN ID silently merges two units' histories, which poisons per-motor baselines. Fix: log CTRE serial numbers at boot ([05 §4a](05-target-architecture.md#4a-device-identity-slot-vs-unit)).
- **Corrupt tails.** A hoot write cut off by a power loss produces "bad data" read errors (CTR Electronics, n.d.-c). Ingest must quarantine such files rather than crash.
