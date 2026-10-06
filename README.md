# Flashpoint Robot Telemetry Platform

> ## flashpoint
> *also flash point.*
> ### noun
> 1: Also Physical Chemistry. the lowest temperature at which a liquid in a specified apparatus will give off sufficient vapor to ignite momentarily on application of a flame.
> 
> 2: a critical point or stage at which something or someone suddenly causes or creates some significant action:


The Flashpoint Robot Telemetry Platform analyzes FRC logs and telemetry over the life of a robot, to detect anomalies and eventually predict failures before they happen. It reads WPILib DataLogs (`.wpilog`) and CTRE Phoenix 6 signal logs (`.hoot`) into a local Parquet lake and works offline on a pit laptop.

- **Code:** the `flashpoint` package in `src/flashpoint/`, with the `flashpoint` CLI.
- **Plan and research:** [`docs/rewrite/`](docs/rewrite/README.md). **Decisions:** [`docs/adr/`](docs/adr/README.md).
- **Specs and changes:** [`openspec/`](openspec/). Current behavior is in `openspec/specs/`, and work in progress is in `openspec/changes/`.
- **Legacy:** the pre-rewrite tree is preserved at the `legacy-2025` tag. The remaining legacy pieces (`datamaps/`, `log_configs/`, `docker-services/`, `drive-backup.py`) are removed stage by stage (`openspec/changes/retire-legacy-code`).

## Getting started

Requires Python 3.11–3.13 and [Poetry](https://python-poetry.org/).

```bash
poetry install
poetry run flashpoint doctor                      # environment, lake, owlet cache status
poetry run flashpoint ingest path/to/logs/        # .wpilog and .hoot files or folders
poetry run flashpoint ingest --profile all FILE   # export every hoot signal (about 3x larger)
```

- **Lake location:** `~/flashpoint-lake`, or set `$FLASHPOINT_LAKE` / `--lake`.
- **What ingest does:** every file is copied to `raw/` (content-addressed). Samples go to `bronze/` as Parquet, and log metadata goes to `meta/`. Re-running ingest skips files it has already seen.
- **owlet:** CTRE's hoot converter is downloaded on first use for the hoot's format version, checksum-verified, and cached in `~/.cache/flashpoint/owlet/`. Run once while online before taking a laptop to an event.

Query with DuckDB:

```python
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint import config

con = connect(LakePaths(config.lake_root(None)))
con.sql("select filename, fms_event, fms_match_number from meta_logs").show()
con.sql("""
    select ts_us, v_f64 from samples
    where log_id = (select log_id from meta_logs where filename like 'GACMP_Q7_rio%')
      and signal = 'Phoenix6/TalonFX-1/SupplyCurrent'
""").show()
```

### Robots, matches, and motor features (P2)

Each robot is described in `config/robots/<robot>.toml` (slots: bus, model, CAN id, subsystem, role, optional gear ratio and swap dates). `flashpoint doctor` lists the configured robots. Ingest then runs `derive` automatically:
- groups each wpilog with its hoots;
- aligns their clocks;
- maps devices to slots and physical units;
- frames the match (auto, gap, teleop);
- writes `silver/` (mapped samples) and `gold/match_features/` (one row per match × phase × motor).

```bash
poetry run flashpoint derive          # only sessions whose inputs changed
poetry run flashpoint derive --all    # rebuild every session (e.g. after editing a robot config)
```

```python
import duckdb
gold = "~/flashpoint-lake/gold/match_features/*/*/*.parquet"
duckdb.sql(f"""
    SELECT match_key, slot_id, round(temp_max_c) AS max_c, round(supply_energy_wh, 2) AS wh
    FROM read_parquet('{gold}', hive_partitioning = true)
    WHERE phase = 'match' AND subsystem = 'drivetrain'
    ORDER BY match_key, slot_id
""").show()
```

### Acquiring logs (P3)

`flashpoint acquire` pulls new logs from the roboRIO (SFTP) and from removable USB volumes into the lake's `inbox/`, verifies them by hash, then runs ingest and derive. It never writes to the robot or a stick.

```bash
poetry run flashpoint acquire --dry-run   # list what would be copied; changes nothing
poetry run flashpoint acquire             # one cycle
poetry run flashpoint acquire --watch     # keep going (Ctrl-C to stop)
poetry run flashpoint backup              # rclone backup, if backup.remote is set
```

Settings live in `acquire.toml` (hosts, roots, poll interval, rclone remote). Running it as a service (systemd user unit on Linux, scheduled task on Windows) and the full config are in [`deploy/README.md`](deploy/README.md).

### Replay and History (P4)

`flashpoint report` turns each match in the lake into Replay data (about 1 MB per match, rebuilt only when its inputs change). `flashpoint serve` runs the app: **Replay** for one match (tracks, markers, the hottest motor, and the raw logs: one click opens them in AdvantageScope on the machine running `serve`, or download them) and **History** for every unit and slot across matches, events, seasons, and robots.

```bash
poetry run flashpoint report                            # build or refresh every match
poetry run flashpoint report --event 2026gacmp          # just one event (or --match 2026gacmp_qm7)
poetry run flashpoint serve                             # builds, then serves http://127.0.0.1:8000/
poetry run flashpoint serve --host 0.0.0.0 --port 8000  # share on the pit network (read-only, no password)
poetry run flashpoint report --static /Volumes/USB/flashpoint   # Replay folder for a USB stick
poetry run flashpoint report --static OUT --no-raw      # without the raw logs (smaller)
```

Open the static export by double-clicking `index.html`; it needs no server or network. History needs `serve`, because it queries the lake. Thresholds for markers and health (65/75 °C, brownout and sag voltages) live in `config/report.toml`. So does an optional AdvantageScope path. Without one, `serve` uses the newest WPILib install, and `flashpoint doctor` shows which one it found. Students can explore further with the marimo notebook in [`notebooks/`](notebooks/README.md).

### Tests

```bash
poetry run pytest                                   # unit tests (fast, no data needed)
poetry run python tools/fetch-corpus.py             # golden log corpus (~180 MB, release corpus-v1)
poetry run pytest -m "corpus and not perf"          # tests against real robot logs
poetry run pytest -m perf -s                        # time and memory budgets
poetry run playwright install chromium              # once, for the browser tests
poetry run pytest -m browser                        # the web app in Chromium (served and file://)
```
