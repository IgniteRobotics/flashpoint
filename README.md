# Flashpoint Robot Telemtry Platform

> ## flashpoint
> *also flash point.*
> ### noun
> 1: Also Physical Chemistry. the lowest temperature at which a liquid in a specified apparatus will give off sufficient vapor to ignite momentarily on application of a flame.
> 
> 2: a critical point or stage at which something or someone suddenly causes or creates some significant action:


The Flashpoint Robot Telemtry Platform is designed to analyze FRC logging and telemtry over they life of a robot in order to detect anomalies and hopefully predict failures before they happen.
## Getting started (rewrite)

> The repo is mid-rewrite. The new package lives in `src/flashpoint/`. The top-level scripts (`ingest_*.py`, `viz.py`, `docker-services/`) are legacy, and are removed stage by stage (`openspec/changes/retire-legacy-code`). Plan and research: [`docs/rewrite/`](docs/rewrite/README.md). Decisions: [`docs/adr/`](docs/adr/README.md).

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

### Tests

```bash
poetry run pytest                                   # unit tests (fast, no data needed)
poetry run python tools/fetch-corpus.py             # golden log corpus (~180 MB, release corpus-v1)
poetry run pytest -m "corpus and not perf"          # tests against real robot logs
poetry run pytest -m perf -s                        # time and memory budgets
```
