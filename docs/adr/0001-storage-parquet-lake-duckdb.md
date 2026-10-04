# 0001. Storage: immutable raw files + hive-partitioned Parquet lake, queried with DuckDB

- Status: Proposed
- Date: 2026-10-04
- Refs: 05 §4, D1; pitfalls #17, #31–#33

## Context
SQLite stores every sample as row-oriented TEXT, with no indexes and with key columns repeated on every row (#32). Re-importing breaks partway through (#17). Most queries are analytical scans over a few signals across many matches.

## Decision
- Keep the original logs immutable under `lake/raw/<sha256>`.
- Derive bronze, silver, and gold Parquet datasets, hive-partitioned by `season/event` (and `match` for silver).
- Query with embedded DuckDB. No database server.

## Consequences
- Columnar scans with partition and zonemap pruning. Files can be copied to a laptop or backed up with rclone.
- Every derived layer can be rebuilt from raw at any time.
- No concurrent writers: ingest is single-process per lake, which is fine for one team.
- Students need to learn SQL over files rather than an ORM.

## Alternatives considered
- **SQLite** (status quo): simple, but a row store with the wrong access pattern.
- **TimescaleDB:** needs a Postgres server. Revisit only for multi-team hosting.

## Update (P1 implementation, 2026-10-04)
- **Ledger and per-log metadata live in SQLite (WAL)**, `lake/meta/flashpoint.sqlite`. They need transactional per-file state changes, which Parquet can't provide. Each run exports every table to `lake/meta/*.parquet`, so DuckDB reads everything as plain Parquet (no extensions; works offline).
- **Bronze layout:** `bronze/samples/season=<YYYY|unknown>/log_id=<sha256>/part-0.parquet` (zstd-3).
  - Rows are sorted by `(signal, ts_us)` **within each row group**. One row group per 32 MB decode window, so ingest memory is bounded.
  - Queries filter by `log_id` partition first, so the file isn't globally sorted.
- **Writes are staged and atomically renamed** into place; readers never see a partial log.
- **Measured:** about 44 MB of bronze per CANivore bus per match (`health` profile); one-signal query in under 1 s.
