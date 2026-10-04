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
