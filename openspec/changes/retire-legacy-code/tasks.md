## 1. Stage 1: dead code (with P0)

- [x] 1.1 Confirm the `legacy-2025` tag exists on origin, pointing at the pre-rewrite main
- [x] 1.2 Confirm nothing references the stage-1 files (`git grep` for each filename). Record the result in the PR
- [x] 1.3 Delete `setup-db.py`, `ingest_dir.sh`, `ingest_dir_scripts/`, `summary_metrics.py`, `manage_imports.py`, `old_sync_scripts/`, `requirements.txt`, `docs/*.excalidraw`
- [x] 1.4 Update `docs/rewrite/README.md` "How code is cited" to use `legacy-2025:path:line`, and rewrite the `main:` locators across `docs/rewrite/*.md`
- [x] 1.5 CI passes. Merge through a PR

## 2. Stage 2a: ingest pipeline (gate: p1-core-ingest-lake archived)

- [x] 2.1 Verify the gate: `openspec/changes/archive/*p1-core-ingest-lake*` exists
- [x] 2.2 Delete `csv_converter.py`, `datalog.py`, `ingest_library.py`, `ingest_system_log.py`, `ingest_device_log.py`, `ingest_match_logs.py`, `ingest-requirements.txt`, `executables/owlet.exe`
- [x] 2.3 CI passes. Merge through a PR

## 3. Stage 2b-i: device maps (gate: p2-semantics-and-physics archived)

- [x] 3.1 Verify the gate: P2 archived; `config/robots/2025-comp.toml` covers every row of the two device CSVs (14 slots, `Pheonix6` corrected). The NetworkTables maps and `log_configs/` are **not** covered (P2 non-goal), so they move to stage 2b-ii (team decision, 2026-10-04)
- [x] 3.2 Delete `datamaps/drivetrain_devices_map.csv` and `datamaps/rio_devices_map.csv`; `tools/migrate-legacy-config.py` now reads them from the `legacy-2025` tag
- [x] 3.3 CI passes. Merge through a PR

## 3b. Stage 2b-ii: NetworkTables maps (gate: a future NT-mapping change is archived)

- [ ] 3b.1 Verify the gate: a change that maps NetworkTables-only signals (subsystem telemetry, vision) into robot configuration is archived, and it covers every row of `datamaps/{2024,2025}/{metrics,vision}_map.csv` and the prefixes in `log_configs/*.json`
- [ ] 3b.2 Delete `datamaps/` and `log_configs/`
- [ ] 3b.3 CI passes. Merge through a PR

## 4. Stage 2c: deployment and backup (gate: p3-log-acquisition archived)

- [ ] 4.1 Verify the gate, and confirm the new acquirer has run unattended at least once against a robot
- [ ] 4.2 Delete `docker-services/`, `docker-compose.yml`, `.dockerignore`, `drive-backup.py`
- [ ] 4.3 CI passes. Merge through a PR

## 5. Stage 2d: viewer (gate: p4-views-and-reports archived)

- [x] 5.1 Verify the gate
- [x] 5.2 Delete `viz.py`, `viz-requirements.txt`, `gw_config.json`
- [ ] 5.3 Rewrite `README.md` for the new package (install, `flashpoint` CLI, links to docs and openspec)
- [ ] 5.4 Confirm that no tracked files remain at the repo root except `README.md`, `.gitignore`, `pyproject.toml`, `poetry.lock`, and the tool dirs (`src/`, `tests/`, `config/`, `docs/`, `openspec/`, `media/`, `.github/`, `.claude/`)
- [ ] 5.5 CI passes. Merge through a PR, then archive this change
