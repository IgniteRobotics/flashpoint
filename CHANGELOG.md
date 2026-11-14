# Changelog

## Unreleased

## 0.2.0 - 2026-11-13

### Added

- Live CAN health view (`#view=live`, served mode): bus utilisation and errors, a device grid by slot, and live events for dropped, rebooted, and faulted devices. `flashpoint serve --robot-host` picks the robot; it defaults to the `acquire` hosts. Needs the robot to publish CAN health over NetworkTables; see `docs/guide/live-can-health.md`. Adds the `pyntcore` dependency. (#35)
- Team name and colours: `theme.toml` with a preset or custom colours, checked for contrast, set with `init --team-name --palette` or the Setup view's Appearance section. The default `amber` preset is the previous look. (#42)
- A team setup guide (`docs/guide/team-setup.md`) for running Flashpoint for another team. (#42)

### Changed

- Every runtime dependency now has an upper version bound. duckdb and polars stay below 2: duckdb 2 removed calls Flashpoint makes, and 0.1.0 let a fresh install pick it up. (#59)
- Ingest quarantines only files that are bad. Machine errors (owlet can't be downloaded, crashes, or times out) leave the file `failed`, and the next `ingest`, `rebuild`, or acquire cycle retries it. A file that crashes owlet or times out 3 times is quarantined. Lakes from earlier versions are fixed on first open: files quarantined for a machine error become `failed`. (#14)
- Re-runs report a file quarantined earlier as `quarantined earlier (<reason>)` instead of `skipped`. `flashpoint rebuild --quarantined` retries quarantined files from the lake's raw copies. (#14)
- `flashpoint ingest` and `flashpoint rebuild` rebuild Replay data after derive. `--no-report` turns it off. (#15)

### Downgrading

An older Flashpoint can't read the new `failed` stage, and its acquire watch errors on those files. Before going back to an older version, run this on the lake's `meta/flashpoint.sqlite`:

```sql
UPDATE files SET stage = 'received' WHERE stage = 'failed';
```
