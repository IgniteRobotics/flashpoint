# Changelog

## Unreleased

### Added

- Non-Pro hoots are expected input. History reads **NEEDS PRO** for a motor whose hoots aren't Phoenix Pro-licensed, and **N/A** for a gyro or CANcoder, where it used to read NO DATA. Replay names each bus without temperature. Detection skips missing temperature and counts what it skipped. `flashpoint doctor` and the Setup view report Phoenix Pro coverage, as information only. (#75, #10)
- AdvantageKit logs: `/DriverStation/` FMS fields, `akit_…` filenames and the epoch-time entry are read, so an AdvantageKit wpilog gets its match identity and pairs with its hoots. When a wpilog has no identity, the hoots' filenames give it. (#76)
- The Anomalies view shows a BETA tag and a note linking its guide page. (#77)

### Changed

- CAN-gap DRIFT, STEP and SIBLING floors scale with a device's frame rate above 125 frames/s, so normal timing jitter on a 250 Hz CANivore no longer raises findings. roboRIO-rate devices are unchanged. A brownout judged on the median fallback (no `battery_proxy`) is at most MED. The first `derive` after upgrading rebuilds every session, and Replay data is rebuilt. (#77)
- The missing-`ml`-extra warning gives the pipx, uv, pip and Poetry commands. (#77)
- When the derive that `flashpoint ingest` or `flashpoint rebuild` runs afterwards fails, the command now says Replay data was not built, names `flashpoint derive --report` as the retry, and exits 1, even if every file imported. It used to exit 0. Scripts that call `ingest` may see the new exit code. (#107)
- Flashpoint now requires duckdb 2 and polars 2 (`duckdb >=2.0,<3`, `polars >=2.0,<3`). pipx, uv and pip pick them up on upgrade. An environment that must keep duckdb 1 or polars 1 can't run this version. Lakes are unchanged: Parquet written by either version reads in the other. (#56, #57)

### Fixed

- A relative lake path (`--lake lake`, `FLASHPOINT_LAKE`, or `lake` in `acquire.toml`) no longer crashes detection or the Anomalies view. Every command turns the lake into an absolute folder, taken from the folder it runs in, and `~` is expanded from every source. (#107)

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
