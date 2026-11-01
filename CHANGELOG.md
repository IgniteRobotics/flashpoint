# Changelog

## Unreleased

### Changed

- Ingest quarantines only files that are bad. Machine errors (owlet can't be downloaded, crashes, or times out) leave the file `failed`, and the next `ingest`, `rebuild`, or acquire cycle retries it. A file that crashes owlet or times out 3 times is quarantined. Lakes from earlier versions are fixed on first open: files quarantined for a machine error become `failed`. (#14)
- Re-runs report a file quarantined earlier as `quarantined earlier (<reason>)` instead of `skipped`. `flashpoint rebuild --quarantined` retries quarantined files from the lake's raw copies. (#14)
- `flashpoint ingest` and `flashpoint rebuild` rebuild Replay data after derive. `--no-report` turns it off. (#15)

### Downgrading

An older Flashpoint can't read the new `failed` stage, and its acquire watch errors on those files. Before going back to an older version, run this on the lake's `meta/flashpoint.sqlite`:

```sql
UPDATE files SET stage = 'received' WHERE stage = 'failed';
```
