## 1. Foundations

- [ ] 1.1 Add runtime dependencies (numpy, numba, polars, pyarrow, duckdb) and dev dependency robotpy-wpiutil. Confirm wheels install on Python 3.11 and 3.12 in CI
- [x] 1.2 `flashpoint.config`: resolve the lake root (`--lake`, then `$FLASHPOINT_LAKE`, then `~/flashpoint-lake`) and the cache root (`~/.cache/flashpoint`). Add `PIPELINE_VERSION`. Tests first
- [x] 1.3 Add a corpus fixture helper that returns a path per corpus group and fails clearly if the corpus hasn't been fetched

## 2. wpilog reader (spec: wpilog-reading)

- [x] 2.1 Tests: synthetic wpilog builder (header, Start/Finish/SetMetadata, every scalar type, string, raw, out-of-order timestamps, truncated tail). Expectations cover records, types, values, and `truncated_bytes`
- [x] 2.2 Compiled framing scan over a memory-mapped buffer, returning entry, timestamp, offset, and size arrays plus `truncated_bytes`
- [x] 2.3 Control-record decoding and the entry catalog (id → name, type, metadata, with entry ids reused after Finish)
- [x] 2.4 Typed gather kernels (double, float, int64, boolean). Text for string and json; raw bytes for every other type, with `structschema` entries captured
- [x] 2.5 Header and version validation (`invalid-header`, `unsupported-version`)
- [x] 2.6 Oracle test (corpus): Q7 wpilog decoded output equals the official reader's (count, catalog, all scalar values)
- [x] 2.7 Throughput test (corpus, `@pytest.mark.perf`): Q7 all-signals hoot export of about 37 M records decodes in under 4 s after warm-up

## 3. owlet registry and hoot conversion (spec: hoot-conversion)

- [x] 3.1 `tools/update-owlet-manifest.py`: build `owlet-manifest.toml` from CTRE's index (stable channels, latest version per compliancy, all four platforms, sha1 from CTRE, sha256 computed). Commit the generated manifest
- [x] 3.2 Tests, then implementation: read compliancy from byte 70; `empty-file`, `invalid-header`, `too-old` (below 6), and `unsupported-compliancy:<n>`
- [x] 3.3 Tests, then implementation: binary resolution (platform key, cache path, download, sha256 verify, refuse to run on mismatch, works offline once cached)
- [x] 3.4 Signal profiles: `--scan` parsing, the `health` allowlist, and `-s` id selection, with the profile recorded per log
- [x] 3.5 Run `--check-pro`; convert into a temp dir; surface owlet failures as quarantine reasons
- [x] 3.6 Corpus tests: Q7 (C19) and 2025 Q19 (C13) convert with the right owlet; Q11 is `empty-file`; profile `health` has fewer signals than `all`

## 4. Ledger and lake (specs: log-ingest-ledger, telemetry-lake)

- [x] 4.1 SQLite ledger schema (WAL): files, aliases, stage transitions, quarantine reasons, pipeline version. Tests for idempotency and aliases
- [x] 4.2 Raw store: content-addressed copy with hash verify. Never overwrites
- [x] 4.3 Bronze writer: sample columns, sorted by `(signal, ts_us)`, zstd, written to staging then atomically renamed. Crash-safety test (kill between write and rename leaves nothing visible)
- [x] 4.4 Metadata tables (logs, hoot_logs, entries, inventory) and their Parquet snapshots in `meta/`
- [x] 4.5 DuckDB query helper `flashpoint.lake.query(sql)` with views over bronze and meta. Test: one-signal query under 1 s

## 5. Log metadata (spec: log-metadata)

- [ ] 5.1 FMS fields (last non-empty), absent rather than empty
- [ ] 5.2 Build metadata parsing that splits on the first `": "` only
- [ ] 5.3 Wall-clock anchor from `systemTime` (linear fit), with the filename as fallback and the source recorded
- [ ] 5.4 Hoot metadata: bus from the filename, compliancy, owlet version, Pro, profile, first and last timestamp
- [ ] 5.5 CAN inventory capture and schema-v1 validation (synthetic fixture: valid, error payload, malformed)
- [ ] 5.6 Corpus tests: Q7 resolves to GACMP Q7 from FMS; `2025-nofms` has no FMS match; anchor scenario; bus detection

## 6. Ingest orchestration and CLI

- [ ] 6.1 `flashpoint ingest`: discovery (files and directories), hash, ledger, raw, convert (parallel), decode, bronze, metadata, success. Per-file quarantine; exit codes 0, 1, 2
- [ ] 6.2 Integrity check: hoot `short-coverage` against an overlapping wpilog (E10 truncated corpus test)
- [ ] 6.3 `flashpoint rebuild` (reprocess files from an older pipeline version, from raw)
- [ ] 6.4 `flashpoint doctor` (lake path, ledger counts, owlet cache per compliancy, numba warm-up status)
- [ ] 6.5 E2E corpus test: ingest the entire corpus into an empty lake. Expected success and quarantine sets, idempotent second run
- [ ] 6.6 Budget test (`perf`): Q7 group under 30 s and under 1 GB peak RSS. Record the measured numbers in the PR

## 7. Docs and wrap-up

- [ ] 7.1 Update ADR-0001 (SQLite ledger + Parquet snapshots), ADR-0003 (compiled scanner, official reader as oracle, spike numbers), and ADR-0004 (CTRE index, byte-70 compliancy, signal profiles)
- [ ] 7.2 Update `docs/rewrite/05-target-architecture.md` (§2 and §4 layout) and `06-roadmap.md` (P1 status)
- [ ] 7.3 README "Getting started" for `flashpoint ingest` (the README is rewritten fully later, in retire-legacy-code stage 2d)
- [ ] 7.4 Add a `perf` CI job (non-blocking at first) that runs the budget tests on ubuntu-latest
