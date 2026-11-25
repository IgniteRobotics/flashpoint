## Why

On 2026-11-24 the `docs` workflow's **Media** step failed the whole docs deploy because owlet crashed on one sample hoot ([run 45482738725](https://github.com/IgniteRobotics/flashpoint/actions/runs/45482738725), `development` at 68ae73e). owlet crashed (`exit -11: no output`) on 4 hoots. Ingest's own retry recovered 3. `GACMP_E10_rio_2026-04-11_19-26-19.hoot` crashed again and was marked failed ("will retry"). Ingest exited 1, and `tools/docs-media.py` stopped with `sample lake: flashpoint ingest failed`. A re-run of the job passed. Nothing in the change being deployed caused it. A file marked failed is exactly what a second `ingest` picks up, so the sample-lake build can ride out the flake instead of failing the deploy. Filed as issue #123.

This continues the MVP documentation work (#68, change `mvp-documentation`, media pipeline decision D-f), after phases P0–P6 in `docs/rewrite/06-roadmap.md`. It changes no D1–D11 decision in `05-target-architecture.md` §6. It fixes no numbered pitfall from `03-pitfalls-and-performance.md`. (GitHub issue #17 is the owlet crash; pitfall #17 in `03` is unrelated.)

## What Changes

- When a sample-lake ingest reports files that **failed** in this run (the machine's fault) and none it **quarantined**, the build runs `flashpoint ingest` again with the same files, up to 2 more runs. Each retry prints the files it retries and the run number.
- If a file is still failed after the last run, the build exits non-zero, naming each file and the number of runs.
- A run that quarantined a file is not retried. A second run would count it as "quarantined earlier" and exit 0, which would hide a real problem with a sample file. So the decision reads this run's failed and quarantined counts, not just the exit code.
- A non-zero ingest with no failed or quarantined files (for example a failed derive), or output whose summary can't be read, is not retried. It fails as it does today.
- The quarantine sample lake, which expects a quarantine, keeps building.
- The same rule covers every sample lake the media command builds (sample, quarantine, tutorial). The `report` step is unchanged.

## Non-goals

- Fixing the owlet crash itself (#17).
- Retrying the `corpus` CI job, which flaked on the same crash.
- Changing ingest's own owlet retry, its exit codes, or its summary output.
- Adding a machine-readable output (`--json`) to `flashpoint ingest`.
- Untangling a run that both fails and quarantines files in the quarantine sample lake. It's judged by the exit code alone, as today.
- Any user-facing behavior change. The media command runs only in docs CI and on maintainers' machines, so there's no changelog entry.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `project-documentation`: adds a requirement, "Sample lake rides out a failed file". It sits next to "Generated screenshots and animations", which is still in the open `mvp-documentation` change. It's a separate requirement so the two changes can archive in either order.

## Impact

- Code: `tools/docs-media.py`, `build_sample_lake` only. It reads the ingest summary's FAILED lines and count line, and loops.
- Tests: a new `tests/tools/test_docs_media_retry.py` with a fake `flashpoint` (subprocess stubbed). It covers: fails once then succeeds; keeps failing; quarantined; a non-file failure. A drift test feeds the real ingest summary output through the parser, so a wording change in `flashpoint ingest` breaks a unit test, not a deploy.
- CI: the `docs` workflow's Media step stops failing on a single owlet flake. Worst-case extra time is 2 more ingest runs. Each one re-ingests only the failed files plus an incremental derive.
- Docs: none.
