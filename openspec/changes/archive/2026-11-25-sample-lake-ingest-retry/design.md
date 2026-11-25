## Context

`build_sample_lake` in `tools/docs-media.py` runs `python -m flashpoint ingest --lake … <files>` and then `report`, as subprocesses. It aborts when an exit code isn't in `ok`. `capture` calls it three times: the sample lake `ok=(0,)`, the quarantine lake `ok=(0, 1)` (the empty hoot is expected to quarantine), and the tutorial lake `ok=(0,)`.

`flashpoint ingest` exits 1 when this run quarantined **or** failed anything (`IngestReport.exit_code`). A file quarantined in an earlier run is reported as "quarantined earlier" and doesn't count. The ingest also runs derive and an incremental Replay build when anything imported. A failed derive also exits 1 (#107). Ingest already retries owlet internally. A file it still can't read is left **failed**, so the next run picks it up again.

The stdout `_summarize` prints is the only place the counts appear outside the process:

```
  FAILED      <name>: failed (<reason>), will retry
  QUARANTINED <name>: <reason>
<n> ingested, <n> skipped, <n> failed, <n> quarantined, <n> quarantined earlier in <s>s
```

See proposal.md for the motivation.

## Goals / Non-Goals

**Goals:**
- A single transient owlet crash on a sample file doesn't fail the docs deploy.
- A real problem with a sample file (quarantine), a file that keeps failing, and a non-file failure (derive) still fail the build, loudly and by name.

**Non-Goals:**
- A general retry helper for other tools or CI jobs.
- Retrying `report`.

## Decisions

### 1. Decide from this run's parsed counts, not the exit code

Exit 1 means "failed or quarantined" and can't tell them apart. A blind re-run turns a quarantine into "quarantined earlier", exit 0. So each ingest run's stdout is parsed:
- the count line, matched by a regex anchored on `ingested, … skipped, … failed, … quarantined, … quarantined earlier`. It is the last matching line, since derive output can follow it.
- the `FAILED` and `QUARANTINED` line names, so messages can name files.

Rules per run (this order):
1. Summary not found → no retry; judge by `ok` as today (the build fails when the code isn't in `ok`). Print the output.
2. `quarantined > 0` → no retry; judge by `ok` as today. On failure, name the quarantined files.
3. `failed > 0` → retry if runs remain (3 runs total). Otherwise fail, naming the still-failed files and the run count.
4. Otherwise → judge by `ok` as today. This covers the clean run and the failed-derive case.

A retry passes the same file list. Files already imported are skipped by the ledger, so only the failed ones are read again.

*Alternative: add `flashpoint ingest --json`.* It's a cleaner contract, but it changes the public CLI and the CLI reference for one internal tool. The issue scopes this to the build. Rejected for now. The drift test in decision 3 gives the parse the protection a contract would.

*Alternative: retry on any exit 1, then check the ledger for quarantines.* That reads lake internals from a docs tool, and it's more coupling than reading stdout. Rejected.

### 2. Retry limit: 2 more runs (3 total)

The 2026-11-24 flake hit 4 of ~50 hoots in one run, and ingest's own retry already recovered 3. One more full attempt per failed file is very likely enough. Two leaves margin, and the worst case stays bounded. It's a module constant (`INGEST_RUNS = 3`), not a flag.

### 3. Drift test against the real summary

The parser lives in `tools/docs-media.py`. A unit test builds an `IngestReport` with one failed, one quarantined and one quarantined-earlier result. It runs the real `flashpoint.cli._summarize` on it (capturing stdout) and checks that the parser reads back the same counts and names. If someone rewords the summary, the unit suite fails on every PR, not the docs deploy weeks later.

### 4. Tests stub the subprocess, not a fake executable on PATH

`build_sample_lake` runs `[sys.executable, "-m", "flashpoint", …]`, so a fake on PATH wouldn't be picked up. The tests monkeypatch the module's `subprocess.run` with a scripted fake. The fake returns a `CompletedProcess` whose stdout is the summary text for each call, and it records the calls. That counts runs without touching owlet, the corpus or a real lake. The `REPO / "config"` copy still happens into `tmp_path`, which is cheap.

### Time and memory budgets

No full log is read by the new code. Parsing is linear in the ingest's stdout (a few KB). A retry run costs the failed files' ingest plus an incremental derive and Replay build. For one hoot that's about 10–20 s on CI, and at most 2 such runs. The Media step's current runtime isn't budgeted, and this adds at most about 40 s in the failure case and 0 s otherwise. Memory is unchanged.

### Decisions D1–D11

None touched. This stays within the media pipeline (mvp-documentation D-f). No ADR.

## Risks / Trade-offs

- [Parsing human-readable output is brittle] → Decision 3's drift test runs on every PR. An unreadable summary falls back to today's behavior (fail), never to a silent pass.
- [A persistently flaky owlet could pass on run 3 and hide a worsening crash rate] → Every retry prints the file and run number in the Media step log. #17 stays open to track the crash itself.
- [The quarantine lake (`ok=(0, 1)`) used to accept a failed file on exit 1] → It now retries it like any lake, and fails if the file stays failed. The one gap left: a run there that quarantines *and* fails files takes rule 2, is accepted, and isn't retried. That's the same as today, and rare.
- [The test freeze] → The tests in task group 1 are written before implementation starts. After that, `tests/` isn't edited.
