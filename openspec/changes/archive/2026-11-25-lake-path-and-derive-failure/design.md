## Context

See proposal.md (Why). Every command builds its lake in one place: `cli.main` calls `LakePaths(config.lake_root(args.lake))`. `lake_root` returns the first of `--lake`, `FLASHPOINT_LAKE`, `lake` in `acquire.toml` (with `~` expanded), and `~/flashpoint-lake`. It returns them as given, so a relative path stays relative and is taken from the current folder wherever it is used. Two places call `Path.as_uri()` on the ledger without resolving it first, and that raises on a relative path: `anomaly/restarts.py` (detect, the #107 crash) and `views/anomalies.py` (serve). `acquire/backup.py` and `acquire/watch.py` already call `.resolve()` first. `setup/init.py` already writes `lake.expanduser().absolute()`.

After ingest, `_derive_and_report` runs derive in a child process (`check=False`) and returns `None`. A non-zero derive only skips the Replay build. `main` returns `report.exit_code`, which is 1 only when a file failed or was quarantined in this run (log-ingest-ledger). Derive's own output, including a traceback, is not captured and goes straight to the terminal. Inside derive, detect's handled failures (lock held, configuration error) are printed and do not fail derive. An uncaught detect error (#107) does make derive exit non-zero.

## Goals / Non-Goals

**Goals:**
- Turn the lake into an absolute folder in one place, so every caller, including future ones, gets an absolute root.
- Make a failed post-ingest derive impossible to miss, both in the output and in the exit code.

**Non-Goals:**
- Patching each `as_uri()` site. The absolute root makes them safe, and the two that already call `.resolve()` stay as they are.
- Retrying an owed derive from `ingest` (see proposal).

## Decisions

**D1. Resolve in `config.lake_root`, not in `LakePaths`.**
Every command's lake comes through `lake_root` (`cli.py`, one call), and so do the derive and report child processes, which receive `str(lake.root)`, now absolute. *Alternative:* `LakePaths.__post_init__` normalising `root`. Rejected: a frozen dataclass doing filesystem work, and `lake_root` is already the one place every command goes through. Library and test callers build `LakePaths` from paths that are already absolute.

**D2. Absolute without following symlinks: `os.path.abspath(path.expanduser())`.**
This normalises `..` (so `--lake ../lake` shows `/work/lake`, not `/work/sub/../lake`) and keeps a symlinked lake looking like the path the user gave. That matters for an external drive mounted through a symlink. `init` already uses `.absolute()`, which gives the same folder; it isn't touched. *Alternative:* `Path.resolve()`. Rejected because it rewrites symlinks, which makes `doctor` show a folder the user didn't type.

**D3. Every source is taken from the current folder, `acquire.toml` included.**
Josh's call, 2026-11-24. It is one rule for all three sources, and it is today's behaviour for `acquire.toml`, so nobody's working setup moves. *Alternative:* take a relative `acquire.toml` value from the configuration folder, the usual rule for paths in a config file. Rejected. The trade-off is kept: a service (systemd unit, Windows scheduled task) runs from `$HOME` or a system folder, so a relative `lake` in `acquire.toml` names a different lake there than in the user's shell. `init` and the Setup view always write an absolute lake, so only a hand edit can produce a relative one. The configuration reference recommends an absolute path for the acquire service.

**D4. A failed derive makes `ingest` and `rebuild` exit 1, with a message to stderr.**
`_derive_and_report` returns whether derive succeeded. `main` returns `EXIT_FAILED` (1) when derive failed, else `report.exit_code`. Any non-zero derive code maps to 1, including derive's own 2 for a configuration error. Exit 2 means a usage error in the command the user typed, and here the typed command was fine. The message comes after derive's output so it is the last thing on screen: `derive failed: Replay data was not built. Fix the error above, then run: flashpoint derive --report`. A Replay build failure after a good derive keeps the match-reports rule (printed, exit unchanged). *Alternative:* message only, exit 0. Rejected by Josh on 2026-11-24, because scripts and CI could not tell that Replay wasn't built.

**D5. Tests.**
- Unit (`lake_root`): each source relative and taken from the current folder, `~` in each, and `..` normalised. These use `monkeypatch.chdir` and a temporary configuration folder.
- CLI, with the existing `FakeRuns` fixture: derive code 1 and 2 each give exit 1, the message, and no report step; quarantine plus failed derive gives exit 1 with both shown; `rebuild` gives the same. These go in a new test file.
- Real derive failure: an invalid robot TOML with a real subprocess derive (no fake), proving the message follows derive's real error.
- Corpus: `flashpoint ingest --lake lake .` run from a temporary folder on Q7 builds `lake/report/data/matches.js` and exits 0.
- The existing case `test_report_skipped[flags2-1]` in `tests/test_cli.py` asserts exit 0 on a failed derive. It must change to expect 1. It is edited in the test-first task, before any source change, and the PR lists it.

## Risks / Trade-offs

- [A script that ignored a broken derive now sees exit 1] → Intended (BREAKING in the proposal). Only manual and scripted `ingest` is affected; the watch uses `--no-derive`. The changelog and troubleshooting guide say so.
- [A hand-edited relative `lake` in `acquire.toml` names a different lake for the acquire service than for the user's shell (D3)] → This isn't new: it is today's behaviour. `doctor` now shows the absolute lake in use, and the configuration reference recommends an absolute path for the service. `init` and Setup never write a relative value.
- [A derive locked out by another derive?] → Derive takes no lock today, so this has no new failure mode. Detect's lock-held case is handled inside derive and still returns 0.
- No full-log work: no time or memory budget applies. The corpus check reuses the Q7 ingest already covered by the ingest budget.

## Migration Plan

None for lakes. Ship with a changelog line under "Changed": a failed derive after `ingest` now exits 1. Rollback is a revert.
