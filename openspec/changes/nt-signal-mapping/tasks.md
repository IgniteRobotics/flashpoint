## 1. Season config and signal declarations (spec: robot-config, "Season configuration file", "Robot configuration file")

- [x] 1.1 Write the tests first, and confirm they fail:
  - a valid season file loads its five roots;
  - an unknown key, a missing season, or an empty root is rejected, with the file and the key named;
  - a robot config with `[[signal]]` tables loads;
  - each of these is rejected with both entries or the root named: a duplicate signal id, a duplicate (root, entry), a root the season doesn't define, and signals with no season file;
  - a non-snake_case metric is rejected;
  - the existing configs without signals still load unchanged.
- [x] 1.2 Implement the `Signal` model and the season loader in `semantics/robot_config.py`, plus `load_seasons(config_dir/"seasons")`. 1.1 passes, and so does the existing robot-config test suite.
- [x] 1.3 `flashpoint doctor` lists each season's roots and the number of signals per robot. Test the output, and check it by hand on this laptop.

## 2. Silver signal samples (spec: nt-signals, "Declared signals matched by name", "Signal samples in silver"; telemetry-lake, "Silver and gold layers")

- [x] 2.1 Write tests first against a synthetic wpilog fixture and a fixture config:
  - a double entry and an int64 entry produce numeric samples with the right labels;
  - a boolean beam break that turns true twice yields 1 at those times and 0 elsewhere;
  - an undeclared SmartDashboard entry yields no signal samples but is still in bronze;
  - a session marked `robot = unknown` yields none;
  - match time and phase come from framing, and are empty for an unframed session;
  - the tests fail before step 2.2.
- [x] 2.2 Implement `semantics/signals.py` (one DuckDB query per session over the wpilog's bronze partition) and write `silver/signals/` through the staging-then-rename path. 2.1 passes.
- [x] 2.3 Wire it into `Deriver.run()` for every session with a robot, including sessions with no hoots.
  - Add the signal declarations and season roots to the derive fingerprint.
  - When a session's robot declares no signals, re-derive removes that session's old `silver/signals/` partition.
  - Tests: editing one signal re-derives only that robot's sessions; deleting all signals leaves no partitions behind.
- [x] 2.4 Expose `silver/signals` as a DuckDB view in `lake/query.py`, next to `samples`. A test queries it.

## 3. Missing and unusable signals (spec: nt-signals, "Missing and unusable signals reported")

- [x] 3.1 Write tests first, and confirm they fail:
  - a declared entry absent from the wpilog is recorded as `missing`, derive succeeds, and the other signals are written;
  - a string or array entry is recorded as `non-numeric`, with no samples;
  - a fixture wpilog truncated mid-record keeps the samples before the truncation, and the session isn't quarantined.
- [x] 3.2 Add the `missing_signals(session_id, signal_id, reason)` ledger table and write it during derive. Expose it as a `meta_missing_signals` view, and log one derive warning per session that has missing signals. 3.1 passes.

## 4. 2025 legacy migration (spec: robot-config, "Legacy configuration migration")

- [x] 4.1 Write tests first using small in-test CSVs, JSON, and a fixture reference wpilog, and confirm they fail:
  - `corraler/Motor Current` is reported as superseded by the corraler CAN slot;
  - an entry absent from the log is reported as *not in reference log*;
  - an array entry is reported as *non-numeric in reference log*;
  - a present boolean beam break becomes a signal with a snake_case metric;
  - a vision row with an empty metric gets its key snake-cased (`targetPitch` becomes `target_pitch`);
  - every prefix in the log config becomes a season root or is reported;
  - the existing `Pheonix6` test still passes.
- [x] 4.2 Extend `semantics/legacy_migration.py` and `tools/migrate-legacy-config.py`:
  - add `--reference-log`, and read `log_configs/config2025.json` from the `legacy-2025` tag;
  - write `config/seasons/2025.toml`, plus the `[[signal]]` tables and the report head comment into `config/robots/2025-comp.toml`.

  4.1 passes.
- [x] 4.3 Run the migration against corpus `2025-gadal-q30` and commit the generated files.
  - Corpus test: every row of `datamaps/2025/{metrics,vision}_map.csv` and every prefix of `config2025.json` (read from the tag) is either mapped or reported with a reason.
  - Expect 6 metrics rows and the numeric vision rows mapped, and the 18 motor rows (17 V/I/T plus the wrist motor position) superseded.

## 5. 2026 mapping (spec: robot-config, "Robot configuration file"; nt-signals, "Declared signals matched by name")

- [ ] 5.1 Write a throwaway script, not committed, that lists the numeric and boolean NT entries in corpus `2026-gacmp-q7` under the robot and PhotonVision roots, with type and sample rate. Draft `config/seasons/2026.toml` and the candidate `[[signal]]` list following design §6.
- [ ] 5.2 **[HUMAN]** Josh reviews the candidate 2026 signal list and labels, and signs off. Record the decision in the PR.
- [ ] 5.3 Commit the 2026 season file and signals. The `Valid 2026 configuration` test asserts signals for Intake, Shooter, Indexer, Hunter, Drivetrain, and each of the three cameras. The 23-slot assertions are unchanged.

## 6. Corpus end-to-end and budgets

- [ ] 6.1 Corpus tests:
  - every declared 2026 signal matches in `2026-gacmp-q7`, and every declared 2025 signal matches in `2025-gadal-q30`;
  - `2026-gacmp-p2` (no hoots) gets signal samples;
  - `2025-nofms` gets samples with empty match time;
  - `missing_signals` is empty for Q7 and Q30.
- [ ] 6.2 Extend `tests/test_ingest_perf.py`:
  - Q7 ingest, including signals, still meets 30 s and 1 GB;
  - print the time and size of the signal step, and assert it adds at most 2 s and 150 MB against a run with no signals declared;
  - record the Q7 `silver/signals/` partition size in the PR.
- [ ] 6.3 Run the full suite locally (unit, `corpus and not perf`, `perf`, `browser`), and check that the gold, Replay, and History outputs for Q7 are byte-identical before and after this change. CI is green on all jobs.

## 7. Docs

- [x] 7.1 Update the docs, then check the links:
  - `docs/rewrite/06-roadmap.md`: set the NetworkTables signal mapping status;
  - `docs/rewrite/05-target-architecture.md`: config layout, `seasons/<year>.toml` contents, and `silver/signals` in the lake layout;
  - `README.md`: a short "Signals" section showing how to declare one, and `derive --all`.
- [ ] 7.2 Add a note in the PR that `retire-legacy-code` 3b.1's gate is met once this change is archived. Do not delete `datamaps/` or `log_configs/` here.
