## Why

The rewrite replaces all of the legacy code on main, but no change owns removing it. Per-phase proposals only list what they "retire". Left alone, dead scripts linger, confuse new students, and keep broken entry points that look runnable (#1–#9, #60–#67). This change is the single owner of the removal inventory. It applies alongside every roadmap phase (P0–P4), not as a phase of its own.

## What Changes

- **Stage 1 (with P0): delete code that is already dead.** These files are broken, unreferenced, or superseded, so removing them loses nothing:
  - `setup-db.py`, `ingest_dir.sh`, `ingest_dir_scripts/`, `summary_metrics.py`, `manage_imports.py`, `old_sync_scripts/`
  - `requirements.txt` (a dev-machine pip freeze)
  - `docs/*.excalidraw` (stale; superseded by the mermaid diagrams in `docs/rewrite/`)
- **Stage 2 (gated): delete each legacy component only after the change that replaces it is archived:**

  | Legacy component | Gate |
  |---|---|
  | `csv_converter.py`, `datalog.py`, `ingest_library.py`, `ingest_system_log.py`, `ingest_device_log.py`, `ingest_match_logs.py`, `ingest-requirements.txt`, `executables/owlet.exe` | `p1-core-ingest-lake` archived (owlet.exe moved here from stage 1 during P0: `ingest_library.py:50` still uses it on Windows, and its replacement, the owlet registry, arrives in P1) |
  | `datamaps/`, `log_configs/` | `p2-semantics-and-physics` archived, **and** the config migration has run |
  | `docker-services/`, `docker-compose.yml`, `.dockerignore`, `drive-backup.py` | `p3-log-acquisition` archived |
  | `viz.py`, `viz-requirements.txt`, `gw_config.json` | `p4-views-and-reports` archived |

- Re-point the `main:file:line` locators in `docs/rewrite/` at the `legacy-2025` tag, so the citations survive the deletions
- Rewrite `README.md` for the new package once stage 2 is finished

## Capabilities

### New Capabilities
None. This change removes code and doesn't alter any spec-level behavior (`skip_specs: true`).

### Modified Capabilities
None.

## Branching

All of these deletions land on the `rewrite` integration branch, never directly on `main`; stage 1 is the exception, already on `main` via #9. `main` keeps the legacy code until the one-time `rewrite` → `main` merge.

## Non-goals

- **Rewriting git history** to purge binaries (about 20 MB of owlet builds). That needs a force-push to a shared repo; revisit only if clone size becomes a real problem
- Editing or fixing legacy code. It is deleted, never patched
- Cleaning up `origin/development` or `origin/feature/power-tracking`. They are archived as tags in P0

## Impact

- Removes about 40 tracked files from main over P0–P4. Everything stays recoverable from the `legacy-2025` tag
- Anyone running legacy scripts locally loses them at each gate (there are no working entry points today)
- `docs/rewrite/README.md` locator convention changes to `legacy-2025:path:line`
