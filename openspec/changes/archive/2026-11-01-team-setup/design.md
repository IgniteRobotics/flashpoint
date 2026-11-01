## Context

See proposal.md for the motivation and the two specs for behaviour. What the code does today:

- **Config root** (`config.py:config_root`): explicit path, then `FLASHPOINT_CONFIG`, then the checkout's `config/` (`parents[2]`, only exists in a source checkout), then `~/.config/flashpoint`. A pipx install lands on the last one, which usually doesn't exist, and nothing says so (flashpoint-private#54).
- **Config files** in `config/`: `seasons/*.toml`, `robots/*.toml`, `units/units-seed.csv`, `report.toml`, `anomaly.toml`, `anomaly-causes.toml`, `failure-labels.toml`, and optional `acquire.toml`. All are read with `tomllib` into pydantic models with `extra="forbid"`.
- **6829 is hard-coded** only in `AcquireConfig.hosts`.
- **`doctor`** (`cli.py:_doctor`) prints versions, the lake, ledger stages, seasons, robots, the owlet cache, AdvantageScope and acquire status, as text. It has no structured checks and no exit status for problems.
- **Writes in the app**: ADR-0015 allows one guarded write (triage). The guard is loopback bind + loopback client + `Host`/`Origin` + JSON body ≤ 4 KB + `X-Flashpoint` header.
- **Packaging**: Poetry, `name = "flashpoint"`, version 0.1.0. CI is `.github/workflows/ci.yml` only. `flashpoint` and `flashpoint-frc` are both unclaimed on PyPI (checked 2026-10-19).
- **Corpus**: `tools/fetch-corpus.py` downloads Ignite match logs from a GitHub release of this repo, which is private today.

## Goals / Non-Goals

**Goals:**
- One check list in one place: `doctor` and the Setup view render the same results.
- Config edits go through the same models as loading, so the UI can never write a file the CLI would reject.
- Every write the app makes uses the ADR-0015 guard. Shared on the network, the app stays read-only.

**Non-Goals:**
- No config schema changes beyond `acquire.toml` gaining `team` and `lake`. Robot, season and threshold files keep their formats.
- No multi-team or multi-user configuration on one machine.
- No running `derive`, `detect` or `report` from the UI. The view names the command (spec: "What an edit affects").

## Decisions

### D-a. Publish as `flashpoint` on PyPI, with trusted publishing
- Claim the name `flashpoint` (`flashpoint-frc` as the fallback if it's taken first), so ADR-0009's `pipx install flashpoint` holds.
- A release workflow builds the sdist and wheel on a `v*` tag and publishes with PyPI trusted publishing (OIDC, no stored token).
- The version comes from the tag. One Python range per release (ADR-0009): 3.11–3.13.
- *Alternative:* pipx from a git URL. Rejected by Josh: every install needs git and a build.

### D-b. Bundled defaults ship as package data; the repo's `config/` becomes Ignite's team configuration
- New `src/flashpoint/defaults/` holds the shared files:
  - `seasons/*.toml` with the WPILib roots (FMS, PhotonVision, CameraPublisher, Preferences, `flashpoint`); the team-specific `robot` root is left for `init`/the editor;
  - `report.toml`, `anomaly.toml`, `anomaly-causes.toml`;
  - an empty `units.csv` header.
- The repo's `config/` keeps Ignite's robots, units, failure labels and season `robot` roots. It is excluded from the wheel and used as the test fixture and as Ignite's own `FLASHPOINT_CONFIG`.
- **File resolution** is per file: configuration root first, then bundled default. Robots and units have no default.
- The `parents[2]` checkout fallback is removed. That's the **BREAKING** part: a developer checkout must set `FLASHPOINT_CONFIG=config`. The test conftest already sets it explicitly; the developer guide says so.
- *Alternative:* keep the checkout fallback for developers. Rejected: it's the reason flashpoint-private#54 is silent, and it makes the installed and dev behaviour differ.

### D-c. Per-user folder without a new dependency
- **Linux and macOS:** `$XDG_CONFIG_HOME/flashpoint`, else `~/.config/flashpoint`. That's what `config_root()` already falls back to, and what the guide tells users.
- **Windows:** `%APPDATA%\flashpoint`.
- *Alternative:* `platformdirs`. Rejected: it puts macOS under `~/Library/Application Support`, which students can't find, and the rule above is a few lines of code.

### D-d. `acquire.toml` gains `team` and `lake`; hosts are derived
- `team: int | None` and `lake: Path | None`.
- `hosts` defaults to empty. The effective host list is the `hosts` setting if non-empty, else the list derived from `team`, else acquire refuses (spec).
- `lake_root()` order becomes: explicit argument, `FLASHPOINT_LAKE`, the `acquire.toml` `lake`, then `~/flashpoint-lake`.
- **Reconciling with P3:** the unarchived `acquisition-service` delta says "No config file → default hosts". When `p3-log-acquisition` archives, its "No config file" scenario is updated to "refuses, naming `flashpoint init`" (task 9.2). Until then, `team-setup` holds the requirement.

### D-e. One check registry
- `flashpoint.setup.checks` holds a list of checks. Each returns `CheckResult(id, status, message, fix_action | None, command | None)`.
- `doctor` prints them, plus the existing informational lines. It exits 1 when any check is a problem, which is new: today it always exits 0. The API serves the same results as JSON at `GET /api/setup/checks`.
- Checks are fast (< 2 s total). Owlet checksums use the cached manifest hash and never download. The acquire-service check reads systemd (`systemctl --user is-active`) or Task Scheduler (`schtasks /Query`) state, and reports "not applicable" on macOS.

### D-f. Guarded write routes, one ADR
- New routes:
  - `POST /api/setup/fix/<action>`;
  - `PUT /api/config/<section>` (team, robots/<id>, thresholds/<file>, units).
- They use the ADR-0015 guard with `X-Flashpoint: setup`. The body limit rises to 256 KB for config saves (a robot file is about 20 KB).
- New ADR-0017 amends ADR-0013 and ADR-0015: the app may write configuration files and run a fixed list of setup actions, never lake data. ADR-0016 is taken by `live-can-health`.
- Fix actions are a fixed allow-list of Python functions, never shell strings:
  - `init`: the same code as the CLI;
  - `owlet`: the registry's download and verify;
  - `advantagescope`: writes the path into `report.toml` through the editor;
  - `service-install`: copies the shipped unit or task with the resolved executable path;
  - `service-start`.

### D-g. Editing keeps comments: `tomlkit` round-trip, pydantic validation
- A save:
  1. loads the file with `tomlkit`;
  2. applies the edit to the document;
  3. renders it, and validates the result by parsing with `tomllib` into the same pydantic model the loaders use (robot files also get the cross-file checks: season roots, battery proxy);
  4. writes it via temp file and `os.replace`.
- Units use `csv` with the column order kept.
- **Conflict check:** the view sends the SHA-256 of the file it loaded; the save is refused if the file's current hash differs.
- **Backups:** `<config root>/.backups/<relative path>/<UTC timestamp>.<ext>`, pruned to the last 20 per file.
- *Alternative:* regenerate files from the models. Rejected: it drops the comments that the robot files depend on (wiring notes, sources).

### D-h. Robot draft from a session
- Query bronze for the session's hoot devices (bus, model, CAN id), using the same device list `can-health` measures.
- Join the session's CAN inventory for serials (D11), and read the bus-status entry names for robot-code bus names.
- The draft is held in the browser until saved. Saving goes through D-g as a new file, so validation is identical.
- Slot ids are suggested as `<model>-<id>` for the user to rename.

### D-i. Setup view
One view file, `web/static/views/setup.js`, with `needsApi: true`, so static exports hide it. It has three tabs:
- **Checks:** results, with fix buttons only when `/api/setup/checks` reports `writable: true`;
- **Configuration:** sections with inline edit forms and a save per section;
- **Draft robot.**

Forms are built with `h()`, with text only, as in every other view.

### Architecture decisions
- **D9 / ADR-0009:** this change carries it out (pipx from PyPI).
- **D6 / D7 / ADR-0013:** extended by a view.
- **ADR-0015:** generalised by ADR-0017.
- **D11:** identity rules are unchanged; drafts use them.
- D1–D5, D8 and D10 are untouched.

## Budgets
- Setup checks: < 2 s in total on the reference laptop, with no network access unless a fix action asks for it.
- Config save: < 200 ms for a robot file, including validation.
- Robot draft: one DuckDB query over one session's bronze, < 1 s and < 100 MB for corpus Q7.
- Wheel size: under 5 MB. No logs, and no owlet binaries (D4: those are downloaded and cached).

## Risks / Trade-offs

- [Going public exposes Ignite's match logs (corpus release), robot files (CAN ids, serials) and history] → [HUMAN] review before flipping the repo (task 1.1). Options: keep the corpus in a separate private repo with fetch-corpus pointed at it, or accept that it's public. **Decided 2026-10-22:** the corpus stays public. Before going public, the history was rewritten into a new `IgniteRobotics/flashpoint` without the CTRE owlet binaries (pitfall #65) and with contributors' personal emails replaced by GitHub noreply addresses. The old repository is `IgniteRobotics/flashpoint-private`; closed issues and merged PRs stay there.
- [Removing the checkout fallback breaks every developer's local setup once] → The developer guide and the conftest set `FLASHPOINT_CONFIG`. `doctor` names the fix.
- [A second team's season roots differ (their `robot` root)] → `init` leaves the `robot` root empty with a comment. Signals stay unmapped and reported as `missing` until it's set, which is the existing behaviour.
- [Fix actions that install services touch the OS] → An allow-list, local only, guarded. Each action is idempotent, and its result says what changed.
- [`tomlkit` edits to deeply nested arrays of tables (slots) could reformat whitespace] → Tests compare the untouched parts of the 2026 robot file byte-for-byte before and after a slot edit.
- [The PyPI name is taken before release] → Claim it as early as possible with a 0.1.0 placeholder release (task 1.2). The fallback is `flashpoint-frc`, with the command still `flashpoint`.
- [Size: this is an L change] → Task groups are ordered so that packaging and the config home (1–4) can ship as one PR, and the Setup view and editor (5–8) as a second.

## Migration Plan

1. Ignite: copy (or point `FLASHPOINT_CONFIG` at) the repo's `config/` before upgrading. `doctor` flags a missing root.
2. Release 0.2.0 to PyPI once groups 1–4 are merged. Update the user guide's install section to `pipx install flashpoint`.
3. Rollback: reinstall the previous version from the checkout. Config files keep their formats, so they work with either version.
