# Developer guide

For people who change Flashpoint's code. Read [How it works](how-it-works.md) first for the pipeline, the lake, and the identity model. To run Flashpoint without changing it, see the [user guide](user-guide.md).

## Build and run from source

You need:
- Python 3.11, 3.12, or 3.13 (`requires-python = ">=3.11,<3.14"`). [pyenv](https://github.com/pyenv/pyenv) is the easy way to get one;
- [Poetry](https://python-poetry.org/) 2;
- optionally, the [GitHub CLI](https://cli.github.com/): `tools/fetch-corpus.py` uses it when it is installed.

```sh
git clone https://github.com/IgniteRobotics/flashpoint.git && cd flashpoint
poetry install
poetry run flashpoint doctor
```

`poetry install` installs the package in editable mode, along with the dev tools (pytest, ruff, mypy, Playwright, robotpy-wpiutil). The repo's `config/` folder is Ignite's team configuration and the tests' fixture. Flashpoint doesn't find it on its own: set `export FLASHPOINT_CONFIG=config` (from the repo root) in a checkout, or `flashpoint` uses your per-user folder instead. The tests set it themselves (`tests/conftest.py`). The shared defaults every team starts from live in `src/flashpoint/defaults/` and ship in the package. Optional extras:

```sh
poetry install --extras ml        # scikit-learn, for the optional MODEL anomaly detector
```

Use a scratch lake while developing, so you don't mix test data into your real one:

```sh
export FLASHPOINT_LAKE=/tmp/fp-lake
poetry run flashpoint ingest ~/.cache/flashpoint/corpus/corpus-v1/
poetry run flashpoint serve
```

### Ubuntu notes

Ubuntu sets up a few traps for a fresh install:

- **Poetry 1.x from apt.** `apt install python3-poetry` gives you Poetry 1.8. It can't read the `[project]` table in `pyproject.toml`, so `poetry install` fails with "the Poetry configuration is invalid" and complains that `name`, `version`, `description`, and `authors` are missing. Remove it (`sudo apt remove python3-poetry`) and install Poetry 2 with pipx: `sudo apt install pipx && pipx ensurepath`, open a new shell, then `pipx install poetry`.
- **pyenv shims exit with status 127.** That means the selected Python doesn't have the command Poetry ran. If you use pyenv, make a virtualenv and select it in the checkout: `pyenv install 3.12`, `pyenv virtualenv 3.12 flashpoint`, `pyenv local flashpoint` (or `pyenv activate flashpoint` for one shell). With `pyenv global system`, the failing command is `python`: Ubuntu only ships `python3`. Run `poetry env use python3`, or `sudo apt install python-is-python3`.
- **No `venv` in the system Python.** If Poetry builds its venv from Ubuntu's own Python and fails with an `ensurepip` error, run `sudo apt install python3-venv` and try again.

## Code layout

| Path | What's there |
|---|---|
| `src/flashpoint/cli.py` | The `flashpoint` command; one handler per subcommand |
| `src/flashpoint/readers/` | wpilog reading and hoot conversion (the owlet registry) |
| `src/flashpoint/lake/` | Lake paths, the ledger, Parquet writing, DuckDB views |
| `src/flashpoint/ingest.py` | `ingest` and `rebuild` |
| `src/flashpoint/semantics/` | `derive`: sessions, alignment, match identity, framing, device identity, physics, silver, gold, CAN health |
| `src/flashpoint/anomaly/` | `detect` and `backtest` |
| `src/flashpoint/report/` | Replay data and the static export |
| `src/flashpoint/setup/` | `init`, the setup checks behind `doctor`, the config editor and API, fix actions, and robot drafts: the backend of the Setup view |
| `src/flashpoint/theme.py` | `theme.toml`: the palette presets, contrast rules, and the colour maths that turns a palette into every colour token |
| `src/flashpoint/web/branding.py` | Builds `theme.css` and the team name for the page. The server and the static export both use it |
| `src/flashpoint/config.py`, `src/flashpoint/defaults/` | Finding and loading configuration; the bundled defaults used when a file is missing |
| `src/flashpoint/views/`, `src/flashpoint/web/` | History and Anomalies queries, the HTTP server, the browser app, the AdvantageScope launcher |
| `src/flashpoint/acquire/` | Robot and USB pulls, the watch loop, backup and restore |
| `src/flashpoint/live/` | Live CAN: the NetworkTables contract, the detector, the robot connection, and the fake robot (`python -m flashpoint.live.fake_robot`) |
| `src/flashpoint/meta/`, `src/flashpoint/deploy/` | The meta Parquet snapshots; the systemd and Windows service files |
| `config/` | Ignite's own configuration: `acquire.toml`, robots, seasons, units, failure labels. A file that isn't here (thresholds in `anomaly.toml`, `report.toml`, `theme.toml`) comes from the bundled defaults in `src/flashpoint/defaults/` |
| `tools/` | `fetch-corpus.py`, `update-owlet-manifest.py`, `migrate-legacy-config.py` |
| `examples/robot-java/` | Suggested robot-code samples (not built or compiled in CI) |
| `tests/` | The test suite, mirroring `src/flashpoint/` |

The code conventions are the [coding standards](#coding-standards) below.

The domain rules (long format at rest, as-of joins with explicit tolerance, never trust filenames over FMS data, offline first, never delete robot logs) are in [`openspec/config.yaml`](../../openspec/config.yaml).

### The browser app

- A new view needs three things: a file in `src/flashpoint/web/static/views/`, an `FP.view({ id, label, needsApi, mount, unmount })` call in it, and a `<script src="static/views/NAME.js">` tag in `web/static/index.html`. Without the tag the file never loads. If the view needs the server, add `data-needs-api` to the tag and set `needsApi: true`. The static export then leaves the file out and the shell hides the view.
- Colours come from the role-named tokens in `web/static/tokens.css`: `--accent`, `--accent-hi`, `--accent-mid`, `--accent-dim`, `--accent-muted`, `--warn`, `--on-accent`, `--prose`, the `-rgb` channel forms (`--accent-rgb`, `--warn-rgb`, `--bg-rgb`), and the colours built from them (`--glow-NN`, `--grid-line`). `theme.css` loads after `tokens.css` and overrides them with the team's palette; `flashpoint.theme` derives that map from `theme.toml`. Never write a colour by hand. `tests/web/test_static_lint.py` fails on an `rgb()` or `rgba()` colour other than black outside `tokens.css`, on a hex colour in a `.css` file, and on the old amber names (`--amber*`, `--on-amber`, `--hot`). A hex colour in a `.js` file is not caught, so check those yourself.
- Charts can't read CSS variables. Get colours from `FP.tokens()` (`accent`, `accentHi`, `accentMid`, `accentDim`, `accentMuted`, `line`, `lineSoft`, `lineStrong`, `warn`, `accentRgb`, `surface1`, `font`). They're read once and cached until the page reloads.

## Checks and coding standards

```sh
poetry run pre-commit run --all-files   # every formatter and linter, at CI's versions
poetry run mypy src tests tools
```

The first command runs ruff (format and lint), Biome (the app's JS and CSS), and a few file-hygiene hooks, using the versions pinned in `.pre-commit-config.yaml`. CI runs the same file, so if it passes for you it passes in CI. It fixes what it can and leaves the rest for you. Biome needs Node.js on your `PATH`; if pre-commit can't set it up, `npx @biomejs/biome@2.6.7 check` runs the same check. `poetry run pre-commit install` makes it run on every commit, which is optional.

The JS and CSS were reformatted in one commit when Biome was adopted. Run `git config blame.ignoreRevsFile .git-blame-ignore-revs` once so `git blame` skips it (GitHub's blame view already does).

Coverage is measured on the unit run, with Numba's compiler off so coverage can see the reader kernels:

```sh
NUMBA_DISABLE_JIT=1 poetry run pytest --cov
```

It fails below 93% overall. CI also fails if `readers/`, `lake/`, or `semantics/physics.py` drops below 85%. Floors go up, never down; lowering one needs a mentor's approval in the pull request.

### Coding standards

These rules are also the [Flashpoint section of the team coding standards](https://igniterobotics.github.io/ignition-labs/programming/coding-standards/#flashpoint-python-and-javascript). That page is the source: when the two differ, it's right and this section gets fixed. **Check** rules fail the required CI check. **Review** rules are a reviewer's job.

**Python: formatting, naming, and types**

- Formatting and import order come from `ruff format` and `ruff check`. *(Check)*
- `snake_case` functions and variables, `PascalCase` classes, `SCREAMING_SNAKE_CASE` constants, `kebab-case` file and folder names outside the Python package. *(Check, except filenames: Review)*
- Type hints on everything; `mypy --strict` passes. *(Check)*

Why: one style means a diff shows only what changed, and strict types catch a class of bug before the code runs.

**Python: suppressions name the rule and the reason**

- `# noqa: S608 - table name from the ledger schema`, and `# type: ignore[arg-type]  # fixture dict is untyped`. A bare `# noqa`, a type ignore with no code or no reason, or a suppression for a rule that isn't firing fails the check. *(Check)*

Why: a suppression claims someone looked at this line. Without a rule it hides every future problem on the line; without a reason nobody can tell if they were right.

**Python: processes, shells, and SQL**

- Only the command-line entry point (`cli.py`, `__main__.py`) and `tools/` exit the process or print. *(Check)*
- Subprocesses take an argument list, never a shell string. `shell=True`, `os.system`, and `os.popen` are never allowed, even with a suppression. Each subprocess call says where its arguments come from. *(Check)*
- SQL is built only from internal identifiers (lake paths, fixed column and view names); values go in as bound parameters. A line that builds SQL from strings says which identifiers it fills in. *(Check)*

Why: one bad log calling `sys.exit` mid-batch stops every log after it (#57). A shell string with a filename in it breaks on a space and can run what the name smuggled in (#54). SQL built from data is the same bug.

**Python: errors, time, and complexity**

- No catch-all `except` except at a boundary that records the error and carries on (a per-file ingest loop, a request handler, a service loop); the suppression names the boundary. A handler that logs the exception passes on its own. *(Check)*
- Date-times are timezone-aware. *(Check)*
- Functions stay at cyclomatic complexity 12 or under. The nine that were over when this was adopted carry `# noqa: C901 - grandfathered (code-quality-gates)` and must not get worse. *(Check for new code; Review for grandfathered functions)*

Why: a catch-all in the wrong place turns a visible crash into silently wrong numbers. Robot logs come from different clocks, and a naive date-time is off by hours without saying so.

**Python: tests**

- Every change comes with tests. Unit tests use synthetic fixtures; tests that need real logs are marked `corpus`. *(Review)*
- Coverage stays at or above the floors above. *(Check)*
- Once implementation of a change starts, nobody edits its tests to make them pass. *(Review)*
- Tests are exempt from a few rules on purpose (asserts, local servers and subprocesses, fixture arguments, compound asserts); see `[tool.ruff.lint.per-file-ignores]` in `pyproject.toml`.

**Browser app**

- Classic scripts only: no `import`/`export`, no build step. Each file runs in strict mode and touches only the `FP` namespace, `FP_MODE`, and `uPlot`. *(Check)*
- Views build the page only through `h()`. No `innerHTML`, inline handlers, `eval`, or `new Function` (#56). *(Check)*
- No remote hosts, and colours only from the role-named tokens (see [the browser app](#the-browser-app)). *(Check)*
- JS and CSS pass Biome's recommended rules with warnings as errors, and match its formatter. A `biome-ignore` comment carries a reason. Vendored files are never edited. *(Check)*

Why: the Replay export opens by double-click from a pit laptop with no network, and browsers refuse ES modules from `file://`. Log data includes names from robot code, so anything that turns strings into HTML is an injection waiting for the wrong match name.

`tests/test_quality.py` keeps the gates honest: a deliberately bad file for each check rule (in `tests/fixtures/quality/`) must fail it, and the suppression and workflow rules are checked there too.

## Tests

```sh
poetry run pytest                     # unit tests: fast, no data, no network
```

The default run excludes three markers. Each needs something extra:

| Marker | Needs | Run with |
|---|---|---|
| `corpus` | The golden log corpus (below) | `poetry run pytest -m "corpus and not perf and not browser"` |
| `browser` | The corpus plus Chromium (`poetry run playwright install chromium`, once) | `poetry run pytest -m "browser and not perf"` |
| `perf` | The corpus. Checks time and memory budgets | `poetry run pytest -m perf -s` |

`--strict-markers` is on, so a typo in a marker name is an error. Every test has a 300 s timeout. A hang dumps every thread's stack.

The unit suite includes `tests/test_docs.py`. It fails when a relative link in `deploy/README.md` or `docs/guide/` points at a missing file, when the README has any relative link (the README is also the PyPI page, so every link there must be a full `https://github.com/IgniteRobotics/flashpoint/blob/main/...` URL to a file that exists), when a `flashpoint` subcommand has no section in the [CLI reference](cli-reference.md), or when the contract table in [Live CAN health](live-can-health.md) doesn't match the code. If you add a subcommand, document it there.

### The golden corpus

About 180 MB of real robot logs (release `corpus-v1`), described with sizes and hashes in `tests/corpus/manifest.toml`:

```sh
poetry run python tools/fetch-corpus.py
```

It downloads with `gh release download` if the GitHub CLI is installed (run `gh auth login` first), otherwise over plain HTTPS, checks every hash, and stores the files in `~/.cache/flashpoint/corpus/corpus-v1/`. Set `FLASHPOINT_CORPUS_DIR` to put them elsewhere, or `--from DIR` to copy from a local folder. Never replace a corpus file in place; bump the version and publish a new release.

## CI

`.github/workflows/ci.yml` runs on every pull request and on pushes to `main` and `development`. A first `changes` job checks which files changed. When they're all docs (`docs/`, `openspec/`, `media/`, `.claude/`, and Markdown outside `src/` and `tests/`), only the `docs` job runs; otherwise everything does.

| Job | Runs | Blocks merge |
|---|---|---|
| `docs` | Docs-only changes: `tests/test_docs.py` (links, CLI reference) | Through `ci-ok` |
| `lint-test (py3.11)`, `lint-test (py3.12)` | pre-commit (ruff, Biome, hygiene hooks), mypy, unit tests, `systemd-analyze verify` on the service unit. py3.12 adds the `ml` extra and runs the unit tests with coverage and the floors | Through `ci-ok` |
| `windows (py3.12)` | pre-commit, mypy, unit tests on Windows | No, but fix it |
| `clean install (ubuntu, macos, windows)` | The built wheel in an empty venv: `--version`, and `doctor` with no configuration | Through `ci-ok` |
| `corpus` | The corpus tests, including the rclone restore drill | Through `ci-ok` |
| `browser (Chromium)` | The web app in Chromium, served and from `file://` | Through `ci-ok` |
| `perf (budgets, non-blocking)` | Time and memory budgets, on pushes to `main` and `development`. Allowed to fail; read it when it does | No |
| `codeql` (separate workflow) | Code scanning for Python and JavaScript, on PRs, pushes, and weekly. A high or critical alert blocks the merge through the ruleset | Yes, through the ruleset |
| `ci-ok` | Passes when `changes` passed, `docs` or `lint-test` ran, and none of `docs`, `lint-test`, `corpus`, `browser`, or `clean install` failed. The required check | Yes |

The corpus jobs fetch through `gh` using the workflow's `GH_TOKEN`, and cache the corpus and owlet between runs. Every workflow runs with a read-only token unless a job needs more, and actions are pinned to a commit; Dependabot opens one grouped update PR a month for Python packages and one for actions, against `development`.

## How changes are made

Work is planned with [OpenSpec](https://github.com/Fission-AI/OpenSpec), which keeps the plan for each change next to the code:

- `openspec/specs/<capability>/spec.md` is the current behavior: requirements, each with WHEN/THEN scenarios.
- `openspec/changes/<change>/` holds a change in progress:
  - `proposal.md`: why;
  - `specs/`: the requirements it adds or changes;
  - `design.md`: how;
  - `tasks.md`: the checklist.
- `openspec/changes/archive/` holds finished changes.

The flow, using the `/opsx:*` commands in Claude Code (`.claude/commands/`) or the `openspec` CLI:
1. **Propose** (`/opsx:propose`). Write the proposal, specs, design, and tasks, and validate them with `openspec validate <change> --strict`.
2. **Apply** (`/opsx:apply`). Work through `tasks.md`, tests first, ticking each task as it's done.
3. **Archive** (`/opsx:archive`). Merge the change's specs into `openspec/specs/` and move the change to the archive.

Tasks marked `[HUMAN]` need a person: a pit check, a field test. They stay open until someone does them.

### Branching

- **`development`** is the integration branch. Each change gets a `feature/<name>` branch cut from `development`, and merges back through a pull request with CI.
- **`main`** gets changes only through a pull request from `development`. Release tags (`v*`) are cut from `main`.
- Both branches are protected the same way: the `ci-ok` check must pass, a pull request needs one approving review, and only org admins and the `code-reviewers` team can merge. Nobody can force-push or delete them. Never push to either directly.
- The rewrite was built on a separate `rewrite` branch and merged into `main` on 2026-10-22. The pre-rewrite tree is kept at the `legacy-2025` tag.

## Where decisions and research live

| Path | What |
|---|---|
| [`docs/adr/`](../adr/README.md) | Architecture decision records. Add one when you make or change a decision |
| [`docs/rewrite/`](../rewrite/README.md) | The research behind the rewrite: the [target architecture](../rewrite/05-target-architecture.md), the [roadmap](../rewrite/06-roadmap.md), and the [pitfalls list](../rewrite/03-pitfalls-and-performance.md) (cited as `#N`) |
| [`docs/UX/`](../UX/) | The UI design canvas |
| [`notebooks/`](../../notebooks/README.md) | marimo notebooks for questions the app doesn't answer yet |
| [`openspec/`](../../openspec/) | Specs and changes (above) |

## Updating owlet

CTRE releases new owlet builds for new hoot format versions. The versions and checksums are pinned in `src/flashpoint/readers/owlet-manifest.toml`. `poetry run python tools/update-owlet-manifest.py` rewrites that file from CTRE's download index: the newest stable owlet for each format version, on every platform, with checksums computed by downloading each binary. It runs as soon as you start it (it takes no options), so expect several minutes of downloads, and review the diff before committing.
