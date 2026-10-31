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
| `src/flashpoint/views/`, `src/flashpoint/web/` | History and Anomalies queries, the HTTP server, the browser app, the AdvantageScope launcher |
| `src/flashpoint/acquire/` | Robot and USB pulls, the watch loop, backup and restore |
| `src/flashpoint/live/` | Live CAN: the NetworkTables contract, the detector, the robot connection, and the fake robot (`python -m flashpoint.live.fake_robot`) |
| `config/` | Team-edited data: robots, seasons, thresholds, failure labels |
| `tools/` | `fetch-corpus.py`, `update-owlet-manifest.py`, `migrate-legacy-config.py` |
| `examples/robot-java/` | Suggested robot-code samples (not built or compiled in CI) |
| `tests/` | The test suite, mirroring `src/flashpoint/` |

Conventions:
- Type hints everywhere (mypy `--strict`).
- `snake_case` functions, `PascalCase` classes, `SCREAMING_SNAKE_CASE` constants, `kebab-case` file and folder names outside the Python package.
- Imports go external, then internal, then local.

The domain rules (long format at rest, as-of joins with explicit tolerance, never trust filenames over FMS data, offline first, never delete robot logs) are in [`openspec/config.yaml`](../../openspec/config.yaml).

## Lint and type checks

```sh
poetry run ruff check .
poetry run ruff format --check .      # drop --check to reformat
poetry run mypy src tests tools
```

CI runs all three; a failure blocks the merge.

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

The unit suite includes `tests/test_docs.py`, which fails when a relative link in the README, `deploy/README.md`, or `docs/guide/` points at a missing file, or when a `flashpoint` subcommand has no section in the [CLI reference](cli-reference.md). If you add a subcommand, document it there.

### The golden corpus

About 180 MB of real robot logs (release `corpus-v1`), described with sizes and hashes in `tests/corpus/manifest.toml`:

```sh
poetry run python tools/fetch-corpus.py
```

It downloads with `gh release download` if the GitHub CLI is installed (run `gh auth login` first), otherwise over plain HTTPS, checks every hash, and stores the files in `~/.cache/flashpoint/corpus/corpus-v1/`. Set `FLASHPOINT_CORPUS_DIR` to put them elsewhere, or `--from DIR` to copy from a local folder. Never replace a corpus file in place; bump the version and publish a new release.

## CI

`.github/workflows/ci.yml` runs on every pull request and on pushes to `main`. A first `changes` job checks which files changed. When they're all docs (`docs/`, `openspec/`, `media/`, `.claude/`, and Markdown outside `src/` and `tests/`), only the `docs` job runs; otherwise everything does.

| Job | Runs | Blocks merge |
|---|---|---|
| `docs` | Docs-only changes: `tests/test_docs.py` (links, CLI reference) | Through `ci-ok` |
| `lint-test (py3.11)`, `lint-test (py3.12)` | ruff, ruff format, mypy, unit tests, `systemd-analyze verify` on the service unit. py3.12 adds the `ml` extra | Through `ci-ok` |
| `windows (py3.12)` | ruff, mypy, unit tests on Windows | No, but fix it |
| `clean install (ubuntu, macos, windows)` | The built wheel in an empty venv: `--version`, and `doctor` with no configuration | No, but fix it |
| `corpus` | The corpus tests, including the rclone restore drill | Through `ci-ok` |
| `browser (Chromium)` | The web app in Chromium, served and from `file://` | No, but fix it |
| `perf (budgets, non-blocking)` | Time and memory budgets, on pushes to `main` only. Allowed to fail; read it when it does | No |
| `ci-ok` | Passes when `changes` passed, `docs` or `lint-test` ran, and nothing it depends on failed. The only required check | Yes |

The corpus jobs fetch through `gh` using the workflow's `GH_TOKEN`, and cache the corpus and owlet between runs.

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

- **`main`** is the only long-lived branch. Each change gets a `feature/<name>` branch cut from `main`, and merges back through a pull request with CI. Never push to `main` directly. It's protected: the `ci-ok` check must pass, and a pull request needs one approving review.
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
