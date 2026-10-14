# Developer guide

For people who change Flashpoint's code. Read [How it works](how-it-works.md) first for the pipeline, the lake, and the identity model. To run Flashpoint without changing it, see the [user guide](user-guide.md).

## Build and run from source

You need:
- Python 3.11, 3.12, or 3.13 (`requires-python = ">=3.11,<3.14"`). [pyenv](https://github.com/pyenv/pyenv) is the easy way to get one;
- [Poetry](https://python-poetry.org/) 2;
- the [GitHub CLI](https://cli.github.com/), because the repository and the golden corpus are private.

```sh
gh auth login
gh repo clone IgniteRobotics/flashpoint && cd flashpoint
git switch rewrite
poetry install
poetry run flashpoint doctor
```

`poetry install` installs the package in editable mode, along with the dev tools (pytest, ruff, mypy, Playwright, robotpy-wpiutil). In a checkout, Flashpoint reads the robot configuration from the repo's `config/` folder automatically; `FLASHPOINT_CONFIG` overrides that. Optional extras:

```sh
poetry install --extras ml        # scikit-learn, for the optional MODEL anomaly detector
```

Use a scratch lake while developing, so you don't mix test data into your real one:

```sh
export FLASHPOINT_LAKE=/tmp/fp-lake
poetry run flashpoint ingest ~/.cache/flashpoint/corpus/corpus-v1/
poetry run flashpoint serve
```

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
| `config/` | Team-edited data: robots, seasons, thresholds, failure labels |
| `tools/` | `fetch-corpus.py`, `update-owlet-manifest.py`, `migrate-legacy-config.py` |
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

It downloads through `gh release download` (the release is private, so run `gh auth login` first), checks every hash, and stores the files in `~/.cache/flashpoint/corpus/corpus-v1/`. Set `FLASHPOINT_CORPUS_DIR` to put them elsewhere, or `--from DIR` to copy from a local folder. Never replace a corpus file in place; bump the version and publish a new release.

## CI

`.github/workflows/ci.yml` runs on every pull request and on pushes to `main` and `rewrite`:

| Job | Runs | Blocks merge |
|---|---|---|
| `lint-test (py3.11)`, `lint-test (py3.12)` | ruff, ruff format, mypy, unit tests, `systemd-analyze verify` on the service unit. py3.12 adds the `ml` extra | Yes |
| `windows (py3.12)` | ruff, mypy, unit tests on Windows | No, but fix it |
| `corpus` | The corpus tests, including the rclone restore drill | Yes |
| `browser (Chromium)` | The web app in Chromium, served and from `file://` | No, but fix it |
| `perf (budgets, non-blocking)` | Time and memory budgets. Allowed to fail; read it when it does | No |

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

- **`rewrite`** is the integration branch for the rewrite. Each change gets a `feature/<name>` branch cut from `rewrite`, and merges back through a pull request with CI. Never push to `rewrite` directly. It's protected: the required checks must pass, and a pull request needs one approving review.
- **`main`** still has the legacy code. `rewrite` merges into it once, when the rewrite is ready. The pre-rewrite tree is kept at the `legacy-2025` tag.
- Merge `main` into `rewrite` to pick up hotfixes; never rebase `rewrite`.

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
