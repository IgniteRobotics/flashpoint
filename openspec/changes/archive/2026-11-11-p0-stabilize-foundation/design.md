## Context

Main has no CI and no protection, and five people push to it directly (#60, #61). There is no Python package yet. Real logs exist on the team Shared Drive (2026: GADAL, GACMP) and in local downloads (2025). Three robot code repos exist. This change covers two of them: Robot-2026 (the comp robot) and Phoenix-2026 (Phoenix 6 v26.1.0). Robot-2025 is out of scope.

Facts found while planning (2026-10-04):
- `owlet --check-pro` reports the team's 2025 and 2026 hoots as **Pro-licensed**, so DeviceTemp can be exported. This retires the main temperature risk in 03 §6 and 06 §5.
- `owlet --compliancy` prints the hoot format version: 2026 logs are **19**, 2025 logs are **13**. owlet 26.1.0 reads the version of older logs but refuses to convert them ("unsupported version"). This gives the owlet registry its key (see ADR-0004).
- `GADAL_Q11`'s CANivore hoot on the Drive is **0 bytes**: a real-world empty log.
- The flashpoint repo is public.

## Goals / Non-Goals

**Goals:**
- `main` accepts changes only through a reviewed PR with green CI
- An installable, empty `flashpoint` package that passes lint, type check, and tests in CI
- D1–D11 recorded as ADRs. D1–D10 are **Proposed** for team review; D11 is **Accepted**
- A versioned golden corpus that CI can fetch and verify
- A CAN inventory logger running on the 2026 robots

**Non-Goals:**
- Any ingest or analysis code (P1)
- Robot-2025 changes

## Decisions

- **Corpus hosting: GitHub Release `corpus-v1` assets, plus `tests/corpus/manifest.toml` (SHA-256, size, provenance) and `tools/fetch-corpus.py` (stdlib only).** CI caches the files, keyed by the manifest's hash. The logs become public (preferences, git SHAs, match data), and the team accepted that. Versioning: a new corpus means a new release tag and a manifest bump. Assets are never replaced in place.
  - *Alternatives:* Git LFS (bandwidth quota, also public), the private Drive (CI can't reach it).
- **Corpus contents** (picked to cover the edge cases each phase needs):

  | id | Files | Covers |
  |---|---|---|
  | `2026-gacmp-q7` | wpilog + CANivore hoot + rio hoot | Full qual session, compliancy 19 |
  | `2026-gacmp-e10` | wpilog + 2 CANivore + 2 rio hoots | Playoff (`E`), hoot sessions split across a restart |
  | `2026-gacmp-p2` | wpilog | Practice match (`P`) |
  | `2026-gadal-q11-empty` | CANivore hoot, 0 bytes | Real empty file (quarantine path) |
  | `2026-gacmp-e10-truncated` | rio hoot cut to 60% | Corrupt tail (derived from `2026-gacmp-e10`) |
  | `2025-gadal-q30` | wpilog | 2025 season config |
  | `2025-gacmp-q19` | CANivore hoot + rio hoot | Compliancy 13 (old owlet) |
  | `2025-nofms` | wpilog with no event or match suffix | Non-FMS log |

- **CI:** GitHub Actions on ubuntu, Python 3.11 and 3.12. Runs ruff (lint and format check), mypy `--strict` on `src/`, and pytest `--strict-markers`. Corpus tests use the `corpus` marker and run in a separate job, so lint failures don't burn the download.
- **Branch protection:** require a PR, 1 approving review, and the `ci` status checks. Admins can bypass in an emergency (`enforce_admins: false`). It is turned on **after** the P0 PR's first CI run, so the check names exist.
- **Tags:** `legacy-2025` points to `70be731`, the last pre-rewrite main and the commit the docs/rewrite locators were taken from. `archive/development` and `archive/power-tracking` are added as tags; the branches stay until their salvage is ported.
- **CAN inventory logger (robot side):**
  - A self-contained `CanInventoryLogger` class.
  - On a daemon thread it polls `http://localhost:1250/?action=getdevices` with backoff until it gets a non-empty device list (at most 60 s).
  - It normalizes the result to the 05 §4a schema v1 and writes it to the wpilog string entry `/Flashpoint/CANInventory`.
  - It re-checks on each entry to Disabled and logs again only if the payload changed. It never queries while enabled, because the query triggers a bus enumeration (99 doc). This was originally planned for the enable edge and was corrected during implementation.
  - It never throws into robot code. On failure it logs `{"schema":1,"error":...}`.
  - The normalizer accepts the documented Phoenix 5 field names (`SerialNo`, `CANbus`, `ID`, `Model`, `CurrentVers`, `HardwareRev`, `BootloaderRev`, `ManDate`) and fails soft on missing ones. Field names must be checked on a live Phoenix 6 robot before this is trusted (a human task).
- **ADR format:** a light MADR style (Status, Context, Decision, Consequences, Alternatives), stored at `docs/adr/NNNN-kebab-title.md` with an index in `docs/adr/README.md`.

## Risks / Trade-offs

- **Publishing logs exposes robot preferences and code SHAs.** The team accepted this. Only match logs go into the corpus, never anything with secrets (there are no credentials in wpilog).
- **The CI corpus download is about 190 MB.** Mitigated by caching keyed on the manifest's hash; corpus tests run in their own job.
- **The diagnostics server field names may differ on Phoenix 6.** The normalizer fails soft. The human verification task blocks archiving P0.
- **Requiring 1 review slows a small team.** Admins can bypass, and the setting can be revisited after P1.
