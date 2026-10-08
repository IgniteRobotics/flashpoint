## Context

Legacy code lives at the repo root and has no tests (#60). New code lives in `src/flashpoint/`. The `legacy-2025` tag (created in P0) preserves the pre-rewrite tree, so deleting from main is reversible.

## Goals / Non-Goals

**Goals:**
- Exactly one place that tracks what legacy code remains and when it goes
- No window where the team has neither the legacy tool nor its replacement for a function that works today

**Non-Goals:**
- Git history rewriting (see proposal)

## Decisions

- **Stage by replacement, not all at once.** Main has no working entry points, so deleting everything immediately would be safe for main. But the team salvages logic and data (datamaps, motor maps, regexes) from these files during P1–P4. Deleting each piece when its replacement is archived keeps that material in place while it is being ported.
  - *Alternative:* delete everything in P0 and salvage from the tag. Rejected: it adds friction for students porting code.
- **The gate is the archive of the replacing change.** The `archive` operation guidance in `config.yaml` requires checking this change's matching task group. That stops removals from being forgotten.
- **This change is archived last,** after the P4 gate and the README rewrite.
- **Deletions are their own commits** (`chore(legacy): remove <component>`), so `git log -- <path>` points at the reason.

## Risks / Trade-offs

- [The `development` branch Docker stack is in use on a team laptop] → Stage 2 only touches main. The archived branch tags keep that stack runnable from its tag until P3 ships.
- [Locators in docs/rewrite break] → Task 1.4 re-points them to `legacy-2025` before any deletion lands.
