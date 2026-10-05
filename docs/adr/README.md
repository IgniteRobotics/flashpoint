# Architecture Decision Records

These record the rewrite's key decisions (D1–D11 in [05-target-architecture.md §6](../rewrite/05-target-architecture.md#6-key-decisions)). To change a decision, write a new ADR that supersedes the old one. Never edit an Accepted ADR's decision in place.

**Status:** `Proposed` (awaiting team review) → `Accepted` → (`Superseded by NNNN` | `Deprecated`)

**To accept:** open a PR that changes `Status: Proposed` to `Status: Accepted (YYYY-MM-DD)`. The PR review is the decision record.

| # | Decision | Status |
|---|---|---|
| [0001](0001-storage-parquet-lake-duckdb.md) | Storage: Parquet lake + DuckDB | Proposed |
| [0002](0002-transforms-polars.md) | Transform engine: Polars | Proposed |
| [0003](0003-wpilog-reader-robotpy-wpiutil.md) | wpilog reader: robotpy-wpiutil | Proposed |
| [0004](0004-owlet-registry-by-compliancy.md) | owlet: fetched registry chosen by hoot compliancy | Proposed |
| [0005](0005-temperature-source.md) | Temperature source: hoot DeviceTemp (logs are Pro) | Proposed |
| [0006](0006-per-match-ui-static-site.md) | Per-match UI: static site | Proposed |
| [0007](0007-lifetime-ui-marimo.md) | Lifetime UI: marimo | Proposed |
| [0008](0008-deep-dive-link-advantagescope.md) | Deep dive: link to AdvantageScope | Proposed |
| [0009](0009-deploy-pipx-single-container.md) | Deploy: pipx + per-user services (container deferred) | Proposed |
| [0010](0010-anomaly-v1-rules-baselines.md) | Anomaly v1: rules + baselines + residuals | Proposed |
| [0011](0011-device-identity-ctre-serials.md) | Device identity: CTRE serial numbers | **Accepted** |
| [0012](0012-clock-alignment-shared-payload.md) | Hoot-to-wpilog clock alignment by shared payload | Proposed |

## Template

```markdown
# NNNN. Title

- Status: Proposed
- Date: YYYY-MM-DD
- Deciders: …
- Refs: docs/rewrite/…, pitfalls #N

## Context
## Decision
## Consequences
## Alternatives considered
```
