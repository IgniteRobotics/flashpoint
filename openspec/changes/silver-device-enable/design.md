## Context

See proposal.md (Why). Silver is written in one DuckDB `COPY` per hoot, straight from bronze to Parquet (D1, D2). The device value is a single `COALESCE` over the numeric and boolean columns, and rows with a NULL value are dropped. Bronze keeps every hoot signal's type, and owlet exports Phoenix 6 `DeviceEnable` as `string`. On the current lake, the only strings seen are `Enabled` (13.0 M rows) and `Disabled` (38.0 M rows), with no numeric or boolean values. The existing unit test feeds `DeviceEnable` as a boolean, which is why it passes while real logs lose the metric.

Re-derive is driven by a per-session fingerprint that includes `DERIVE_VERSION`. Bumping it re-derives every session on the next `flashpoint derive`, from bronze only (no raw re-read).

Q7 measurement (real lake, both buses combined): about 99 % of `auto` and `teleop` enable samples are `Enabled`, and under 0.01 % of `gap` and `post` samples are. The rio bus alone reads 74 % enabled in `auto` (270 samples).

## Goals / Non-Goals

**Goals:**
- Decode the enable string in the existing silver query, with no extra pass over bronze.
- Leave every other metric's value bit-for-bit unchanged.
- Upgrade existing lakes through the normal derive, with no manual step.

**Non-Goals:**
- A general string-to-number mapping table for hoot signals. One signal needs it today. Add a table when a second one does.
- Any change to framing, usage, gold features, or views (see proposal Non-goals).

## Decisions

**1. Decode in the value expression and limit it to `DeviceEnable`.**
The value becomes `COALESCE(v_f64, CAST(v_bool AS DOUBLE), CAST(v_i64 AS DOUBLE), <enable decode>)`, where the decode is `CASE WHEN sig = 'DeviceEnable' THEN CASE v_str WHEN 'Enabled' THEN 1.0 WHEN 'Disabled' THEN 0.0 END END`. Appending it last keeps numeric precedence and leaves the boolean path (the existing test) working. Gating on `sig` stops any future string signal added to `METRICS` from being silently decoded to NULL-or-number.
*Alternatives:* (a) Decode at ingest into `v_bool` in bronze. Rejected: bronze is the raw-faithful layer, and fixing it there needs a raw rebuild of every lake, not a re-derive. (b) A registered `string_map` table joined on `(sig, v_str)`. Rejected for now: a join for one signal, and it costs more than the inline `CASE` on 51 M rows.

**2. Exact, case-sensitive match. Anything else is NULL and dropped.**
These are the only two values Phoenix 6 emits and the only two in the lake. Silently mapping a surprise value (for example to 0) would invent a "disabled" state. Dropping matches how silver already treats unusable values. No warning is logged: it would fire per sample, and the corpus test catches a vocabulary change.

**3. Bump `DERIVE_VERSION` 5 → 6 (silver content changed).**
This follows the existing convention, with a comment on the constant naming the change. `PIPELINE_VERSION` is not bumped, because bronze does not change.

**4. Tests.** Unit: a string-typed `DeviceEnable` fixture (`Enabled`, `Disabled`, and an unknown string), next to the existing boolean case. The bronze fixture helper must accept `v_str`. Corpus: a Q7 assertion of the spec's thresholds (at least 95 % in `auto`/`teleop`, at most 1 % in `pre`/`gap`/`post`, values only 0 or 1, every TalonFX slot present). `powered_s` and `enabled_s` must match their pre-change values, so the existing `test_usage_corpus` expectations must still pass unchanged.

## Risks / Trade-offs

- [Silver grows about 8 % in rows (about 690 k on Q7)] → Values are long constant runs, so zstd dictionary and RLE encoding keep the bytes small. Measure silver size on Q7 before and after, and record it in the PR.
- [Q7 ingest budget (30 s / 1 GB) is already overrun in CI] → The decode is a per-row `CASE` inside the existing scan, with no new pass. Gold's per-motor load picks up the extra rows. Run `test_ingest_perf` before and after on the reference laptop and record both. Budget for this change: at most +1 s wall and +50 MB peak RSS on Q7. If gold's load exceeds that, filter it to the metrics it uses. That is a one-line `WHERE`, and the only allowed scope growth.
- [Usage hold windows fall back to all metrics' timestamps for slots with no `supply_voltage`] → For a slot with `DeviceEnable` but no `SupplyVoltage`, the extra timestamps could shift `powered_s`. TalonFX logs both. The unchanged-usage corpus check covers this.
- [Full re-derive on upgrade takes time on large lakes] → This is a one-off, the same as earlier version bumps (v2–v5). It runs in the normal derive or the watch cycle.

## Migration Plan

Ship with the version bump. The next `flashpoint derive` (manual, the watch cycle, or `serve`'s derive button) rebuilds every session. Rollback is reverting the commit, after which the next derive goes back to v5 output.

## Open Questions

- Why the rio bus reads only 74 % enabled in Q7 `auto`: enable latency at the auto start edge, or devices that really were disabled. This doesn't affect this change, because the threshold is per session and both buses together sit at about 99 %. It is the first thing a follow-up that uses per-device enabled time should look at.
  - Partly answered on the main lake (2026-11-24): it is not latency. `indexer-accelerator` and `indexer-main` report disabled for all of Q7 `auto` (0.0), and `intake-roller` for about a third of it (0.633). Every other rio-bus slot reads 1.0. Why those devices report disabled while the robot is enabled is still open, for example whether Phoenix reports a device with no control request as disabled.
