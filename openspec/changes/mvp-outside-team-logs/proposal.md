## Why

Every log Flashpoint has seen is Ignite's (Pro, CANivore, CTRE swerve). Outside logs take untested paths:

- A non-Pro hoot has no temperature, and History reads NO DATA without saying why.
- An AdvantageKit session comes out "non-match" even when its hoots are named `GACMP_Q7_…`.
- On a 250 Hz CANivore, timing jitter becomes HIGH and MED "CAN frames missed" findings (22% of our own lake's anomalies).
- Brownouts found on the median-device fallback show as HIGH.

Roadmap #68's M2 (2026-12-12) needs each fixed or labelled.

MVP work under `docs/rewrite/06-roadmap.md` "MVP public release", after P2, P5 and the battery-proxy and can-health follow-ups. It implements 05 §6 D5 and D10 and the identity order in `openspec/config.yaml`. Pitfalls fixed: none numbered in `03`. It acts on the 03 §6 note that free hoots carry no `DeviceTemp`. Issues: #75 (folds in #10), #76, #77, #78.

## What Changes

- **Non-Pro hoots (#75, #10):**
  - Temperature reads "not logged (needs Phoenix Pro)" per bus in Replay, History and unit health.
  - Devices whose temperature isn't tracked (CANcoder, Pigeon 2) read N/A.
  - Missing temperature never raises a finding.
  - `doctor` reports Pro coverage (informational, always ok).
- **AdvantageKit identity (#76):**
  - `/DriverStation/` match fields count as in-log FMS data.
  - `akit_…` filenames parse.
  - If the wpilog yields no identity, derive uses the hoots' filename label (source `hoot-filename`).
  - An AdvantageKit epoch-time entry anchors a wpilog with no `systemTime`.
- **Anomalies (#77):**
  - The view and its guide section are labelled beta.
  - A brownout on the fallback proxy is at most MED.
  - CAN-gap spread floors scale with frame rate above 125 Hz; rio rates keep today's floors.
  - The missing-extra hint gives pip, pipx and uv commands.
  - The guide explains `battery_proxy` and `[[siblings]]`.
- **Outside logs and beta (#78):**
  - [HUMAN] Logs from 2 or more outside teams by 2026-11-28.
  - [HUMAN] An outside-mentor beta by 2026-12-14.
  - Opt-in private tests run on those logs.
  - Findings are triaged; limits go into the support matrix (#84).
  - If no logs arrive, ship on synthetic fixtures and mark the matrix "not tested on real logs".

## Capabilities

### New Capabilities
- (none)

### Modified Capabilities
- `match-identity`: `/DriverStation` fields, `akit_` filenames, hoot-label fallback.
- `log-metadata`: those fields and filenames; the AdvantageKit anchor.
- `hoot-conversion`: non-Pro hoots convert and are never quarantined for missing temperature.
- `lifetime-trends`: health gains N/A and NEEDS PRO.
- `match-reports`: per-bus "needs Phoenix Pro" reasons.
- `anomaly-detection`: brownout cap, rate-relative CAN-gap floors, no findings from missing temperature, missing-extra hint.
- `anomaly-review`: beta label.
- `can-health`: nominal frame rate per device.
- `team-setup`: Pro coverage check.
- `session-grouping`: AdvantageKit enable entry for enable-edge alignment, only if a real AdvantageKit log arrives (task 2.5).

## Non-goals

- Full AdvantageKit alignment and framing, beyond the enable-edge alias.
- The hoot-only clock path (#8). `mvp-install-and-first-run` owns its warning.
- REV, Phoenix 5, Systemcore.
- Non-Pro temperature from fault bits or robot-side logging.
- A second tuning pass (P5 9.3). Robot-side logging (#6, #7).
- Outside logs in the public corpus.

## Impact

- **Code:**
  - `meta/extract.py`, `semantics/match_identity.py`, `semantics/derive.py`, `semantics/can_health.py`;
  - `anomaly/` (settings, detect, siblings, spec rules, model);
  - `views/queries.py`, `report/build.py`, `setup/checks.py`;
  - `semantics/alignment.py` (if task 2.5 adopts the alias); `history.js`, `anomalies.js`, `defaults/anomaly.toml`.
- **Versions:** `DERIVE_VERSION` +1 and `REPORT_VERSION` +1, so one re-derive and a Replay rebuild.
- **Docs:** content for `mvp-documentation`'s Anomalies, robot-config, non-Pro and AdvantageKit pages and matrix rows; ADR-0005 and ADR-0010 amendments; the roadmap's false "doctor flags non-Pro" row corrected.
- **Tests:** synthetic no-temperature, mixed-Pro, AdvantageKit and 250 Hz CAN fixtures; an opt-in `outside` marker.
