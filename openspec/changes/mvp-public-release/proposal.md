## Why

No team but 6829 has used Flashpoint. PyPI serves 0.1.0, which installs duckdb 2 and breaks at derive. "Latest release" is the log corpus; issues go to robot-code templates. Roadmap #68 sets the bar: announce on Chief Delphi before the 2027 kickoff, with a non-6829 team installing from PyPI and getting useful, honest results from stock `.wpilog` and `.hoot` logs, unaided.

It follows the Team setup and Team branding follow-ups in `docs/rewrite/06-roadmap.md`, after P3–P5, and adds its "MVP public release" section. D9 is amended only by `mvp-install-and-first-run`. Pitfalls fixed: none from `03`.

## What Changes

- **Four changes, one per track:**
  - `mvp-public-release`: M0 hygiene and 0.2.0, the release process, support files, the M3 release candidate, the M4 announcement, and the milestone gates.
  - `mvp-install-and-first-run` (M1): #70, #71, #72 (with #37, #38, #44), #73, #74, and the #8 warning.
  - `mvp-outside-team-logs` (M2): #75 (with #10), #76, #77, #78.
  - `mvp-documentation` (M1–M4): #82, #83, #84; screenshots and animations captured late from the release UI.
- **Milestones:** M0 stop the bleeding 2026-11-21; M1 installs anywhere, first run makes sense 12-05; M2 honest results on other teams' logs 12-12; M3 Windows, beta, 0.3.0 12-19; M4 announce 2027-01-04.
- **Release process (#80):** a tag creates a GitHub Release with its changelog section beside the PyPI publish. Corpus releases are never "Latest". Prep PRs never use `Closes`. The developer guide gets a release checklist.
- **Support (#79):** repo issue forms replace the org's robot-code templates; Discussions Q&A, CONTRIBUTING, SECURITY with private reporting, and a code of conduct.
- **Announcement (#81):** a reviewed post, an FAQ, a thread owner, a kickoff-week 0.3.1.
- **Hygiene:** close shipped epics, untick false `[HUMAN]` ticks, label Live CAN experimental, delete the `p6-failure-prediction` stub, archive `p3-log-acquisition`.

## Cutline

- **MVP-blocking:** #61, #24 (gates 4.3 and 9.4 only), #17 (live-rio check by 12-07 or a "beta" label), #70, #71, #72, #37, #38, #73, #74, #8 (warning only), #75, #76 (hoot-label fallback), #77, #78, #79, #80, #81, #82 (with the hosted demo), #83, #84, plus project-documentation 6.4 (user-guide half).
- **Nice-to-have:** #44 and #10 standalone (folded into #72 and #75), #66, #11 (PR #32), #12, #43, #39, the doctor problem list from #19, P3 11.1–11.4, USB on Linux/Windows, the #8 hoot-clock path, full AdvantageKit alignment, a compile CI job for the samples.
- **Post-MVP:** #5, #6, #7, #9's children, #18, #19 (full view), the second anomaly tuning pass (P5 9.3), #21, #23, #45, #56, #57, #60, #67, live-can-health 7.3, team-branding 5.3.
- **Close now:** #3, #4, #9 (split), #16, #20 (after #77), #22, #25, #58.

Fallback, label before fixing: auto-pull "beta", Live CAN "experimental", Anomalies "beta", untested matrix rows "not tested", hoot-only sessions warned.

## Capabilities

### New Capabilities
- `public-release`: what a tag publishes (PyPI, GitHub Release with its changelog section), corpus releases never latest, the moving-link check, the checklist, and the support surface (issue forms, Discussions Q&A, CONTRIBUTING, SECURITY, code of conduct).

### Modified Capabilities
- `live-can-health`: the view and guide say Live CAN is experimental (7.3 failed).

## Non-goals

- REV, Phoenix 5 and Systemcore support.
- Full AdvantageKit alignment. Only the identity fallback.
- Robot-side code changes (#6, #7, #9). Samples and docs only.
- Any docs host other than GitHub Pages, and any paid hosting.
- A standalone installer, accounts or telemetry.
- A required Windows unit job.

## Impact

- `.github/`: a GitHub Release job in `release.yml`; issue forms, CONTRIBUTING, SECURITY, code of conduct.
- Repo settings: Discussions, private vulnerability reporting, the latest-release marker.
- Docs: the release checklist (developer guide); the MVP section (roadmap).
- PyPI: 0.2.0 and 0.3.0 published, 0.1.0 yanked.
