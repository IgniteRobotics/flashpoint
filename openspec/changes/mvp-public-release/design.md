## Context

See proposal.md for the scope and cutline, and `specs/public-release/spec.md` for the release and support behaviour. The current state that shapes the plan (checked 2026-11-14):

- **PyPI** has only 0.1.0 (`requires-python >=3.11,<3.14`, duckdb and polars uncapped). **TestPyPI** has `0.2.0.dev1` from the team-setup dry run (2026-10-30), so that version can't be uploaded again.
- **0.2.0** is in flight: PR #69 (`development` → `main`, no closing keyword) is open. `CHANGELOG.md` has a `0.2.0 - 2026-11-13` section. Dependabot #64 and #65 landed after it and change only the lock file and dev tools; the runtime caps are unchanged.
- **`release.yml`** builds on a `v*` tag and publishes to PyPI through the `pypi` environment by trusted publishing; a manual run publishes to TestPyPI. It creates no GitHub Release, so the repo's latest release is `corpus-v1`.
- **Repo:** public; default branch `development`; Discussions off; private vulnerability reporting off; community profile 50% (no CONTRIBUTING, code of conduct or issue templates). New issues use the org's `bug`, `feature` and `hotfix` forms, which are written for robot code. PRs from non-collaborators are closed by the org's daily sweep (PR #32, a first-time contributor's fix for #11, was closed unmerged on 2026-10-29 and asked for feedback).
- **Merging:** the `dev-main-pr-gate` ruleset needs one approval and an up-to-date branch on `main` and `development`. Josh is the only regular approver; he has said he will admin-merge.
- **Docs links** in `pyproject.toml` and `README.md` point at `blob/main`.
- **OpenSpec:** `p3-log-acquisition` is open at 45/49 (only `[HUMAN]` 11.x left). `p6-failure-prediction` is a proposal-only stub from the OpenSpec init. Issue #13 has false ticks from 2026-11-11 (P0 7.2, P3 11.2, P5 9.3).

## Goals / Non-Goals

**Goals:**
- One plan with dates that the three sibling changes hang off, and gates that say when each milestone is done.
- A release process where the tag does all the publishing and nobody has to remember the GitHub Release or the latest marker.
- An outside team that arrives from Chief Delphi finds forms, a Q&A space and clear expectations, not robot-code templates.

**Non-Goals:**
- Implementing anything owned by the sibling changes. This change only checks their gates.
- Automating the announcement or the yank. Both stay with a person.
- A required Windows unit job, or making the smoke part of `ci-ok` (#71 keeps it outside, as the release gate).

## Decisions

### D-a. Four changes, one per track, each with its own issues
- `mvp-public-release` (umbrella): M0 hygiene, 0.2.0, #79, #80, the M3 walkthroughs and 0.3.0, #81, the gates and 0.3.1.
- `mvp-install-and-first-run` (M1): #70, #71, #72 (with #37, #38, #44), #73, #74, #8 messaging floor.
- `mvp-outside-team-logs` (M2): #75 (with #10), #76, #77, #78.
- `mvp-documentation` (M1–M4): #82, #83, #84.
- *Why:* AGENTS.md wants an issue, a spec, a branch and an archive per change. About 20 blocking items as separate changes is too much ceremony for volunteers; one per milestone track keeps specs cohesive and lets one mentor own each.
- *Alternative:* one change for everything. Rejected: a 60-task change can't be reviewed, and its spec deltas would cross six capabilities.

### D-b. Milestones and exit criteria

| Milestone | Date | Exit criteria |
|---|---|---|
| **M0** Stop the bleeding | Sat 2026-11-21 | 0.2.0 on PyPI, 0.1.0 yanked, GitHub Release `v0.2.0` is latest. `pipx install --python python3.12 flashpoint` then `init`, ingest one corpus match and `report` works on macOS and Linux. #13 matches reality. Shipped epics closed. Outreach sent (Josh). |
| **M1** Installs anywhere, first run makes sense | Sat 12-05 | `mvp-install-and-first-run` merged. Smoke green on 3 OSes. A fresh Mac with only Python 3.14 or 3.15 installs with the one documented command. macOS and Linux walkthrough (9.4 + 6.4) done with stock logs, findings filed as checkable items. #80 automation merged. `mvp-documentation`'s M1 part merged: the site live at `/dev/` with its page moves (by 11-28), Install pages with the uv command, Quickstart, versioned deploy, README pin step and metadata URL. |
| **M2** Honest results on other teams' logs | Sat 12-12 | `mvp-outside-team-logs` merged. Two outside log sets give correct match identity and populated views, or the matrix says "not tested on real logs". No HIGH brownout or CAN-gap findings from the fallback proxy or jitter. Robot-side guide reviewed by a mentor. `mvp-documentation`'s support matrix, known limits, troubleshooting and how-tos merged, with outside-log rows filled from the 11-28 outcome. |
| **M3** Windows, beta, release candidate | Sat 12-19 | 0.3.0 on PyPI. Smoke plus a human pass on macOS, Linux and Windows (Windows may be rough but works end to end). Outside mentor beta done (12-14) and triaged. #79 in place. Docs media captured from the release UI and the matrix's Windows and outside-log rows final. Every MVP-blocking issue closed or cut with its label. |
| **M4** Announce | Mon 2027-01-04 | Release criteria (D-e) all true; post live; thread owner watching; 0.3.1 scheduled. Kickoff is Sat 01-09. |

- **Two dated checkpoints inside the plan:** 11-28 (outside logs in hand, else synthetic fixtures) and 12-07 (P3 11.1 on a real rio, else robot pull stays "beta"; the robot session is booked before then).
- *Why 01-04:* after Christmas week (low Chief Delphi traffic) and five days before kickoff. Josh confirmed it.

### D-c. Critical path
1. **Outside logs** (Josh is asking; hard date 11-28). AK identity, non-Pro handling, matrix rows, the anomaly sanity check and the beta all wait on them.
2. **Josh as the only merger.** Strict up-to-date means every merge forces the next PR to rebase and re-run CI (about 20–40 min). Over Thanksgiving (11-26) and winter break this is the slowest link. Mitigation: small PRs; Josh merges in two batches a week (D-g); a second mentor in `code-reviewers` approves when available.
3. **The robot session** for P3 11.1, booked before 12-07.
4. **The outside mentor beta**, done by 12-14. It installs `0.3.0rc1` from PyPI (D-f).
5. **A Windows machine and a person** for the M3 walkthrough. Nobody has done a Windows install end to end yet.

### D-d. Capacity and the label-before-fix fallback
- About 22 blocking items (mostly S) plus about 7 M items is 25–35 person-days, plus 3–4 human sessions, in roughly 4 effective weeks (Thanksgiving, then winter break from about 12-20). It fits only if mentors take the M items and students take copy, docs and the S fixes.
- **Fallback, decided now:** when an item misses its milestone, it ships labelled, not fixed:
  - robot auto-pull and service mode: "beta, tested on one robot" (CLI help and `auto-import.md`);
  - Live CAN: "experimental, needs robot code, known false dropouts on fast buses";
  - Anomalies: "beta" (always, per #77);
  - matrix rows nobody could test: "not tested";
  - hoot-only sessions: the import/`doctor` warning, not the hoot-clock fix (that ships in 0.3.x and the announcement says so).
- **Cut order** if more has to go: #39, #12, #43, #66, the hosted demo's extra matches (keep one), the sample compile check, the #19 problem list, P3 11.3/11.4.
- The fallback keeps the bar "nobody gets wrong results or gives up". It never ships a wrong number unlabelled.

### D-e. Release criteria (all true before the post)
1. 0.3.0 is on PyPI and is the latest GitHub Release; 0.1.0 is yanked.
2. The cross-OS smoke passed on the `v0.3.0` tag on all three OSes.
3. Human passes done: macOS and Linux (M1), Windows (M3), team-setup 4.3 on 3 clean machines, the outside mentor beta. Every blocking finding closed or labelled.
4. Outside logs: at least 2 teams ingested, or the matrix says "not tested on real logs" for those rows.
5. Every MVP-blocking issue in the cutline closed or cut with its D-d label.
6. The docs site for 0.3 is live with Why Flashpoint, quickstart, how-tos, troubleshooting, known limits, screenshots and animations from the release UI, and the hosted demo.
7. Support: issue forms, Discussions Q&A, CONTRIBUTING, SECURITY with private reporting, code of conduct.
8. The post is mentor-reviewed, a thread owner is named, and 0.3.1 has an owner and a date.

### D-f. Release process
- **The tag does the publishing** (spec "A tag publishes the package and a GitHub Release"). `release.yml` gains a step in `build` that extracts the version's changelog section (failing before any publish if it's missing), and a `github-release` job after `pypi` that creates the release from that text with `contents: write` on that job only. Final versions get `--latest`; pre-release versions get `--prerelease`.
  - *Alternative:* a release-drafter app. Rejected: another third-party action to pin, and the changelog is already hand-written per PR.
- **Publish waits for the smoke.** The `pypi` job `needs` the #71 smoke run for the tag on all three OSes. That gate is specified once, in `mvp-install-and-first-run`'s ADDED ci-quality-gates requirement "First-run smoke on every platform", and wired there (its task 9.4). This change does not restate it; it only makes the GitHub Release follow a successful publish (spec scenario "Publish held back", task 4.3). The `pypi` environment approval stays as the human gate. Until #71 merges, 0.2.0 ships under today's workflow and the checklist's manual clean installs.
  - *Alternative:* smoke only in the checklist. Rejected: a skipped step is invisible; a `needs` isn't.
- **Release candidates go to PyPI as pre-releases** (`v0.3.0rc1`). pip and uv skip pre-releases unless pinned, so nobody gets one by accident, and the beta mentor installs it the way a real team will. TestPyPI stays the dry run (`0.3.0.devN`; dev numbers are never reused).
- **No closing keyword in release-prep PRs.** The default branch is `development`, so a closing keyword fires on the prep merge (that's how #61 closed early). Release issues are closed by hand once PyPI shows the version. Feature PRs keep using the closing keyword as AGENTS.md says.
- **Docs follow releases.** What the links say is owned elsewhere: the versioned site, the README's absolute version-path images and the metadata's Documentation URL (the site root, which opens `latest`) are `mvp-documentation`'s project-documentation requirements (#82); the in-app documentation addresses are #72's team-setup "Documentation links". This change adds only the release-build gate (spec "Release build refuses moving-branch documentation links"): no `blob/main` or `blob/development` link, no relative image, no image from another version's docs path. The gate checks the README after `mvp-documentation`'s pin step (its task 13.3) has rewritten `/latest/` images to `/X.Y/` and `blob/main` links to the tag, and the metadata after its task 13.6 has pointed `Documentation` at the site. Both must be on `development` before `0.3.0rc1` (task 4.7, ~12-07), or the rc build fails the gate; that ordering is deliberate. Docs-only fixes may redeploy the current minor's docs without a package release.
- **Corpus releases** are created with `--latest=false`, and the developer guide's corpus section says so.
- **Yank policy:** yank a version only when installing it gives wrong results or breaks (0.1.0: uncapped duckdb/polars). The reason text names the version to install instead.
- The release requirement lives in a new `public-release` capability so it doesn't collide with `team-setup` ("Installable public package"), which `mvp-install-and-first-run` modifies for the Python range, or with `project-documentation`, which `mvp-documentation` modifies. The checklist is specified here because it is release behaviour, even though it lives in the developer guide.

### D-g. Branching and merge flow
- **Planning:** all four change folders land together from `feature/68-mvp-public-release` into `development` in one PR that references #68 without a closing keyword.
- **Implementation:** one `feature/<issue>-<name>` branch per issue (or per tight bundle, such as #72 with #37/#38/#44), cut from `development`, PR into `development` with a closing keyword for that issue. Each sibling's last PR carries its `openspec archive` commit.
- **Docs collisions:** `mvp-install-and-first-run` and `mvp-outside-team-logs` edit guide text in place. `mvp-documentation` owns moving and restructuring `docs/guide/` and does its moves in one early PR (by 11-28); after that, nobody else moves guide files.
- **Merge rhythm:** Josh admin-merges on fixed days (Tue and Sat). A PR that is green and approved by then goes in; others rebase after.
- **Release PRs:** `development` → `main` for each release (0.2.0, 0.3.0rc1, 0.3.0, 0.3.1), then the tag on `main`. Hotfixes go `hotfix/*` → `main` and back into `development`.
- **Archive order** (checked against every delta on 2026-11-14):
  1. `p3-log-acquisition` first (M0, task 1.8). `mvp-install-and-first-run` adds requirements to `log-acquisition`, which exists only in that unarchived change.
  2. `mvp-install-and-first-run` and `mvp-outside-team-logs`, in either order, as each finishes. Both touch `team-setup` and `match-reports`, but no requirement is touched by both: install MODIFIES "Installable public package", "Configuration home", "Initialising a configuration" and "Setup checks" and ADDS "Documentation links" and "Empty Replay explains what to do"; outside ADDS only "Phoenix Pro coverage check" and "Temperature on non-Pro hoots". Whichever archives second re-runs `openspec validate --all` first. Install drops its "Robot pull labelled beta" requirement before archive if the robot session passes (its task 8.5). Outside folds the old `match-reports` "Replay view" sentence on non-Pro cells into its new requirement at archive (its design Risks).
  3. `mvp-documentation` after the 0.3.0 tag (its group 15 checks the tagged deploy). It is the only change touching `project-documentation`, so it doesn't depend on the others' order.
  4. This change last, after 0.3.1, with any open `[HUMAN]` tasks moved to #13; the archive comment goes on #68. It is the only change touching `public-release` and `live-can-health`.
  - Every other capability has one owner: `backup-status`, `config-editor`, `session-grouping`, `ci-quality-gates`, `log-acquisition` (install); `match-identity`, `log-metadata`, `hoot-conversion`, `lifetime-trends`, `anomaly-detection`, `anomaly-review`, `can-health` (outside).

### D-h. Kickoff-week 0.3.1
- **Owner:** Josh unless another mentor is named in the checklist by M3. **Target:** Sat 2027-01-16, one week after kickoff.
- **Contents:**
  - owlet entries for the 2027 hoot format, once CTRE publishes them (`tools/update-owlet-manifest.py`, D4);
  - the pyntcore cap widened for 2027 once its wheels exist;
  - fixes from the first announce-week reports.
- **Gate:** the same checklist and smoke; plus one 2027 hoot (from a kickoff test or CTRE sample) ingested on the release candidate.
- If CTRE ships nothing in time, 0.3.1 ships the reported fixes, and the known-limits page keeps "2027 hoots: not yet".

### D-i. Decisions this change relies on
- **D9** (pipx and per-user services): amended by `mvp-install-and-first-run`'s ADR (uv primary, pipx alternative; its D-a and task 1.5). Publishing stays on PyPI and services are unchanged.
- **D4** (owlet fetched by checksum manifest): the smoke downloads owlet on all three OSes, and 0.3.1 updates the manifest.
- **D5** (Pro temperature): outside teams are often non-Pro; `mvp-outside-team-logs` handles it and amends the record.
- **D6** (static export): the hosted demo is a static export, no server.
- **D8** (link to AdvantageScope): the FAQ points single-log deep dives at AdvantageScope.
- **D10** (anomalies v1): the Anomalies view ships labelled beta.
- **D11** (serial identity): outside teams have no `/Flashpoint/CANInventory`, so the walkthroughs use logs without it (legacy epochs).
- This change itself deviates from none of them, so it adds no ADR.

### D-j. Budgets
This change touches full logs only through the gates:
- **Smoke:** at most 20 min wall time per OS for one corpus match, including the owlet download. Owned by #71; this change only waits on it.
- **Release workflow additions:** the changelog extraction and the link check read two small text files and the built wheel; under 10 s and negligible memory, inside the existing 15 min `build` timeout.
- **Per match:** the P1 budget stays at 30 s and 1 GB for a qual match (wpilog + 2 hoots). CI perf shows 33–40 s; that overrun is accepted for MVP and named in known limits, not gated.
- **First run at outside-team scale:** during the walkthroughs, time one full event (about 12 matches with hoots) on a mid-range laptop, cold, including the first numba compile. Target under 15 min and under 1 GB peak memory; record time and lake size per event. If it's over, the docs state the real figure; the release isn't held for it.

## Risks / Trade-offs

- [No outside logs by 11-28] → AK and non-Pro fixes ship against synthetic fixtures, and the matrix says "not tested on real logs". The announcement asks for logs.
- [Josh unavailable over a holiday] → the merge rhythm stops; nothing new starts in the last 3 days before a milestone, and the fallback labels take up the slack.
- [Windows walkthrough finds a blocker on 12-19] → 0.3.0 ships with Windows marked "rough" in the matrix if the issue has a workaround; otherwise 0.3.0 slips, not the post (01-04 still has two weeks of slack).
- [Smoke flakes on Windows block a tag] → re-run the failed job. owlet retries already absorb the known crash (#4). A repeat failure is a real bug.
- [The prep PR or a sibling accidentally uses a closing keyword on a release issue] → the checklist rule, plus reopening it by hand, as with #61.
- [A first-time contributor's PR is swept again] → CONTRIBUTING says PRs are collaborators-only and offers issues and forks; PR #32 gets a reply and its fix lands with credit.
- [0.2.0 dry run reuses `0.2.0.dev1`] → TestPyPI rejects re-uploads; use `0.2.0.dev2` (PR #69's after-merge note says `dev1`).
- [Posting into a holiday] → the date is fixed at 01-04, after Christmas week.

## Migration Plan

- 0.2.0: promote, dry run as `0.2.0.dev2`, tag, approve, yank 0.1.0 ("uncapped duckdb/polars; install 0.2.0"), and create the GitHub Release by hand (the automation lands with #80). Roll back by yanking 0.2.0 too and pointing the README at a pinned install; 0.1.0 stays installable when pinned.
- The GitHub Release job is additive. If it fails after PyPI publishes, the release is created by hand from the same changelog section, and the job is fixed before the next tag.
- Issue forms replace the org templates only on this repo; removing `.github/ISSUE_TEMPLATE/` brings them back.

## Open Questions

- Who is the contact named in the code of conduct for reports? (Security reports go through private vulnerability reporting, so they need no address.)
- Who besides Josh can do the Windows walkthrough on a clean machine (task 8.2, with `mvp-install-and-first-run` 1.4's Windows row), and on what date by 12-17?
