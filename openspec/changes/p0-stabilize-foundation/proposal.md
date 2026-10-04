## Why

Main has never had CI, and none of its entry points run (#1–#9). Work is split across long-lived branches, and decisions D1–D10 are still open. The rewrite needs a safe repo, recorded decisions, and real test logs before any code lands. CAN-inventory logging on the robots has to ship now: every log recorded without it is a log we can't attribute to a physical motor. Roadmap phase **P0** (docs/rewrite/06-roadmap.md §2).

## What Changes

- Protect `main`: require PRs and CI. Add a CI skeleton (lint, type check, pytest) that passes on an empty `src/flashpoint/` package
- Tag `main` as `legacy-2025`. Archive `origin/development` and `origin/feature/power-tracking` as `archive/*` tags once their salvage is listed
- Record D1–D10 as ADRs in `docs/adr/`. D11 (serial-number identity) is already decided; record it too
- Build the **golden log corpus** (about 6 real logs: qual with rio and CANivore hoots, practice, non-FMS, corrupt tail, a 2025 log and a 2026 log), stored by fetch script or Git LFS
- Run `owlet --check-pro` on the corpus and record the result (it decides where temperature data comes from)
- **Robot code, all robots:** log `/Flashpoint/CANInventory` per the contract in 05 §4a. First confirm the Phoenix 6 `getdevices` field names on a live robot
- Seed the unit registry: a Tuner X walk of every robot and the spare shelf, recording each serial against its slot. Physically label the motors

## Capabilities

### New Capabilities
None. This change covers tooling, process, and robot code only, so `skip_specs: true`.

### Modified Capabilities
None.

## Non-goals

- Any ingest or analysis code (that starts in P1)
- Fixing legacy scripts on main

## Impact

- Repo settings (branch protection), `.github/workflows/`, `docs/adr/`, `pyproject.toml`
- The robot code repos for every robot (CAN inventory logger)
- Tags `legacy-2025`, `archive/development`, `archive/power-tracking`
