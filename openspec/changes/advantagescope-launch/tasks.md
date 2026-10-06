## 1. Locating AdvantageScope (spec: advantagescope-launch, "Locating AdvantageScope")

- [x] 1.1 Tests first: build fake install trees for each platform key (WPILib 2025 and 2026, standalone, nothing) and an explicit config path that is present or missing. Assert the chosen path and `found_by` for each, and that a missing explicit path is an error with no fallback; tests fail
- [x] 1.2 Implement discovery in a new launcher module, and add the `[advantagescope] path` key (empty by default) to `config/report.toml`; 1.1 passes on macOS, Linux and Windows CI
- [x] 1.3 `flashpoint doctor` prints the AdvantageScope path and the rule that found it (or "not found"), and the staging mode (link or copy); test the output for each case and verify by hand with `flashpoint doctor` on this laptop (it should show WPILib 2026)

## 2. Staging (spec: advantagescope-launch, "Raw logs staged under readable names")

- [x] 2.1 Tests first on the corpus lake: staging a match yields one folder of `<match_key>__…` files whose SHA-256 equals the ledger hash; the raw store is byte-identical afterwards; a repeat run reuses the folder with no duplicates; a size-mismatched leftover is replaced
- [x] 2.2 Tests first: when the hard link fails (forced through the seam), staging falls back to a copy and the result is the same; a missing raw file stages nothing and names the hash; an `incomplete-read` hoot is staged and flagged
- [x] 2.3 Tests first: the start-up cleanup removes only `flashpoint-as/*` folders older than 24 h, leaves newer folders, sibling temp files and symlink targets alone, and never follows symlinks
- [x] 2.4 Implement staging (hard link, then a streaming copy with a 1 MiB buffer) and the cleanup; 2.1–2.3 pass, including on Windows CI

## 3. Launch and request guard (spec: advantagescope-launch, "Guarded launch request", "Launch is local only", "AdvantageScope opened on the match's wpilog")

- [x] 3.1 Tests first for the guard matrix (bound host × client address × `Host` × `Origin` × `X-Flashpoint` × content type): only the all-local row reaches the spawn seam, every other row gets 403 with a reason, and GET, OPTIONS and other methods never spawn
- [x] 3.2 Tests first for the request body: hostile and unknown match keys give 404 and an unchanged lake; extra `path`/`args` fields have no effect; a hoot-only match gives 409 "no wpilog" and spawns nothing; a spawn `OSError` gives 500 with a reason
- [x] 3.3 Implement `GET /api/launch` (availability) and `POST /api/launch` in `web/server.py` and `web/api.py`; every other non-GET route still answers 405. Update the existing read-only test; 3.1–3.2 pass
- [x] 3.4 Implement the spawn function: `open -a` for a macOS `.app`, a direct executable otherwise; `shell=False`, detached, all I/O to `DEVNULL`, never waited on. POSIX test with a stand-in script that records its argv: exactly one argument (the staged wpilog), a staging path with spaces and a quote arrives intact, and the server answers another request while the stand-in is still running
- [x] 3.5 Run the start-up cleanup in `flashpoint serve`, and run discovery once at start. A serve test shows the launch is reported unavailable with `--host 0.0.0.0`

## 4. Front end (spec: match-reports, "Open in AdvantageScope"; lifetime-trends, "Drill-through to Replay and raw logs")

- [x] 4.1 Browser tests first (served, with the stand-in configured): the Replay aside shows "Open in AdvantageScope"; choosing it starts the stand-in and shows the staged folder and the hoots to insert; the downloads stay. History's raw-log list offers the same action
- [x] 4.2 Browser tests first: no launch action with `--host 0.0.0.0` (the page shows the "only on the machine running Flashpoint" reason) or on `file://`; when AdvantageScope is not found, the reason is shown and the downloads work
- [x] 4.3 Implement it: `shell.js` reads `GET /api/launch` once in served mode; `replay.js` and `history.js` render the action and its result with `h()` only (no `innerHTML`). The request sends `X-Flashpoint: launch` and JSON; 4.1–4.2 pass in Chromium

## 5. Platform checks

- [x] 5.1 Windows CI: discovery against the fake `C:\Users\Public\wpilib\<year>` tree, staging by hard link and by copy, and the guard; these all pass on `windows-latest`
- [ ] 5.2 [HUMAN] On a Linux laptop with WPILib 2026, record the AdvantageScope executable's path under `~/wpilib/2026/advantagescope/` and the standalone install path, then correct the design table and the discovery rule if they differ
- [ ] 5.3 [HUMAN] On the Windows pit machine, confirm the WPILib and standalone AdvantageScope paths and whether the lake and `%TEMP%` share a volume (link or copy), and record the results in design.md

## 6. Docs

- [x] 6.1 Write ADR-0014 "Guarded local AdvantageScope launch" (amends 0013, refines D8, cites #54), and add it to `docs/adr/README.md`
- [x] 6.2 Update `docs/rewrite/05-target-architecture.md` D8 (mechanism: launch when local, otherwise download), the P4 follow-up note in `docs/rewrite/06-roadmap.md`, and the README's `serve` section; `openspec validate advantagescope-launch` passes
- [x] 6.3 Full gate: ruff, mypy --strict, pytest (unit, corpus, browser), then open the PR into `rewrite`

## 7. [HUMAN] Field check

- [ ] 7.1 Josh, on this laptop: `flashpoint serve`, Replay Q15, then "Open in AdvantageScope". AdvantageScope opens one window titled with the readable Q15 name. Repeat while AdvantageScope is already open, and a second window opens
- [ ] 7.2 In that window, File › Insert log, then select both staged Q15 hoots: they merge, and a TalonFX current from a hoot lines up with the same motor in the wpilog around enable. Record aligned or offset (and by how much) in design.md
