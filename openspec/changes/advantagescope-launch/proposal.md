## Why

P4's "Open in AdvantageScope" is a download: each deep dive copies 50–140 MB into the browser's download folder, then needs a double-click. In served mode the app and AdvantageScope run on the same machine (a mentor laptop or the pit machine), so the server can open the logs in place. This is a follow-up to roadmap phase **P4** (`docs/rewrite/06-roadmap.md`). It refines D8 (`05-target-architecture.md` §6): deep dives still go to AdvantageScope.

A spike against AdvantageScope 26.0.0 (WPILib build) found:
- It has **no URL scheme**, so a page cannot launch it. Only a local process can.
- Every external entry point (command-line paths, the macOS open-file event, the AdvantageKit path file) opens **only the first file**, unmerged. Merging is available only through the in-app File › Insert log dialog.
- It titles the log with the file name it is given, and the raw store names files by hash.

## What Changes

- In **served mode on the local machine**, Replay and History's raw-log list offer **one "Open in AdvantageScope" action per match**. The server:
  - stages that match's raw logs (the wpilog and every hoot) in a per-match folder under readable names, without copying where the OS allows;
  - starts AdvantageScope on the match's wpilog;
  - tells the user that the hoots are in the same folder, to add with File › Insert log.
- The server locates AdvantageScope from an explicit setting, the newest WPILib install, or a standalone install. `flashpoint doctor` reports what it found.
- The launch is refused, and the page falls back to the existing download, when any of these holds:
  - the server is shared on the network;
  - the request is not from the local machine;
  - AdvantageScope is not found;
  - the match has no wpilog.
- The launch request only accepts a match key. Paths never come from the client. The app is started with an argument list and never through a shell. Cross-site requests are rejected.
- The download stays everywhere, and it is the only option in static exports.
- A new ADR records the change to the "never launch any application" rule.

Fixes pitfall **#54** (AdvantageScope launched with `os.system` and paths built from strings) by doing the launch safely rather than banning it.

## Capabilities

### New Capabilities
- `advantagescope-launch`: staging a match's raw logs under readable names, locating AdvantageScope, the guarded local launch request, and the fallback rules

### Modified Capabilities
- `match-reports`: "Open in AdvantageScope" adds a launch action in local served mode. The rule "the app SHALL never launch any application" becomes "only through the guarded launch".
- `lifetime-trends`: "Drill-through to Replay and raw logs" offers the same launch beside the download

## Non-goals

- Opening the hoots merged in one click. AdvantageScope cannot accept that from outside. An upstream AdvantageScope change is deferred (Josh, 2026-10-05).
- Building a merged, pre-aligned wpilog from the wpilog and hoots. That is a separate idea if the alignment from Insert log proves wrong.
- Opening AdvantageScope at a chosen time or marker. AdvantageScope has no way to accept this.
- Launching on behalf of remote clients, and any auth (D8 stays local-only).
- A custom URL protocol handler or an installer.
- Launching any other application.

## Impact

- `web/server.py` (first non-GET route, guarded), `web/api.py`, and a new launcher module (discovery, staging, process start).
- `replay.js`, `history.js`, `shell.js`; `config/report.toml` (optional AdvantageScope path); `flashpoint doctor`; the README's `serve` section.
- A new ADR amending ADR-0013 and refining D8.
- No lake writes; staging lives in the OS temp folder.
