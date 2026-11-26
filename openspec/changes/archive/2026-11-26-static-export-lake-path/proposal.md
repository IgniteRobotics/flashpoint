## Why

`flashpoint report --static` copies the built Replay index (`data/matches.js`) into the export as-is, and that index records the lake's absolute folder path. An export a team puts on a USB stick or shares therefore carries a path from the laptop that made it, usually with the account name in it (`/Users/<name>/flashpoint-lake`, `C:\Users\<name>\...`). The export never uses that path: it can't open the lake, and the header shows "STATIC EXPORT" instead of the lake (#112, found while building the hosted demo in `mvp-documentation` group 9, #106).

Roadmap phase: P4 (Views), `docs/rewrite/06-roadmap.md` § P4; D6 (Replay view with a static export) in `05-target-architecture.md`. No pitfall from `03-pitfalls-and-performance.md` applies; this is a privacy leak in the export.

## What Changes

- The static export writes its own copy of the match index without the lake's folder path. The per-match data files are copied unchanged (they hold no local paths today).
- The lake's own index is unchanged, so served mode still shows the lake path in the header and in the empty Replay's ingest command.
- In a static export, the empty Replay's "no imported logs" text no longer shows an empty path box; the ingest command keeps its `<lake>` placeholder.
- The USB-stick guide says the export holds no folder path from the laptop.
- Re-running the export (or the next hosted-demo deploy) removes the path from an existing export; old copies already handed out keep it.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `match-reports`: "Served and static modes" gains a rule that a static export holds no absolute path from the machine that made it, with a scenario.

## Impact

- Code: `src/flashpoint/report/export.py` (write the exported index without `lake`); `src/flashpoint/web/static/views/replay.js` (static no-sessions text). `report/build.py` and the served app are unchanged.
- Tests: new cases in `tests/report/test_export.py` (exported index has no `lake` key and no lake path anywhere in the export; the lake's own index still has it).
- Docs: `docs/guide/how-to/replay-on-a-usb-stick.md`, one sentence.
- Hosted demo: picks up the fix on its next deploy; no change to the demo workflow.

## Non-goals

- Changing what served mode shows or where it gets the lake path.
- Scrubbing raw log file names or contents: raw downloads are the team's own logs, named from the match key and the log's original file name (no folder).
- Rewriting exports already copied elsewhere.
- Removing the lake path from the lake's own `report/data/matches.js`.
