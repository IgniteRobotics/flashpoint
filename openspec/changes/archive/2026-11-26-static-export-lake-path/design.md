## Context

`ReportBuilder.write_index` writes `<lake>/report/data/matches.js` with `"lake": str(self.lake.root)`. Served mode reads that file directly: `shell.js` fills `#lake-path` from `FP.indexData.lake`, and `replay.js`'s empty state puts it in the "no imported logs" text and the `flashpoint ingest --lake` command. `export_static` copies the whole data folder with `copytree` and then reads `matches.js` back for the match list. A check of a real lake (Q7 corpus build) found the lake path only in `matches.js`; per-match files, `mode.js`, `theme.css` and the anomaly snapshot carry none.

This follows D6 (Replay view with a static export); no decision changes.

## Goals / Non-Goals

**Goals:** the export has no local absolute path; served mode is unchanged.

**Non-Goals:** see proposal.md, Non-goals.

## Decisions

**Strip the field at export time, not at build time.** After the `copytree`, the export rewrites `out/data/matches.js` from the index it already parses, without the `lake` key, using the same `FP.index` script wrapper the build writes (`to_script`).
- Alternative: drop `lake` from the built index and give served mode the path through the API. Rejected: it changes the build, the server and the shell to fix an export-only problem, and the served header would need a request before it can show the path.
- Alternative: blank the field (`"lake": ""`). Rejected: omitting it is simpler, and the JS already treats a missing `lake` as absent (`index.lake || ''`).

**Guard the whole export, not one field.** The test scans every text file in the export for the lake's root path (and its `as_posix()` form), so a future field that leaks a path fails the test even if it isn't `lake`.

**Static empty state.** In `replay.js`'s no-sessions branch, render the path `<code>` only when `index.lake` is set, so a static export doesn't show an empty box. The ingest command already falls back to `<lake>`. `report` refuses a lake with no derived sessions, so this branch is hard to reach from an export; the change is defensive and two lines.

## Risks / Trade-offs

- [The export's `matches.js` is no longer byte-identical to the lake's] → nothing compares them; `export_static` already re-reads the index rather than trusting the copy.
- [A path could leak through another field later] → the whole-export scan in the test catches it.

No full logs are read: the rewrite touches one small JSON file, so time and memory budgets are unchanged.
