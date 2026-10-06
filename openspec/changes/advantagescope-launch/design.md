## Context

See proposal.md (Why) for motivation and the spike results. The current state:

- `flashpoint serve` (ADR-0013) is a stdlib `ThreadingHTTPServer`. It is GET/HEAD only: every other method answers 405 "read-only". It binds to `127.0.0.1` by default; `--host` shares it with a warning, and `is_local()` already decides whether a host is loopback.
- `/api/match/<key>` already returns each match's raw `sources`. Each source carries `sha256`, `kind`, `part` and `download`, the readable name `<match_key>__<wpilog or bus>__<original>` built by `report.names.safe_name`. `lake.raw.raw_path()` maps a hash to its raw-store file, which is read-only (`-r--r--r--`) and named `<sha>.<kind>`.
- AdvantageScope 26.0.0 (WPILib build), read from its bundled `main.js`:
  - It takes the **first** log path in `process.argv` (`main.js:10583`), with `merge: false`.
  - On macOS, an `open-file` event while it is running opens a **new hub window** with that one file (`main.js:10612`).
  - It has no single-instance lock and no URL scheme.
  - It titles the log with the base name of the path it is given.
- D8 (link to AdvantageScope, don't rebuild it) stands. This change replaces P4's "nothing launches AdvantageScope" (P4 design, Raw downloads) with a guarded launch. That is a change to ADR-0013's read-only app, so **ADR-0014** records it (see Decisions).

## Goals / Non-Goals

**Goals:**
- One request opens a match's wpilog in AdvantageScope from the raw store, under a readable name, with no copy on the common single-volume setup.
- Nothing a web page, another machine, or a crafted match key sends can start a program other than the located AdvantageScope with one staged file.
- Every existing download path keeps working unchanged.

**Non-Goals:**
- Reaping or tracking AdvantageScope after start; it is the user's window from then on.
- Configuring AdvantageScope (layouts, tabs, preferences).
- Linux desktop-file or Windows registry integration.

## Decisions

### 1. Launch from the server, not the browser
AdvantageScope has no URL scheme, and only a local process can start a program. The server already runs on the user's machine in the only supported mode (local serve, Josh 2026-10-05).
- *Rejected:* a `flashpoint://` protocol handler. It needs per-OS registration and an installer, for the same result.
- *Rejected:* opening the file with the OS default handler. `.hoot` may belong to Tuner X, and the default handler is invisible to `doctor`.

### 2. Request guard: four independent checks
`POST /api/launch` with the JSON body `{"match_key": "..."}` is the only non-GET route. Every other path and method still answers 405. The request is refused (403, with a reason) unless **all** of these hold:
1. **Server bound to loopback.** `is_local(bound host)`; otherwise the launch is "shared on the network".
2. **Client is loopback.** `ipaddress(client_address).is_loopback`.
3. **`Host` header is local.** It must be `localhost`, `127.0.0.1` or `[::1]` with the server's port. This defeats DNS rebinding.
4. **`Origin` equals `http://<Host>`, and the `X-Flashpoint: launch` header is present** with `Content-Type: application/json`. Both force a CORS preflight for any cross-origin page, and the server never answers `OPTIONS` with CORS headers, so the browser never sends the real request.

`GET /api/launch` returns `{available, reason, app, found_by}` and starts nothing. The shell calls it once in served mode and caches the answer for the page.

The match key is checked against `^[0-9a-z_]+$`, then looked up through the same query that backs `/api/match/<key>`. Unknown keys answer 404. Only the ledger's hashes reach the file system; `path` and `args` fields in the body are ignored.
- *Rejected:* a GET launch link. Any `<img src>` on any page could fire it.
- *Rejected:* a per-session token in the page. It adds state for no gain over checks 3 and 4 on a loopback-only server.

### 3. Staging: hard links, copy fallback, never symlinks
- **Folder:** `tempfile.gettempdir()/flashpoint-as/<match_key>/`. Each file is named with the source's existing `download` name, so staged names equal download names.
- **Order:** `os.link` (a hard link, so no bytes are copied), then a streaming copy if linking fails (another volume, or a file system without hard links).
- **Symlinks are not used.** macOS LaunchServices and Electron can resolve them to the target, which would bring back the hash title. On Windows they need developer mode.
- **Reuse:** an existing staged file is kept if its size matches the raw file, and replaced otherwise. The folder's mtime is touched on each launch.
- **Cleanup at server start:** delete `flashpoint-as/*` directories whose mtime is more than 24 h old. Use `os.scandir`, never follow symlinks, and only touch entries directly under `flashpoint-as/`. Removing a hard link never affects the raw store.
- The raw store is never opened for writing: `os.link` reads metadata only, and the copy opens the source `rb`.
- **Windows always copies** (found in implementation). Raw-store files are read-only, a hard link shares that flag, and Windows refuses to delete a read-only file. Removing a staged link would therefore mean clearing the raw file's protection. This still meets the spec ("link where the OS allows"), and `doctor` prints "copy" on Windows.

### 4. Starting AdvantageScope
- **macOS, `.app` bundle:** `["open", "-a", <bundle>, <staged wpilog>]`. If AdvantageScope is running, this sends `open-file`, which opens a new hub window for that match. Otherwise it starts AdvantageScope on the file. Running the bundle's binary directly would start a second Electron process instead.
- **Windows and Linux, or any non-bundle path:** `[<executable>, <staged wpilog>]`.
- **Process options:**
  - `subprocess.Popen` with `shell=False`;
  - `stdin`, `stdout` and `stderr` set to `DEVNULL`;
  - `start_new_session=True` on POSIX, or `DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP` on Windows;
  - never waited on. The `subprocess` module reaps finished children on later `Popen` calls; `open -a` exits within a second.
- **Start failure:** `OSError` from `Popen` returns 500 `{started: false, reason}`. The page then shows the download.
- **Result:** `{started, folder, wpilog, hoots: [{name, incomplete}]}`. `incomplete` comes from the ledger's `incomplete-read` status.
- **Seam for tests:** spawning goes through one function that the tests replace. That function is the only place that calls `Popen`.

### 5. Locating AdvantageScope
The first match wins. `found_by` is one of `config`, `wpilib <year>` or `standalone`.

| Rule | macOS | Windows | Linux |
|---|---|---|---|
| config | `[advantagescope] path` in `config/report.toml` (a missing path is an error, with no fallback) | same | same |
| WPILib, newest numeric year | `~/wpilib/<year>/advantagescope/AdvantageScope (WPILib).app` | `%PUBLIC%\wpilib\<year>\advantagescope\AdvantageScope (WPILib).exe` | `~/wpilib/<year>/advantagescope/AdvantageScope (WPILib)` (verify, task 5.2) |
| standalone | `/Applications/AdvantageScope.app` | `%LOCALAPPDATA%\Programs\AdvantageScope\AdvantageScope.exe` (verify) | `advantagescope` on `PATH` (verify) |

Discovery runs once at server start and on `doctor`. Restart `serve` after installing AdvantageScope.

### 6. ADR-0014: guarded local launch
A new ADR records that the local app may start exactly one program, AdvantageScope, on one staged raw file, under the guard in Decision 2. It amends ADR-0013's "read-only app" (the lake stays read-only; only this side effect is added) and refines D8's mechanism. Pitfall #54 is cited as the reason for each guard.

### Budgets
- **Memory:** the server's RSS is unchanged; no log is parsed. The copy fallback streams with a 1 MiB buffer.
- **Time, linked:** launch responds in under 0.5 s; the work is O(number of files).
- **Time, copy fallback:** about 1 s per 200 MB on an SSD (Q15 has 218 MB of raw logs).
- **Disk:** with links, zero bytes. With copies, at most the raw size of the matches launched in the last 24 h, reclaimed at the next server start.

### Testing
- **Unit:**
  - Guard matrix: bind host × client × `Host` × `Origin` × marker header. Only the all-local row reaches the spawn seam.
  - Hostile and unknown keys answer 404 and the lake is byte-identical.
  - Extra body fields are ignored.
  - Staging on the corpus lake: names, hashes, reuse, the link-fails-then-copy path, and cleanup of folders over 24 h without touching neighbours.
  - Discovery against fake WPILib trees for each platform key, and the config-missing error.
  - `doctor` output.
- **Real exec, POSIX only:** a stand-in executable script writes its argv to a file. The test asserts one argument and checks that a name with spaces and a quote survives intact. Windows CI covers staging and discovery through the seam.
- **Browser (Playwright, served):**
  - The action is visible with the stand-in configured.
  - It is absent with `--host 0.0.0.0` and on `file://`.
  - The result panel lists the staged folder and the hoots.
  - The download still works.
  - The existing 405 test is updated: only `POST /api/launch` is allowed.

## Risks / Trade-offs

- [The pit machine keeps the lake on another volume from temp, so every launch copies about 200 MB] → Correct but slower. `doctor` prints "staging: link" or "staging: copy" so the cost is visible. A setting for the staging root is a later option if this matters.
- [AdvantageScope's Insert-log alignment of the hoots may be off] → Not checked yet. Task 7.2 checks it by hand. If it is wrong, the merged pre-aligned wpilog (proposal non-goal) becomes its own change.
- [A future AdvantageScope adds a single-instance lock or changes its argv handling] → The launch still passes one file. The macOS path already uses `open-file`. The manual check (7.1) is re-run on each WPILib year.
- [The staged hard link outlives a lake `rebuild` that removes a raw file] → The link keeps the old bytes until cleanup. That is harmless, because staged files are never read back by Flashpoint.
- [Windows or Linux install paths are guesses] → Each is marked "verify", and an explicit config path always works.
- [Local malware could call the endpoint] → Out of scope. It can already start any program.

## Migration Plan

- Additive. Existing downloads, the static export and remote serve behave as before. The only new on-disk state is under the OS temp folder.
- To roll back, revert the change. Staged folders age out, or can be deleted by hand.

## Open Questions

- Should the launch also bring the existing AdvantageScope window forward on Windows and Linux, as `open -a` does on macOS? This is cosmetic and can be decided after the first pit use.
