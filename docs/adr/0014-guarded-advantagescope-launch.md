# 0014. Guarded local AdvantageScope launch

- Status: Proposed
- Date: 2026-10-05
- Amends: [0013](0013-views-one-local-app.md) (the read-only app); refines D8
- Deciders: Josh
- Refs: openspec change `advantagescope-launch` (design.md), D8, pitfall #54

## Context
D8 hands deep dives to AdvantageScope. P4 did this with a download, and its spec said the app "SHALL never launch any application", because the legacy viewer launched AdvantageScope with `os.system` and paths built from strings (#54). Each deep dive then copied 50–140 MB into the download folder, and the user had to find and open it. A spike against AdvantageScope 26.0.0 showed:
- It has no URL scheme, so only a local process can start it.
- Every external entry point opens only the first file it is given, unmerged.
- It titles the log with the file's own name, and the raw store names files by hash.

`flashpoint serve` runs on the same machine as AdvantageScope in the only supported setup: a mentor laptop or the pit machine, local only.

## Decision
The local app may start **exactly one program, AdvantageScope, on one staged raw file**, through `POST /api/launch`. Each guard below answers one failure in #54:
- **Local only:** the server is bound to loopback, the client is loopback, and `Host` names this machine. Shared on the network, the app offers only the download.
- **Only the app's own page can send the request:** `Origin` must be the app's own, and the request needs an `X-Flashpoint: launch` header and a JSON body. Together these force a CORS preflight that the server never answers.
- **No paths or arguments from the client:** the request carries a match key. The ledger resolves it to raw hashes.
- **No shell:** the program is started from an argument list, detached, and never waited on. On macOS a `.app` bundle is started with `open -a`.
- **Readable names without touching the lake:** the match's logs are hard-linked (copied on Windows or across volumes) into `<temp>/flashpoint-as/<match_key>/` under their download names. Folders older than a day are removed when the server starts.

AdvantageScope is found from a configured path, then the newest WPILib year, then a standalone install. `flashpoint doctor` reports which one it found. The lake stays read-only, and every other route still answers GET only.

## Consequences
- One click opens the match's wpilog in AdvantageScope under a readable name, with no download. The hoots wait in the same folder for File › Insert log, because AdvantageScope accepts only one file from outside.
- The server gains its first state-changing route. It is covered by a guard-matrix test, and only the all-local row reaches the spawn.
- Static exports and network-shared servers behave as before: download only.

## Alternatives considered
- **Keep download-only (P4):** safe, but every deep dive leaves a copy behind and needs a manual open.
- **A `flashpoint://` protocol handler:** per-OS registration and an installer, for the same result.
- **Open with the OS default handler:** `.hoot` may belong to Tuner X, and `doctor` can't report what will open.
- **A pre-merged, pre-aligned wpilog** (the wpilog plus owlet-converted hoots): it would open in one click with Flashpoint's alignment, but it is a new derived artifact. It is deferred unless the alignment from Insert log proves wrong.
- **An upstream AdvantageScope change** to accept several files with merge: deferred (Josh, 2026-10-05).
