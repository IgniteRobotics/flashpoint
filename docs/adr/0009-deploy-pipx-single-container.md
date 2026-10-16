# 0009. Deploy: `pipx install` plus per-user services (container deferred)

- Status: Proposed
- Date: 2026-10-04
- Refs: D9; pitfalls #1–#7, #41, #64

## Context
The three-service compose stack never built cleanly on main. Builds took about 10 minutes, it needed X11 and AppArmor hacks, and the Python versions disagreed across images.

## Decision
- The primary install is `pipx install flashpoint` on a pit laptop.
- `flashpoint acquire --watch` is the one acquisition path. It runs in the foreground on a pit laptop, or as a per-user service on an always-on box:
  - **Linux:** a systemd user unit (`deploy/systemd/flashpoint-acquire.service`).
  - **Windows:** a Task Scheduler task (`deploy/windows/flashpoint-acquire.xml`). Windows is a supported platform.
  - **macOS:** foreground run; a launchd plist waits until someone runs an always-on Mac.
- The optional single multi-arch container is **deferred out of P3** (amended 2026-10-04, change `p3-log-acquisition`). It returns only if a pit server needs it.
- One Python version per release.

## Consequences
- Simple local development and event use.
- There is no container to build or maintain in P3. Linux and Windows boxes run the same pipx install under their own service manager.

## Alternatives considered
- **Multi-service compose** (status quo): operationally heavy for one team.
