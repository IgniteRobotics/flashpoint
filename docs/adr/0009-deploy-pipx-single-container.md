# 0009. Deploy: `pipx install` plus per-user services (container deferred)

- Status: Proposed
- Date: 2026-10-04
- Refs: D9; pitfalls #1–#7, #41, #64

## Context
The three-service compose stack never built cleanly on main. Builds took about 10 minutes, it needed X11 and AppArmor hacks, and the Python versions disagreed across images.

## Decision
- The primary install is `pipx install flashpoint` on a pit laptop.
- `flashpoint acquire --watch` is the one acquisition path. It runs in the foreground on a pit laptop, or as a per-user service on an always-on box:
  - **Linux:** a systemd user unit (`src/flashpoint/deploy/systemd/flashpoint-acquire.service`).
  - **Windows:** a Task Scheduler task (`src/flashpoint/deploy/windows/flashpoint-acquire.xml`). Windows is a supported platform.
  - **macOS:** foreground run; a launchd plist waits until someone runs an always-on Mac.
- The optional single multi-arch container is **deferred out of P3** (amended 2026-10-04, change `p3-log-acquisition`). It returns only if a pit server needs it.
- One Python version per release.

## Consequences
- Simple local development and event use.
- There is no container to build or maintain in P3. Linux and Windows boxes run the same pipx install under their own service manager.

## Alternatives considered
- **Multi-service compose** (status quo): operationally heavy for one team.

## Implementation notes (team-setup, 2026-10-21)
- `pipx install flashpoint` is real once the first release is published: `.github/workflows/release.yml` builds on a `v*` tag, takes the version from the tag, and publishes to PyPI by trusted publishing (no stored token). A manual run publishes a dry run to TestPyPI.
- One Python range per release: 3.11 to 3.13. A `clean install` CI job installs the built wheel into an empty venv on macOS, Windows and Linux.
- The wheel carries the shared default configuration and the two service files (moved into the package), never a team's robots, units or logs. Configuration lives in a per-user folder created by `flashpoint init`.
- The Setup view can install and start the service on Linux and Windows (ADR-0017).
