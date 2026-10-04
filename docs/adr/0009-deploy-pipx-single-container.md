# 0009. Deploy: `pipx install` plus an optional single multi-arch container

- Status: Proposed
- Date: 2026-10-04
- Refs: D9; pitfalls #1–#7, #41, #64

## Context
The three-service compose stack never built cleanly on main. Builds took about 10 minutes, it needed X11 and AppArmor hacks, and the Python versions disagreed across images.

## Decision
- The primary install is `pipx install flashpoint` on a pit laptop.
- An optional single container (amd64 and arm64) runs `flashpoint acquire --watch` plus ingest for an always-on pit server.
- One Python version per release.

## Consequences
- Simple local development and event use.
- The container is optional, not required.

## Alternatives considered
- **Multi-service compose** (status quo): operationally heavy for one team.
