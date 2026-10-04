# 0008. Single-log deep dive: link to AdvantageScope, don't rebuild it

- Status: Proposed
- Date: 2026-10-04
- Refs: 04 §2, D8; pitfall #54

## Context
AdvantageScope already opens wpilog, hoot, DS logs, and REV logs, with graphing and a 3D field view. The development branch launched it on the server through `os.system` and X11 hacks.

## Decision
Views offer an "Open in AdvantageScope" action: a download of the raw wpilog (and matching hoots), which the user opens on their own machine. Flashpoint never launches desktop apps.

## Consequences
- No graphing-engine scope creep.
- Users need AdvantageScope installed, which is already standard in FRC.

## Alternatives considered
- **Re-implementing graphs:** large scope for no gain.
- **Server-side launch:** broken, and insecure.
