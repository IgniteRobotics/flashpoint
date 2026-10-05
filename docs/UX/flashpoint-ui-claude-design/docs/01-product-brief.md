# 01 · Product brief

## What Flashpoint is

Ignite Robotics' telemetry tool. It watches the robot during matches, records and explains what happened after them, checks the robot before them, and remembers how every motor and battery has behaved across seasons so problems show up before they cost a match.

This document covers the web UI. **(assumed)** Flashpoint's existing robot-side logging and any current tooling should be inventoried first; the UI plugs into whatever already records data.

## Who uses it

| Role | When | What they need |
|---|---|---|
| **Telematics student** (competition role) | During matches, in the stands or pit | One screen that says what's wrong right now, in plain words, fast |
| **Pit crew** | Between matches | A go/no-go verdict with specific fixes; which battery to use |
| **Programming / electrical leads** | After matches, at night, between events | Scrub logs, find causes, decide fixes |
| **Mentors** | Across the season | Which devices are wearing out, what to replace before the next event, what to buy |

Students are the primary audience. The UI explains, not just displays: "Brownout: outputs disabled 0.21 s" comes with a likely cause and a suggested fix in plain language.

## The five screens

| Screen | Moment | Job |
|---|---|---|
| **Live** | During a match | Vitals, every mechanism, events and alerts, at a glance |
| **Replay** | After a match | Scrub the log, jump to faults, read likely causes and fixes |
| **Pit** | Before a match | Run checks, get GO / DECIDE / NO-GO, pick a battery |
| **History** | Across seasons | Every device's performance over its whole life, by serial number |
| **Anomalies** | Continuous | Devices behaving outside spec, with evidence and past cases |

A command palette (Ctrl/⌘ K) jumps to any mechanism, device, match log or anomaly.

## Core flows

1. **Match in progress.** Telematics student watches Live. Indexer faults, battery browns out. Alerts land in the queue; the student acknowledges them and radios the pit: "indexer jam at 1:28, brownout at 0:53."
2. **Between matches.** Pit crew opens the alert in Replay, reads the likely cause, creates a pit task. Before queueing, they run Pit checks: NO-GO for a sticky fault. They clear it after review, acknowledge a firmware warning, pick a healthy battery: GO.
3. **Night at the event.** A lead opens Anomalies: Shooter L heats 41% faster than Shooter R. "Seen this before" points to the motor it replaced, which had the same pattern. Conclusion: the problem is the belt or mount, not the motor. They check tension instead of swapping another motor.
4. **Between events.** A mentor opens History, all time, batteries: B03 is crossing the match spec. They order a replacement before District Champs.

## Principles

- **Plain words first, numbers second.** Every warning says what it means and what to do.
- **Track the hardware, not the slot.** A motor's story follows it from robot to robot.
- **Show the evidence.** Anomalies show actual vs expected, the method used, and the chart. Nothing is flagged on vibes.
- **Never block silently.** Warnings ask for a decision; only faults stop you.
- **Calm under load.** Live renders at about 2 Hz, keeps controls stable, and works on an old laptop on bad venue Wi-Fi.
- **Same family as Mnemosyne.** One design system, one CRT preference, consistent patterns.

## Out of scope for v1

- Controlling the robot or changing configuration from the UI (read-only toward the robot, except clearing sticky faults through an explicit action).
- Scouting other teams. Flashpoint is about our robot.
- Real-time collaboration features beyond shared links and pit tasks.

## Definition of done for v1

- Live works against a real robot over NT4 and against the log-replay simulator.
- Every match is logged, parsed and viewable in Replay within a few minutes of the match ending.
- Pit checks run real probes and produce a verdict.
- History shows at least one full season of real per-match metrics, tracked by serial.
- The anomaly detector runs after each match and flags at least SPEC, DRIFT and SIBLING cases with evidence.
- Axe-clean, keyboard-complete, usable at 1280×720 on a shop projector and on a phone in the stands.
