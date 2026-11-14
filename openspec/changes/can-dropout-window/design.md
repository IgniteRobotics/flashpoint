## Context

The window has to live where both consumers can read it. The live detector only sees what robot code published each 100 ms: the `Dropouts` total and `MaxAgeMs`, the oldest the watched signal got in that window. Derive measures frame intervals in the hoots directly. Both already resolve a bus through the robot file's `[[bus]]` table: live by robot-code name, derive from the hoot bus via `bus_name_for`.

## Goals / Non-Goals

**Goals:**
- One number per bus, in one file, used by both live and after-match.

**Non-Goals:**
- Changing the robot-side contract or what robot code counts.
- A window per device or per model. Per bus is what the evidence supports: the bus's signal rates set the noise floor.
- Editing the window from the UI. The robot file is edited by hand or through Setup, like every other robot setting.

## Decisions

- **Live filter on `MaxAgeMs`.** A rise in `Dropouts` is kept only when that update's `MaxAgeMs` is at least the window. If any loop in the window exceeded it, `MaxAgeMs` does too, so no real long dropout is lost. The cost: a window that holds one long and one short dropout reports the robot's full count. That's acceptable, because the event exists to show *that* the device dropped, and the count is secondary. If `MaxAgeMs` is missing or bad, the dropout is kept, as today.
- **The filter runs before the bus-wide check**, so short jitter on some devices can't make a real isolated dropout look bus-wide.
- **Derive's rule becomes `interval ≥ max(2.5 × nominal, window)`**, applied to trailing gaps as well. Missed frames are still derived from the nominal period.
- **A hoot bus with no `[[bus]]` entry has no window.** Hoots name CANivores by hex id, and the existing `bus_name_for` mapping (an exact id beats `canivore`) decides which entry applies.
- **Milliseconds, as a float greater than 0, optional.** It matches how the view and the guide talk about ages (`MaxAgeMs`). Unknown keys stay rejected.

## Risks / Trade-offs

- [A window hides real short dropouts] → It's per bus and opt-in, and the guide says what it costs. Ignite's 35 ms is provisional: the 2026-11-14 evidence points at roboRIO stalls (a 97%-full disk, a starved CPU) that may shrink once fixed.
- [Live and gold differ for a while after an edit] → Gold needs a re-derive, and the guide says so.
