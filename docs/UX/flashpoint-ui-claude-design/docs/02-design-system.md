# 02 · Design system (Flashpoint)

Flashpoint uses the **Ignite design system** shared with Mnemosyne. Tokens are identical: `design/tokens.json`, mirrored in `prototype/css/tokens.css` and `design/tailwind.preset.js`. Do not fork them; if Flashpoint needs a new token, add it to the shared set.

This document covers the shared basics briefly and Flashpoint's additions in full. Class names match `prototype/css/components.css`.

## Shared basics (summary)

- **Ground:** warm black `--bg #0A0806`, surfaces stepped `--surface-1/2/3`, `--surface-raised` for selection.
- **Ink:** one amber in five steps (`--amber #FFB000`, `--amber-hi`, `--amber-mid`, `--amber-dim`, `--amber-muted`) plus `--prose` for reading text. All pass WCAG AA on every surface.
- **Signal:** `--hot #FF5A1F` is the only other hue. Faults, open problems, retire-grade batteries, DEMO badge.
- **Type:** VT323 for display and big readouts only; IBM Plex Mono for interface; IBM Plex Sans for anything read as sentences (causes, fixes, why-flagged, alert messages).
- **Shape:** zero radius, lines and border styles carry hierarchy, glow only on what's "lit."
- **Texture:** scanline overlay is a user toggle (CRT), off under reduced motion. The preference key `ignite.scanlines` is shared with Mnemosyne.
- **Casing:** caps for system chrome (labels, tabs, buttons, status words); sentence case for content.

Full detail on the basics lives in the Mnemosyne handoff's `docs/02-design-system.md`; the rules are the same.

## Flashpoint additions

### Status language (the most important rule in this product)

| State | Glyph | Word | Text color | Box border |
|---|---|---|---|---|
| OK | ● | OK | `--amber-dim` | `1px solid --line` |
| Warning | ▲ | WARN | `--amber-hi` | `1px solid --amber-hi` |
| Fault | ✕ | FAULT | `--hot` | `1px dashed --hot` |
| Retired / no data | — | RETIRED / NO LOG | `--amber-muted` | `1px solid --line`, 60% opacity |

Glyph + word + border style together, every time, so status survives color blindness, glare on a pit laptop, and grayscale printouts. Implemented as `.status--{ok,warn,fault,retired}` and the `.is-warn` / `.is-fault` box modifiers.

The same three levels map to anomaly severity (`high` ✕, `med` ▲, `low` ●) and to event log levels (`FAULT`, `WARN`, `INFO`).

### Readouts

Big numbers are VT323: `--fs-d3` (52px) in vitals, 96px for the Pit verdict, 48px for the match clock. Units sit beside the number in Plex Mono at `--fs-base`, `--amber-dim`. Decimal places are fixed per metric so digits don't jitter: voltage 2, current 1 (0 for totals), temperature 0–1, percent 0.

### Charts

All charts share one visual grammar (production: uPlot styled with tokens):

| Element | Style |
|---|---|
| Primary series | `--amber`, 1.5px (2px in focus/evidence views) |
| Selected series (multi-device) | `--amber-hi`, 2.5px, soft glow, drawn last |
| Other devices | `--line-strong`, 1.2px |
| Peer / sibling series | `--amber-dim`, dashed 2 3 |
| Temperature overlay | `--amber-dim`, dashed 5 3 |
| Limit / threshold | `--hot`, 1px dashed 4 3, labeled at the right edge |
| Expected band | 9% amber fill, `--amber-muted` dashed edges |
| Points outside band | 7px `--hot` squares |
| Cursor (scrubber, match clock) | 2px `--amber-hi` with glow |
| Match phases | Auto and endgame shaded at 5% amber |
| Event/season segments | thin `--line-soft` dividers with micro labels |

Every chart has a text equivalent: an `aria-label` summary plus a table or readout with the same numbers.

Sparkline y-ranges: in a grid of cards, use the metric's fixed range so cards are comparable. In a table's trend column, use the device's own range so its shape is readable. Say which in a tooltip.

### Components

**Vital** (`.vital`): label + status word, big readout, 20-second sparkline with threshold line, footnote (min this match, peak, overrun count). Goes `.is-fault` when its threshold has been crossed this match.

**Mechanism card** (`.mech`): a `<button>`. Name + status, amps + temperature + CAN ID, 20-second current sparkline (fixed range = limit × 1.1). Pressed = focused in the rail. Border follows status.

**Phase bar** (`.phasebar`): AUTO / TELEOP / ENDGAME segments sized to match timing from robot config, current phase filled, cursor at match time.

**Alert** (`.alert`): level · source, time, plain-language message, ACKNOWLEDGE (primary) and OPEN IN REPLAY. Acknowledged = solid line border, 55% opacity, ACKED.

**Event log** (`.log`): newest first, `T+` timestamp · level · source · message, trailing `listening ▌`.

**Track** (`.track`): Replay row with label, value at cursor, unit range, plot with phase shading, threshold line and shared cursor.

**Marker** (`.marker`): 36px button above the tracks at the event's time. ▲ solid for WARN, ✕ dashed for FAULT, filled when selected.

**Verdict** (`.verdict`): NOT CHECKED / CHECKING / NO-GO / DECIDE / GO in 96px VT323. GO = 2px amber border + glow. NO-GO = 2px dashed hot.

**Check row** (`.check`): glyph, name + detail, status word, action button when needed (CLEAR FAULTS, ACKNOWLEDGE / UNDO). Blinking cursor glyph while running.

**Fact tile** (`.fact`): micro label + value. Used for focus stats, device facts, anomaly facts.

**Lifeline** (`.lifeline`): vertical rule with square markers; amber for normal events, hot for anomalies, retirements and threshold crossings. Newest first.

**Anomaly item** (`.anom`): severity · device, method, plain-language title, first seen, state. High severity gets a dashed hot border.

**Pill** (`.pill`): robot link state, recording indicator (`.pill--rec`, hot, blinking dot).

## Voice

- Name problems the way the pit crew talks: "Indexer jam," "Brownout," "Running hot," not "Stator current threshold exceedance."
- Say what to do: every fault has a suggested fix or an action button.
- Label guesses as guesses: "Likely cause," "Causes are suggestions … not a diagnosis."
- Keep numbers honest and specific: "Heats 41% faster than Shooter R at the same load."
- Empty states direct: "All clear. Warnings and faults land here as they happen."
