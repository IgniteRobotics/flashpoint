# CLAUDE.md — Flashpoint web UI

You are building the web UI for **Flashpoint**, the telemetry tool for Ignite Robotics (FRC Team 6829). It has five views: **Live** (during a match), **Replay** (after a match), **Pit** (before a match), **History** (across seasons, per device) and **Anomalies** (devices outside spec).

Paths assume this handoff lives at `design/flashpoint-ui-handoff/`. Adjust if not.

**First task in any new session:** look at what already exists in this repo (robot-side logging, any existing Flashpoint code, log formats) and reconcile it with `docs/04`. Sections marked **(assumed)** were written without seeing the codebase. Where reality differs, reality wins; record the difference in `docs/DECISIONS.md`.

## Sources of truth (priority order)

1. The user's instructions in the current session.
2. `docs/05-implementation-plan.md` — what to build next and how to know it's done.
3. `docs/03-screen-specs.md` — per-screen behavior, states, keyboard.
4. `docs/02-design-system.md` + `design/tokens.json` — visuals. Shared with Mnemosyne; don't fork tokens.
5. `prototype/` — the visual and interaction reference. Docs win over prototype; flag disagreements.
6. `data-contracts/` — shapes the UI consumes. Propose contract changes; don't invent fields.

## Stack (proposed; confirm against the repo)

- **Vite + React 18 + TypeScript strict**, React Router.
- **Styling:** Tailwind with `design/tailwind.preset.js` + global `tokens.css`. No raw hex in components.
- **Charts:** **uPlot** for every time-series (live sparklines, replay tracks, history, evidence). It handles tens of thousands of points per series smoothly. Wrap it once (`<TimeSeries>`), style it with tokens.
- **Live data:** an NT4 client in the browser (for example `ntcore-ts-client`; check it's maintained) subscribed to the robot. Fallback: a small relay service that subscribes to NT4 and serves WebSocket/SSE to many viewers.
- **Command palette:** cmdk. **Server state:** TanStack Query.
- **Backend:** FastAPI (Python) for logs, devices, metrics, pit checks and anomalies. WPILog parsing with RobotPy's `wpiutil` DataLog reader. Analytics store: **DuckDB** (or SQLite to start).
- **Tests:** Vitest + Testing Library, Playwright + axe-core.

## Non-negotiables

- **Status language is fixed:** ● OK (amber-dim), ▲ WARN (amber-hi, solid border), ✕ FAULT (hot, dashed border). Always glyph + word + border style. Never color alone.
- **Devices are identified by serial number.** A robot slot (Drive FL, Shooter L) is where a device is installed for a span of time. History lines and anomalies attach to devices, not slots.
- **Live renders at about 2 Hz** regardless of the incoming rate, updates elements in place (keyboard focus must survive ticks), and re-renders lists only when their contents change.
- **Pit verdict rule:** any FAULT → NO-GO; else any unacknowledged WARN → DECIDE; else GO. Never block silently.
- **Every anomaly shows** its method, actual vs expected, evidence chart, why, likely causes, and past similar cases when they exist. Causes are labeled as suggestions, not diagnoses.
- **Honesty:** never display a value the robot or backend didn't produce. Demo/fixture data carries the DEMO DATA badge. Heuristic text ("likely cause") is labeled as heuristic.
- **Accessibility:** WCAG 2.2 AA, 44px targets, visible focus (`--amber-hi`), real elements, `aria-pressed` / `aria-current`, charts have text equivalents (aria-label summary + the readout table). Reduced motion: no blinking, no scanlines, no animated transitions.
- **Shareable URLs:** every view's state lives in the URL (see `docs/03`). A lead pasting a Replay link into Slack must land on the same match, moment and event.

## Working style

- Build phase by phase from `docs/05`; finish acceptance criteria before moving on; report what passed, what's stubbed, open questions.
- Keep `data-contracts/examples/` wired into MSW mocks so the UI runs without a robot or backend. Mock and real API share generated types.
- For Live, build a **log-replay NT4 simulator** early (replays a WPILog as if it were live) so Live can be developed and tested without a robot.
- Small commits; one component or behavior each. Ambiguity → match the prototype, log it in `docs/DECISIONS.md`, keep going.

## Don'ts

- No rounded corners, gradients, emoji, second accent color, or UI kits with their own look.
- Don't hardcode mechanism lists, CAN IDs, limits or thresholds in components. They come from robot config and the device registry.
- Don't compute anomaly scores or "likely causes" in the browser. The detector runs server-side; the UI renders results.
- Don't poll the robot from multiple tabs directly in competition; use one NT4 connection (or the relay). Field networks are fragile.
- Don't store anything sensitive in localStorage. UI preferences only (`ignite.scanlines` is shared with Mnemosyne on purpose).

## Commands to create in Phase 0

```
pnpm dev            # Vite + MSW mocks
pnpm dev:sim        # NT4 simulator replaying a WPILog fixture
pnpm dev:api        # FastAPI on :8000
pnpm test           # Vitest
pnpm e2e            # Playwright + axe
pnpm gen:types      # JSON Schema → src/types/contracts.ts
```
