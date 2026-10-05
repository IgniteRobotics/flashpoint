# Flashpoint UI handoff

Design specs, tokens, data contracts and a working clickable prototype for the Flashpoint web UI: Ignite Robotics' telemetry tool for live match monitoring, log replay, pit checks, season-long device history and anomaly detection.

Flashpoint is a sibling to Mnemosyne and shares the Ignite design system (warm black, phosphor amber, square corners). The tokens here are identical to Mnemosyne's on purpose.

## What's in the box

```
flashpoint-ui-handoff/
├── README.md                     you are here
├── CLAUDE.md                     instructions for Claude Code (read first if you're Claude)
├── docs/
│   ├── 01-product-brief.md       who it's for, the five screens, principles, v1 done
│   ├── 02-design-system.md       Flashpoint additions to the Ignite system: status language, readouts, charts
│   ├── 03-screen-specs.md        Live, Replay, Pit, History, Anomalies: layout, data, states, keys
│   ├── 04-architecture-and-data.md  live data path, log pipeline, device registry, anomaly detection, API
│   └── 05-implementation-plan.md phased build plan with acceptance criteria
├── design/
│   ├── tokens.json               shared Ignite tokens (source of truth)
│   └── tailwind.preset.js        Tailwind preset generated from the tokens
├── data-contracts/
│   ├── live-snapshot.schema.json     one render tick of live data
│   ├── match-log.schema.json         a parsed match: tracks, events, analysis
│   ├── pit-check.schema.json         a pit check run and its verdict
│   ├── device.schema.json            device registry entry + install history (by serial)
│   ├── device-metrics.schema.json    per-match aggregate metrics per device
│   ├── anomaly.schema.json           a detected anomaly with evidence
│   └── examples/                     fixtures exported from the prototype's data
├── prototype/                    zero-build HTML/CSS/JS prototype of all five screens
│   ├── index.html live.html replay.html pit.html history.html anomalies.html
│   ├── css/ tokens.css base.css components.css screens.css
│   └── js/  data.js shell.js live.js replay.js pit.js history.js anomalies.js
└── scripts/
    ├── export-fixtures.mjs       regenerates data-contracts/examples from prototype/js/data.js
    └── validate-fixtures.py      checks the examples against the schemas (pip install jsonschema)
```

## Look at the prototype

Open `prototype/index.html` in a browser. No server, no build.

Things to try:

- **Live:** hit 4× and watch the indexer jam, the brownout, and Shooter L overheat. Click any mechanism card to focus it. Acknowledge an alert, then follow OPEN IN REPLAY.
- **Replay:** click the fault markers above the timeline, drag the scrubber, switch to Qual 38 (clean) and back. COPY LINK gives a URL that reopens this exact moment.
- **Pit:** RUN CHECKS → NO-GO. Clear the sticky fault and acknowledge the firmware warning → GO. Put B07 in the robot → NO-GO again.
- **History:** ALL TIME + PEAK TEMP, then select the retired shooter motor `…19D0`. Switch to BATTERIES.
- **Anomalies:** set STRICT, open Drive BR, create a pit task and mark it resolved, then check the RESOLVED tab.
- **Ctrl K / ⌘ K** anywhere: try `drive br`, `b03`, `qual 42`.

The same five problems show up consistently across every screen; that's the point of the demo data. Everything is under a DEMO DATA badge.

## Hand it to Claude Code

1. Copy this folder into the Flashpoint repo, for example as `design/flashpoint-ui-handoff/`.
2. Put `CLAUDE.md` at the repo root (or merge it) and fix the paths at the top if needed.
3. Start with: *"Read CLAUDE.md and docs/05-implementation-plan.md, then do Phase 0."*

## Read this before building

`docs/04-architecture-and-data.md` marks every assumption about Flashpoint's existing code and the robot software with **(assumed)**. This handoff was designed without access to Flashpoint's current codebase, so those sections are a proposal to reconcile with what exists, not a description of it.

The two ideas the design leans on hardest:

1. **Track devices by serial number, not robot slot.** Motors and batteries move between slots and robots. History and anomalies only make sense per physical device.
2. **A standard pit spin test.** Running each mechanism at a fixed duty cycle on blocks before every event gives a clean, comparable friction baseline that match data can't. It's what makes drift detection reliable.
