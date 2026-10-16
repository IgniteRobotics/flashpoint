/* ==========================================================================
   Flashpoint demo data. ALL VALUES ARE ILLUSTRATIVE (shown under a DEMO badge).
   One robot, one story across every screen:
     - Shooter L runs hot (Live → Replay → History → Anomalies)
     - Indexer jams after DCMP Q12 (Live fault → Replay → Anomalies STEP)
     - Brownout in Qual 42 (Live → Replay)
     - Drive BR friction drift (History → Anomalies DRIFT/SIBLING)
     - Battery B03 aging (Pit → History → Anomalies SPEC)
   Shapes match /data-contracts/*.schema.json. Loaded as window.FP (works from file://).
   ========================================================================== */
(function () {
  const rndSeed = (seed) => (x) => { const s = Math.sin(x * 127.1 + 311.7 + seed * 17) * 43758.5453; return s - Math.floor(s); };
  const rnd = rndSeed(0);

  /* ---------- Robot configuration ---------- */
  const robot = { team: 6829, address: '10.68.29.2', nt4: 'ws://10.68.29.2:5810', loopHz: 50, matchLength: 150,
    phases: [{ id: 'AUTO', start: 0, end: 15 }, { id: 'TELEOP', start: 15, end: 130 }, { id: 'ENDGAME', start: 130, end: 150 }],
    brownoutV: 6.75 };

  /* Mechanism slots on the current robot. `device` = serial of what's installed now. */
  const mechanisms = [
    { id: 'drive_fl', name: 'Drive FL', can: 1, kind: 'drive', off: 0, limit: 80, heat: 0.2, device: '…3A17' },
    { id: 'drive_fr', name: 'Drive FR', can: 2, kind: 'drive', off: 0.6, limit: 80, heat: 0.2, device: '…3A52' },
    { id: 'drive_bl', name: 'Drive BL', can: 3, kind: 'drive', off: 1.2, limit: 80, heat: 0.19, device: '…3B09' },
    { id: 'drive_br', name: 'Drive BR', can: 4, kind: 'drive', off: 1.8, limit: 80, heat: 0.21, device: '…3B44' },
    { id: 'intake', name: 'Intake', can: 9, kind: 'intake', off: 0, limit: 40, heat: 0.12, device: '…5C11' },
    { id: 'indexer', name: 'Indexer', can: 10, kind: 'indexer', off: 0, limit: 60, heat: 0.15, device: '…0EC8' },
    { id: 'shooter_l', name: 'Shooter L', can: 11, kind: 'shooter', off: 0, limit: 70, heat: 0.38, device: '…7B42' },
    { id: 'shooter_r', name: 'Shooter R', can: 12, kind: 'shooter', off: 0.3, limit: 70, heat: 0.27, device: '…7B90' },
    { id: 'climber', name: 'Climber', can: 14, kind: 'climber', off: 0, limit: 80, heat: 0, device: '…6D73' }
  ];

  /* ---------- Match simulation (deterministic). Prototype only: production reads NT4 / WPILog. ---------- */
  function makeMatch(opts) {
    const r = rndSeed(opts.seed || 0);
    const shooting = (t) => Math.sin(t * 0.5 + 1 + (opts.seed || 0)) > 0.35;
    const surge = (t) => opts.brownout && Math.abs(t - 97) < 0.75;   /* drive accel + shooter spin-up overlap */
    const amps = (m, t) => {
      if (t < 0) return 0;
      const x = r(t * 3.1 + m.can);
      if (surge(t) && m.kind === 'drive') return 68 + 8 * x;
      if (surge(t) && m.kind === 'shooter') return 62 + 6 * x;
      if (m.kind === 'drive') return t > 130 ? 6 + 8 * x : 14 + 38 * Math.abs(Math.sin(t * 0.7 + m.off)) + 10 * x;
      if (m.kind === 'intake') return t < 130 && Math.sin(t * 0.35 + (opts.seed || 0)) > 0.2 ? 22 + 8 * x : 1.2;
      if (m.kind === 'indexer') return opts.jam && t >= 88 && t < 89.5 ? 60 : (t < 130 && shooting(t) ? 26 + 10 * x : 1.5);
      if (m.kind === 'shooter') return t > 130 ? 4 + x : (shooting(t + m.off) ? 44 + 10 * x : 9 + 3 * x);
      return t > 131.5 && t < 147 ? 48 + 18 * x : 0;
    };
    const heatOf = (m) => (m.id === 'shooter_l' ? opts.heatL : m.heat);
    const temp = (m, t) => m.kind === 'climber' ? 24 + (t > 131.5 ? (Math.min(t, 147) - 131.5) * 1.1 : 0) : 24 + heatOf(m) * Math.max(t, 0);
    const total = (t) => mechanisms.reduce((s, m) => s + amps(m, t), 6);
    const volts = (t) => 12.7 - 0.005 * t - 0.011 * total(t) - (surge(t) ? 1.2 : 0);
    const can = (t) => 52 + 14 * r(t * 3) + (Math.abs(t - 41.6) < 1 ? 12 : 0);
    const loop = (t) => 12 + 5 * r(t * 7) + (r(t * 11) > 0.95 ? 9 : 0);
    const status = (m, t) => {
      const T = temp(m, t), A = amps(m, t);
      if (T >= 75 || (m.kind === 'indexer' && opts.jam && t >= 88 && t < 89.5)) return 'fault';
      if (T >= 65 || A > 0.85 * m.limit) return 'warn';
      return 'ok';
    };
    return { amps, temp, total, volts, can, loop, status };
  }

  /* Events for Qual 42 (time, level, source, message) + replay analysis */
  const q42Events = [
    { id: 'start', time: 0.0, level: 'INFO', src: 'fms', msg: 'Match start · AUTO' },
    { id: 'auto1', time: 3.2, level: 'INFO', src: 'auto', msg: 'Routine "Center-4" started' },
    { id: 'auto2', time: 14.8, level: 'INFO', src: 'auto', msg: 'Routine complete · 4 of 4 segments' },
    { id: 'tele', time: 15.0, level: 'INFO', src: 'fms', msg: 'TELEOP' },
    { id: 'can', time: 41.6, level: 'WARN', src: 'can', msg: 'CAN utilization peaked at 78%',
      cause: 'Peak during heavy drive and intake activity. Sustained use above about 70% risks delayed or dropped status frames.',
      fix: 'Lower status-frame rates on devices that don’t need fast updates (intake, climber), or move high-rate devices to a CANivore bus.' },
    { id: 'loop', time: 63.0, level: 'WARN', src: 'loop', msg: 'Loop overrun: 26.4 ms',
      cause: 'A single overrun that lines up with a vision pipeline frame. Isolated; nothing similar before or after.',
      fix: 'No action now. If overruns cluster, move vision processing off the main loop.' },
    { id: 'jam', time: 88.0, level: 'FAULT', src: 'indexer', msg: 'Stator current limit held for 1.5 s',
      cause: 'The indexer sat at its 60 A limit with almost no motion while the shooter was idle. That pattern usually means a jammed game piece.',
      fix: 'Add jam detection: if current stays at the limit for 0.25 s, reverse briefly and retry. Inspect the indexer path for a pinch point.' },
    { id: 'brownout', time: 97.0, level: 'FAULT', src: 'power', msg: 'Brownout: outputs disabled 0.21 s',
      cause: 'Shooter spin-up overlapped hard drive acceleration. Total draw peaked near {PEAK} A, pulling the battery under 6.75 V.',
      fix: 'Stagger shooter spin-up from drive acceleration, set supply-current limits on the drive motors, and check battery B04’s internal resistance.' },
    { id: 'heat_w', time: 108.0, level: 'WARN', src: 'shooter_l', msg: 'Temperature 65 °C · warning threshold',
      cause: 'Shooter L has run hotter than Shooter R all match on the same commands, so the extra heat is likely mechanical: belt tension, a dragging bearing or a rubbing flywheel.',
      fix: 'Spin both shooters by hand in the pit and compare. Check belt tension and bearings on the left side before the next match.' },
    { id: 'endgame', time: 130.0, level: 'INFO', src: 'fms', msg: 'ENDGAME' },
    { id: 'climb1', time: 131.5, level: 'INFO', src: 'climber', msg: 'Climb sequence started' },
    { id: 'heat_f', time: 134.5, level: 'FAULT', src: 'shooter_l', msg: 'Temperature 75 °C · derating to 80% output',
      cause: 'Continued heating from the same left-side drag. The controller derated output to protect the motor.',
      fix: 'Same as the 65 °C warning. Consider a cool-down routine between matches until the mechanical issue is fixed.' },
    { id: 'climb2', time: 147.0, level: 'INFO', src: 'climber', msg: 'Climber at target' },
    { id: 'end', time: 150.0, level: 'INFO', src: 'fms', msg: 'Match end' }
  ];

  const matches = [
    { id: 'p3', label: 'Practice 3', meta: 'Fri · battery B02', battery: 'B02', seed: 7.7, heatL: 0.25, brownout: false, jam: false },
    { id: 'q38', label: 'Qual 38', meta: 'W · battery B01', battery: 'B01', seed: 3.3, heatL: 0.30, brownout: false, jam: false },
    { id: 'q42', label: 'Qual 42', meta: 'L · battery B04', battery: 'B04', seed: 0, heatL: 0.38, brownout: true, jam: true },
    { id: 'q47', label: 'Qual 47', meta: 'queued · battery [TBD]', battery: null, seed: 9.1, heatL: 0, brownout: false, jam: false, empty: true }
  ];
  const eventsFor = (m) => {
    if (m.empty) return [];
    const sim = makeMatch(m), sl = mechanisms[6];
    return q42Events.filter((e) => (e.id !== 'brownout' || m.brownout) && (e.id !== 'jam' || m.jam) &&
      (e.id !== 'heat_w' || sim.temp(sl, 108) >= 64.5) && (e.id !== 'heat_f' || sim.temp(sl, 134.5) >= 74.5));
  };

  /* ---------- Pit ---------- */
  const batteries = [
    { id: 'B01', restV: 12.88, irMilliOhm: 12, cycles: 41 },
    { id: 'B02', restV: 12.71, irMilliOhm: 13, cycles: 38 },
    { id: 'B03', restV: 12.35, irMilliOhm: 19, cycles: 77 },
    { id: 'B04', restV: 12.94, irMilliOhm: 14, cycles: 22 },
    { id: 'B05', restV: 12.97, irMilliOhm: 11, cycles: 9 },
    { id: 'B07', restV: 12.20, irMilliOhm: 26, cycles: 112 }
  ];
  const batteryGrade = (b) => (b.irMilliOhm > 20 ? 'fault' : (b.irMilliOhm > 15 || b.restV < 12.6 ? 'warn' : 'ok'));
  const pitChecks = (ctx) => {
    const B = batteries.find((b) => b.id === ctx.battery) || batteries[3];
    const bg = batteryGrade(B);
    return [
      { id: 'can', name: 'CAN bus', state: 'ok', detail: '24 of 24 devices responding · 0 bus-off events' },
      { id: 'fw', name: 'Firmware', state: 'warn', detail: 'Shooter R on 26.1.0.0; all other TalonFX on 26.1.1.1', action: 'ack' },
      { id: 'sticky', name: 'Sticky faults', state: ctx.cleared ? 'ok' : 'fault',
        detail: ctx.cleared ? 'None. Cleared after review.' : 'Indexer: stator current limit (from Qual 42). Review in Replay, then clear.', action: ctx.cleared ? null : 'clear' },
      { id: 'battery', name: 'Battery ' + B.id, state: bg, action: bg === 'warn' ? 'ack' : null,
        detail: B.restV.toFixed(2) + ' V resting · ' + B.irMilliOhm + ' mΩ · ' + B.cycles + ' cycles' + (bg === 'fault' ? ' · Swap battery: pick a healthy one from the fleet.' : '') },
      { id: 'temps', name: 'Motor temps', state: 'ok', detail: 'All motors under 45 °C · hottest Shooter L at 43 °C' },
      { id: 'radio', name: 'Radio + driver station', state: 'ok', detail: 'Link 2.1 ms · 0 dropped packets · firmware current' },
      { id: 'vision', name: 'Vision', state: 'ok', detail: '2 cameras · pipelines loaded · 30 fps' },
      { id: 'homing', name: 'Mechanism homing', state: 'ok', detail: 'Climber and intake at home position' }
    ];
  };

  /* ---------- History: two seasons of events, devices tracked by serial ---------- */
  const segments = [
    { label: '2025 · DIST 1', n: 12, kind: 'event' }, { label: '2025 · DIST 2', n: 12, kind: 'event' },
    { label: '2025 · DCMP', n: 14, kind: 'event' }, { label: '2025 · OFF', n: 6, kind: 'off' },
    { label: '2026 · DIST 1', n: 14, kind: 'event' }, { label: '2026 · DIST 2', n: 14, kind: 'event' },
    { label: '2026 · DCMP', n: 16, kind: 'event' }, { label: '2026 · OFF', n: 8, kind: 'off' }
  ];
  let acc = 0; segments.forEach((s) => { s.start = acc; acc += s.n; });
  const N = acc;
  const segOf = (i) => segments.reduce((k, s, j) => (i >= s.start ? j : k), 0);
  const whenOf = (i) => { const s = segments[segOf(i)]; return s.label + ' · M' + (i - s.start + 1); };
  const ranges = { '2025': [0, 44], '2026': [44, N], all: [0, N] };

  const devices = [
    { id: 'm1', serial: '…3A17', name: 'Drive FL', kind: 'motor', model: 'Kraken X60', since: 0, until: N, base: 2.05, tb: 46, td: 0.03, extra: (i) => (i < 38 ? i * 0.012 : 0),
      life: [[0, 'Installed · Drive FL (2025 robot)'], [30, 'Flagged: spin-test current drifting up'], [38, 'Bearing replaced · back in band'], [44, 'Moved with its swerve module to the 2026 robot'], [70, 'Firmware 26.1.1.1']],
      summary: 'Two robots, one motor. It drifted in 2025, got a new bearing, and has stayed in band since.' },
    { id: 'm2', serial: '…3A52', name: 'Drive FR', kind: 'motor', model: 'Kraken X60', since: 0, until: N, base: 2.1, tb: 47, td: 0.03, extra: () => 0,
      life: [[0, 'Installed · Drive FR (2025 robot)'], [44, 'Moved with its swerve module to the 2026 robot'], [70, 'Firmware 26.1.1.1']] },
    { id: 'm3', serial: '…3B09', name: 'Drive BL', kind: 'motor', model: 'Kraken X60', since: 0, until: N, base: 2.0, tb: 46, td: 0.03, extra: () => 0,
      life: [[0, 'Installed · Drive BL (2025 robot)'], [44, 'Moved with its swerve module to the 2026 robot'], [70, 'Firmware 26.1.1.1']] },
    { id: 'm4', serial: '…3B44', name: 'Drive BR', kind: 'motor', model: 'Kraken X60', since: 0, until: N, base: 2.08, tb: 47, td: 0.035, extra: (i) => (i > 72 ? (i - 72) * 0.027 : 0), anomaly: 'a2',
      life: [[0, 'Installed · Drive BR (2025 robot)'], [44, 'Moved with its swerve module to the 2026 robot'], [70, 'Firmware 26.1.1.1'], [76, 'Anomaly opened: spin-test current climbing']],
      summary: 'Flat for a season and a half, then spin-test current started climbing at 2026 District Champs. The other three modules didn’t move.' },
    { id: 'm5', serial: '…0EC8', name: 'Indexer', kind: 'motor', model: 'Kraken X60', since: 44, until: N, base: 1.7, tb: 40, td: 0.06, extra: () => 0, anomaly: 'a4',
      life: [[44, 'Installed new · Indexer (2026 robot)'], [70, 'Firmware 26.1.1.1'], [84, 'Anomaly opened: current-limit hits stepped up']],
      summary: 'Healthy temperatures, but current-limit hits jumped after DCMP Qual 12.' },
    { id: 'm6', serial: '…7B42', name: 'Shooter L', kind: 'motor', model: 'Kraken X60', since: 58, until: N, base: 2.3, tb: 54, td: 0.36, extra: () => 0, anomaly: 'a1',
      life: [[58, 'Installed new · Shooter L (replaced …19D0)'], [64, 'Anomaly opened: heating faster than Shooter R'], [70, 'Firmware 26.1.1.1']],
      summary: 'Replaced the old shooter motor mid-season and is already running hot. Same pattern as the motor it replaced.' },
    { id: 'm7', serial: '…7B90', name: 'Shooter R', kind: 'motor', model: 'Kraken X60', since: 44, until: N, base: 2.25, tb: 52, td: 0.05, extra: () => 0,
      life: [[44, 'Installed new · Shooter R (2026 robot)'], [70, 'Firmware 26.1.0.0 (behind fleet)']] },
    { id: 'm8', serial: '…5C11', name: 'Intake', kind: 'motor', model: 'Kraken X60', since: 44, until: N, base: 1.9, tb: 42, td: 0.04, extra: () => 0, anomaly: 'a5',
      life: [[44, 'Installed new · Intake (2026 robot)'], [70, 'Firmware 26.1.1.1'], [90, 'Anomaly opened: noisy idle current']] },
    { id: 'm9', serial: '…6D73', name: 'Climber', kind: 'motor', model: 'Kraken X60', since: 44, until: N, base: 2.2, tb: 44, td: 0.04, extra: () => 0,
      life: [[44, 'Installed new · Climber (2026 robot)'], [60, 'Gearbox ratio changed · spec updated'], [70, 'Firmware 26.1.1.1']] },
    { id: 'm10', serial: '…19D0', name: 'Shooter …19D0', kind: 'motor', model: 'Kraken X60', since: 0, until: 58, base: 2.3, tb: 50, td: 0.12, extra: (i) => (i > 48 ? (i - 48) * 0.05 : 0), retired: true,
      life: [[0, 'Installed · Shooter (2025 robot)'], [44, 'Moved to the 2026 robot · Shooter L'], [50, 'Temps climbing match over match'], [57, 'Retired · bearing noise and heat']],
      summary: 'Retired. Its last ten matches show the slow heat climb that now shows up on its replacement.' },
    { id: 'b1', serial: 'B01', name: 'B01', kind: 'battery', model: 'SLA 12 V 18 Ah', since: 0, until: N, ir0: 11, slope: 0.03, life: [[0, 'Commissioned']] },
    { id: 'b2', serial: 'B02', name: 'B02', kind: 'battery', model: 'SLA 12 V 18 Ah', since: 0, until: N, ir0: 12, slope: 0.012, life: [[0, 'Commissioned']] },
    { id: 'b3', serial: 'B03', name: 'B03', kind: 'battery', model: 'SLA 12 V 18 Ah', since: 0, until: N, ir0: 13, slope: 0.065, anomaly: 'a3',
      life: [[0, 'Commissioned'], [62, 'Crossed 15 mΩ match spec'], [80, 'Anomaly opened: resistance climbing']],
      summary: 'Steady aging all season. Past the 15 mΩ match spec; on track to hit 20 mΩ in about 12 cycles.' },
    { id: 'b4', serial: 'B04', name: 'B04', kind: 'battery', model: 'SLA 12 V 18 Ah', since: 44, until: N, ir0: 12, slope: 0.04, life: [[44, 'Commissioned']] },
    { id: 'b5', serial: 'B05', name: 'B05', kind: 'battery', model: 'SLA 12 V 18 Ah', since: 80, until: N, ir0: 11, slope: 0.01, life: [[80, 'Commissioned']] },
    { id: 'b6', serial: 'B06', name: 'B06', kind: 'battery', model: 'SLA 12 V 18 Ah', since: 0, until: 40, ir0: 12, slope: 0.03, retired: true, life: [[0, 'Commissioned'], [39, 'Retired · cracked case after a drop']] },
    { id: 'b7', serial: 'B07', name: 'B07', kind: 'battery', model: 'SLA 12 V 18 Ah', since: 0, until: N, ir0: 15, slope: 0.11, life: [[0, 'Commissioned'], [46, 'Crossed 20 mΩ: practice only'], [92, 'Recommended for retirement']],
      summary: 'Past the retirement line since early 2026. Practice use only.' }
  ];

  const metrics = {
    spin:   { kind: 'motor', label: 'SPIN TEST', title: 'PIT SPIN-TEST CURRENT', hint: 'amps at 50% duty, robot on blocks', lo: 1.4, hi: 3.0, spec: 2.6, specLabel: 'SPEC 2.6 A', unit: 'A', dec: 2,
              fn: (d, i) => d.base + d.extra(i) + 0.08 * (rnd(i * 1.7 + d.base * 9) - 0.5) },
    temp:   { kind: 'motor', label: 'PEAK TEMP', title: 'PEAK MOTOR TEMPERATURE', hint: '°C, hottest point each match', lo: 30, hi: 90, spec: 65, specLabel: 'WARN 65 °C', unit: '°C', dec: 0,
              fn: (d, i) => d.tb + d.td * (i - d.since) + (d.id === 'm10' && i > 48 ? (i - 48) * 1.2 : 0) + 5 * rnd(i * 2.3 + d.tb) - (segments[segOf(i)].kind === 'off' ? 4 : 0) },
    faults: { kind: 'motor', label: 'FAULTS', title: 'FAULTS PER MATCH', hint: 'sticky + live, all types', lo: 0, hi: 4, spec: 3, specLabel: '3 / MATCH', unit: '', dec: 0,
              fn: (d, i) => (rnd(i * 3.9 + d.base * 7) > 0.93 ? 1 : 0) + (d.id === 'm5' && i >= 84 ? (rnd(i) > 0.3 ? 1 : 0) + (rnd(i * 2) > 0.6 ? 1 : 0) : 0) + (d.id === 'm6' && i > 80 && rnd(i * 5) > 0.6 ? 1 : 0) },
    ir:     { kind: 'battery', label: 'RESISTANCE', title: 'BATTERY INTERNAL RESISTANCE', hint: 'mΩ, tested before each match', lo: 8, hi: 28, spec: 20, specLabel: 'RETIRE 20 mΩ', unit: 'mΩ', dec: 1,
              fn: (d, i) => d.ir0 + d.slope * (i - d.since) + 0.6 * (rnd(i * 1.3 + d.ir0) - 0.5) },
    sag:    { kind: 'battery', label: 'MIN VOLTAGE', title: 'LOWEST VOLTAGE UNDER LOAD', hint: 'V, worst moment each match', lo: 6, hi: 11, spec: 7.0, specLabel: 'DANGER 7.0 V', unit: 'V', dec: 2,
              fn: (d, i) => 9.6 - 0.11 * (d.ir0 + d.slope * (i - d.since) - 11) + 0.4 * (rnd(i * 2.9 + d.ir0) - 0.5) }
  };

  /* ---------- Anomalies ---------- */
  const methods = {
    SPEC: 'outside a fixed limit from the vendor or the team',
    DRIFT: 'slowly moving away from its own baseline',
    SIBLING: 'different from identical devices doing the same job',
    STEP: 'changed suddenly after a specific match',
    NOISE: 'erratic when it should be steady'
  };
  const anomalies = [
    { id: 'a1', device: 'm6', dev: 'Shooter L', serial: '…7B42', severity: 'high', method: 'SIBLING', state: 'new', firstSeen: '2026 · DIST 2 M7',
      title: 'Heats 41% faster than Shooter R at the same load, and the gap is widening.',
      metric: '°C of temperature rise per 100 A·s of work', actual: '2.4', expected: '1.6 – 1.8', unit: '°C / 100 A·s', matchesAffected: 24, confidence: 'high',
      lo: 1.0, hi: 3.0, bandLo: 1.6, bandHi: 1.8, xFrom: '2026 · DIST 2 M1', xTo: '2026 · OFF M8', peerName: 'Shooter R',
      f: (k) => 1.7 + (k > 6 ? (k - 6) * 0.021 : 0) + 0.06 * (rnd(k * 1.3) - 0.5), peer: (k) => 1.7 + 0.05 * (rnd(k * 2.1) - 0.5),
      why: 'Both shooters get the same commands, so they should heat at the same rate. Since its seventh match, Shooter L has heated faster per unit of work than Shooter R every match, and the difference keeps growing.',
      causes: ['Belt over-tensioned on the left side', 'Dragging bearing or rubbing flywheel', 'Motor wearing out early'],
      similar: 'The motor this one replaced (SN …19D0) showed the same climb for nine matches before it was retired with bearing noise. If the cause is in the mount or belt path rather than the motor, a new motor will not fix it.' },
    { id: 'a4', device: 'm5', dev: 'Indexer', serial: '…0EC8', severity: 'high', method: 'STEP', state: 'new', firstSeen: '2026 · DCMP M12',
      title: 'Current-limit hits jumped from almost none to 1.4 per match after District Champs Qual 12.',
      metric: 'stator current-limit events per match', actual: '1.4', expected: '≤ 0.2', unit: '/ match', matchesAffected: 12, confidence: 'high',
      lo: 0, hi: 3, bandLo: 0, bandHi: 0.2, xFrom: '2026 · DIST 2 M10', xTo: '2026 · OFF M8', peerName: '',
      f: (k) => (k < 20 ? (rnd(k * 3.3) > 0.9 ? 1 : 0) * 0.3 : 1.0 + 0.8 * rnd(k * 1.9)), peer: null,
      why: 'This is a sudden change, not a slow drift. Something happened around Qual 12, and every match since has had at least one jam-like current spike. Before that, the indexer went whole events without one.',
      causes: ['A mechanical change or hit around Q12: check the pit log for that day', 'A belt or roller slipping since then', 'Worn game pieces jamming more often'], similar: '' },
    { id: 'a2', device: 'm4', dev: 'Drive BR', serial: '…3B44', severity: 'med', method: 'DRIFT', state: 'investigating', firstSeen: '2026 · DCMP M4',
      title: 'Spin-test current up 31% since District Champs. The other three modules stayed flat.',
      metric: 'pit spin-test current, amps at 50% duty', actual: '2.72 A', expected: '2.0 – 2.2 A', unit: 'A', matchesAffected: 24, confidence: 'med',
      lo: 1.6, hi: 3.0, bandLo: 2.0, bandHi: 2.2, xFrom: '2026 · DIST 2 M1', xTo: '2026 · OFF M8', peerName: 'Drive FL / FR / BL average',
      f: (k) => 2.1 + (k > 14 ? (k - 14) * 0.024 : 0) + 0.05 * (rnd(k * 1.7) - 0.5), peer: (k) => 2.06 + 0.04 * (rnd(k * 2.7) - 0.5),
      why: 'The weekly spin test runs every module at the same duty cycle with the robot on blocks, so friction shows up as extra current. Drive BR has climbed steadily since District Champs while its three siblings did not move, which points to that one module.',
      causes: ['Wheel bearing wear', 'Debris or carpet fiber in the gear mesh', 'A bent bevel gear or misaligned module'],
      similar: 'Drive FL showed the same drift in 2025. A new wheel bearing put it back in band within one event, and it has stayed there for two robots.' },
    { id: 'a3', device: 'b3', dev: 'Battery B03', serial: 'B03', severity: 'med', method: 'SPEC', state: 'new', firstSeen: '2026 · DIST 2 M4',
      title: 'Internal resistance is 19 mΩ, past the 15 mΩ match spec, and rising about 0.065 mΩ per cycle.',
      metric: 'internal resistance before each match, mΩ', actual: '19.2 mΩ', expected: '≤ 15 mΩ', unit: 'mΩ', matchesAffected: 30, confidence: 'high',
      lo: 8, hi: 24, bandLo: 8, bandHi: 15, xFrom: '2026 · DIST 1 M1', xTo: '2026 · OFF M8', peerName: '',
      f: (k) => 15.0 + k * 0.105 + 0.3 * (rnd(k * 1.1) - 0.5), peer: null,
      why: 'This battery has aged steadily all season. It passed the 15 mΩ match spec mid-season and, at its current rate, reaches the 20 mΩ retirement line in roughly 12 more cycles. Higher resistance means deeper voltage sag under the same load.',
      causes: ['Normal aging for a battery with 90+ cycles', 'Deep discharges at events speed it up'], similar: 'B07 followed the same curve a year earlier and is now practice-only.' },
    { id: 'a5', device: 'm8', dev: 'Intake', serial: '…5C11', severity: 'low', method: 'NOISE', state: 'new', firstSeen: '2026 · OFF M4',
      title: 'Idle current is three times noisier than usual while the intake is commanded off.',
      metric: 'standard deviation of current at idle, amps', actual: '0.9 A', expected: '≤ 0.3 A', unit: 'A', matchesAffected: 5, confidence: 'low',
      lo: 0, hi: 1.4, bandLo: 0, bandHi: 0.3, xFrom: '2026 · DCMP M1', xTo: '2026 · OFF M8', peerName: '',
      f: (k) => (k < 34 ? 0.18 + 0.06 * rnd(k * 2.3) : 0.75 + 0.3 * rnd(k * 1.7)), peer: null,
      why: 'A motor that is commanded off should draw flat current. Short spikes at idle usually mean an intermittent electrical connection rather than a mechanical problem. Only five matches so far, so confidence is low.',
      causes: ['Loose power lug or connector', 'Motor lead damaged near a pinch point', 'Encoder or signal cable chafing'], similar: '' },
    { id: 'a8', device: 'm7', dev: 'Shooter R', serial: '…7B90', severity: 'low', method: 'DRIFT', state: 'new', firstSeen: '2026 · OFF M2', strictOnly: true,
      title: 'Peak temperature trending up about 4% over the offseason.',
      metric: 'peak temperature per match, °C', actual: '58 °C', expected: '50 – 57 °C', unit: '°C', matchesAffected: 6, confidence: 'low',
      lo: 40, hi: 70, bandLo: 50, bandHi: 57, xFrom: '2026 · DCMP M1', xTo: '2026 · OFF M8', peerName: '',
      f: (k) => 53 + (k > 30 ? (k - 30) * 0.5 : 0) + 2 * (rnd(k * 1.9) - 0.5), peer: null,
      why: 'A small upward trend that only shows at strict sensitivity. Worth watching, not acting on.',
      causes: ['Warmer venue temperatures in the offseason', 'Early wear'], similar: '' },
    { id: 'a6', device: 'm1', dev: 'Drive FL', serial: '…3A17', severity: 'med', method: 'DRIFT', state: 'resolved', firstSeen: '2025 · DCMP M6',
      title: 'Spin-test current drifted up 24% over the 2025 season.',
      metric: 'pit spin-test current, amps at 50% duty', actual: '2.48 A (peak)', expected: '2.0 – 2.2 A', unit: 'A', matchesAffected: 14, confidence: 'med',
      lo: 1.6, hi: 3.0, bandLo: 2.0, bandHi: 2.2, xFrom: '2025 · DIST 2 M1', xTo: '2026 · DIST 1 M8', peerName: 'Other modules average',
      f: (k) => (k < 26 ? 2.05 + k * 0.017 : 2.07) + 0.04 * (rnd(k * 1.5) - 0.5), peer: (k) => 2.06 + 0.04 * (rnd(k * 2.5) - 0.5),
      why: 'Same pattern as Drive BR today: one module slowly pulling more current than its siblings in the spin test.',
      causes: ['Wheel bearing wear (confirmed)'], similar: '',
      resolution: 'Wheel bearing replaced during the 2025 offseason. Back in band the next spin test and stable across both robots since.' },
    { id: 'a7', device: 'm9', dev: 'Climber', serial: '…6D73', severity: 'low', method: 'SPEC', state: 'dismissed', firstSeen: '2026 · DIST 2 M3',
      title: 'Peak current of 92 A during climbs, above the 80 A spec.',
      metric: 'peak current during climb, amps', actual: '92 A', expected: '≤ 80 A (old spec)', unit: 'A', matchesAffected: 8, confidence: 'high',
      lo: 40, hi: 110, bandLo: 40, bandHi: 80, xFrom: '2026 · DIST 1 M1', xTo: '2026 · DIST 2 M14', peerName: '',
      f: (k) => (k < 16 ? 72 + 6 * rnd(k * 1.3) : 89 + 5 * rnd(k * 1.7)), peer: null,
      why: 'Climb current stepped up after a mid-season change.', causes: ['Gearbox ratio change (confirmed)'], similar: '',
      resolution: 'Dismissed as expected: the climber gearbox ratio changed between events. Spec raised to 100 A, so this no longer flags.' }
  ];

  window.FP = { meta: { demo: true, season: '2026' }, robot, mechanisms, makeMatch, matches, q42Events, eventsFor,
    batteries, batteryGrade, pitChecks, segments, N, segOf, whenOf, ranges, devices, metrics, methods, anomalies, rnd };
})();
