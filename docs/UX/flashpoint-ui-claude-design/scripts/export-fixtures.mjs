#!/usr/bin/env node
/**
 * Regenerates data-contracts/examples/*.json from prototype/js/data.js so the
 * prototype, the MSW mocks and the schemas stay in lockstep.
 *   node scripts/export-fixtures.mjs && python3 scripts/validate-fixtures.py
 */
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const out = join(root, 'data-contracts', 'examples');
mkdirSync(out, { recursive: true });
const sb = { window: {} };
vm.runInNewContext(readFileSync(join(root, 'prototype/js/data.js'), 'utf8'), sb);
const FP = sb.window.FP;
const r2 = (v) => Math.round(v * 100) / 100;
const write = (n, o) => { writeFileSync(join(out, n), JSON.stringify(o, null, 2) + '\n'); console.log('wrote', n); };

/* live-snapshot at T+100 of Qual 42 */
const q42 = FP.matches.find((m) => m.id === 'q42'); const sim = FP.makeMatch(q42); const t = 100;
let minV = 99, over = 0; for (let x = 0; x <= t; x += 0.5) { minV = Math.min(minV, sim.volts(x)); if (sim.loop(x) > 20) over++; }
write('live-snapshot.json', {
  t, match: { label: 'Qual 42', alliance: 'Blue 2', phase: 'TELEOP', remaining: 150 - t, recording: 'Q42.wpilog' },
  vitals: { batteryV: r2(sim.volts(t)), minBatteryV: r2(minV), totalCurrentA: r2(sim.total(t)), canUtilPct: r2(sim.can(t)), loopMs: r2(sim.loop(t)), overruns: over, brownout: minV < FP.robot.brownoutV },
  mechanisms: FP.mechanisms.map((m) => ({ slot: m.id, serial: m.device, currentA: r2(sim.amps(m, t)), tempC: r2(sim.temp(m, t)), status: sim.status(m, t), faults: [] })),
  events: FP.q42Events.filter((e) => e.time <= t).map(({ id, time, level, src, msg }) => ({ id, time, level, src, msg })),
  link: { rxMs: 3.1, dropped: 0, rateHz: 50 }
});

/* match-log for Qual 42 (2 Hz tracks) */
const grid = []; for (let x = 0; x <= 150; x += 0.5) grid.push(x);
const peak = Math.round(Math.max(sim.total(96.5), sim.total(97), sim.total(97.5)));
write('match-log-q42.json', {
  id: 'q42', label: 'Qual 42', meta: 'L · battery B04', battery: 'B04', status: 'parsed', sourceFile: 'logs/event/Q42.wpilog',
  phases: FP.robot.phases,
  tracks: [
    { name: 'BATTERY', signal: '/Flashpoint/power/batteryVoltage', unit: 'V', range: [6, 13], threshold: { value: FP.robot.brownoutV, label: 'BROWNOUT 6.75' }, dt: 0.5, values: grid.map((x) => r2(sim.volts(x))) },
    { name: 'TOTAL CURRENT', signal: '/Flashpoint/power/totalCurrent', unit: 'A', range: [0, 500], threshold: { value: 400, label: '400 A' }, dt: 0.5, values: grid.map((x) => r2(sim.total(x))) }
  ],
  events: FP.eventsFor(q42).map((e) => ({ id: e.id, time: e.time, level: e.level, src: e.src, msg: e.msg, cause: e.cause ? e.cause.replace('{PEAK}', peak) : null, fix: e.fix || null }))
});

/* pit-check run (B04, sticky not cleared, firmware unacked) */
const checks = FP.pitChecks({ battery: 'B04', cleared: false });
write('pit-check.json', { runId: 'pit_demo_0001', startedAt: null, battery: 'B04',
  checks: checks.map((c) => ({ id: c.id, name: c.name, state: c.state, detail: c.detail, action: c.action || null, ackedBy: null, ackedAt: null })),
  verdict: checks.some((c) => c.state === 'fault') ? 'NO_GO' : (checks.some((c) => c.state === 'warn') ? 'DECIDE' : 'GO') });

/* devices + metrics */
const when = (i) => FP.whenOf(i);
write('devices.json', FP.devices.map((d) => ({
  serial: d.serial, name: d.name, kind: d.kind, model: d.model, retired: !!d.retired, summary: d.summary || null,
  installs: [{ robot: d.since < 44 ? '2025 robot' : '2026 robot', slot: d.name, fromMatch: when(d.since), toMatch: d.retired ? when(d.until - 1) : null }],
  life: d.life.map(([i, text]) => ({ when: when(i), kind: /Retired/.test(text) ? 'retire' : /Anomaly|Flagged/.test(text) ? 'anomaly' : /Firmware/.test(text) ? 'firmware' : /replaced|Bearing/.test(text) ? 'repair' : /Moved/.test(text) ? 'move' : /Crossed|climbing|Recommended/.test(text) ? 'threshold' : /Installed|Commissioned/.test(text) ? 'install' : 'note', text, author: null })),
  anomaly: d.anomaly || null })));
const dev = FP.devices.find((d) => d.id === 'm4'); const rows = [];
for (let i = 44; i < FP.N; i++) rows.push({ match: when(i), slot: 'drive_br', peakTempC: r2(FP.metrics.temp.fn(dev, i)), spinTestA: r2(FP.metrics.spin.fn(dev, i)), faultCount: FP.metrics.faults.fn(dev, i) });
write('device-metrics-m4.json', { serial: dev.serial, rows });

/* anomalies */
const K = 40;
write('anomalies.json', FP.anomalies.map((a) => ({
  id: a.id, serial: a.serial, dev: a.dev, severity: a.severity, method: a.method, state: a.state, firstSeen: a.firstSeen,
  title: a.title, metric: a.metric, unit: a.unit, actual: a.actual, expected: a.expected, matchesAffected: a.matchesAffected, confidence: a.confidence,
  evidence: { range: [a.lo, a.hi], band: [a.bandLo, a.bandHi], values: Array.from({ length: K }, (_, k) => r2(a.f(k))),
    peer: a.peer ? Array.from({ length: K }, (_, k) => r2(a.peer(k))) : null, peerName: a.peerName || null, xFrom: a.xFrom, xTo: a.xTo },
  why: a.why, causes: a.causes, similar: a.similar || null, resolution: a.resolution || null, history: [] })));
