/* ==========================================================================
   Device history. Core rule: lines follow the PHYSICAL DEVICE (serial number),
   not the robot slot. A slot is just where a device lived for a while.
   Production: per-match aggregates from the metrics store (docs/04, device-metrics schema).
   URL: history.html?type=motors|batteries&range=2025|2026|all&metric=<key>&device=<id>
   ========================================================================== */
(function () {
  const { FP, esc, clamp, params, setParams, statusTag } = window.UI;
  const $ = (id) => document.getElementById(id);
  const p = params();
  const S = { type: p.get('type') || 'motors', range: p.get('range') || '2026', metric: p.get('metric'), device: p.get('device') };
  const kindOf = (t) => (t === 'motors' ? 'motor' : 'battery');
  const metricKeys = () => Object.keys(FP.metrics).filter((k) => FP.metrics[k].kind === kindOf(S.type));
  const devs = () => FP.devices.filter((d) => d.kind === kindOf(S.type));
  const normalize = () => {
    if (!metricKeys().includes(S.metric)) S.metric = metricKeys()[0];
    if (!devs().some((d) => d.id === S.device)) S.device = S.type === 'motors' ? 'm4' : 'b3';
  };

  const btn = (attrs, label, on) => `<button type="button" class="btn btn--sm" ${attrs} aria-pressed="${on}">${label}</button>`;

  function health(d) {
    if (d.retired) return 'retired';
    const i = Math.min(d.until, FP.N) - 1;
    if (d.kind === 'motor') return (FP.metrics.spin.fn(d, i) > 2.6 || FP.metrics.temp.fn(d, i) > 70 || d.anomaly) ? 'warn' : 'ok';
    const ir = FP.metrics.ir.fn(d, i);
    return ir > 20 ? 'fault' : (ir > 15 ? 'warn' : 'ok');
  }
  const HEALTH_WORD = { ok: 'GOOD', warn: 'WATCH', fault: 'RETIRE', retired: 'RETIRED' };

  function render() {
    normalize();
    setParams({ type: S.type, range: S.range, metric: S.metric, device: S.device });
    const [r0, r1] = FP.ranges[S.range];
    const MX = FP.metrics[S.metric];
    const D = devs();
    const sel = D.find((d) => d.id === S.device);
    const idx = (d) => { const out = []; for (let i = Math.max(d.since, r0); i < Math.min(d.until, r1); i++) out.push(i); return out; };
    const span = Math.max(r1 - r0 - 1, 1);
    const X = (i) => ((i - r0) / span * 1000).toFixed(1);
    const Y = (v) => (260 - (clamp(v, MX.lo, MX.hi) - MX.lo) / (MX.hi - MX.lo) * 260).toFixed(1);

    $('ranges').innerHTML = [['2025', '2025'], ['2026', '2026'], ['all', 'ALL TIME']].map(([k, l]) => btn(`data-range="${k}"`, l, S.range === k)).join('');
    $('types').innerHTML = [['motors', 'MOTORS'], ['batteries', 'BATTERIES']].map(([k, l]) => btn(`data-type="${k}"`, l, S.type === k)).join('');
    const faults = FP.devices.filter((d) => d.kind === 'motor').reduce((s, d) => s + idx(d).reduce((t, i) => t + FP.metrics.faults.fn(d, i), 0), 0);
    const retired = FP.devices.filter((d) => d.retired && d.until > r0 && d.until <= r1).length;
    $('stats').innerHTML = [['Devices tracked', FP.devices.length], ['Matches', r1 - r0], ['Motor faults', faults], ['Retired', retired]]
      .map(([k, v]) => `<div><div class="group__label">${k.toUpperCase()}</div><div class="clock__time" style="font-size:36px">${v}</div></div>`).join('');
    $('metrics').innerHTML = metricKeys().map((k) => btn(`data-metric="${k}"`, FP.metrics[k].label, k === S.metric)).join('');
    $('metric-title').innerHTML = `${MX.title} <span class="h-section__aside">per match · ${esc(MX.hint)}</span>`;
    $('yhi').textContent = MX.hi + (MX.unit ? ' ' + MX.unit : '');
    $('ylo').textContent = MX.lo + (MX.unit ? ' ' + MX.unit : '');

    /* chart: event bands, spec line, one polyline per device (selected drawn last) */
    const bands = FP.segments.filter((s) => s.start < r1 && s.start + s.n > r0).map((s) => {
      const a = Math.max(s.start, r0), b = Math.min(s.start + s.n, r1);
      const w = (b - a) / (r1 - r0) * 100;
      return `<div class="chart__seg ${s.kind === 'off' ? 'is-off' : ''}" style="left:${(a - r0) / (r1 - r0) * 100}%;width:${w}%" title="${s.label}"><span>${w < 9 ? s.label.split(' · ')[1] : s.label}</span></div>`;
    }).join('');
    const specTop = 24 + (260 - (MX.spec - MX.lo) / (MX.hi - MX.lo) * 260) * (276 / 260);
    const lines = D.filter((d) => idx(d).length > 1).sort((a, b) => (a.id === sel.id) - (b.id === sel.id))
      .map((d) => `<polyline class="line ${d.id === sel.id ? 'line--sel' : 'line--dim'}" points="${idx(d).map((i) => X(i) + ',' + Y(MX.fn(d, i))).join(' ')}"></polyline>`).join('');
    $('chart').innerHTML = bands + `<div class="chart__spec" style="top:${specTop.toFixed(1)}px"><span>${MX.specLabel}</span></div>`
      + `<svg class="spark" viewBox="0 0 1000 260" preserveAspectRatio="none" aria-hidden="true">${lines}</svg>`;
    $('chart').setAttribute('aria-label', `${MX.title} for ${D.length} ${S.type}, ${sel.name} highlighted. Details in the table below.`);
    $('chart-legend').innerHTML = `<span><span style="color:var(--amber-hi)">━</span> ${esc(sel.name)} (selected)</span><span><span style="color:var(--line-strong)">━</span> other ${S.type}</span><span><span class="hot">┄</span> ${MX.specLabel}</span><span>Gaps = not installed. Lines follow the device by serial number, not the robot slot.</span>`;

    /* table */
    $('table-title').textContent = `${S.type.toUpperCase()} · ${D.length} tracked · ${S.range === 'all' ? 'all time' : S.range + ' season'}`;
    $('col1').textContent = S.type === 'motors' ? 'MATCHES' : 'CYCLES';
    $('col2').textContent = S.type === 'motors' ? 'FAULTS' : 'LATEST';
    $('rows').innerHTML = D.map((d) => {
      const ix = idx(d), on = d.id === sel.id, h = health(d);
      const vals = ix.map((i) => MX.fn(d, i));
      /* trend sparkline uses the device's own range so its shape is readable */
      const vmin = Math.min(...vals), vmax = Math.max(...vals), pad = (vmax - vmin) * 0.1 || 1;
      const sp = vals.length > 1 ? vals.map((v, k) => (k / (vals.length - 1) * 120).toFixed(1) + ',' + (24 - (v - vmin + pad) / (vmax - vmin + 2 * pad) * 24).toFixed(1)).join(' ') : '';
      const c1 = S.type === 'motors' ? ix.length : Math.max(0, Math.min(d.until, r1) - d.since);
      const c2 = S.type === 'motors' ? ix.reduce((s, i) => s + FP.metrics.faults.fn(d, i), 0) : (ix.length ? MX.fn(d, ix[ix.length - 1]).toFixed(MX.dec) + ' ' + MX.unit : '—');
      return `<tr class="${on ? 'is-sel' : ''} ${d.retired ? 'is-retired' : ''}">
        <td><button type="button" class="dev-btn" data-device="${d.id}" aria-pressed="${on}">${esc(d.name)}</button></td>
        <td class="d">${esc(d.serial)}</td>
        <td class="d" style="white-space:nowrap">${FP.segments[FP.segOf(d.since)].label}${d.retired ? ' → ' + FP.segments[FP.segOf(d.until - 1)].label : ''}</td>
        <td style="text-align:right">${c1}</td><td style="text-align:right">${c2}</td>
        <td style="width:140px"><svg class="spark" viewBox="0 0 120 24" preserveAspectRatio="none" style="width:120px;height:24px" aria-hidden="true"><polyline class="line ${h === 'fault' ? 'line--fault' : (on ? 'line--sel' : '')}" points="${sp}"></polyline></svg></td>
        <td>${statusTag(h, HEALTH_WORD[h])}</td></tr>`;
    }).join('');

    /* device rail */
    const ixSel = idx(sel), life = []; for (let i = sel.since; i < Math.min(sel.until, FP.N); i++) life.push(i);
    const last = ixSel.length ? ixSel[ixSel.length - 1] : Math.min(sel.until, FP.N) - 1;
    const vNow = MX.fn(sel, last), vFirst = ixSel.length ? MX.fn(sel, ixSel[0]) : vNow;
    const delta = vFirst ? (vNow - vFirst) / vFirst * 100 : 0;
    const h = health(sel);
    $('device').innerHTML = `
      <h2 class="label">Device</h2>
      <div class="meta" style="letter-spacing:.12em">${esc(sel.model)} · SN ${esc(sel.serial)}</div>
      <h3 class="title-d2" style="margin:6px 0 8px">${esc(sel.name)}</h3>${statusTag(h, HEALTH_WORD[h])}
      <p class="prose" style="margin:12px 0 18px">${esc(sel.summary || 'No trends outside normal range across its service life.')}</p>
      <div class="tiles-2" style="margin-bottom:24px">
        <div class="fact"><div class="fact__k">In service</div><div class="fact__v">${life.length} matches</div></div>
        <div class="fact"><div class="fact__k">${sel.kind === 'motor' ? 'Est. runtime' : 'Cycles'}</div><div class="fact__v">${sel.kind === 'motor' ? (life.length * 0.06 + 2).toFixed(1) + ' h' : life.length}</div></div>
        <div class="fact"><div class="fact__k">Now · ${MX.label}</div><div class="fact__v">${vNow.toFixed(MX.dec)} ${MX.unit}</div></div>
        <div class="fact"><div class="fact__k">Change in range</div><div class="fact__v">${delta >= 0 ? '+' : ''}${delta.toFixed(0)}%</div></div>
      </div>
      <h2 class="label">Lifeline</h2>
      <div class="lifeline" style="margin-bottom:24px">${sel.life.slice().sort((a, b) => b[0] - a[0]).map(([i, text]) => `
        <div class="lifeline__item ${/Anomaly|Retired|retire|Flagged|Crossed|climbing/.test(text) ? 'is-alert' : ''}"><div class="lifeline__when">${FP.whenOf(i)}</div><div class="lifeline__text">${esc(text)}</div></div>`).join('')}</div>
      <div class="seg" style="gap:6px">
        ${sel.anomaly ? `<a class="btn btn--primary" href="anomalies.html?id=${sel.anomaly}">OPEN ANOMALY</a>` : ''}
        <button type="button" class="btn">ADD NOTE</button><button type="button" class="btn">LOG SWAP</button>
      </div>`;
  }

  document.addEventListener('click', (e) => {
    const r = e.target.closest('[data-range]'); if (r) { S.range = r.dataset.range; return render(); }
    const t = e.target.closest('[data-type]'); if (t) { S.type = t.dataset.type; S.metric = null; S.device = null; return render(); }
    const m = e.target.closest('[data-metric]'); if (m) { S.metric = m.dataset.metric; return render(); }
    const d = e.target.closest('[data-device]'); if (d) { S.device = d.dataset.device; return render(); }
  });
  render();
})();
