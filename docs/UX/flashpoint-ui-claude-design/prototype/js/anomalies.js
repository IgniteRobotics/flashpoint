/* ==========================================================================
   Anomalies. Every flag states: method, how far outside (actual vs expected),
   evidence chart, why, likely causes, past similar cases. Humans close the loop
   (investigate / resolve / dismiss-as-expected), and dismissals feed the detector.
   Production: anomalies come from the detector job (docs/04 "Anomaly detection");
   state changes PATCH /api/anomalies/:id. Prototype keeps state in memory.
   URL: anomalies.html?id=<id>&tab=open|resolved|dismissed&sens=relaxed|normal|strict
   ========================================================================== */
(function () {
  const { FP, esc, clamp, params, setParams } = window.UI;
  const $ = (id) => document.getElementById(id);
  const p = params();
  const S = { tab: p.get('tab') || 'open', sens: p.get('sens') || 'normal', sel: p.get('id'), over: {}, tasks: {} };
  const K = 40;
  const SEV = { high: 0, med: 1, low: 2 };
  const stateOf = (a) => S.over[a.id] || a.state;
  const isOpen = (a) => stateOf(a) === 'new' || stateOf(a) === 'investigating';

  $('methods').innerHTML = Object.entries(FP.methods).map(([k, v]) => `<span><b>${k}</b> · ${esc(v)}</span>`).join('');

  function render() {
    const visible = FP.anomalies.filter((a) => !(a.strictOnly && S.sens !== 'strict') && !(S.sens === 'relaxed' && a.severity === 'low'));
    const inTab = (a) => (S.tab === 'open' ? isOpen(a) : stateOf(a) === S.tab);
    const list = visible.filter(inTab).sort((a, b) => SEV[a.severity] - SEV[b.severity]);
    if (!list.some((a) => a.id === S.sel)) S.sel = list[0] ? list[0].id : null;
    setParams({ id: S.sel, tab: S.tab, sens: S.sens });

    const open = visible.filter(isOpen), high = open.filter((a) => a.severity === 'high').length;
    $('open-count').textContent = open.length;
    $('open-count').style.color = high ? 'var(--hot)' : '';
    $('open-sub').textContent = `${high} high · scanned through 2026 · OFF M8`;
    const count = (t) => visible.filter((a) => (t === 'open' ? isOpen(a) : stateOf(a) === t)).length;
    $('tabs').innerHTML = [['open', 'OPEN'], ['resolved', 'RESOLVED'], ['dismissed', 'DISMISSED']]
      .map(([k, l]) => `<button type="button" class="btn btn--sm" data-tab="${k}" aria-pressed="${S.tab === k}">${l} · ${count(k)}</button>`).join('');
    $('sens').innerHTML = ['relaxed', 'normal', 'strict'].map((k) => `<button type="button" class="btn btn--sm" data-sens="${k}" aria-pressed="${S.sens === k}">${k.toUpperCase()}</button>`).join('');

    $('list-title').textContent = `${S.tab.toUpperCase()} · ${list.length} · highest severity first`;
    $('list').innerHTML = list.length ? list.map((a) => `
      <button type="button" class="anom ${a.severity === 'high' ? 'anom--high' : ''}" data-sel="${a.id}" aria-pressed="${a.id === S.sel}">
        <span class="anom__row"><span class="sev sev--${a.severity}">${a.severity.toUpperCase()} · ${esc(a.dev)}</span><span class="muted">${a.method}</span></span>
        <span class="anom__title">${esc(a.title)}</span>
        <span class="anom__row" style="letter-spacing:0"><span class="muted">since ${esc(a.firstSeen)}</span><span class="astate astate--${stateOf(a)}">${stateOf(a).toUpperCase()}${S.tasks[a.id] ? ' · PIT TASK' : ''}</span></span>
      </button>`).join('') : '<p class="meta" style="line-height:1.6">Nothing here. Devices in this view are behaving like their specs, their own history and their siblings.</p>';

    const a = FP.anomalies.find((x) => x.id === S.sel);
    if (!a) { $('main').innerHTML = '<p class="meta">Pick an anomaly from the queue to see the evidence.</p>'; return; }
    const ss = stateOf(a);
    const Y = (v) => 100 - (clamp(v, a.lo, a.hi) - a.lo) / (a.hi - a.lo) * 100;
    const X = (k) => k / (K - 1) * 400;
    const vals = Array.from({ length: K }, (_, k) => a.f(k));
    const outside = vals.map((v, k) => ({ v, k })).filter((o) => o.v > a.bandHi || o.v < a.bandLo);
    const fmtY = (v) => (a.hi - a.lo > 10 ? Math.round(v) : Math.round(v * 10) / 10) + (a.unit.length <= 3 ? ' ' + a.unit : '');
    const device = FP.devices.find((d) => d.id === a.device);
    const actions = isOpen(a)
      ? `<button type="button" class="btn ${S.tasks[a.id] ? '' : 'btn--primary'}" data-task>${S.tasks[a.id] ? 'PIT TASK CREATED' : 'CREATE PIT TASK'}</button>
         ${ss === 'new' ? '<button type="button" class="btn" data-set="investigating">INVESTIGATE</button>' : '<button type="button" class="btn" data-set="resolved">MARK RESOLVED</button>'}
         <button type="button" class="btn" data-set="dismissed">DISMISS AS EXPECTED</button>`
      : '<button type="button" class="btn" data-set="new">REOPEN</button>';

    $('main').innerHTML = `
      <div style="display:flex;flex-wrap:wrap;justify-content:space-between;align-items:flex-start;gap:12px">
        <div style="flex:1 1 420px;min-width:0">
          <div class="sev sev--${a.severity}" style="font-size:12px;letter-spacing:.12em">${a.severity.toUpperCase()} · ${a.method} · <span class="astate astate--${ss}">${ss.toUpperCase()}</span></div>
          <h1 class="title-d3" style="margin:8px 0 6px">${esc(a.dev)}</h1>
          <div class="meta">${esc(device ? device.model : '')} · SN ${esc(a.serial)} · <a href="history.html?type=${device && device.kind === 'battery' ? 'batteries' : 'motors'}&device=${a.device}">device history</a></div>
        </div>
        <div class="seg" style="gap:6px">${actions}</div>
      </div>
      <p class="prose" style="font-size:20px;line-height:1.5;margin:18px 0 20px;max-width:760px">${esc(a.title)}</p>
      <div class="facts" style="margin-bottom:22px">
        ${[['Actual', a.actual], ['Expected', a.expected], ['First seen', a.firstSeen], ['Matches affected', a.matchesAffected], ['Confidence', a.confidence.toUpperCase()], ['Method', a.method]]
          .map(([k, v]) => `<div class="fact"><div class="fact__k">${k}</div><div class="fact__v">${esc(v)}</div></div>`).join('')}
      </div>
      <h2 class="h-section">Evidence <span class="h-section__aside">${esc(a.metric)}</span></h2>
      <div class="chart-wrap">
        <div class="chart-y" style="width:52px;height:220px"><span style="top:0;right:8px">${fmtY(a.hi)}</span><span style="bottom:0;right:8px">${fmtY(a.lo)}</span></div>
        <div class="plot" style="flex:1;min-width:0" role="img" aria-label="${esc(a.dev)}: ${esc(a.metric)}. Actual ${esc(a.actual)}, expected ${esc(a.expected)}. ${outside.length} of ${K} points outside the expected band.">
          <svg class="spark" viewBox="0 0 400 100" preserveAspectRatio="none" aria-hidden="true">
            <polygon class="band" points="0,${Y(a.bandHi)} 400,${Y(a.bandHi)} 400,${Y(a.bandLo)} 0,${Y(a.bandLo)}"></polygon>
            <line class="band-edge" x1="0" x2="400" y1="${Y(a.bandHi)}" y2="${Y(a.bandHi)}"></line>
            <line class="band-edge" x1="0" x2="400" y1="${Y(a.bandLo)}" y2="${Y(a.bandLo)}"></line>
            ${a.peer ? `<polyline class="line line--peer" points="${Array.from({ length: K }, (_, k) => X(k).toFixed(1) + ',' + Y(a.peer(k)).toFixed(1)).join(' ')}"></polyline>` : ''}
            <polyline class="line" style="stroke-width:2" points="${vals.map((v, k) => X(k).toFixed(1) + ',' + Y(v).toFixed(1)).join(' ')}"></polyline>
          </svg>
          ${outside.map((o) => `<span class="plot__dot" style="left:${o.k / (K - 1) * 100}%;top:${Y(o.v)}%"></span>`).join('')}
          <span class="plot__label">EXPECTED ${esc(a.expected)}</span>
        </div>
      </div>
      <div class="timeline__axis" style="margin-left:52px"><span>${esc(a.xFrom)}</span><span>${esc(a.xTo)}</span></div>
      <div class="legend" style="margin:8px 0 24px 52px"><span><span style="color:var(--amber)">━</span> ${esc(a.dev)}</span>${a.peer ? `<span><span style="color:var(--amber-dim)">┅</span> ${esc(a.peerName)}</span>` : ''}<span>▒ expected band</span><span><span class="hot">■</span> outside band</span></div>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:24px">
        <section><h2 class="h-section">Why it was flagged</h2><p class="prose" style="font-size:16px;line-height:1.65">${esc(a.why)}</p></section>
        <section><h2 class="h-section">Likely causes</h2><ul class="prose" style="font-size:16px;line-height:1.65;padding-left:18px">${a.causes.map((c) => `<li>${esc(c)}</li>`).join('')}</ul></section>
      </div>
      ${a.similar ? `<div class="card" style="margin-top:24px"><div class="fact__k" style="margin-bottom:6px">Seen this before</div><p class="prose">${esc(a.similar)}</p></div>` : ''}
      ${a.resolution ? `<div class="card" style="margin-top:24px"><div class="fact__k" style="margin-bottom:6px">Outcome</div><p class="prose">${esc(a.resolution)}</p></div>` : ''}
      <p class="meta muted" style="margin-top:18px">Causes are suggestions ranked from similar past patterns, not a diagnosis. Dismissing with a reason teaches the detector what normal looks like.</p>`;
  }

  document.addEventListener('click', (e) => {
    const t = e.target.closest('[data-tab]'); if (t) { S.tab = t.dataset.tab; return render(); }
    const s = e.target.closest('[data-sens]'); if (s) { S.sens = s.dataset.sens; return render(); }
    const l = e.target.closest('[data-sel]'); if (l) { S.sel = l.dataset.sel; return render(); }
    const st = e.target.closest('[data-set]'); if (st && S.sel) { S.over[S.sel] = st.dataset.set; return render(); }
    if (e.target.closest('[data-task]') && S.sel) { S.tasks[S.sel] = true; return render(); }
  });
  render();
})();
