/* ==========================================================================
   Replay. Prototype samples FP.makeMatch() at 2 Hz for the whole match.
   Production: server-side WPILog parse → downsampled tracks + events (see
   data-contracts/match-log.schema.json); full-rate data on demand.
   URL: replay.html?match=<id>&t=<seconds>&ev=<eventId>  (shareable)
   ========================================================================== */
(function () {
  const { FP, esc, pts, yOf, fmtClock, params, setParams, statusTag } = window.UI;
  const $ = (id) => document.getElementById(id);
  const p = params();
  const S = { match: p.get('match') || 'q42', cur: p.has('t') ? +p.get('t') : null, ev: p.get('ev') };

  let mt, sim, events, tracks;

  function load() {
    mt = FP.matches.find((m) => m.id === S.match) || FP.matches[2];
    sim = FP.makeMatch(mt);
    events = FP.eventsFor(mt).filter((e) => e.level !== 'INFO');
    if (!events.some((e) => e.id === S.ev)) S.ev = mt.brownout ? 'brownout' : (events[0] ? events[0].id : null);
    const ev = events.find((e) => e.id === S.ev);
    if (S.cur == null) S.cur = ev ? ev.time : 75;
    const sl = FP.mechanisms[6], ix = FP.mechanisms[5];
    tracks = [
      { name: 'BATTERY', unit: 'V', lo: 6, hi: 13, ref: FP.robot.brownoutV, refLabel: 'BROWNOUT 6.75', fn: sim.volts, dec: 2 },
      { name: 'TOTAL CURRENT', unit: 'A', lo: 0, hi: 500, ref: 400, refLabel: '400 A', fn: sim.total, dec: 0 },
      { name: 'SHOOTER L TEMP', unit: '°C', lo: 20, hi: 90, ref: 65, refLabel: 'WARN 65', fn: (x) => sim.temp(sl, x), dec: 1 },
      { name: 'INDEXER', unit: 'A', lo: 0, hi: 70, ref: 60, refLabel: 'LIMIT 60', fn: (x) => sim.amps(ix, x), dec: 1 }
    ];
    const grid = []; for (let x = 0; x <= 150; x += 0.5) grid.push(x);

    $('mtitle').textContent = mt.label;
    $('mmeta').textContent = mt.empty ? 'Not played yet. The log appears here when the match ends.' : `Blue 2 · ${mt.meta} · 2:30 · ${events.length} warnings and faults`;
    $('tracks').innerHTML = tracks.map((tr, i) => `
      <div class="track">
        <div class="track__label"><div class="track__name">${tr.name}</div><div class="track__now"><span data-now="${i}">—</span> <span class="meta">${tr.unit}</span></div><div class="track__name" style="letter-spacing:0">${tr.lo} – ${tr.hi} ${tr.unit}</div></div>
        <div class="track__plot">
          <div class="track__phase" style="left:0;width:10%"></div><div class="track__phase" style="right:0;width:13.333%"></div>
          ${mt.empty ? '' : `<svg class="spark" viewBox="0 0 1000 100" preserveAspectRatio="none" aria-hidden="true">
            <line class="ref" x1="0" x2="1000" y1="${yOf(tr.ref, tr.lo, tr.hi, 100)}" y2="${yOf(tr.ref, tr.lo, tr.hi, 100)}"></line>
            <polyline class="line" points="${pts(grid.map(tr.fn), tr.lo, tr.hi, 1000, 100)}"></polyline></svg>`}
          <div class="track__cursor" data-cursor></div>
          <span class="track__ref">${tr.refLabel}</span>
        </div>
      </div>`).join('');
    $('markers').innerHTML = events.map((e) => `<button type="button" class="marker marker--${e.level}" data-ev="${e.id}" style="left:${e.time / 150 * 100}%" aria-label="${e.level} at T+${e.time.toFixed(1)}: ${esc(e.msg)}">${e.level === 'FAULT' ? '✕' : '▲'}</button>`).join('');
    $('matches').innerHTML = FP.matches.map((m) => {
      const n = m.empty ? 0 : FP.eventsFor(m).filter((e) => e.level === 'FAULT').length;
      return `<button type="button" class="match-btn" data-match="${m.id}" ${m.id === mt.id ? 'aria-current="page"' : ''}>
        <span class="match-btn__row"><b>${esc(m.label)}</b><span class="status status--${m.empty ? 'retired' : (n ? 'fault' : 'ok')}" style="font-weight:600">${m.empty ? 'NO LOG' : (n ? n + ' FAULTS' : 'CLEAN')}</span></span>
        <span class="meta">${esc(m.meta)}</span></button>`;
    }).join('');
    update();
  }

  function update() {
    const t = S.cur;
    $('scrub').value = t;
    $('curlabel').textContent = 'T+' + t.toFixed(1) + ' s';
    document.querySelectorAll('[data-cursor]').forEach((c) => { c.style.left = (t / 150 * 100) + '%'; });
    document.querySelectorAll('[data-now]').forEach((el) => { const tr = tracks[+el.dataset.now]; el.textContent = mt.empty ? '—' : tr.fn(t).toFixed(tr.dec); });
    document.querySelectorAll('[data-ev]').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.ev === S.ev)));
    const phase = t < 15 ? 'AUTO' : (t < 130 ? 'TELEOP ' : 'ENDGAME ') + fmtClock(150 - t);
    $('readout-label').textContent = `At T+${t.toFixed(1)} · ${phase}`;
    $('readout').innerHTML = FP.mechanisms.map((m) => `<tr><td class="v">${esc(m.name)}</td><td style="text-align:right">${mt.empty ? '—' : sim.amps(m, t).toFixed(1)}</td><td style="text-align:right">${mt.empty ? '—' : sim.temp(m, t).toFixed(0)}</td><td>${mt.empty ? '' : statusTag(sim.status(m, t), ' ')}</td></tr>`).join('');

    const e = events.find((x) => x.id === S.ev);
    const peak = Math.round(Math.max(sim.total(96.5), sim.total(97), sim.total(97.5)));
    $('event').innerHTML = `<h2 class="section-label"><span>Event</span></h2>` + (e ? `
      <div class="alert ${e.level === 'FAULT' ? 'is-fault' : 'is-warn'}" style="padding:14px 16px">
        <div class="alert__head"><span class="status status--${e.level === 'FAULT' ? 'fault' : 'warn'}">${e.level} · ${esc(e.src)}</span><span class="muted">T+${e.time.toFixed(1)}</span></div>
        <h3 style="margin:8px 0 12px;font-size:16px;font-weight:600;color:var(--amber-hi)">${esc(e.msg)}</h3>
        <div class="fact__k" style="margin-bottom:6px">Likely cause</div><p class="prose">${esc(e.cause.replace('{PEAK}', peak))}</p>
        <div class="fact__k" style="margin:14px 0 6px">Suggested fix</div><p class="prose">${esc(e.fix)}</p>
        <div class="seg" style="gap:6px;margin-top:14px"><button type="button" class="btn btn--sm btn--primary">CREATE PIT TASK</button><button type="button" class="btn btn--sm">MARK RESOLVED</button></div>
      </div>
      <p class="meta muted" style="margin-top:10px">Cause and fix are heuristic suggestions from the log, not a diagnosis.</p>`
      : '<p class="meta">Click a marker above the timeline to see what happened and why.</p>');
    setParams({ match: mt.id, t: t.toFixed(1), ev: S.ev });
  }

  $('scrub').addEventListener('input', (e) => { S.cur = parseFloat(e.target.value); update(); });
  document.addEventListener('click', (e) => {
    const ev = e.target.closest('[data-ev]'); if (ev) { const x = events.find((y) => y.id === ev.dataset.ev); S.ev = x.id; S.cur = x.time; return update(); }
    const m = e.target.closest('[data-match]'); if (m) { S.match = m.dataset.match; S.cur = null; S.ev = null; return load(); }
    if (e.target.closest('#copylink')) {
      navigator.clipboard?.writeText(location.href).then(() => { const b = $('copylink'); b.textContent = 'COPIED'; setTimeout(() => { b.textContent = 'COPY LINK'; }, 1200); });
    }
  });
  load();
})();
