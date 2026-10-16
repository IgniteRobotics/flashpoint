/* ==========================================================================
   Live. Prototype: simulated match clock driving FP.makeMatch().
   Production: NT4 subscription (see docs/04 "Live data path"). Same render rules:
   - Render at 2 Hz even though data arrives at 50 Hz (readability + CPU on old laptops).
   - Build interactive elements once, update in place, so keyboard focus survives ticks.
   - Re-render lists (alerts) only when their contents change.
   URL: live.html?sel=<mechanismId>
   ========================================================================== */
(function () {
  const { FP, esc, pts, yOf, fmtClock, params, setParams, statusTag } = window.UI;
  const $ = (id) => document.getElementById(id);
  const M = FP.mechanisms;
  const sim = FP.makeMatch(FP.matches.find((m) => m.id === 'q42'));
  const S = { t: 84, speed: 1, paused: false, sel: params().get('sel') || 'shooter_l', acked: {} };
  if (!M.some((m) => m.id === S.sel)) S.sel = 'shooter_l';

  const windowTimes = () => { const out = []; for (let k = 0; k <= 40; k++) out.push(S.t - 20 + k * 0.5); return out; };

  /* ---------- Build static structure once ---------- */
  $('phasebar').innerHTML = FP.robot.phases.map((p) => `<div class="phasebar__seg" data-phase="${p.id}" style="flex:0 0 ${(p.end - p.start) / 150 * 100}%">${p.id}</div>`).join('') + '<div class="phasebar__cursor" id="pcursor" aria-hidden="true"></div>';

  const VITALS = [
    { id: 'bat', label: 'BATTERY', unit: 'V', lo: 6, hi: 13, ref: FP.robot.brownoutV, fn: sim.volts, dec: 2 },
    { id: 'cur', label: 'TOTAL CURRENT', unit: 'A', lo: 0, hi: 500, ref: 400, fn: sim.total, dec: 0 },
    { id: 'can', label: 'CAN BUS', unit: '%', lo: 0, hi: 100, ref: 70, fn: sim.can, dec: 0 },
    { id: 'loop', label: 'LOOP TIME', unit: 'ms', lo: 0, hi: 30, ref: 20, fn: sim.loop, dec: 1 }
  ];
  $('vitals').innerHTML = VITALS.map((v) => `
    <div class="vital" id="v-${v.id}">
      <div class="vital__head"><span>${v.label}</span><span data-flag></span></div>
      <div class="readout"><span class="readout__value" data-value></span><span class="readout__unit">${v.unit}</span></div>
      <svg class="spark" viewBox="0 0 200 40" preserveAspectRatio="none" style="height:44px" aria-hidden="true">
        <line class="ref" x1="0" x2="200" y1="${yOf(v.ref, v.lo, v.hi, 40)}" y2="${yOf(v.ref, v.lo, v.hi, 40)}"></line>
        <polyline class="line" data-line></polyline></svg>
      <div class="vital__foot" data-foot></div>
    </div>`).join('');

  $('mech-label').textContent = 'Mechanisms · ' + M.length;
  $('mechs').innerHTML = M.map((m) => `
    <button type="button" class="mech" data-mech="${m.id}" aria-pressed="false">
      <span class="mech__top"><span class="mech__name">${esc(m.name)}</span><span data-status></span></span>
      <span class="mech__nums"><span><b data-amps></b> A</span><span><b data-temp></b> °C</span><span class="mech__can">CAN ${m.can}</span></span>
      <svg class="spark" viewBox="0 0 200 40" preserveAspectRatio="none" style="height:32px" aria-hidden="true"><polyline class="line" data-line></polyline></svg>
    </button>`).join('');

  /* ---------- Tick render ---------- */
  let lastAlertKey = '';
  function render() {
    const t = S.t, win = windowTimes();
    const phase = FP.robot.phases.find((p) => t < p.end) || FP.robot.phases[2];
    const remaining = t >= 150 ? 0 : (t < 15 ? 15 - t : 150 - t);
    $('clock').textContent = fmtClock(remaining);
    $('phase').textContent = t >= 150 ? 'MATCH OVER' : phase.id;
    $('tlabel').textContent = 'T+' + t.toFixed(1) + ' s';
    document.querySelectorAll('[data-phase]').forEach((el) => el.classList.toggle('is-on', el.dataset.phase === phase.id && t < 150));
    $('pcursor').style.left = (t / 150 * 100) + '%';
    $('pause').textContent = S.paused ? (t >= 150 ? 'ENDED' : 'RESUME') : 'PAUSE';

    /* vitals */
    let minV = 99, overruns = 0;
    for (let x = 0; x <= t; x += 0.5) { minV = Math.min(minV, sim.volts(x)); if (sim.loop(x) > 20) overruns++; }
    VITALS.forEach((v) => {
      const el = $('v-' + v.id), now = v.fn(t);
      el.querySelector('[data-value]').textContent = now.toFixed(v.dec);
      el.querySelector('[data-line]').setAttribute('points', pts(win.map(v.fn), v.lo, v.hi, 200, 40));
      let flag = '● OK', alert = false, foot = '';
      if (v.id === 'bat') { alert = minV < FP.robot.brownoutV; flag = alert ? '✕ BROWNOUT' : (now < 8 ? '▲ SAG' : '● OK'); foot = `min this match ${minV.toFixed(2)} V · brownout ${FP.robot.brownoutV} V`; }
      if (v.id === 'cur') { flag = now > 350 ? '▲ HIGH' : '● OK'; foot = 'peak 20 s ' + Math.round(Math.max(...win.map(v.fn))) + ' A'; }
      if (v.id === 'can') { flag = now > 70 ? '▲ BUSY' : '● OK'; foot = '24 devices · 1 Mbps'; }
      if (v.id === 'loop') { flag = now > 20 ? '▲ OVERRUN' : '● OK'; foot = overruns + ' overrun(s) > 20 ms this match'; }
      el.classList.toggle('is-fault', alert);
      const f = el.querySelector('[data-flag]'); f.textContent = flag; f.style.color = alert ? 'var(--hot)' : '';
      el.querySelector('[data-foot]').textContent = foot;
    });

    /* mechanisms (update in place) */
    M.forEach((m) => {
      const el = document.querySelector(`[data-mech="${m.id}"]`), s = sim.status(m, t);
      const a = sim.amps(m, t), tc = sim.temp(m, t);
      el.querySelector('[data-amps]').textContent = a.toFixed(1);
      el.querySelector('[data-temp]').textContent = tc.toFixed(0);
      el.querySelector('[data-status]').innerHTML = statusTag(s);
      const ln = el.querySelector('[data-line]');
      ln.setAttribute('points', pts(win.map((x) => sim.amps(m, x)), 0, m.limit * 1.1, 200, 40));
      ln.classList.toggle('line--fault', s === 'fault');
      el.classList.toggle('is-warn', s === 'warn');
      el.classList.toggle('is-fault', s === 'fault');
      el.setAttribute('aria-pressed', String(m.id === S.sel));
      el.setAttribute('aria-label', `${m.name}, ${s}, ${a.toFixed(0)} amps, ${tc.toFixed(0)} degrees`);
    });

    /* focus */
    const fm = M.find((m) => m.id === S.sel), fs = sim.status(fm, t), fa = win.map((x) => sim.amps(fm, x));
    $('focus').innerHTML = `
      <h2 class="section-label"><span>Focus</span></h2>
      <div style="display:flex;justify-content:space-between;align-items:baseline;gap:8px"><h3 class="title-d2">${esc(fm.name)}</h3>${statusTag(fs)}</div>
      <div class="meta" style="margin:6px 0 14px">TalonFX · CAN ${fm.can} · supply limit ${fm.limit} A · device ${esc(fm.device)} · <a href="history.html?device=${(FP.devices.find((d) => d.serial === fm.device) || {}).id || ''}">history</a></div>
      <div class="focus-plot"><svg class="spark" viewBox="0 0 300 100" preserveAspectRatio="none" aria-hidden="true">
        <line class="ref" x1="0" x2="300" y1="${yOf(fm.limit, 0, fm.limit * 1.15, 100)}" y2="${yOf(fm.limit, 0, fm.limit * 1.15, 100)}"></line>
        <polyline class="line line--temp" points="${pts(win.map((x) => sim.temp(fm, x)), 20, 90, 300, 100)}"></polyline>
        <polyline class="line" style="stroke-width:2" points="${pts(fa, 0, fm.limit * 1.15, 300, 100)}"></polyline></svg></div>
      <div class="legend" style="margin-top:8px"><span><span style="color:var(--amber)">━</span> current</span><span><span style="color:var(--amber-dim)">┅</span> temp</span><span><span class="hot">┄</span> limit</span></div>
      <div class="tiles-2" style="margin-top:16px">
        <div class="fact"><div class="fact__k">Now</div><div class="fact__v">${sim.amps(fm, t).toFixed(1)} A</div></div>
        <div class="fact"><div class="fact__k">Peak 20 s</div><div class="fact__v">${Math.max(...fa).toFixed(1)} A</div></div>
        <div class="fact"><div class="fact__k">Temp</div><div class="fact__v">${sim.temp(fm, t).toFixed(1)} °C</div></div>
        <div class="fact"><div class="fact__k">Temp rate</div><div class="fact__v">${((sim.temp(fm, t) - sim.temp(fm, t - 20)) * 3).toFixed(1)} °C/min</div></div>
      </div>`;

    /* log */
    const past = FP.q42Events.filter((e) => e.time <= t).reverse();
    $('log').innerHTML = past.slice(0, 9).map((e) => `<div class="log__line"><span class="log__n" style="width:64px">T+${e.time.toFixed(1).padStart(5, '0')}</span><span class="lvl lvl--${e.level}">${e.level}</span><span style="width:88px;color:var(--amber-dim)">${esc(e.src)}</span><span class="log__obj">${esc(e.msg)}</span></div>`).join('')
      + '<div class="log__line muted">listening<span class="cursor" aria-hidden="true"> ▌</span></div>';

    /* alerts: re-render only when the set or ack state changes */
    const alertEv = past.filter((e) => e.level !== 'INFO').slice(0, 4);
    const key = alertEv.map((e) => e.id + (S.acked[e.id] ? '1' : '0')).join(',');
    if (key !== lastAlertKey) {
      lastAlertKey = key;
      const open = alertEv.filter((e) => !S.acked[e.id]).length;
      $('alerts-label').textContent = `Alerts · ${open} unacknowledged`;
      $('alerts').innerHTML = alertEv.length ? alertEv.map((e) => {
        const done = !!S.acked[e.id], fault = e.level === 'FAULT';
        return `<div class="alert ${done ? 'is-acked' : (fault ? 'is-fault' : 'is-warn')}">
          <div class="alert__head"><span class="status status--${fault ? 'fault' : 'warn'}">${e.level} · ${esc(e.src)}</span><span class="muted">T+${e.time.toFixed(1)}</span></div>
          <p class="alert__msg">${esc(e.msg)}</p>
          <div class="seg" style="gap:6px"><button type="button" class="btn btn--sm ${done ? '' : 'btn--primary'}" data-ack="${e.id}">${done ? 'ACKED' : 'ACKNOWLEDGE'}</button>
          <a class="btn btn--sm" href="replay.html?match=q42&ev=${e.id}">OPEN IN REPLAY</a></div></div>`;
      }).join('') : '<p class="meta">All clear. Warnings and faults land here as they happen.</p>';
    }
    $('rx').textContent = (2.6 + FP.rnd(t) * 1.2).toFixed(1);
  }

  /* ---------- Clock ---------- */
  setInterval(() => {
    if (S.paused) return;
    S.t = Math.min(150, S.t + 0.5 * S.speed);
    if (S.t >= 150) S.paused = true;
    render();
  }, 500);

  /* ---------- Events ---------- */
  document.addEventListener('click', (e) => {
    const m = e.target.closest('[data-mech]'); if (m) { S.sel = m.dataset.mech; setParams({ sel: S.sel }); return render(); }
    const a = e.target.closest('[data-ack]'); if (a) { S.acked[a.dataset.ack] = !S.acked[a.dataset.ack]; return render(); }
    const sp = e.target.closest('[data-speed]');
    if (sp) { S.speed = +sp.dataset.speed; document.querySelectorAll('[data-speed]').forEach((b) => b.setAttribute('aria-pressed', String(b === sp))); return; }
    if (e.target.closest('#pause') && S.t < 150) { S.paused = !S.paused; return render(); }
    if (e.target.closest('#restart')) { S.t = 0; S.paused = false; S.acked = {}; lastAlertKey = 'x'; return render(); }
  });
  render();
})();
