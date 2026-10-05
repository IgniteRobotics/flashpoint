/* ==========================================================================
   Pit check. Verdict rule (keep exactly): any FAULT → NO-GO; else any
   unacknowledged WARN → DECIDE; else GO. Not run yet → NOT CHECKED.
   Production: POST /api/pit-checks runs real probes over NT4 / Phoenix
   diagnostics and streams results (see docs/04). Prototype reveals canned results.
   ========================================================================== */
(function () {
  const { FP, esc } = window.UI;
  const $ = (id) => document.getElementById(id);
  const S = { done: -1, running: false, battery: 'B04', acked: {}, cleared: false };
  let timer = 0;
  const GLYPH = { ok: '●', warn: '▲', fault: '✕' };
  const COLOR = { ok: 'var(--amber-dim)', warn: 'var(--amber-hi)', fault: 'var(--hot)' };

  function render() {
    const checks = FP.pitChecks({ battery: S.battery, cleared: S.cleared });
    const n = checks.length;
    const complete = !S.running && S.done >= n - 1;

    $('checks').innerHTML = checks.map((c, i) => {
      const shown = i <= S.done, active = S.running && i === S.done + 1, s = shown ? c.state : null;
      const acked = !!S.acked[c.id];
      const needsAct = shown && c.action && s !== 'ok';
      const cls = ['check', active && 'is-active', s === 'fault' && 'is-fault', s === 'warn' && !acked && 'is-warn'].filter(Boolean).join(' ');
      const label = c.action === 'clear' ? 'CLEAR FAULTS' : (acked ? 'UNDO' : 'ACKNOWLEDGE');
      return `<div class="${cls}">
        <span class="check__glyph" style="${shown ? 'color:' + COLOR[s] : ''}" aria-hidden="true">${shown ? GLYPH[s] : (active ? '▌' : '·')}</span>
        <div class="check__body"><div class="check__name">${esc(c.name)}</div><div class="check__detail">${shown ? esc(c.detail) : (active ? 'checking…' : 'pending')}</div></div>
        ${shown ? `<span class="status status--${s}">${s === 'warn' && acked ? 'WARN · ACKED' : s.toUpperCase()}</span>` : ''}
        ${needsAct ? `<button type="button" class="btn btn--sm ${c.action === 'clear' || !acked ? 'btn--primary' : ''}" data-act="${c.id}" data-kind="${c.action}">${label}</button>` : ''}
      </div>`;
    }).join('');
    $('progress').textContent = S.done < 0 ? (S.running ? 'starting' : 'not run') : `${Math.min(S.done + 1, n)} of ${n}`;

    let word = 'NOT CHECKED', cls = '', text = 'Tether the robot and run the checks. Takes about ten seconds.';
    if (S.running) { word = 'CHECKING'; text = 'Reading devices, faults and battery…'; }
    if (complete) {
      const faults = checks.filter((c) => c.state === 'fault').length;
      const warns = checks.filter((c) => c.state === 'warn' && !S.acked[c.id]).length;
      if (faults) { word = 'NO-GO'; cls = 'verdict--nogo'; text = `${faults} fault${faults > 1 ? 's' : ''} must be fixed before queueing. Each one says what to do.`; }
      else if (warns) { word = 'DECIDE'; cls = 'verdict--decide'; text = `${warns} warning${warns > 1 ? 's' : ''} to acknowledge. Read them, then decide.`; }
      else { word = 'GO'; cls = 'verdict--go'; text = 'Everything checks out. Go queue.'; }
    }
    $('verdict').className = 'verdict ' + cls;
    $('verdict').querySelector('.verdict__word').textContent = word;
    $('verdict-text').textContent = text;
    $('run').textContent = S.running ? 'CHECKING…' : (complete ? 'RUN AGAIN' : 'RUN CHECKS');
    $('run').disabled = S.running;

    $('fleet').innerHTML = FP.batteries.map((b) => {
      const g = FP.batteryGrade(b), on = b.id === S.battery;
      return `<tr class="${on ? 'is-sel' : ''}" style="${g === 'fault' ? 'color:var(--hot)' : ''}">
        <td><b style="color:var(--amber-hi)">${b.id}</b> <span class="status status--${g}" aria-label="${g}"></span></td>
        <td style="text-align:right">${b.restV.toFixed(2)}</td><td style="text-align:right">${b.irMilliOhm}</td><td style="text-align:right" class="d">${b.cycles}</td>
        <td style="text-align:right"><button type="button" class="btn btn--sm" data-bat="${b.id}" aria-pressed="${on}" aria-label="${on ? 'In robot: ' : 'Use '}${b.id}">${on ? 'IN ROBOT' : 'USE'}</button></td></tr>`;
    }).join('');
  }

  $('run').addEventListener('click', () => {
    clearInterval(timer);
    const n = FP.pitChecks(S).length;
    S.done = -1; S.running = true; render();
    timer = setInterval(() => {
      S.done++;
      if (S.done >= n - 1) { clearInterval(timer); S.running = false; }
      render();
    }, matchMedia('(prefers-reduced-motion: reduce)').matches ? 60 : 420);
  });
  document.addEventListener('click', (e) => {
    const a = e.target.closest('[data-act]');
    if (a) { if (a.dataset.kind === 'clear') S.cleared = true; else S.acked[a.dataset.act] = !S.acked[a.dataset.act]; return render(); }
    const b = e.target.closest('[data-bat]'); if (b) { S.battery = b.dataset.bat; return render(); }
  });
  render();
})();
