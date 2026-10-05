/* ==========================================================================
   Shell: helpers, header, CRT toggle, command palette (Ctrl/Cmd+K).
   Production equivalents: <AppHeader>, <CommandPalette> (cmdk), useScanlines(),
   and a charts layer on uPlot (see docs/04).
   ========================================================================== */
(function () {
  const FP = window.FP;
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  /* Polyline points for values in a w×h viewBox */
  const pts = (vals, lo, hi, w, h) => vals.map((v, i) => (i / Math.max(vals.length - 1, 1) * w).toFixed(1) + ',' + (h - (clamp(v, lo, hi) - lo) / (hi - lo) * h).toFixed(1)).join(' ');
  const yOf = (v, lo, hi, h) => (h - (clamp(v, lo, hi) - lo) / (hi - lo) * h).toFixed(1);
  const fmtClock = (s) => Math.floor(s / 60) + ':' + String(Math.floor(s % 60)).padStart(2, '0');
  const params = () => new URLSearchParams(location.search);
  const setParams = (obj) => {
    const u = new URL(location.href);
    Object.entries(obj).forEach(([k, v]) => (v == null || v === '' ? u.searchParams.delete(k) : u.searchParams.set(k, v)));
    history.replaceState(null, '', u);
  };
  const STATUS_WORD = { ok: 'OK', warn: 'WARN', fault: 'FAULT', retired: 'RETIRED' };
  const statusTag = (s, text) => `<span class="status status--${s}">${esc(text || STATUS_WORD[s] || s.toUpperCase())}</span>`;

  window.UI = { FP, esc, clamp, pts, yOf, fmtClock, params, setParams, statusTag };

  /* ---------- Header ---------- */
  const TABS = [['live', 'Live', 'live.html'], ['replay', 'Replay', 'replay.html'], ['pit', 'Pit', 'pit.html'], ['history', 'History', 'history.html'], ['anomalies', 'Anomalies', 'anomalies.html']];
  const mount = document.getElementById('app-header');
  if (mount) {
    const active = mount.dataset.active;
    const mac = /Mac|iPhone|iPad/.test(navigator.platform);
    const openAnoms = FP.anomalies.filter((a) => !a.strictOnly && (a.state === 'new' || a.state === 'investigating')).length;
    mount.outerHTML = `
      <header class="app-header">
        <a class="brand" href="index.html" aria-label="Flashpoint home">
          <span class="brand__mark">FLASHPOINT</span>
          <span class="brand__sub">telemetry // ignite robotics ${FP.robot.team}</span>
        </a>
        <nav class="tabs" aria-label="Views">
          ${TABS.map(([k, label, href]) => `<a class="tab" href="${href}" ${k === active ? 'aria-current="page"' : ''}>${label.toUpperCase()}${k === 'anomalies' && openAnoms ? ` <span class="hot" aria-label="${openAnoms} open">· ${openAnoms}</span>` : ''}</a>`).join('')}
        </nav>
        <div class="app-header__spacer"></div>
        <button type="button" class="search-trigger" data-open-palette style="min-width:0" aria-label="Jump to anything">
          <span class="search-trigger__prompt">&gt;_</span><span class="search-trigger__text wide-only">jump to…</span><span class="kbd">${mac ? '⌘' : 'Ctrl'} K</span>
        </button>
        <span class="pill">${FP.robot.address} <span style="color:var(--amber)">● ${active === 'pit' ? 'TETHERED' : 'LINKED'}</span></span>
        <button type="button" class="btn btn--sm btn--ghost" data-crt aria-pressed="true" title="Toggle scanline texture">CRT</button>
        ${FP.meta.demo ? '<span class="badge badge--demo" title="Values on screen are illustrative">DEMO DATA</span>' : ''}
      </header>`;
  }

  /* ---------- CRT toggle (shared key with Mnemosyne on purpose: one Ignite preference) ---------- */
  const KEY = 'ignite.scanlines';
  const setCrt = (on) => {
    document.body.classList.toggle('no-scanlines', !on);
    document.querySelectorAll('[data-crt]').forEach((b) => b.setAttribute('aria-pressed', String(on)));
    try { localStorage.setItem(KEY, on ? '1' : '0'); } catch (_) { /* unavailable */ }
  };
  let crtOn = true;
  try { crtOn = localStorage.getItem(KEY) !== '0'; } catch (_) { /* default */ }
  setCrt(crtOn);
  document.addEventListener('click', (e) => { if (e.target.closest('[data-crt]')) { crtOn = !crtOn; setCrt(crtOn); } });

  /* ---------- Command palette ---------- */
  const go = (href) => () => { location.href = href; };
  const POOL = [
    ...TABS.map(([, label, href]) => ({ group: 'Go to', label: label + ' view', tag: 'VIEW', run: go(href) })),
    ...FP.mechanisms.map((m) => ({ group: 'Mechanisms (live)', label: m.name, tag: 'CAN ' + m.can, run: go('live.html?sel=' + m.id) })),
    ...FP.matches.filter((m) => !m.empty).map((m) => ({ group: 'Match logs', label: m.label, tag: 'LOG', run: go('replay.html?match=' + m.id) })),
    ...FP.devices.map((d) => ({ group: 'Devices (history)', label: d.name + ' · ' + d.serial, tag: d.kind === 'battery' ? 'BAT' : 'MOT', run: go('history.html?type=' + (d.kind === 'battery' ? 'batteries' : 'motors') + '&device=' + d.id) })),
    ...FP.anomalies.map((a) => ({ group: 'Anomalies', label: a.dev + ': ' + a.title, tag: a.severity.toUpperCase(), run: go('anomalies.html?id=' + a.id + '&tab=' + (a.state === 'resolved' || a.state === 'dismissed' ? a.state : 'open')) })),
    { group: 'Actions', label: 'Run pit checks', tag: 'PIT', run: go('pit.html') },
    { group: 'Actions', label: 'Toggle CRT scanlines', tag: 'UI', run: () => { crtOn = !crtOn; setCrt(crtOn); } }
  ].map((it) => ({ ...it, hay: (it.label + ' ' + it.group + ' ' + it.tag).toLowerCase() }));

  const dlg = document.createElement('dialog');
  dlg.className = 'palette';
  dlg.setAttribute('aria-label', 'Command palette');
  dlg.innerHTML = `
    <div class="palette__bar"><span aria-hidden="true">&gt;_</span>
      <input class="palette__input" type="text" role="combobox" aria-expanded="true" aria-controls="pal-list" aria-autocomplete="list" placeholder="Mechanism, device, match, anomaly or view" autocomplete="off" spellcheck="false">
      <span class="kbd">ESC</span></div>
    <ul class="palette__list" id="pal-list" role="listbox"></ul>
    <div class="palette__foot"><span>↑↓ move</span><span>⏎ open</span><span>esc close</span></div>`;
  document.body.appendChild(dlg);
  const input = dlg.querySelector('input'), list = dlg.querySelector('ul');
  let items = [], cursor = 0;
  const render = () => {
    const q = input.value.trim().toLowerCase();
    items = q ? POOL.map((it) => ({ it, s: it.label.toLowerCase().startsWith(q) ? 3 : it.hay.includes(q) ? 2 : 0 })).filter((x) => x.s).sort((a, b) => b.s - a.s).map((x) => x.it).slice(0, 30)
              : POOL.filter((it) => it.group === 'Go to' || it.group === 'Anomalies').slice(0, 10);
    cursor = Math.min(cursor, Math.max(items.length - 1, 0));
    let html = '', last = '';
    items.forEach((it, i) => {
      if (it.group !== last) { html += `<li class="palette__group" role="presentation">${esc(it.group.toUpperCase())}</li>`; last = it.group; }
      html += `<li class="palette__item" role="option" id="pal-${i}" data-i="${i}" aria-selected="${i === cursor}">${esc(it.label)}<span class="palette__item-tag">${esc(it.tag)}</span></li>`;
    });
    list.innerHTML = html || '<li class="palette__group" role="presentation">NO MATCHES. TRY A SHORTER TERM.</li>';
    input.setAttribute('aria-activedescendant', items.length ? 'pal-' + cursor : '');
    list.querySelector('[aria-selected="true"]')?.scrollIntoView({ block: 'nearest' });
  };
  const open = () => { if (dlg.open) return; input.value = ''; cursor = 0; render(); dlg.showModal(); input.focus(); };
  const run = (i) => { const it = items[i]; if (it) { dlg.close(); it.run(); } };
  input.addEventListener('input', () => { cursor = 0; render(); });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); cursor = (cursor + 1) % Math.max(items.length, 1); render(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); cursor = (cursor - 1 + items.length) % Math.max(items.length, 1); render(); }
    else if (e.key === 'Enter') { e.preventDefault(); run(cursor); }
  });
  list.addEventListener('click', (e) => { const li = e.target.closest('[data-i]'); if (li) run(+li.dataset.i); });
  dlg.addEventListener('click', (e) => { if (e.target === dlg) dlg.close(); });
  document.addEventListener('click', (e) => { if (e.target.closest('[data-open-palette]')) open(); });
  document.addEventListener('keydown', (e) => {
    const typing = /INPUT|TEXTAREA|SELECT/.test(document.activeElement?.tagName);
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); open(); }
    else if (e.key === '/' && !typing && !dlg.open) { e.preventDefault(); open(); }
  });
})();
