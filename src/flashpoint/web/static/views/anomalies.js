/* ==========================================================================
   Anomalies: what detection flagged, the evidence, and what the team did about
   it (canvas board "Anomalies"). Served mode only: the queue and detail come
   from /api/anomalies; triage is the app's one write, offered only when the
   server is bound to this machine (ADR-0015).
   URL: #view=anomalies&tab=open|resolved|dismissed&sens=relaxed|normal|strict
        &id=<anomaly id>&group=device (the queue grouped by device; absent or unknown: a list)
        &season=&robot=&event=&unit=<the shared filters, 'all' or a value (FP.filters),
          applied by the server (/api/anomalies)>
   ========================================================================== */
(function () {
  'use strict';
  const FP = window.FP;
  const { h } = FP;

  const TABS = [['open', 'OPEN'], ['resolved', 'RESOLVED'], ['dismissed', 'DISMISSED']];
  const GROUPS = [['none', 'NONE'], ['device', 'DEVICE']];
  const SEV_RANK = { HIGH: 0, MED: 1, LOW: 2 };
  const METHODS = [
    ['SPEC', 'a fixed limit (temperature, stall, reboots, CAN gaps, brownouts) was crossed'],
    ['STEP', 'a unit jumped away from its own history and stayed there'],
    ['DRIFT', 'a unit moved slowly away from its own history'],
    ['SIBLING', 'a unit left the others that do the same job, in the same matches'],
    ['MODEL', 'an optional model found an unusual mix of features (off by default)'],
  ];
  const ACTIONS = {
    investigating: 'INVESTIGATE', resolved: 'MARK RESOLVED', dismissed: 'DISMISS AS EXPECTED', new: 'REOPEN',
  };
  // Anomaly metric -> the History metric that plots it (the closest one History has).
  const HISTORY_METRIC = {
    temp_max_c: 'peak_temp', temp_mean_c: 'peak_temp', temp_rise_c_per_min: 'peak_temp',
    stator_current_p95: 'current_p95', residual_p95_a: 'current_p95', stall_s: 'stall_s',
    reboots: 'reboots', isolated_gaps: 'isolated_gaps', missed_per_min: 'missed_per_min',
    longest_gap_ms: 'longest_gap_ms', supply_resistance_mohm: 'current_p95',
  };
  const MAX_OUTCOME = 500;

  let ui = null;

  const label = (key) => {
    const found = /^[^_]*_([a-z]+)(\d+)(r\d+)?$/.exec(key || '');
    if (!found) return key || '—';
    const level = { qm: 'Q', pm: 'P', e: 'E' }[found[1]] || found[1].toUpperCase();
    return level + found[2] + (found[3] || '');
  };
  const sevClass = (s) => 'sev sev--' + String(s || 'low').toLowerCase();
  const who = (a) => (a.subject === 'power' ? 'Battery · ' + a.robot : (a.role || a.slot_id || a.subject));

  async function mount(el, state) {
    const sens = FP.SENSITIVITIES.includes(state.sens) ? state.sens : FP.sensitivity();
    ui = {
      el,
      tab: TABS.some(([t]) => t === state.tab) ? state.tab : 'open',
      sens,
      id: state.id || null,
      group: state.group === 'device' ? 'device' : 'none', // an unknown value is the plain list
      collapsed: new Set(), // collapsed device groups (not kept)
      combos: [],
      form: null,
      chart: null,
    };
    FP.sensitivity(sens);
    const mine = ui;
    let combos = [];
    try {
      combos = (await FP.api('filters')).combos || [];
    } catch (_) {
      combos = []; // the bar still works; every value is named as having no data
    }
    if (ui !== mine || !el.isConnected) return; // the route changed while waiting
    ui.combos = combos;
    renderBar();
    const band = h('section', { class: 'band', 'aria-label': 'Anomaly summary' },
      h('div', { class: 'clock' },
        h('span', { class: 'anom-count', id: 'open-count' }, '—'),
        h('span', { class: 'meta' }, 'open anomalies', h('br'), h('span', { id: 'open-sub' }))),
      h('div', { class: 'group' }, h('span', { class: 'group__label' }, 'SHOW'), h('span', { class: 'seg', id: 'tabs', role: 'group', 'aria-label': 'Queue' })),
      h('div', { class: 'band__spacer' }),
      h('div', { class: 'group' }, h('span', { class: 'group__label' }, 'SENSITIVITY'),
        h('span', { class: 'seg', id: 'sens', role: 'group', 'aria-label': 'Sensitivity' })));
    const rail = FP.rail({ view: 'anomalies', side: 'l', label: 'Queue', tag: 'nav', attrs: { class: 'rail-l panel panel--rail-l pad', 'aria-label': 'Anomaly queue' } });
    FP.fill(rail.body,
      h('div', { class: 'group mb-3' }, h('span', { class: 'group__label' }, 'GROUP'),
        h('span', { class: 'seg', id: 'grouping', role: 'group', 'aria-label': 'Group the queue' })),
      h('h2', { class: 'label', id: 'list-title' }),
      h('div', { id: 'queue-note', 'aria-live': 'polite' }),
      h('div', { class: 'anom-queue', id: 'queue' }),
      h('h2', { class: 'label mt-6' }, 'How Flashpoint flags things'),
      h('div', { class: 'anom-methods' }, METHODS.map(([m, text]) => h('span', null, h('b', null, m), ' · ', text))));
    const main = h('main', { class: 'main pad', id: 'detail', 'aria-live': 'polite' });
    FP.fill(el, ui.bar, band, h('div', { class: 'layout' }, rail.el, main));
    await refresh();
  }

  function renderBar() {
    const bar = FP.filterBar({ filters: FP.filters(), combos: ui.combos, unknownText: '(not in this lake)', onChange: setFilters });
    ui.bar = ui.bar ? FP.swap(ui.bar, bar) : bar;
  }

  function setFilters(patch) {
    FP.setFilters(patch);
    ui.form = null;
    renderBar();
    refresh();
  }

  function writeState() {
    FP.setState({ view: 'anomalies', tab: ui.tab, sens: ui.sens, id: ui.id, group: ui.group === 'device' ? 'device' : null });
  }

  /* The shared filters as query parameters ('all' is no filter). */
  function scope() {
    const f = FP.filters();
    return Object.fromEntries(FP.SHARED.filter((k) => f[k] !== FP.ALL).map((k) => [k, f[k]]));
  }

  async function refresh() {
    const mine = ui;
    let queue;
    const params = scope();
    try {
      queue = await FP.api('anomalies', { sens: ui.sens, tab: ui.tab, ...params });
      ui.hidden = false;
      if (ui.id && !queue.items.some((i) => i.id === ui.id) && Object.keys(params).length) {
        // Hidden by the filters (it stays open), or gone from this tab (the first item opens)?
        const all = await FP.api('anomalies', { sens: ui.sens, tab: ui.tab });
        ui.hidden = all.items.some((i) => i.id === ui.id);
      }
    } catch (err) {
      if (ui !== mine) return;
      FP.fill(ui.el, h('div', { class: 'notice' }, h('h1', { class: 'notice__title' }, 'Anomalies unavailable'), h('p', { class: 'prose' }, String(err.message))));
      return;
    }
    if (ui !== mine || !ui.el.isConnected) return;
    ui.queue = queue;
    if (!ui.hidden && !queue.items.some((i) => i.id === ui.id)) ui.id = queue.items.length ? queue.items[0].id : null;
    writeState();
    renderBand(queue);
    renderQueue(queue);
    await renderDetail();
  }

  function renderBand(q) {
    const count = document.getElementById('open-count');
    count.textContent = String(q.counts.open);
    count.classList.toggle('is-hot', q.high > 0);
    FP.fill(document.getElementById('open-sub'), q.high + ' high · ',
      q.scanned_through ? 'scanned through ' + q.scanned_through : 'nothing scanned yet');
    FP.fill(document.getElementById('tabs'), TABS.map(([t, word]) => h('button', {
      type: 'button', class: 'btn btn--sm', 'aria-pressed': String(ui.tab === t), dataset: { tab: t },
      on: { click: () => { ui.tab = t; ui.id = null; ui.form = null; refresh(); } },
    }, word + ' · ' + q.counts[t])));
    FP.fill(document.getElementById('sens'), FP.SENSITIVITIES.map((s) => h('button', {
      type: 'button', class: 'btn btn--sm', 'aria-pressed': String(ui.sens === s), dataset: { sens: s },
      on: { click: () => { ui.sens = FP.sensitivity(s); ui.form = null; refresh(); } },
    }, s.toUpperCase())));
    FP.fill(document.getElementById('grouping'), GROUPS.map(([g, word]) => h('button', {
      type: 'button', class: 'btn btn--sm', 'aria-pressed': String(ui.group === g), dataset: { group: g },
      on: { click: () => { ui.group = g; writeState(); renderBand(ui.queue); renderQueue(ui.queue); } },
    }, word)));
  }

  function item(a) {
    return h('button', {
      type: 'button', class: 'anom' + (a.severity === 'HIGH' ? ' anom--high' : ''), 'aria-pressed': String(a.id === ui.id),
      dataset: { id: a.id }, on: { click: () => { ui.id = a.id; ui.hidden = false; ui.form = null; writeState(); markSelected(); renderQueueNote(); renderDetail(); } },
    },
    h('span', { class: 'anom__row' },
      a.severity ? h('span', { class: sevClass(a.severity) }, a.severity + ' · ' + who(a)) : h('span', { class: 'meta' }, who(a)),
      h('span', { class: 'muted' }, a.method || '—')),
    h('span', { class: 'anom__title' }, a.title),
    a.event_boundary ? h('span', { class: 'badge badge--event', dataset: { eventBoundary: '' } }, 'EVENT BOUNDARY · ' + eventText(a.event_boundary)) : null,
    h('span', { class: 'anom__row' },
      h('span', { class: 'muted' }, 'since ' + label(a.onset_match_key)),
      h('span', { class: 'astate astate--' + a.state }, a.state.toUpperCase() + (a.detected ? '' : ' · NO LONGER DETECTED'))));
  }

  /* Device groups: one per physical unit (a robot's power is its battery), ordered by highest
     severity, then the most recent last-flagged match; items keep the queue order. */
  function deviceGroups(items) {
    const byKey = new Map();
    for (const a of items) {
      const key = a.subject_key || a.subject;
      if (!byKey.has(key)) byKey.set(key, []);
      byKey.get(key).push(a);
    }
    const rank = (s) => (s in SEV_RANK ? SEV_RANK[s] : 3);
    const groups = [...byKey.entries()].map(([key, list]) => {
      const recent = list.reduce((b, a) => ((a.last_start || '') > (b.last_start || '') ? a : b));
      const worst = list.map((a) => a.severity).sort((x, y) => rank(x) - rank(y))[0] || null;
      const power = recent.subject === 'power';
      const name = power ? 'Battery · ' + (recent.robot || key.replace(/^power:/, ''))
        : (recent.role || recent.slot_id || recent.subject) + ' · ' + (recent.serial ? 'SN ' + recent.serial : 'legacy id');
      return { key, items: list, worst, last: recent.last_start || '', name };
    });
    return groups.sort((a, b) => rank(a.worst) - rank(b.worst) || (b.last > a.last ? 1 : b.last < a.last ? -1 : 0));
  }

  function renderQueueNote() {
    const note = document.getElementById('queue-note');
    if (note) FP.fill(note, ui.hidden ? h('p', { class: 'meta mb-3', id: 'hidden-note' }, 'The selected anomaly is hidden by the filters.') : null);
  }

  function renderQueue(q) {
    FP.fill(document.getElementById('list-title'), ui.tab.toUpperCase() + ' · ' + q.items.length + ' · highest severity first');
    renderQueueNote();
    const box = document.getElementById('queue');
    const f = FP.filters();
    if (!q.items.length && FP.filtered(f)) {
      const unknown = FP.unknownFilters(f, ui.combos);
      FP.fill(box, h('div', { class: 'callout callout--quiet', id: 'queue-empty' },
        h('p', null, 'Nothing passes these filters.'),
        unknown.length ? h('p', null, 'No match in this lake has ', unknown.join(', '), '.') : null,
        h('button', { type: 'button', class: 'btn btn--sm mt-2', id: 'queue-clear', on: { click: () => setFilters({ season: FP.ALL, robot: FP.ALL, event: FP.ALL, unit: FP.ALL }) } }, 'CLEAR FILTERS')));
      return;
    }
    if (!q.items.length) {
      FP.fill(box, h('div', { class: 'meta', id: 'queue-empty' },
        h('p', null, ui.tab === 'open' ? 'Nothing is flagged at ' + ui.sens + ' sensitivity.' : 'Nothing here.'),
        q.building_baseline ? h('p', { class: 'mt-2', id: 'building' }, q.building_baseline + ' unit(s) are still building their baseline (5 matches before DRIFT and STEP can judge them).') : null,
        q.detected ? null : h('p', { class: 'mt-2' }, 'Detection has not run on this lake yet: run ', h('code', { class: 'code' }, 'flashpoint detect'), '.')));
      return;
    }
    if (ui.group !== 'device') {
      FP.fill(box, q.items.map(item));
      return;
    }
    FP.fill(box, deviceGroups(q.items).map((g, i) => {
      const open = !ui.collapsed.has(g.key);
      const listId = 'group-' + i;
      const toggle = () => {
        if (ui.collapsed.has(g.key)) ui.collapsed.delete(g.key); else ui.collapsed.add(g.key);
        renderQueue(ui.queue);
        const again = document.querySelector('#queue [data-group-head="' + i + '"]');
        if (again) again.focus();
      };
      return h('section', { class: 'anom-group', dataset: { group: g.key }, 'aria-label': g.name },
        h('button', { type: 'button', class: 'anom-group__head', 'aria-expanded': String(open), 'aria-controls': listId, dataset: { groupHead: String(i) }, on: { click: toggle } },
          h('span', { class: 'anom-group__caret', 'aria-hidden': 'true' }, open ? '▾' : '▸'),
          h('span', { class: 'anom-group__name' }, g.name),
          h('span', { class: 'anom-group__count' }, g.items.length + (g.items.length === 1 ? ' anomaly' : ' anomalies')),
          g.worst ? h('span', { class: sevClass(g.worst) + ' anom-group__sev' }, g.worst) : null),
        h('div', { class: 'anom-group__items', id: listId, hidden: !open }, g.items.map(item)));
    }));
  }

  // "2026gadal → 2026gacmp" -> "GADAL → GACMP"
  function eventText(s) { return s.replace(/\b\d{4}([a-z0-9]+)/g, (_, e) => e.toUpperCase()); }

  function markSelected() {
    document.querySelectorAll('#queue .anom').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.id === ui.id)));
  }

  /* ---------- detail ---------- */
  async function renderDetail() {
    const main = document.getElementById('detail');
    if (ui.chart) { ui.chart.destroy(); ui.chart = null; }
    ui.marks = null;
    if (!ui.id) {
      FP.fill(main, h('p', { class: 'meta' }, 'Pick an anomaly from the queue to see the evidence.'));
      return;
    }
    const mine = ui;
    const id = ui.id;
    const a = await FP.api('anomalies/' + encodeURIComponent(id));
    if (ui !== mine || ui.id !== id || !main.isConnected) return;
    ui.detail = a;
    const fact = (k, v) => h('div', { class: 'fact' }, h('div', { class: 'fact__k' }, k), h('div', { class: 'fact__v' }, v == null ? '—' : String(v)));
    const subject = a.subject === 'power'
      ? h('a', { href: FP.link({ view: 'replay', m: a.onset_match_key }), id: 'subject-link' }, 'open in Replay (battery track)')
      : h('a', { href: FP.link({ view: 'history', unit: a.subject_key, metric: HISTORY_METRIC[a.metric] || 'peak_temp' }), id: 'subject-link' }, 'unit history');
    FP.fill(main,
      h('div', { class: 'anom-head' },
        h('div', { class: 'anom-head__who' },
          h('div', { class: 'meta' },
            a.severity ? h('span', { class: sevClass(a.severity) }, a.severity) : null, ' · ', a.method || '—', ' · ',
            h('span', { class: 'astate astate--' + a.state, id: 'state' }, a.state.toUpperCase())),
          h('h1', { class: 'title-d3', id: 'subject' }, who(a)),
          h('div', { class: 'meta' },
            a.model ? a.model + ' · ' : '', a.serial ? 'SN ' + a.serial : (a.subject === 'power' ? 'battery proxy' : 'legacy id ' + a.subject),
            a.slot_id && a.subject !== 'power' ? ' · ' + a.slot_id : '', a.robot ? ' · ' + a.robot : '', ' · ', subject)),
        actions(a)),
      h('div', { id: 'triage-form' }),
      ui.queue && ui.queue.restart_pending ? h('p', { class: 'callout mt-3', id: 'restart-pending' }, 'Baseline restart pending: detection has not run since the restart was recorded. The queue updates when it does.') : null,
      a.detected ? null : h('p', { class: 'callout callout--quiet mt-3', id: 'not-detected' }, 'No longer detected: the latest detection run does not produce this anomaly (its configuration or data changed). Its triage is kept.'),
      h('p', { class: 'anom-lead', id: 'title' }, a.title),
      a.detected ? h('div', { class: 'facts mb-6' },
        fact('Actual', a.actual), fact('Expected', a.expected), fact('First seen', label(a.onset_match_key)),
        fact('Matches affected', a.matches_affected), fact('Confidence', a.confidence), fact('Method', a.method)) : null,
      a.low_alignment ? h('p', { class: 'badge badge--low mb-3' }, '▲ LOW ALIGNMENT IN A FLAGGED MATCH') : null,
      a.event_boundary ? h('p', { class: 'badge badge--event mb-3', id: 'event-boundary' }, 'EVENT BOUNDARY · ' + eventText(a.event_boundary) + ' · STARTED AT THE FIRST MATCH OF THE NEW EVENT') : null,
      a.evidence && a.evidence.points && a.evidence.points.length ? evidence(a) : null,
      a.detected ? h('div', { class: 'anom-cols' },
        h('section', null, h('h2', { class: 'h-section' }, 'Why it was flagged'), h('p', { class: 'prose', id: 'why' }, a.why)),
        h('section', null, h('h2', { class: 'h-section' }, 'Likely causes'),
          a.causes.items.length
            ? h('ul', { class: 'prose', id: 'causes' }, a.causes.items.map((c) => h('li', null, c)))
            : h('p', { class: 'prose', id: 'causes' }, a.causes.note),
          h('p', { class: 'meta' }, 'Suggestions from the team catalogue (config/anomaly-causes.toml), not a diagnosis.'))) : null,
      h('div', { class: 'card mt-6', id: 'seen-before' },
        h('div', { class: 'fact__k mb-3' }, 'Seen this before'),
        a.seen_before.length ? h('ul', { class: 'prose' }, a.seen_before.map((s) => h('li', null,
          h('a', { href: FP.link({ view: 'anomalies', tab: s.state, sens: ui.sens, id: s.id }) }, label(s.onset_match_key) + ' · ' + s.subject),
          ' · ', s.state, ': ', h('span', { class: 'hi' }, s.outcome || '—'))))
          : h('p', { class: 'meta' }, 'No closed anomaly like this on this slot or unit yet.')),
      a.state === 'resolved' || a.state === 'dismissed' ? h('div', { class: 'card mt-6' },
        h('div', { class: 'fact__k mb-3' }, 'Outcome'), h('p', { class: 'prose', id: 'outcome' }, a.outcome || '—')) : null,
      a.history.length ? h('section', { class: 'mt-6' }, h('h2', { class: 'h-section' }, 'History'),
        h('ol', { class: 'meta', id: 'history' }, a.history.map((r) => h('li', null,
          FP.when(r.at), ' · ', r.from_state, ' → ', r.to_state, r.restart_baseline ? ' · baseline restarted' : '',
          r.outcome ? [': ', h('span', { class: 'hi' }, r.outcome)] : null)))) : null);
    renderForm();
  }

  function evidence(a) {
    const points = a.evidence.points;
    const box = h('div', { class: 'chart-box', id: 'evidence' });
    const legendPeer = a.evidence.peer;
    const runs = FP.competitions(points.map((p) => String(p.match_key || '').split('_')[0]));
    ui.marks = FP.competitionMarks(runs);
    const section = h('section', null,
      h('h2', { class: 'h-section' }, 'Evidence ', h('span', { class: 'h-section__aside' }, a.metric_name + (a.metric_unit ? ' · ' + a.metric_unit : '') + ' · per match')),
      h('p', { class: 'meta mb-2', id: 'competitions' }, FP.competitionsText(runs)),
      box,
      h('p', { class: 'legend mt-2' },
        h('span', null, h('span', { class: 'hi' }, '━'), ' ', who(a)),
        legendPeer ? h('span', null, h('span', { class: 'dim' }, '┅'), ' ', legendPeer) : null,
        h('span', null, '▒ expected'),
        h('span', null, h('span', { class: 'hot' }, '■'), ' flagged'),
        h('span', null, 'Select a point to open that match in Replay.')));
    const t = FP.tokens();
    const xs = points.map((_, i) => i);
    const flagged = points.map((p) => (p.flagged ? p.value : null));
    const hasBand = points.some((p) => p.lo != null || p.hi != null);
    const series = [{
      label: a.metric_name, values: points.map((p) => p.value), stroke: t.accent, width: 2,
      points: { show: true, size: 4, fill: t.accent, stroke: t.accent },
      band: hasBand ? { lower: points.map((p) => (p.lo == null ? (p.hi == null ? null : -1e9) : p.lo)), upper: points.map((p) => p.hi) } : null,
    }];
    if (legendPeer) series.push({ label: legendPeer, values: points.map((p) => p.peer), stroke: t.accentDim, width: 1.5, dash: [5, 3] });
    series.push({ label: 'flagged', values: flagged, stroke: t.warn, width: 0, paths: () => null, points: { show: true, size: 9, fill: t.warn, stroke: t.warn } });
    const values = points.flatMap((p) => [p.value, p.peer, p.hi, p.lo]).filter((v) => v != null && Math.abs(v) < 1e8);
    const lo = Math.min(...values); const hi = Math.max(...values); const pad = (hi - lo) * 0.1 || 1;
    requestAnimationFrame(() => {
      if (!box.isConnected) return;
      ui.chart = FP.track(box, {
        height: 220,
        x: xs,
        xRange: () => [-0.5, points.length - 0.5],
        yRange: () => [lo - pad, hi + pad],
        xIncrs: [1, 2, 5, 10, 20],
        series,
        marks: ui.marks,
        xLabel: (v) => { const i = Math.round(v); return Math.abs(v - i) < 1e-6 && points[i] ? label(points[i].match_key) : ''; },
        yLabel: (v) => FP.fmt(v, Math.abs(v) >= 100 ? 0 : 1),
        ariaLabel: a.metric_name + ' per match for ' + who(a) + ': actual ' + a.actual + ', expected ' + a.expected + '. '
          + points.filter((p) => p.flagged).length + ' of ' + points.length + ' matches flagged.',
        onPick: (x) => openMatch(a, points[Math.max(0, Math.min(points.length - 1, Math.round(x)))].match_key),
      });
    });
    return section;
  }

  function openMatch(a, key) {
    if (a.subject === 'power') FP.go({ view: 'replay', m: key });
    else FP.go({ view: 'replay', m: key, track: a.slot_id });
  }

  /* ---------- triage ---------- */
  function actions(a) {
    if (!a.writable) return h('p', { class: 'meta', id: 'read-only' }, 'Read-only: triage is only available on the machine running Flashpoint.');
    return h('div', { class: 'seg', id: 'actions' }, a.actions.map((to) => h('button', {
      type: 'button', class: 'btn' + (to === 'investigating' ? ' btn--primary' : ''), dataset: { to },
      on: { click: () => choose(to) },
    }, ACTIONS[to])));
  }

  function choose(to) {
    if (to === 'resolved' || to === 'dismissed') {
      ui.form = { to, outcome: '', restart: false, error: null };
      renderForm();
      const field = document.getElementById('outcome-input');
      if (field) field.focus();
    } else {
      send({ to });
    }
  }

  function renderForm() {
    const box = document.getElementById('triage-form');
    if (!box) return;
    const f = ui.form;
    if (!f) { FP.clear(box); return; }
    const text = h('textarea', { id: 'outcome-input', maxlength: String(MAX_OUTCOME), 'aria-describedby': 'outcome-help',
      on: { input: (e) => { f.outcome = e.target.value; } } });
    text.value = f.outcome;
    FP.fill(box, h('form', { class: 'anom-form', on: { submit: (e) => { e.preventDefault(); send({ to: f.to, outcome: f.outcome, restart_baseline: f.restart }); } } },
      h('label', { class: 'group__label', for: 'outcome-input' }, f.to === 'resolved' ? 'WHAT WAS FOUND OR FIXED' : 'WHY THIS IS EXPECTED'),
      text,
      h('span', { class: 'meta', id: 'outcome-help' }, 'Required, up to ' + MAX_OUTCOME + ' characters. Shown with this anomaly and in "seen this before".'),
      f.to === 'dismissed' ? h('label', { class: 'check' },
        h('input', { type: 'checkbox', id: 'restart-input', checked: f.restart, on: { change: (e) => { f.restart = e.target.checked; } } }),
        ' Restart this unit\'s baseline for ' + (ui.detail ? ui.detail.metric_name : 'this metric') + ' from the onset match (for example after a gearbox change)') : null,
      f.error ? h('p', { class: 'callout', id: 'triage-error', role: 'alert' }, f.error) : null,
      h('div', { class: 'seg' },
        h('button', { type: 'submit', class: 'btn btn--primary', id: 'triage-save' }, 'SAVE · ' + ACTIONS[f.to]),
        h('button', { type: 'button', class: 'btn', id: 'triage-cancel', on: { click: () => { ui.form = null; renderForm(); } } }, 'CANCEL'))));
  }

  async function send(body) {
    const mine = ui;
    let reply;
    try {
      const response = await fetch('api/anomalies/' + encodeURIComponent(ui.id) + '/triage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Flashpoint': 'triage', Accept: 'application/json' },
        body: JSON.stringify(body),
      });
      reply = await response.json();
    } catch (err) {
      reply = { saved: false, reason: String(err.message) };
    }
    if (ui !== mine) return;
    if (!reply.saved) {
      ui.form = ui.form || { to: body.to, outcome: '', restart: false };
      ui.form.error = 'Not saved: ' + (reply.reason || 'unknown error') + '.';
      renderForm();
      return;
    }
    ui.form = null;
    FP.refreshBadges();
    await refresh();
  }

  // For tests: the evidence chart's competition boundaries ([{ x, label }]).
  FP.anomalyMarks = () => (ui && ui.marks ? ui.marks : null);
  // For tests: the plotted evidence ({ name: values }).
  FP.anomalyChart = () => (ui && ui.chart ? Object.fromEntries(ui.chart.u.series.slice(1).map((s, i) => [s.label, ui.chart.u.data[i + 1]])) : null);

  FP.view({
    id: 'anomalies',
    label: 'Anomalies',
    needsApi: true,
    mount,
    unmount: () => { if (ui && ui.chart) ui.chart.destroy(); ui = null; },
    badge: async () => {
      const b = await FP.api('anomalies/summary');
      return { text: String(b.open), hot: b.high > 0, label: b.open + ' open anomalies' + (b.high ? ', ' + b.high + ' high' : '') };
    },
  });
})();
