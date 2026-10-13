/* ==========================================================================
   History: how each physical unit (by serial) and each slot trends across
   matches, events, seasons and robots. Served mode only: every filter runs as
   a DuckDB query behind /api (nothing is filtered in the browser).
   URL: #view=history&season=&robot=&event=&unit=<the shared filters, 'all' or a value
          (FP.filters); unit selects that unit (its line, row, and the right rail)>
        &match_type=&phase=&subsystem=&slot=<History-only filters, restored from the nav>
        &metric=<id>&by=unit|slot
   ========================================================================== */
(function () {
  'use strict';
  const FP = window.FP;
  const { h } = FP;

  const { ALL } = FP;
  // History-only filters, after the shared ones (season, robot, event, unit) in the filter bar.
  const EXTRAS = [
    ['match_type', 'TYPE', 'All types'],
    ['phase', 'PHASE', null],
    ['subsystem', 'SUBSYSTEM', 'All subsystems'],
    ['slot', 'SLOT', 'All slots'],
  ];
  const EXTRA_KEYS = EXTRAS.map(([key]) => key);
  const DEFAULT_PHASE = 'match';
  const TYPE_LABEL = { qm: 'Qualification', pm: 'Practice', e: 'Elimination', sf: 'Semifinal', f: 'Final' };
  const HEALTH = { OK: ['ok', 'OK'], WARN: ['warn', 'WARN'], HOT: ['fault', 'HOT'], 'NO-DATA': ['retired', 'NO DATA'] };
  const KIND = { 'first-seen': 'First seen', moved: 'Moved', 'replaced-by': 'Replaced by', 'last-seen': 'Last seen' };

  let ui = null;
  const NEAR_PX = 10; // a line within this many pixels of the pointer is the one it is on

  /* Query parameters: the shared filters that are set, then History's own. */
  const filterParams = (s) => {
    const out = {};
    for (const key of ['season', 'robot', 'event']) if (s[key] && s[key] !== ALL) out[key] = s[key];
    for (const key of EXTRA_KEYS) if (s[key]) out[key] = s[key];
    out.phase = s.phase || DEFAULT_PHASE;
    return out;
  };
  const selectedUnit = () => (ui.state.unit && ui.state.unit !== ALL ? ui.state.unit : null);

  async function mount(el, state) {
    ui = { el, state: Object.assign({ metric: 'peak_temp', by: 'unit' }, state), chart: null };
    let filters;
    try {
      filters = await FP.api('filters');
      if (!el.isConnected) return; // the route changed while waiting
    } catch (err) {
      FP.fill(el, h('div', { class: 'notice' }, h('h1', { class: 'notice__title' }, 'History unavailable'), h('p', { class: 'prose' }, String(err.message))));
      return;
    }
    if (filters.empty) {
      FP.fill(el, h('div', { class: 'notice' },
        h('h1', { class: 'notice__title' }, 'No matches ingested'),
        h('p', { class: 'prose' }, 'The lake at ', h('code', { class: 'code' }, filters.lake), ' has no match features yet.'),
        h('p', { class: 'prose' }, 'Ingest logs with ', h('code', { class: 'code' }, 'flashpoint ingest <folder of .wpilog and .hoot files>'), ', then reload.')));
      return;
    }
    ui.values = filters.values;
    ui.combos = filters.combos;
    renderBar();
    const band = h('section', { class: 'band', 'aria-label': 'Summary' },
      h('div', { class: 'band__spacer' }),
      h('div', { class: 'tiles', id: 'tiles', 'aria-live': 'polite' }));
    const main = h('div', { class: 'main pad' },
      h('div', { class: 'title-row' },
        h('h2', { class: 'h-section', id: 'metric-title' }),
        h('div', { class: 'row' },
          h('span', { class: 'seg', id: 'metrics', role: 'group', 'aria-label': 'Metric' }),
          h('span', { class: 'seg toggle', role: 'group', 'aria-label': 'Lines per' },
            ['unit', 'slot'].map((by) => h('button', { type: 'button', class: 'btn btn--sm', 'aria-pressed': String(ui.state.by === by), dataset: { by }, on: { click: () => setBy(by) } }, 'PER ' + by.toUpperCase()))))),
      h('div', { class: 'chart-box', id: 'chart' }),
      h('p', { class: 'chart-hover', id: 'chart-hover', 'aria-live': 'polite' }),
      h('p', { class: 'legend', id: 'legend' }),
      h('h2', { class: 'section-label mt-6' }, h('span', { id: 'table-title' }, 'Units')),
      h('div', { class: 'table-wrap' }, h('table', { class: 'table' },
        h('thead', null, h('tr', null,
          h('th', { scope: 'col' }, 'Unit · slot'), h('th', { scope: 'col' }, 'Serial'), h('th', { scope: 'col' }, 'In service'),
          h('th', { scope: 'col', class: 'num' }, 'Matches'), h('th', { scope: 'col', class: 'num' }, 'Latest'), h('th', { scope: 'col', class: 'num' }, 'Change'),
          h('th', { scope: 'col' }, 'Trend'), h('th', { scope: 'col' }, 'Health'))),
        h('tbody', { id: 'rows' }))));
    const aside = FP.rail({ view: 'history', side: 'r', label: 'Unit', attrs: { class: 'rail-r panel panel--rail-r', id: 'device', 'aria-label': 'Unit details' } });
    ui.device = aside.body;
    FP.fill(el, ui.bar, band, h('div', { class: 'layout' }, main, aside.el));
    renderMetrics();
    await refresh();
  }

  function renderBar() {
    const s = ui.state;
    const extras = EXTRAS.map(([key, label, allLabel]) => {
      const options = ui.values[key] || [];
      const text = (v) => (key === 'match_type' ? (TYPE_LABEL[v] || v) : key === 'phase' ? v.toUpperCase() : v);
      const value = key === 'phase' ? (s.phase && s.phase !== DEFAULT_PHASE ? s.phase : null) : (s[key] || null);
      // A value the lake doesn't hold (from an address) stays chosen and named.
      const all = value && !options.includes(value) ? [value, ...options] : options;
      return { key, label, allLabel, value, fallback: key === 'phase' ? DEFAULT_PHASE : '', options: all.map((v) => [v, text(v)]) };
    });
    const bar = FP.filterBar({
      filters: FP.filters(s), combos: ui.combos, extras, only: 'HISTORY ONLY', unknownText: '(not in this lake)', onChange: setFilters,
    });
    ui.bar = ui.bar ? FP.swap(ui.bar, bar) : bar;
  }

  function renderMetrics() {
    const box = document.getElementById('metrics');
    FP.fill(box, ui.values.metric.map((m) => h('button', {
      type: 'button', class: 'btn btn--sm', 'aria-pressed': String(ui.state.metric === m.id), dataset: { metric: m.id },
      on: { click: () => setMetric(m.id) },
    }, m.label.toUpperCase())));
  }

  async function refresh() {
    const params = filterParams(ui.state);
    const own = Object.fromEntries(EXTRA_KEYS.map((key) => [key, ui.state[key] || null]));
    if (own.phase === DEFAULT_PHASE) own.phase = null;
    FP.setState({ view: 'history', ...own, metric: ui.state.metric, by: ui.state.by });
    const mine = ui;
    const [summary, trend, devices] = await Promise.all([
      FP.api('summary', params),
      FP.api('trend', { ...params, metric: ui.state.metric, by: ui.state.by }),
      FP.api('devices', { ...params, metric: ui.state.metric }),
    ]);
    if (ui !== mine || !ui.el.isConnected) return; // superseded by a newer view or refresh
    ui.trend = trend;
    renderTiles(summary.tiles);
    renderChart(trend);
    renderTable(devices.rows, trend);
    await renderDevice();
  }

  function renderTiles(tiles) {
    FP.fill(document.getElementById('tiles'), [
      ['UNITS TRACKED', tiles.units], ['MATCHES', tiles.matches], ['POWERED-ON H', FP.fmt(tiles.powered_h, 1)], ['UNITS WARN/HOT', tiles.units_warn_hot],
    ].map(([k, v]) => h('div', null, h('div', { class: 'group__label' }, k), h('div', { class: 'tile__v' }, String(v)))));
  }

  function renderChart(trend) {
    const box = document.getElementById('chart');
    if (ui.chart) { ui.chart.destroy(); ui.chart = null; }
    FP.clear(box);
    const title = document.getElementById('metric-title');
    FP.fill(title, trend.label + ' ', h('span', { class: 'h-section__aside' }, 'per match · ' + trend.unit + (trend.reference != null ? ' · reference ' + trend.reference + ' ' + trend.unit : '')));
    const matches = trend.matches;
    if (!matches.length || !trend.points.length) {
      const unknown = FP.unknownFilters(FP.filters(ui.state), ui.combos);
      FP.fill(box, h('div', { class: 'notice prose', id: 'chart-empty' },
        h('p', null, 'No matches in this range.'),
        unknown.length ? h('p', null, 'No match in this lake has ', unknown.join(', '), '.') : null));
      FP.fill(document.getElementById('legend'));
      return;
    }
    const index = new Map(matches.map((m, i) => [m.match_key, i]));
    const bySeries = new Map();
    for (const p of trend.points) {
      if (!bySeries.has(p.series)) bySeries.set(p.series, new Array(matches.length).fill(null));
      bySeries.get(p.series)[index.get(p.match_key)] = p.value;
    }
    const selected = trend.by === 'unit' ? selectedUnit() : null;
    const tokens = FP.tokens();
    const names = [...bySeries.keys()].sort((a, b) => (a === selected) - (b === selected));
    const series = names.map((name) => ({
      label: name,
      values: bySeries.get(name),
      stroke: name === selected ? tokens.amberHi : tokens.lineStrong,
      width: name === selected ? 2.5 : 1.2,
      points: { show: true, size: name === selected ? 5 : 3, fill: name === selected ? tokens.amberHi : tokens.lineStrong, stroke: name === selected ? tokens.amberHi : tokens.lineStrong },
    }));
    const lowIdx = new Set(trend.points.filter((p) => p.low_alignment).map((p) => index.get(p.match_key)));
    const mine = selected ? trend.points.filter((p) => p.series === selected) : [];
    const markerSeries = (pred, color, size) => {
      const values = new Array(matches.length).fill(null);
      for (const p of mine) if (pred(p)) values[index.get(p.match_key)] = p.value;
      return { label: 'marks', values, stroke: color, width: 0, paths: () => null, points: { show: true, size, fill: color, stroke: color } };
    };
    if (selected) {
      series.push(markerSeries((p) => p.low_alignment, tokens.hot, 9));
      series.push(markerSeries((p) => p.slot_changed, tokens.amber, 11));
    }
    const shade = [...lowIdx].map((i) => ({ from: i - 0.5, to: i + 0.5, alpha: 0.06 }));
    const label = (v) => {
      const i = Math.round(v);
      return Math.abs(v - i) < 1e-6 && matches[i] ? matches[i].match_key.split('_')[1] : '';
    };
    ui.hot = null;
    ui.chart = FP.track(box, {
      height: 318,
      x: matches.map((_, i) => i),
      xRange: () => [-0.5, matches.length - 0.5],
      xIncrs: [1, 2, 5, 10, 20, 50, 100, 200, 500],
      series,
      refs: [{ value: trend.reference, label: trend.reference != null ? 'REF ' + trend.reference + ' ' + trend.unit : null }],
      shade,
      xLabel: label,
      yLabel: (v) => FP.fmt(v, Math.abs(v) >= 100 ? 0 : 1),
      ySize: 52,
      ariaLabel: trend.label + ' per match for ' + bySeries.size + ' ' + trend.by + ' lines' + (selected ? ', ' + selected + ' emphasised' : '') + '. Values are in the table below.',
      highlight: () => (ui.hot && bySeries.has(ui.hot) ? { values: bySeries.get(ui.hot), stroke: tokens.amberHi, width: 2.5 } : null),
      hover: (idx) => showHover(idx, matches, trend),
      onPick: (x, event) => pickMatch(Math.round(x), event, matches, trend),
    });
    FP.fill(document.getElementById('legend'),
      h('span', null, h('span', { class: 'hi' }, '━'), ' ', selected || 'select a unit to emphasise it'),
      h('span', null, h('span', { class: 'dim' }, '━'), ' other ', trend.by === 'unit' ? 'units' : 'slots'),
      trend.reference != null ? h('span', null, h('span', { class: 'hot' }, '┄'), ' reference') : null,
      lowIdx.size ? h('span', null, h('span', { class: 'hot' }, '■'), ' low alignment confidence') : null,
      selected ? h('span', null, h('span', { class: 'hi' }, '◆'), ' slot change') : null,
      h('span', null, 'Gaps = not installed. Lines follow the unit by serial, not the slot.'));
  }

  /* The line nearest a pointer `top` px from the top of the plot at match idx, within NEAR_PX;
     the selected unit wins a tie (overlapping lines). */
  function nearest(idx, top) {
    if (!ui.chart || idx == null || top == null || top < 0) return null;
    const { u } = ui.chart;
    const selected = ui.trend.by === 'unit' ? selectedUnit() : null;
    let best = null; let gap = NEAR_PX;
    u.series.forEach((s, i) => {
      const v = i && s.label !== 'marks' ? u.data[i][idx] : null;
      if (v == null) return;
      const d = Math.abs(u.valToPos(v, 'y') - top);
      if (d < gap || (d === gap && s.label === selected)) { best = s.label; gap = d; }
    });
    return best;
  }

  /* The readout and the emphasised line follow one rule: the line at the pointer, else the
     selected unit's point, else only the match. */
  function showHover(idx, matches, trend) {
    const box = document.getElementById('chart-hover');
    if (!box || !ui.chart) return;
    const hot = idx == null ? null : nearest(idx, ui.chart.u.cursor.top);
    if (hot !== ui.hot) { ui.hot = hot; ui.chart.redraw(); }
    if (idx == null || !matches[idx]) return;
    const key = matches[idx].match_key;
    const name = hot || (trend.by === 'unit' ? selectedUnit() : null);
    const point = name ? trend.points.find((p) => p.match_key === key && p.series === name) : null;
    FP.fill(box, key, ' · ', FP.when(matches[idx].t),
      point ? [' · ', point.series, ' · ', FP.fmt(point.value, 1), ' ', trend.unit, ' · slot ', point.slot_id, ' · alignment ', point.alignment || '—', point.slot_changed ? ' · slot changed' : ''] : '');
  }

  /* A press on the chart: another unit's line selects that unit; the selected unit's point (its
     line nearest, or no other line near) or a slot's line opens Replay; anything else only
     updates the readout. Drag repeats (pointermove while held) are ignored. */
  function pickMatch(idx, event, matches, trend) {
    if (!matches[idx] || (event && event.type !== 'pointerdown')) return;
    const key = matches[idx].match_key;
    const top = event ? event.clientY - ui.chart.u.over.getBoundingClientRect().top : null;
    const hot = nearest(idx, top);
    const unit = trend.by === 'unit' ? selectedUnit() : null;
    if (trend.by === 'unit' && hot && hot !== unit) { selectUnit(hot); return; }
    const name = trend.by === 'slot' ? hot : unit;
    const point = name ? trend.points.find((p) => p.match_key === key && p.series === name) : null;
    if (point) FP.go({ view: 'replay', m: key, track: point.slot_id });
    else showHover(idx, matches, trend);
  }

  /* One trend scale for every row: from zero unless a value is negative (lowest to highest). */
  const sharedRange = (rows) => {
    const nums = rows.flatMap((r) => r.spark.filter((v) => v != null));
    return nums.length ? [Math.min(0, ...nums), Math.max(...nums)] : null;
  };

  function renderTable(rows, trend) {
    const range = sharedRange(rows);
    // Whole numbers (counts) print without decimals; anything else like the LATEST column.
    const digits = rows.every((r) => r.spark.every((v) => v == null || Number.isInteger(v))) ? 0 : 1;
    const span = (lo, hi) => (lo === hi ? FP.fmt(lo, digits) : FP.fmt(lo, digits) + '–' + FP.fmt(hi, digits));
    const describe = (spark) => {
      const nums = spark.filter((v) => v != null);
      if (!range) return 'No values in range';
      return 'This unit ' + (nums.length ? span(Math.min(...nums), Math.max(...nums)) : 'no values')
        + ' · shared scale ' + span(...range) + ' ' + trend.unit;
    };
    FP.fill(document.getElementById('table-title'), (trend.by === 'unit' ? 'Units' : 'Units') + ' · ' + rows.length + ' in range · ' + trend.label);
    FP.fill(document.getElementById('rows'), rows.map((r) => {
      const on = r.unit_id === selectedUnit();
      const [level, word] = HEALTH[r.health] || HEALTH['NO-DATA'];
      const about = describe(r.spark);
      return h('tr', { class: on ? 'is-sel' : null },
        h('td', null, h('button', { type: 'button', class: 'dev-btn', 'aria-pressed': String(on), dataset: { unit: r.unit_id }, on: { click: () => selectUnit(r.unit_id) } },
          r.role || r.slot_id, h('span', { class: 'meta' }, ' · ' + r.slot_id))),
        h('td', { class: 'd wrap-anywhere' }, r.serial),
        h('td', { class: 'd nowrap' }, FP.day(r.first_t), ' → ', FP.day(r.last_t)),
        h('td', { class: 'num' }, String(r.matches)),
        h('td', { class: 'num' }, FP.fmt(r.latest, 1)),
        h('td', { class: 'num' }, r.change == null ? '—' : (r.change >= 0 ? '+' : '') + FP.fmt(r.change, 1)),
        h('td', { class: 'spark-cell', title: about }, FP.spark(r.spark, { range, label: about, stroke: on ? FP.tokens().amberHi : FP.tokens().amber })),
        h('td', null, FP.status(level, word), r.anomalies ? [' ', h('a', {
          class: 'badge sev sev--' + r.anomalies.severity.toLowerCase(), dataset: { anomalies: r.unit_id },
          href: FP.link({ view: 'anomalies', tab: 'open', sens: 'normal', unit: r.unit_id }),
          title: 'Open anomalies for this unit',
        }, r.anomalies.count + ' ' + r.anomalies.severity)] : null));
    }));
  }

  async function renderDevice() {
    const aside = ui.device;
    if (!selectedUnit()) {
      FP.fill(aside, h('p', { class: 'meta' }, 'Select a unit in the table to see its odometry and lifeline.'));
      return;
    }
    const mine = ui;
    const { season, robot } = filterParams(ui.state);
    const unit = await FP.api('unit/' + encodeURIComponent(selectedUnit()), { season, robot });
    if (ui !== mine || !aside.isConnected) return;
    if (!unit.found) {
      FP.fill(aside,
        h('p', { class: 'device__title' }, 'Not found'),
        h('p', { class: 'prose' }, 'No unit ', h('code', { class: 'code' }, unit.unit_id), ' in this lake.'),
        unit.suggestions.length ? h('div', { class: 'suggestions' }, h('span', { class: 'group__label' }, 'CLOSEST'),
          unit.suggestions.map((s) => h('a', { class: 'navlist__item', href: FP.link({ view: 'history', unit: s }) }, s))) : null);
      return;
    }
    const [level, word] = HEALTH[unit.health] || HEALTH['NO-DATA'];
    const totals = unit.totals;
    const fact = (k, v) => h('div', { class: 'fact' }, h('div', { class: 'fact__k' }, k), h('div', { class: 'fact__v' }, v));
    const points = (ui.trend.points || []).filter((p) => p.series === unit.unit_id).slice().reverse().slice(0, 12);
    FP.fill(aside,
      h('div', { class: 'meta' }, (unit.model || 'unknown model') + ' · ' + (unit.identity === 'legacy' ? 'legacy id' : 'SN ' + unit.serial)),
      h('p', { class: 'device__title' }, unit.current.role || unit.current.slot_id || unit.unit_id),
      FP.status(level, word),
      h('div', { class: 'tiles-2 mt-4 mb-6' },
        fact('Matches in service', String(totals.matches)),
        fact('Powered-on', FP.fmt(totals.powered_h, 2) + ' h'),
        fact('Supply energy', FP.fmt(totals.supply_wh, 1) + ' Wh'),
        fact('Thermal cycles', totals.thermal_cycles == null ? '—' : String(totals.thermal_cycles)),
        fact('Stall', FP.fmt(totals.stall_s, 1) + ' s'),
        fact('Max temperature', totals.temp_max_c == null ? '—' : FP.fmt(totals.temp_max_c, 0) + ' °C')),
      h('p', { class: 'meta mb-3' }, 'Sessions without aligned samples: ', h('b', { class: 'hi' }, String(unit.sessions_without_aligned_samples)), ' (not counted above)'),
      h('h2', { class: 'label' }, 'Lifeline'),
      h('div', { class: 'lifeline mb-6' }, unit.lifeline.map((e) => h('div', { class: 'lifeline__item' + (e.kind === 'replaced-by' ? ' is-alert' : '') },
        h('div', { class: 'lifeline__when' }, FP.day(e.t), ' · ', e.match_key || 'practice'),
        h('div', { class: 'lifeline__text' }, KIND[e.kind] || e.kind, e.other_unit ? ' ' + e.other_unit : '', ' · ', e.robot, ' ', e.role || e.slot_id),
        h('div', { class: 'meta' }, e.slot_id, e.bus ? ' · bus ' + e.bus : '', e.can_id != null ? ' · CAN ' + e.can_id : '', ' · ', e.source === 'legacy' ? 'legacy epoch' : 'inventory')))),
      h('h2', { class: 'label' }, 'Matches'),
      h('div', { class: 'match-links', id: 'unit-matches' }, points.length ? points.map((p) => h('div', { class: 'row row--between' },
        h('a', { class: 'btn btn--sm', href: FP.link({ view: 'replay', m: p.match_key, track: p.slot_id }) }, 'REPLAY ' + p.match_key),
        h('button', { type: 'button', class: 'btn btn--sm', on: { click: (ev) => showLogs(ev.currentTarget, p.match_key) } }, 'LOGS'))) : h('p', { class: 'meta' }, 'No matches in this range.')),
      h('div', { id: 'unit-logs', class: 'mt-3', 'aria-live': 'polite' }),
      h('a', { class: 'btn btn--sm mt-4', href: FP.link({ view: 'history', unit: unit.unit_id }) }, 'LINK TO THIS UNIT'));
  }

  async function showLogs(button, key) {
    const box = document.getElementById('unit-logs');
    const info = await FP.api('match/' + encodeURIComponent(key));
    FP.fill(box, h('h3', { class: 'label' }, 'Raw logs · ' + key),
      info.built ? null : h('p', { class: 'callout callout--quiet' }, 'Replay data for this match is not built: run ', h('code', { class: 'code' }, info.build_command)),
      info.sources.length ? FP.launchPanel(key) : null,
      h('div', { class: 'downloads' }, info.sources.map((s) => h('a', { class: 'download', href: FP.rawHref(s), download: s.download },
        h('span', null, '⇩ ', s.part === 'wpilog' ? 'wpilog' : 'hoot · ' + s.part), h('span', { class: 'download__hash' }, s.name)))));
  }

  /* ---------- interactions ---------- */
  /* One path for every filter change: shared keys go to the address for every view, History's
     own stay with History. */
  function setFilters(patch) {
    const shared = {};
    for (const [key, value] of Object.entries(patch)) {
      if (FP.SHARED.includes(key)) shared[key] = value;
      ui.state[key] = value;
    }
    FP.setFilters(shared);
    renderBar();
    refresh();
  }
  function setMetric(id) {
    ui.state.metric = id;
    document.querySelectorAll('[data-metric]').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.metric === id)));
    refresh();
  }
  function setBy(by) {
    ui.state.by = by;
    document.querySelectorAll('[data-by]').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.by === by)));
    refresh();
  }
  function selectUnit(id) { setFilters({ unit: selectedUnit() === id ? ALL : id }); }

  // For tests: the plotted series ({ name: values per match }), the match keys, the emphasised
  // line, and at(name, index): the page coordinates of a plotted point.
  FP.historyChart = () => (ui && ui.chart ? {
    matches: ui.trend.matches.map((m) => m.match_key),
    series: Object.fromEntries(ui.chart.u.series.slice(1).map((s, i) => [s.label, ui.chart.u.data[i + 1]])),
    highlighted: ui.hot,
    at: (name, index) => {
      const { u } = ui.chart;
      const i = u.series.findIndex((s) => s.label === name);
      const box = u.over.getBoundingClientRect();
      return { x: box.left + u.valToPos(index, 'x'), y: box.top + u.valToPos(u.data[i][index], 'y') };
    },
  } : null);

  FP.view({
    id: 'history',
    label: 'History',
    needsApi: true,
    keep: EXTRA_KEYS, // History's own filters come back when it is opened from the navigation
    mount,
    unmount: () => { if (ui && ui.chart) ui.chart.destroy(); ui = null; },
  });
})();
