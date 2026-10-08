/* ==========================================================================
   Replay: one match from its precomputed envelopes (report/build.py).
   URL: #view=replay&m=<match key>&o=<overlay key>&t=<cursor s>&ev=<marker>
        &tr=<track,track,track,track>&track=<slot from a History drill-through>
        &season=&robot=&event=<match list filters, 'all' or a value; shared with History>
        &z=<from s>,<to s> (the visible window; omitted at the full match window)
   Tracks: battery | total | <metric>:<slot>, metric one of METRICS below.
   ========================================================================== */
(function () {
  'use strict';
  const FP = window.FP;
  const { h } = FP;

  const METRICS = {
    supply_current: { label: 'supply current', unit: 'A', digits: 1 },
    stator_current: { label: 'stator current', unit: 'A', digits: 1 },
    rotor_velocity_rps: { label: 'rotor velocity', unit: 'rps', digits: 1 },
    motor_voltage: { label: 'motor voltage', unit: 'V', digits: 2 },
    temp_c: { label: 'temperature', unit: '°C', digits: 0 },
  };
  const HOLD_S = 1.0; // a value carries forward this long across empty buckets (4 Hz signals)
  const MIN_SPAN_S = 0.5; // the narrowest visible window
  const ZOOM_STEP = 2; // buttons and +/- keys
  const DETAIL_DEBOUNCE_MS = 150;
  const DETAIL_BELOW = 0.5; // served: fetch finer envelopes when the window is under half the match
  const WIDTH_STEP_S = 0.01; // envelope bucket widths are whole 10 ms (report/envelope.py)

  let ui = null;

  /* ---------- data helpers ---------- */
  const grid = (d) => {
    const xs = new Array(d.window.n);
    for (let i = 0; i < d.window.n; i += 1) xs[i] = +(d.window.t0 + i * d.window.width).toFixed(3);
    return xs;
  };
  const bucketOf = (d, t) => Math.floor((t - d.window.t0) / d.window.width + 1e-9);
  const held = (arr, d, i) => {
    if (!arr) return null;
    const back = Math.ceil(HOLD_S / d.window.width);
    for (let k = Math.min(i, arr.length - 1); k >= 0 && k >= i - back; k -= 1) if (arr[k] != null) return arr[k];
    return null;
  };
  const tempAt = (points, t) => {
    if (!points || !points.length) return null;
    let value = null;
    for (const [pt, v] of points) { if (pt <= t) value = v; else break; }
    return value;
  };
  const env = (d, slot, metric) => (d.series && d.series[slot] ? d.series[slot][metric] : null);
  const notLogged = (d, slot, metric) => Boolean(d.not_logged && d.not_logged[slot] && d.not_logged[slot].includes(metric));
  const slotIds = (d) => (d.slots || []).map((s) => s.id);
  const slotLabel = (d, slot) => {
    const found = (d.slots || []).find((s) => s.id === slot);
    return found && found.role ? slot + ' · ' + found.role : slot;
  };

  function totalCurrent(d) {
    const n = d.window.n; const out = new Array(n).fill(null);
    for (const slot of slotIds(d)) {
      const mean = env(d, slot, 'supply_current') && env(d, slot, 'supply_current').mean;
      if (!mean) continue;
      for (let i = 0; i < n; i += 1) {
        const v = held(mean, d, i);
        if (v != null) out[i] = (out[i] || 0) + v;
      }
    }
    return out.map((v) => (v == null ? null : +v.toFixed(1)));
  }

  /* A track's values on a match's own grid: { label, unit, digits, values, band, ref, missing } */
  function trackData(d, spec) {
    const limits = d.limits || {};
    if (spec === 'battery') {
      return { label: 'Battery proxy (lowest device)', unit: 'V', digits: 2, values: d.battery.mean, band: { lower: d.battery.min, upper: d.battery.max }, lowSeries: d.battery.min, refs: [{ value: limits.brownout_v, label: 'BROWNOUT ' + limits.brownout_v }] };
    }
    if (spec === 'total') {
      return { label: 'Total supply current', unit: 'A', digits: 0, values: totalCurrent(d), refs: [] };
    }
    const [metric, slot] = spec.split(':');
    const meta = METRICS[metric];
    if (!meta || !slot) return null;
    const label = slot + ' ' + meta.label;
    if (metric === 'temp_c') {
      const points = d.temps && d.temps[slot];
      const values = points ? grid(d).map((x) => tempAt(points, x)) : null;
      return { label, unit: meta.unit, digits: meta.digits, values, points, missing: !points, refs: [{ value: limits.temp_warn_c, label: 'WARN ' + limits.temp_warn_c }] };
    }
    const e = env(d, slot, metric);
    return { label, unit: meta.unit, digits: meta.digits, values: e ? e.mean : null, band: e ? { lower: e.min, upper: e.max } : null, missing: !e, refs: [] };
  }

  function valueAt(d, spec, t) {
    if (d == null || t == null) return null;
    const td = trackData(d, spec);
    if (!td || td.missing) return null;
    if (td.points) return tempAt(td.points, t);
    const i = bucketOf(d, t);
    if (i < 0 || i >= d.window.n) return null;
    return held(td.values, d, i);
  }

  /* Overlay values resampled onto the primary grid, aligned on match time */
  function overlayValues(primary, overlay, spec) {
    return grid(primary).map((x) => {
      const v = valueAt(overlay, spec, x);
      return v == null ? null : v;
    });
  }

  function defaultTracks(d) {
    const maxima = Object.entries(d.maxima || {});
    const hottest = maxima.filter(([, m]) => m.temp_max_c != null).sort((a, b) => b[1].temp_max_c - a[1].temp_max_c)[0];
    const busiest = maxima.filter(([, m]) => m.supply_current_max != null).sort((a, b) => b[1].supply_current_max - a[1].supply_current_max)[0];
    const first = slotIds(d)[0];
    return [
      'battery',
      'total',
      'temp_c:' + (hottest ? hottest[0] : first),
      'supply_current:' + (busiest ? busiest[0] : first),
    ];
  }

  /* ---------- rendering ---------- */
  function trackOptions(d) {
    const options = [h('option', { value: 'battery' }, 'Battery proxy (V)'), h('option', { value: 'total' }, 'Total supply current (A)')];
    for (const slot of slotIds(d)) {
      for (const [metric, meta] of Object.entries(METRICS)) {
        options.push(h('option', { value: metric + ':' + slot }, slot + ' · ' + meta.label + ' (' + meta.unit + ')'));
      }
    }
    return options;
  }

  function matchList(entries, selected, filters) {
    const events = {};
    for (const e of entries) (events[e.event] = events[e.event] || []).push(e);
    return Object.keys(events).sort().reverse().map((event) => h('section', { class: 'match-list__event', 'aria-label': 'Event ' + event },
      h('h3', { class: 'label' }, event),
      h('div', { class: 'match-list' }, events[event].map((e) => {
        const counts = e.markers || { WARN: 0, FAULT: 0 };
        const status = e.status !== 'ok'
          ? FP.status('retired', 'NO ALIGNED SAMPLES')
          : h('span', { class: 'match-btn__counts' },
            counts.FAULT ? FP.status('fault', counts.FAULT + ' FAULT') : null,
            counts.WARN ? FP.status('warn', counts.WARN + ' WARN') : null,
            !counts.FAULT && !counts.WARN ? FP.status('ok', '0 MARKERS') : null);
        return h('a', {
          class: 'match-btn' + (e.low_alignment && e.status === 'ok' ? ' is-low' : ''),
          href: FP.link({ view: 'replay', m: e.key, ...filters }),
          'aria-current': e.key === selected ? 'page' : null,
          dataset: { match: e.key },
        },
        h('span', { class: 'match-btn__row' }, h('b', null, e.label), status),
        h('span', { class: 'meta' }, e.robot, ' · ', FP.when(e.start_utc)),
        e.low_alignment && e.status === 'ok' ? h('span', { class: 'badge badge--low' }, '▲ LOW ALIGNMENT · ' + e.alignment) : null);
      }))));
  }

  /* ---------- match list filters ---------- */
  const ALL = 'all';
  const FILTERS = [['season', 'SEASON', 'All seasons'], ['robot', 'ROBOT', 'All robots'], ['event', 'EVENT', 'All events']];

  /* Entries that pass every chosen filter ('all' passes everything). */
  function filterEntries(entries, f) {
    return entries.filter((e) => FILTERS.every(([key]) => f[key] === ALL || e[key] === f[key]));
  }
  const startKey = (e) => (e.start_utc ? '1' + e.start_utc : '0') + '|' + e.key; // no start sorts by key
  const latest = (entries) => entries.reduce((best, e) => (best == null || startKey(e) > startKey(best) ? e : best), null);
  /* Distinct values of `key`, newest first (seasons by value, the rest by their latest match). */
  function newestValues(entries, key) {
    const last = {};
    for (const e of entries) if (e[key] != null && (last[e[key]] == null || startKey(e) > last[e[key]])) last[e[key]] = startKey(e);
    const values = Object.keys(last);
    return key === 'season' ? values.sort().reverse() : values.sort((a, b) => (last[b] > last[a] ? 1 : last[b] < last[a] ? -1 : 0));
  }
  // An index built before REPORT_VERSION 2 has no season; an empty one has nothing to rebuild.
  const seasonsKnown = (index) => Boolean(index) && (!index.matches.length || (index.version >= 2 && index.matches.every((e) => e.season != null)));

  function initialFilters(state, entries, selected, seasons) {
    if (FILTERS.every(([key]) => state[key] == null || state[key] === '')) {
      const base = selected || latest(entries);
      return { season: base && seasons ? base.season : ALL, robot: ALL, event: base ? base.event : ALL };
    }
    const f = {};
    for (const [key] of FILTERS) f[key] = state[key] || ALL;
    if (!seasons) f.season = ALL;
    return f;
  }

  function renderRail() {
    const f = ui.filters;
    const entries = ui.entries;
    const shown = filterEntries(entries, f);
    const inSeason = f.season === ALL ? entries : entries.filter((e) => e.season === f.season);
    const values = { season: newestValues(entries, 'season'), robot: newestValues(entries, 'robot'), event: newestValues(inSeason, 'event') };
    const unknown = FILTERS.filter(([key]) => f[key] !== ALL && !entries.some((e) => e[key] === f[key])).map(([key]) => key + ' ' + f[key]);
    const bar = h('div', { class: 'filters stack', role: 'group', 'aria-label': 'Filter matches' }, FILTERS.map(([key, label, allLabel]) => {
      const options = values[key].includes(f[key]) || f[key] === ALL ? values[key] : [f[key], ...values[key]];
      const disabled = key === 'season' && !ui.seasons;
      return h('div', { class: 'filter' },
        h('label', { class: 'group__label', for: 'f-' + key }, label),
        h('select', { class: 'select', id: 'f-' + key, disabled, on: { change: (event) => setFilter(key, event.target.value) } },
          h('option', { value: ALL, selected: f[key] === ALL }, allLabel),
          options.map((v) => h('option', { value: v, selected: f[key] === v }, v + (unknown.includes(key + ' ' + v) ? ' (not in this index)' : '')))),
        disabled ? h('p', { class: 'meta' }, 'Rebuild with ', h('code', { class: 'code' }, 'flashpoint report'), ' to filter by season.') : null);
    }));
    const hidden = ui.key && !shown.some((e) => e.key === ui.key) ? entries.find((e) => e.key === ui.key) : null;
    const note = h('div', { id: 'filter-note', 'aria-live': 'polite' },
      !shown.length ? h('div', { class: 'callout callout--quiet' },
        h('p', null, 'No matches for these filters.'),
        unknown.length ? h('p', null, 'No match in this index has ', unknown.join(', '), '.') : null,
        h('button', { type: 'button', class: 'btn btn--sm mt-2', id: 'filters-clear', on: { click: clearFilters } }, 'CLEAR FILTERS')) : null,
      shown.length && hidden ? h('p', { class: 'meta' }, 'Selected match ' + hidden.label + ' is hidden by the filters.') : null);
    FP.fill(ui.rail, h('h2', { class: 'label' }, 'Matches'), bar, note, matchList(shown, ui.key, f));
  }

  function setFilter(key, value) {
    ui.filters[key] = value;
    if (key === 'season' && value !== ALL && ui.filters.event !== ALL && !ui.entries.some((e) => e.season === value && e.event === ui.filters.event)) ui.filters.event = ALL;
    FP.setState({ ...ui.filters });
    renderRail();
  }
  function clearFilters() {
    ui.filters = { season: ALL, robot: ALL, event: ALL };
    FP.setState({ ...ui.filters });
    renderRail();
    const first = ui.rail.querySelector('select:not([disabled])');
    if (first) first.focus();
  }

  function downloads(sources) {
    const raw = FP.mode.raw !== false;
    if (!sources || !sources.length) return h('p', { class: 'meta' }, 'No raw logs are recorded for this match.');
    if (!raw) {
      return h('div', { class: 'downloads' },
        h('p', { class: 'callout callout--quiet' }, 'Raw logs not included in this export. Their lake hashes:'),
        sources.map((s) => h('div', { class: 'download' }, h('span', null, s.name), h('span', { class: 'download__hash' }, 'sha256 ', s.sha256))));
    }
    return h('div', { class: 'downloads' }, sources.map((s) => h('a', { class: 'download', href: FP.rawHref(s), download: s.download },
      h('span', null, '⇩ ', s.part === 'wpilog' ? 'wpilog' : 'hoot · ' + s.part),
      h('span', { class: 'download__hash' }, s.name))));
  }

  function asideDownloads(sources, key) {
    const launchable = sources && sources.length && FP.mode.raw !== false;
    return h('section', { 'aria-label': 'Open in AdvantageScope' },
      h('h2', { class: 'section-label' }, h('span', null, 'Open in AdvantageScope')),
      h('p', { class: 'meta mb-3' }, 'Open the raw logs in AdvantageScope for a full-rate deep dive, or download them.'),
      launchable ? FP.launchPanel(key) : null,
      downloads(sources));
  }

  function notice(title, ...body) {
    return h('div', { class: 'notice' }, h('h1', { class: 'notice__title' }, title), body);
  }

  async function mount(el, state) {
    ui = { el, state, tracks: [], data: null, overlay: null };
    const index = await FP.loadIndex();
    if (!el.isConnected) return; // the route changed while waiting
    const entries = index ? index.matches : [];
    if (!entries.length && !state.m) {
      FP.fill(el, notice('No Replay data',
        h('p', { class: 'prose' }, 'No match has been built yet. On the laptop that holds the lake, run:'),
        h('p', { class: 'prose' }, h('code', { class: 'code' }, 'flashpoint report')),
        index && index.sessions_without_match_key ? h('p', { class: 'meta mt-3' }, index.sessions_without_match_key + ' session(s) without a match key have no Replay entry.') : null));
      return;
    }
    ui.entries = entries;
    ui.seasons = seasonsKnown(index);
    ui.filters = initialFilters(state, entries, entries.find((e) => e.key === state.m), ui.seasons);
    const shown = filterEntries(entries, ui.filters);
    const key = state.m || (shown.find((e) => e.status === 'ok') || shown[0] || entries.find((e) => e.status === 'ok') || entries[0]).key; // state.m may be unbuilt
    const entry = entries.find((e) => e.key === key);
    const layout = h('div', { class: 'layout' });
    const rail = h('nav', { class: 'rail-l panel panel--rail-l', 'aria-label': 'Matches' });
    const main = h('div', { class: 'main pad', id: 'replay-main' });
    const aside = h('aside', { class: 'rail-r panel panel--rail-r stack', 'aria-label': 'Marker, readout, and raw logs' });
    FP.fill(el, FP.fill(layout, rail, main, aside));
    ui.main = main; ui.aside = aside; ui.rail = rail; ui.key = key;
    renderRail();
    FP.setState({ ...ui.filters });

    if (!entry) {
      FP.fill(main, notice(key, h('p', { class: 'prose' }, 'Replay data for this match is not built.'),
        FP.mode.static ? h('p', { class: 'prose' }, 'This export does not include it.') : h('p', { class: 'prose' }, 'Build it with ', h('code', { class: 'code' }, 'flashpoint report --match ' + key))));
      if (!FP.mode.static) {
        try {
          const info = await FP.api('match/' + encodeURIComponent(key));
          FP.fill(aside, asideDownloads(info.sources, key));
        } catch (err) { FP.fill(aside, h('p', { class: 'meta' }, String(err.message))); }
      }
      return;
    }
    if (entry.status !== 'ok') {
      const d = await FP.loadMatch(entry);
      FP.fill(main, notice(entry.label + ' · no aligned samples', h('p', { class: 'prose' }, d.reason), h('p', { class: 'meta mt-3' }, d.match_key, ' · ', d.robot)));
      FP.fill(aside, asideDownloads(d.sources, d.match_key));
      return;
    }
    const mine = ui;
    ui.data = await FP.loadMatch(entry);
    if (ui !== mine || !el.isConnected) return;
    const overlayEntry = state.o && state.o !== key ? entries.find((e) => e.key === state.o && e.status === 'ok') : null;
    ui.overlay = overlayEntry ? await FP.loadMatch(overlayEntry) : null;
    if (ui !== mine || !el.isConnected) return;
    ui.specs = parseTracks(state, ui.data);
    ui.full = { from: ui.data.window.t0, to: +(ui.data.window.t0 + ui.data.window.n * ui.data.window.width).toFixed(3) };
    ui.view = parseView(state.z);
    ui.detail = null; ui.detailSeq = 0; ui.detailError = null;
    ui.cursor = state.t != null && state.t !== '' && !Number.isNaN(+state.t) ? +state.t : null;
    ui.marker = state.ev != null && state.ev !== '' ? +state.ev : null;
    render();
  }

  function parseTracks(state, d) {
    const defaults = defaultTracks(d);
    const specs = (state.tr || '').split(',').filter(Boolean);
    const valid = (s) => s === 'battery' || s === 'total' || (/^[a-z_]+:.+$/.test(s) && METRICS[s.split(':')[0]]);
    const out = defaults.map((def, i) => (specs[i] && valid(specs[i]) ? specs[i] : def));
    if (state.track && slotIds(d).includes(state.track) && !state.tr) {
      out[2] = d.temps && d.temps[state.track] ? 'temp_c:' + state.track : out[2];
      out[3] = 'supply_current:' + state.track;
    }
    return out;
  }

  function render() {
    const d = ui.data;
    const o = ui.overlay;
    const main = ui.main;
    const entries = ui.entries.filter((e) => e.status === 'ok' && e.key !== d.match_key);
    const header = h('div', { class: 'title-row' },
      h('h1', { class: 'title-d3' }, d.label),
      h('span', { class: 'meta' }, d.match_key),
      h('button', { type: 'button', class: 'btn btn--sm', on: { click: copyLink } }, 'COPY LINK'));
    const meta = h('div', { class: 'match-meta' },
      h('span', null, 'robot ', h('b', null, d.robot)),
      h('span', null, 'event ', h('b', null, d.event)),
      h('span', null, 'start ', h('b', null, FP.when(d.start_utc))),
      h('span', null, 'duration ', h('b', null, FP.fmt(d.duration_s, 1) + ' s')),
      h('span', null, 'alignment ', h('b', null, d.alignment + (d.alignment_method ? ' (' + d.alignment_method + ')' : ''))),
      h('span', { class: 'wrap-anywhere' }, 'logs ', h('b', null, (d.sources || []).map((s) => s.name).join(', '))));
    const notes = h('div', { class: 'notes' },
      d.low_alignment ? h('p', { class: 'callout' }, '▲ Low alignment confidence (' + d.alignment + '): hoot and wpilog times may disagree. Check events against AdvantageScope.') : null,
      d.temperature && !d.temperature.available ? h('p', { class: 'callout' }, 'Temperature not logged. ' + (d.temperature.reason || '')) : null,
      (d.notes || []).map((n) => h('p', { class: 'callout callout--quiet' }, n)));
    const markers = h('section', { class: 'markers', id: 'markers', 'aria-label': 'Markers on the timeline' });
    const zoomBar = h('div', { class: 'zoom-bar' },
      h('span', { class: 'group__label' }, 'WINDOW'),
      h('span', { class: 'zoom-bar__window', id: 'view-window', 'aria-live': 'polite' }),
      h('span', { class: 'meta', id: 'resolution' }),
      h('span', { class: 'app-header__spacer' }),
      h('button', { type: 'button', class: 'btn btn--sm', id: 'zoom-out', 'aria-label': 'Zoom out', title: 'Zoom out (-)', on: { click: () => zoomBy(1 / ZOOM_STEP) } }, '−'),
      h('button', { type: 'button', class: 'btn btn--sm', id: 'zoom-in', 'aria-label': 'Zoom in', title: 'Zoom in (+), Ctrl/⌘ + scroll, or shift-drag a track', on: { click: () => zoomBy(ZOOM_STEP) } }, '+'),
      h('button', { type: 'button', class: 'btn btn--sm', id: 'zoom-reset', title: 'Full match (0)', on: { click: () => setView(ui.full.from, ui.full.to) } }, 'FULL MATCH'));
    const tracks = h('div', { class: 'tracks', id: 'tracks' });
    const scrub = h('input', {
      id: 'scrub', type: 'range', min: String(d.window.t0), max: String(+(d.window.t0 + d.window.n * d.window.width).toFixed(3)),
      step: String(d.window.width), value: String(ui.cursor == null ? d.window.t0 : ui.cursor), 'aria-label': 'Cursor time',
      on: { input: (event) => setCursor(+event.target.value) },
    });
    const scrubRow = h('div', { class: 'scrub' },
      h('label', { for: 'scrub', class: 'group__label' }, 'SCRUB'), scrub,
      h('span', { class: 'scrub__label', id: 'scrub-label' }),
      h('button', { type: 'button', class: 'btn btn--sm', id: 'clear-cursor', on: { click: () => setCursor(null) } }, 'MAXIMA'));
    const compare = h('div', { class: 'compare mt-6' },
      h('label', { class: 'group__label', for: 'compare' }, 'COMPARE'),
      h('select', { class: 'select', id: 'compare', on: { change: (event) => setOverlay(event.target.value) } },
        h('option', { value: '' }, 'no overlay'),
        entries.map((e) => h('option', { value: e.key, selected: o && o.match_key === e.key }, e.label + ' · ' + e.key))),
      o ? h('p', { class: 'meta' }, 'Overlay ', h('b', null, o.label), ' drawn dashed, aligned on match time.') : null);
    const timeline = h('div', { class: 'timeline', id: 'timeline', on: { keydown: timelineKey } }, zoomBar, markers, tracks, scrubRow);
    FP.fill(main, header, meta, notes, timeline, compare);
    renderMarkers(markers);
    if (ui.resize) ui.resize.disconnect();
    ui.resize = new ResizeObserver(placeMarkers);
    ui.resize.observe(markers);
    renderTracks(tracks);
    renderAside();
    updateCursor();
    setView(ui.view.from, ui.view.to);
  }

  /* Markers sit over the plots' own x scale (the first charted track's plot area). */
  /* Markers outside the visible window are hidden and counted on their side. */
  function placeMarkers() {
    const strip = document.getElementById('markers');
    const chart = ui && ui.tracks.find((t) => t.chart);
    if (!strip || !chart) return;
    // From ui.view, not u.valToPos: uPlot commits a new scale in a microtask after setX.
    const over = chart.chart.u.over.getBoundingClientRect();
    const offset = over.left - strip.getBoundingClientRect().left;
    const toPx = (t) => ((t - ui.view.from) / (ui.view.to - ui.view.from)) * over.width;
    let left = 0; let right = 0;
    strip.querySelectorAll('[data-marker]').forEach((b) => {
      const m = ui.data.markers[+b.dataset.marker];
      const before = m.t < ui.view.from; const after = m.t > ui.view.to;
      left += before; right += after;
      b.hidden = before || after;
      b.style.left = (offset + toPx(m.t)).toFixed(1) + 'px';
    });
    const side = (id, n, text) => {
      const b = document.getElementById(id);
      if (!b) return;
      b.hidden = n === 0;
      b.textContent = text;
      b.setAttribute('aria-label', n + ' hidden marker' + (n === 1 ? '' : 's') + (id === 'markers-left' ? ' before' : ' after') + ' the window; select the nearest');
    };
    side('markers-left', left, '◂ ' + left);
    side('markers-right', right, right + ' ▸');
  }
  /* The nearest marker hidden on one side of the window (-1 left, +1 right). */
  function nearestHidden(dir) {
    let best = null;
    (ui.data.markers || []).forEach((m, i) => {
      if (dir < 0 ? m.t < ui.view.from : m.t > ui.view.to) {
        if (best == null || (dir < 0 ? m.t > ui.data.markers[best].t : m.t < ui.data.markers[best].t)) best = i;
      }
    });
    return best;
  }

  function renderMarkers(strip) {
    const d = ui.data;
    const span = d.window.n * d.window.width;
    for (const [id, dir] of [['markers-left', -1], ['markers-right', 1]]) {
      strip.appendChild(h('button', { type: 'button', class: 'marker-count marker-count--' + (dir < 0 ? 'left' : 'right'), id, hidden: true,
        on: { click: () => { const i = nearestHidden(dir); if (i != null) selectMarker(i); } } }));
    }
    (d.markers || []).forEach((m, i) => {
      const left = ((m.t - d.window.t0) / span) * 100;
      strip.appendChild(h('button', {
        type: 'button',
        class: 'marker marker--' + m.level,
        style: { left: left.toFixed(3) + '%' },
        'aria-pressed': String(ui.marker === i),
        'aria-label': m.level + ' ' + m.source + ' at T+' + m.t.toFixed(1) + ' s: ' + m.message,
        title: m.message,
        dataset: { marker: String(i) },
        on: { click: () => selectMarker(i) },
      }, m.level === 'FAULT' ? '✕' : '▲'));
    });
  }

  /* The primary match as drawn: stored, or with the served detail merged over it. Same shape, so
     trackData() and valueAt() work on either. */
  const shown = () => ui.detail ? ui.detail.merged : ui.data;

  function renderTracks(container) {
    for (const t of ui.tracks) t.chart && t.chart.destroy();
    ui.tracks = [];
    const d = shown();
    const xs = grid(d);
    const tokens = FP.tokens();
    const phases = (d.phases || []).filter((p) => p.name === 'auto' || p.name === 'teleop');
    ui.specs.forEach((spec, index) => {
      const td = trackData(d, spec) || { label: spec, missing: true, refs: [] };
      const now = h('span', { 'data-now': String(index) }, '—');
      const picker = h('select', {
        class: 'select track__pick', 'aria-label': 'Track ' + (index + 1) + ' signal',
        on: { change: (event) => setTrack(index, event.target.value) },
      }, trackOptions(d));
      picker.value = spec;
      const plot = h('div', { class: 'track__plot' });
      const label = h('div', { class: 'track__label' },
        h('div', { class: 'track__name' }, td.label.toUpperCase()),
        h('div', { class: 'track__now' }, now, ' ', h('span', { class: 'meta' }, td.unit || '')),
        h('div', { class: 'track__range', 'data-range': String(index) }),
        picker);
      container.appendChild(h('div', { class: 'track' }, label, plot));
      const entry = { spec, td, now, plot, range: label.querySelector('[data-range]') };
      if (td.missing || !td.values) {
        FP.fill(plot, h('p', { class: 'notlogged notice' }, 'not logged'));
        entry.range.textContent = 'not logged';
      } else {
        const series = [{ label: td.label, values: td.values, stroke: tokens.amber, width: 1.5, band: td.band }];
        if (ui.overlay) {
          const ov = trackData(ui.overlay, spec);
          if (ov && !ov.missing) series.push({ label: ui.overlay.label, values: overlayValues(d, ui.overlay, spec), stroke: tokens.amberDim, width: 1.5, dash: [5, 3] });
        }
        entry.xs = xs;
        entry.chart = FP.track(plot, {
          height: 120,
          x: xs,
          series,
          refs: td.refs,
          shade: phases.map((p) => ({ from: p.start, to: p.end == null ? xs[xs.length - 1] : p.end, alpha: p.name === 'auto' ? 0.07 : 0.035 })),
          gapX: HOLD_S,
          cursor: () => ui.cursor,
          onPick: (x) => setCursor(Math.max(xs[0], Math.min(xs[xs.length - 1], x))),
          onSelect: (a, b) => setView(a, b),
          onZoom: (factor, x) => zoomBy(factor, x),
          xLabel: (v) => 'T+' + (ui.view.to - ui.view.from < 10 ? v.toFixed(1) : Math.round(v)),
          yLabel: (v) => FP.fmt(v, Math.abs(v) >= 100 ? 0 : 1),
          ariaLabel: td.label + ' over the visible window. The range and the values at the cursor are in the track label and the readout.',
        });
      }
      ui.tracks.push(entry);
    });
    // uPlot sizes a new plot in a microtask: place the markers again once the first one has its size.
    const first = ui.tracks.find((t) => t.chart);
    if (first && ui.resize) ui.resize.observe(first.chart.u.over);
  }

  function readoutRows() {
    const d = ui.data; const o = ui.overlay; const t = ui.cursor;
    const limits = { warn: d.limits ? d.limits.temp_warn_c : 65, fault: d.limits ? d.limits.temp_fault_c : 75 };
    const slots = slotIds(d);
    const rows = slots.map((slot) => {
      const amps = t == null ? (d.maxima[slot] || {}).supply_current_max : valueAt(d, 'supply_current:' + slot, t);
      const temp = t == null ? (d.maxima[slot] || {}).temp_max_c : valueAt(d, 'temp_c:' + slot, t);
      let other = null;
      if (o) {
        const present = slotIds(o).includes(slot);
        other = present ? {
          amps: t == null ? (o.maxima[slot] || {}).supply_current_max : valueAt(o, 'supply_current:' + slot, t),
          temp: t == null ? (o.maxima[slot] || {}).temp_max_c : valueAt(o, 'temp_c:' + slot, t),
          tempLogged: !notLogged(o, slot, 'temp_c'),
        } : null;
      }
      return { slot, amps, temp, tempLogged: !notLogged(d, slot, 'temp_c'), ampsLogged: !notLogged(d, slot, 'supply_current'), other, present: !o || other != null };
    });
    if (t == null) rows.sort((a, b) => (b.temp == null ? -1 : b.temp) - (a.temp == null ? -1 : a.temp));
    return { rows, limits };
  }

  function renderAside() {
    const d = ui.data;
    const event = h('section', { id: 'event', 'aria-live': 'polite', 'aria-label': 'Selected marker' });
    const readout = h('section', { 'aria-label': 'Readout' },
      h('h2', { class: 'section-label' }, h('span', { id: 'readout-label' })),
      h('div', { class: 'table-wrap' }, h('table', { class: 'table readout-table' },
        h('thead', null, h('tr', null,
          h('th', { scope: 'col' }, 'Slot'), h('th', { scope: 'col', class: 'num' }, 'A'), h('th', { scope: 'col', class: 'num' }, '°C'), h('th', { scope: 'col' }, h('span', { class: 'sr-only' }, 'Status')),
          ui.overlay ? [h('th', { scope: 'col', class: 'num' }, ui.overlay.label + ' A'), h('th', { scope: 'col', class: 'num' }, ui.overlay.label + ' °C')] : null)),
        h('tbody', { id: 'readout' }))));
    FP.fill(ui.aside, event, readout, asideDownloads(d.sources, d.match_key));
    renderEvent();
  }

  function renderEvent() {
    const box = document.getElementById('event');
    if (!box) return;
    const d = ui.data;
    const m = ui.marker != null ? (d.markers || [])[ui.marker] : null;
    FP.fill(box, h('h2', { class: 'section-label' }, h('span', null, 'Marker')),
      m ? h('div', { class: 'alert event-card ' + (m.level === 'FAULT' ? 'is-fault' : 'is-warn') },
        h('div', { class: 'alert__head' }, FP.status(m.level === 'FAULT' ? 'fault' : 'warn', m.level + ' · ' + m.source), h('span', { class: 'muted' }, 'T+' + m.t.toFixed(1) + ' s')),
        h('p', { class: 'event-card__msg' }, m.message),
        m.end != null ? h('p', { class: 'meta mt-2' }, 'Until T+' + m.end.toFixed(1) + ' s') : null,
        h('p', { class: 'meta muted mt-3' }, 'Markers come from fixed rules (config/report.toml) and state what the log shows; they do not name a cause.'))
        : h('p', { class: 'meta' }, (d.markers || []).length ? 'Select a marker above the tracks to jump to it.' : 'No rule fired in this match.'));
  }

  function cell(value, digits, logged) {
    if (!logged) return h('td', { class: 'num notlogged' }, 'not logged');
    return h('td', { class: 'num' }, FP.fmt(value, digits));
  }

  function updateCursor() {
    const d = ui.data; const t = ui.cursor;
    const label = document.getElementById('scrub-label');
    const scrub = document.getElementById('scrub');
    const phase = t == null ? null : (d.phases || []).find((p) => (p.start == null || t >= p.start) && (p.end == null || t < p.end));
    if (label) label.textContent = t == null ? 'no cursor' : 'T+' + t.toFixed(2) + ' s';
    if (scrub && t != null) scrub.value = String(t);
    const clear = document.getElementById('clear-cursor');
    if (clear) clear.disabled = t == null;
    const covered = ui.detail && t != null && t >= ui.detail.from && t < ui.detail.to;
    for (const track of ui.tracks) {
      const v = t == null ? null : valueAt(covered ? ui.detail.merged : d, track.spec, t);
      track.now.textContent = track.td.missing ? 'not logged' : FP.fmt(v, track.td.digits);
      if (track.chart) track.chart.redraw();
    }
    const heading = document.getElementById('readout-label');
    if (heading) heading.textContent = t == null ? 'Match maxima · hottest first' : 'At T+' + t.toFixed(1) + ' s' + (phase ? ' · ' + phase.name.toUpperCase() : '');
    const body = document.getElementById('readout');
    if (body) {
      const { rows, limits } = readoutRows();
      FP.fill(body, rows.map((r) => {
        const level = r.tempLogged ? FP.tempLevel(r.temp, limits) : null;
        return h('tr', { class: level === 'fault' ? 'is-low' : null, dataset: { slot: r.slot } },
          h('td', { class: 'v wrap-anywhere' }, slotLabel(d, r.slot)),
          cell(r.amps, 1, r.ampsLogged),
          cell(r.temp, 0, r.tempLogged),
          h('td', null, level ? FP.status(level) : null),
          ui.overlay ? (r.other ? [cell(r.other.amps, 1, true), cell(r.other.temp, 0, r.other.tempLogged)] : [h('td', { class: 'num absent' }, 'absent'), h('td', { class: 'num absent' }, 'absent')]) : null);
      }));
    }
    writeState();
  }
  function writeState() {
    const d = ui.data; const t = ui.cursor;
    FP.setState({ view: 'replay', m: d.match_key, o: ui.overlay ? ui.overlay.match_key : null, t: t == null ? null : t.toFixed(2), ev: ui.marker, tr: ui.specs.join(','), track: null, ...ui.filters, z: isFull() ? null : ui.view.from.toFixed(2) + ',' + ui.view.to.toFixed(2) });
  }

  /* ---------- the shared x window ---------- */
  const signed = (t) => (t < 0 ? 'T−' + (-t).toFixed(1) : 'T+' + t.toFixed(1));
  function clampView(from, to) {
    const full = ui.full;
    const span = Math.min(full.to - full.from, Math.max(MIN_SPAN_S, to - from));
    let lo = Math.max(full.from, Math.min(from, full.to - span));
    if (to - from < MIN_SPAN_S) lo = Math.max(full.from, Math.min((from + to) / 2 - span / 2, full.to - span));
    return { from: lo, to: lo + span };
  }
  /* `z=<from>,<to>` from the address; anything invalid or wholly outside the match is the full window. */
  function parseView(z) {
    const parts = String(z || '').split(',');
    const [from, to] = parts.map(Number);
    const valid = parts.length === 2 && parts.every((x) => x.trim() !== '') && Number.isFinite(from) && Number.isFinite(to)
      && from < to && to > ui.full.from && from < ui.full.to;
    return valid ? clampView(from, to) : { ...ui.full };
  }
  const isFull = () => ui.view.from <= ui.full.from + 1e-6 && ui.view.to >= ui.full.to - 1e-6;

  /* Every time-positioned element reads ui.view: charts, markers, range readouts, the label. */
  function setView(from, to) {
    ui.view = clampView(from, to);
    if (ui.detail && !detailFits()) dropDetail();
    for (const track of ui.tracks) if (track.chart) track.chart.setX(ui.view.from, ui.view.to);
    placeMarkers();
    updateRanges();
    const label = document.getElementById('view-window');
    if (label) label.textContent = signed(ui.view.from) + ' – ' + signed(ui.view.to) + ' s';
    updateResolution();
    writeState();
    scheduleDetail();
  }
  function zoomBy(factor, at) {
    const anchor = at != null ? at : (ui.cursor != null && ui.cursor >= ui.view.from && ui.cursor <= ui.view.to ? ui.cursor : (ui.view.from + ui.view.to) / 2);
    const from = anchor - (anchor - ui.view.from) / factor;
    const to = anchor + (ui.view.to - anchor) / factor;
    setView(from, to);
  }
  function panTo(t) {
    if (t >= ui.view.from && t <= ui.view.to) return;
    const half = (ui.view.to - ui.view.from) / 2;
    setView(t - half, t + half);
  }
  function timelineKey(event) {
    if (event.target.tagName === 'SELECT' || event.ctrlKey || event.metaKey || event.altKey) return;
    if (event.key === '+' || event.key === '=') zoomBy(ZOOM_STEP);
    else if (event.key === '-' || event.key === '_') zoomBy(1 / ZOOM_STEP);
    else if (event.key === '0') setView(ui.full.from, ui.full.to);
    else return;
    event.preventDefault();
  }
  function updateRanges() {
    for (const track of ui.tracks) {
      if (!track.chart) continue;
      const nums = [];
      track.xs.forEach((x, i) => { const v = track.td.values[i]; if (v != null && x >= ui.view.from && x <= ui.view.to) nums.push(v); });
      track.range.textContent = nums.length ? FP.fmt(Math.min(...nums), track.td.digits) + ' – ' + FP.fmt(Math.max(...nums), track.td.digits) + ' ' + track.td.unit : 'no samples';
    }
  }
  const zoomedIn = () => ui.view.to - ui.view.from < DETAIL_BELOW * (ui.full.to - ui.full.from);
  const ms = (width) => Math.round(width * 1000) + ' ms';
  /* The resolution the tracks are drawn at, once zoomed in past the stored buckets. */
  function updateResolution() {
    const note = document.getElementById('resolution');
    if (!note) return;
    if (ui.detail) {
      const overlay = ui.overlay ? ' · overlay ' + ui.overlay.label + ' at ' + ms(ui.overlay.window.width) : '';
      note.textContent = ms(ui.detail.merged.window.width) + ' buckets from the lake' + overlay;
    } else if (zoomedIn()) {
      note.textContent = 'bucket resolution (' + ms(ui.data.window.width) + ')' + (ui.detailError ? ' · finer data unavailable: ' + ui.detailError : '');
    } else note.textContent = '';
  }

  /* ---------- served detail: finer envelopes for the visible window ---------- */
  /* The detail still covers the window at no worse resolution than a fresh fetch would. */
  function detailFits() {
    const span = ui.view.to - ui.view.from;
    const fresh = Math.max(WIDTH_STEP_S, Math.ceil(span / 1000 / WIDTH_STEP_S - 1e-9) * WIDTH_STEP_S);
    return zoomedIn() && ui.detail.from <= ui.view.from + 1e-6 && ui.detail.to >= ui.view.to - 1e-6
      && ui.detail.merged.window.width <= fresh + 1e-9;
  }
  function redrawTracks() {
    const tracks = document.getElementById('tracks');
    if (!tracks) return;
    renderTracks(FP.clear(tracks));
    for (const track of ui.tracks) if (track.chart) track.chart.setX(ui.view.from, ui.view.to);
    updateRanges();
    updateCursor();
  }
  function dropDetail() {
    ui.detail = null;
    redrawTracks();
  }
  function scheduleDetail() {
    clearTimeout(ui.detailTimer);
    if (FP.mode.static || !zoomedIn() || (ui.detail && detailFits())) {
      ui.detailSeq += 1; // a response still in flight is stale now
      return;
    }
    ui.detailTimer = setTimeout(fetchDetail, DETAIL_DEBOUNCE_MS);
  }
  async function fetchDetail() {
    const mine = ui;
    const seq = (ui.detailSeq += 1);
    const view = { ...ui.view };
    let body;
    try {
      body = await FP.api('envelope/' + encodeURIComponent(ui.data.match_key), { from: view.from.toFixed(3), to: view.to.toFixed(3) });
    } catch (err) {
      body = { available: false, reason: String(err.message) };
    }
    if (ui !== mine || seq !== ui.detailSeq) return; // stale: the window moved on
    if (!body.available) {
      ui.detailError = body.reason || 'unknown error';
      updateResolution();
      return;
    }
    ui.detailError = null;
    const w = body.window;
    ui.detail = {
      from: w.t0, to: w.t0 + w.n * w.width,
      merged: { ...ui.data, window: w, series: body.series, battery: body.battery, temps: body.temps, not_logged: body.not_logged },
    };
    redrawTracks();
    placeMarkers();
    updateResolution();
  }

  /* ---------- interactions ---------- */
  function setCursor(t) {
    ui.cursor = t;
    updateCursor();
  }
  function selectMarker(i) {
    ui.marker = i;
    panTo(ui.data.markers[i].t);
    document.querySelectorAll('[data-marker]').forEach((b) => b.setAttribute('aria-pressed', String(+b.dataset.marker === i)));
    renderEvent();
    setCursor(ui.data.markers[i].t);
  }
  function setTrack(index, spec) {
    ui.specs[index] = spec;
    renderTracks(FP.clear(document.getElementById('tracks')));
    updateCursor();
    setView(ui.view.from, ui.view.to);
  }
  async function setOverlay(key) {
    const entry = ui.entries.find((e) => e.key === key);
    ui.overlay = entry ? await FP.loadMatch(entry) : null;
    render();
  }
  function copyLink() {
    const text = window.location.href;
    if (navigator.clipboard && window.isSecureContext) navigator.clipboard.writeText(text).catch(() => {});
    const button = document.activeElement;
    if (button && button.tagName === 'BUTTON') { button.textContent = 'COPIED'; setTimeout(() => { button.textContent = 'COPY LINK'; }, 1200); }
  }

  FP.view({
    id: 'replay',
    label: 'Replay',
    needsApi: false,
    mount,
    unmount: () => {
      if (ui) {
        clearTimeout(ui.detailTimer);
        for (const t of ui.tracks) if (t.chart) t.chart.destroy();
        if (ui.resize) ui.resize.disconnect();
      }
      ui = null;
    },
  });
  FP.replay = { // for tests
    trackData, valueAt, defaultTracks, filterEntries,
    charts: () => (ui ? ui.tracks.filter((t) => t.chart).map((t) => t.chart.u) : []),
    view: () => (ui && ui.view ? { ...ui.view } : null),
    detail: () => (ui && ui.detail ? { ...ui.detail.merged.window } : null),
  };
})();
