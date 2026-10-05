/* ==========================================================================
   Replay: one match from its precomputed envelopes (report/build.py).
   URL: #view=replay&m=<match key>&o=<overlay key>&t=<cursor s>&ev=<marker>
        &tr=<track,track,track,track>&track=<slot from a History drill-through>
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

  function matchList(entries, selected) {
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
          href: FP.link({ view: 'replay', m: e.key }),
          'aria-current': e.key === selected ? 'page' : null,
          dataset: { match: e.key },
        },
        h('span', { class: 'match-btn__row' }, h('b', null, e.label), status),
        h('span', { class: 'meta' }, e.robot, ' · ', FP.when(e.start_utc)),
        e.low_alignment && e.status === 'ok' ? h('span', { class: 'badge badge--low' }, '▲ LOW ALIGNMENT · ' + e.alignment) : null);
      }))));
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

  function asideDownloads(sources) {
    return h('section', { 'aria-label': 'Open in AdvantageScope' },
      h('h2', { class: 'section-label' }, h('span', null, 'Open in AdvantageScope')),
      h('p', { class: 'meta mb-3' }, 'Download the raw logs, then open them in AdvantageScope for a full-rate deep dive.'),
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
    const key = state.m || (entries.find((e) => e.status === 'ok') || entries[0]).key; // state.m may be unbuilt
    const entry = entries.find((e) => e.key === key);
    const layout = h('div', { class: 'layout' });
    const rail = h('nav', { class: 'rail-l panel panel--rail-l', 'aria-label': 'Matches' }, h('h2', { class: 'label' }, 'Matches'), matchList(entries, key));
    const main = h('div', { class: 'main pad', id: 'replay-main' });
    const aside = h('aside', { class: 'rail-r panel panel--rail-r stack', 'aria-label': 'Marker, readout, and raw logs' });
    FP.fill(el, FP.fill(layout, rail, main, aside));
    ui.main = main; ui.aside = aside; ui.entries = entries;

    if (!entry) {
      FP.fill(main, notice(key, h('p', { class: 'prose' }, 'Replay data for this match is not built.'),
        FP.mode.static ? h('p', { class: 'prose' }, 'This export does not include it.') : h('p', { class: 'prose' }, 'Build it with ', h('code', { class: 'code' }, 'flashpoint report --match ' + key))));
      if (!FP.mode.static) {
        try {
          const info = await FP.api('match/' + encodeURIComponent(key));
          FP.fill(aside, asideDownloads(info.sources));
        } catch (err) { FP.fill(aside, h('p', { class: 'meta' }, String(err.message))); }
      }
      return;
    }
    if (entry.status !== 'ok') {
      const d = await FP.loadMatch(entry);
      FP.fill(main, notice(entry.label + ' · no aligned samples', h('p', { class: 'prose' }, d.reason), h('p', { class: 'meta mt-3' }, d.match_key, ' · ', d.robot)));
      FP.fill(aside, asideDownloads(d.sources));
      return;
    }
    const mine = ui;
    ui.data = await FP.loadMatch(entry);
    if (ui !== mine || !el.isConnected) return;
    const overlayEntry = state.o && state.o !== key ? entries.find((e) => e.key === state.o && e.status === 'ok') : null;
    ui.overlay = overlayEntry ? await FP.loadMatch(overlayEntry) : null;
    if (ui !== mine || !el.isConnected) return;
    ui.specs = parseTracks(state, ui.data);
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
    FP.fill(main, header, meta, notes, markers, tracks, scrubRow, compare);
    renderMarkers(markers);
    renderTracks(tracks);
    renderAside();
    updateCursor();
    placeMarkers();
    ui.resize = new ResizeObserver(placeMarkers);
    ui.resize.observe(markers);
  }

  /* Markers sit over the plots' own x scale (the first charted track's plot area). */
  function placeMarkers() {
    const strip = document.getElementById('markers');
    const chart = ui && ui.tracks.find((t) => t.chart);
    if (!strip || !chart) return;
    const u = chart.chart.u;
    const offset = u.over.getBoundingClientRect().left - strip.getBoundingClientRect().left;
    strip.querySelectorAll('[data-marker]').forEach((b) => {
      const m = ui.data.markers[+b.dataset.marker];
      b.style.left = (offset + u.valToPos(m.t, 'x')).toFixed(1) + 'px';
    });
  }

  function renderMarkers(strip) {
    const d = ui.data;
    const span = d.window.n * d.window.width;
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

  function renderTracks(container) {
    for (const t of ui.tracks) t.chart && t.chart.destroy();
    ui.tracks = [];
    const d = ui.data;
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
        const nums = td.values.filter((v) => v != null);
        entry.range.textContent = nums.length ? FP.fmt(Math.min(...nums), td.digits) + ' – ' + FP.fmt(Math.max(...nums), td.digits) + ' ' + td.unit : 'no samples';
        entry.chart = FP.track(plot, {
          height: 120,
          x: xs,
          series,
          refs: td.refs,
          shade: phases.map((p) => ({ from: p.start, to: p.end == null ? xs[xs.length - 1] : p.end, alpha: p.name === 'auto' ? 0.07 : 0.035 })),
          gapX: HOLD_S,
          cursor: () => ui.cursor,
          onPick: (x) => setCursor(Math.max(xs[0], Math.min(xs[xs.length - 1], x))),
          xLabel: (v) => 'T+' + Math.round(v),
          yLabel: (v) => FP.fmt(v, Math.abs(v) >= 100 ? 0 : 1),
          ariaLabel: td.label + ' over the match, ' + entry.range.textContent + '. Values at the cursor are in the readout.',
        });
      }
      ui.tracks.push(entry);
    });
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
    FP.fill(ui.aside, event, readout, asideDownloads(d.sources));
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
    for (const track of ui.tracks) {
      const v = t == null ? null : valueAt(d, track.spec, t);
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
    FP.setState({ view: 'replay', m: d.match_key, o: ui.overlay ? ui.overlay.match_key : null, t: t == null ? null : t.toFixed(2), ev: ui.marker, tr: ui.specs.join(','), track: null });
  }

  /* ---------- interactions ---------- */
  function setCursor(t) {
    ui.cursor = t;
    updateCursor();
  }
  function selectMarker(i) {
    ui.marker = i;
    document.querySelectorAll('[data-marker]').forEach((b) => b.setAttribute('aria-pressed', String(+b.dataset.marker === i)));
    renderEvent();
    setCursor(ui.data.markers[i].t);
  }
  function setTrack(index, spec) {
    ui.specs[index] = spec;
    renderTracks(FP.clear(document.getElementById('tracks')));
    updateCursor();
    placeMarkers();
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
        for (const t of ui.tracks) if (t.chart) t.chart.destroy();
        if (ui.resize) ui.resize.disconnect();
      }
      ui = null;
    },
  });
  FP.replay = { trackData, valueAt, defaultTracks }; // for tests
})();
