/* ==========================================================================
   Flashpoint app shell: view registry, hash router, the h() DOM builder,
   script-tag data loading, the History API client, and track(), the uPlot
   wrapper every chart uses.

   Classic scripts on purpose: browsers refuse ES modules from file://, and the
   static Replay export must open by double-click. No build step.

   Escaping rule (#56): views build DOM only through h(), which sets text with
   text nodes and refuses event-handler attributes and script URLs. Nothing in
   the app writes HTML strings. A test greps every file for the HTML sinks.
   ========================================================================== */
(function () {
  'use strict';

  const FP = window.FP || (window.FP = {});
  const MODE = window.FP_MODE || { static: window.location.protocol === 'file:', raw: true };
  FP.mode = MODE;

  /* ---------- DOM builder ---------- */
  const PROPS = new Set(['value', 'checked', 'selected', 'disabled', 'htmlFor', 'tabIndex']);
  function append(el, children) {
    for (const child of children) {
      if (child == null || child === false) continue;
      if (Array.isArray(child)) append(el, child);
      else if (child instanceof Node) el.appendChild(child);
      else el.appendChild(document.createTextNode(String(child)));
    }
    return el;
  }
  function h(tag, attrs, ...children) {
    const el = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs || {})) {
      if (value == null || value === false) continue;
      if (key === 'class') el.className = value;
      else if (key === 'on') {
        for (const [type, fn] of Object.entries(value)) el.addEventListener(type, fn);
      } else if (key === 'style') {
        if (typeof value !== 'object') throw new Error('h(): style must be an object');
        Object.assign(el.style, value);
      } else if (key === 'dataset') Object.assign(el.dataset, value);
      else if (PROPS.has(key)) el[key] = value;
      else {
        if (/^on/i.test(key)) throw new Error('h(): event-handler attributes are not allowed');
        const text = value === true ? '' : String(value);
        if ((key === 'href' || key === 'src') && /^\s*(javascript|data|vbscript):/i.test(text)) {
          throw new Error('h(): script URLs are not allowed');
        }
        el.setAttribute(key, text);
      }
    }
    return append(el, children);
  }
  FP.h = h;
  FP.clear = (el) => { while (el.firstChild) el.removeChild(el.firstChild); return el; };
  FP.fill = (el, ...children) => append(FP.clear(el), children);

  /* ---------- Formatting and the status language ---------- */
  FP.fmt = (value, digits = 1) => (value == null || Number.isNaN(value) ? '—' : Number(value).toFixed(digits));
  FP.when = (iso) => {
    if (!iso) return '—';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  };
  FP.day = (iso) => {
    if (!iso) return '—';
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? String(iso) : d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
  };
  const STATUS = { ok: 'OK', warn: 'WARN', fault: 'FAULT', retired: 'NO DATA' };
  FP.status = (level, word) => h('span', { class: 'status status--' + level }, word || STATUS[level]);
  FP.tempLevel = (celsius, limits) => {
    if (celsius == null) return null;
    if (celsius >= limits.fault) return 'fault';
    if (celsius >= limits.warn) return 'warn';
    return 'ok';
  };

  /* ---------- Tokens for canvas drawing (uPlot cannot read CSS variables) ---------- */
  let tokens = null;
  FP.tokens = () => {
    if (tokens) return tokens;
    const css = getComputedStyle(document.documentElement);
    const v = (name) => css.getPropertyValue(name).trim();
    tokens = {
      amber: v('--amber'), amberHi: v('--amber-hi'), amberMid: v('--amber-mid'), amberDim: v('--amber-dim'),
      amberMuted: v('--amber-muted'), line: v('--line'), lineSoft: v('--line-soft'), lineStrong: v('--line-strong'),
      hot: v('--hot'), surface1: v('--surface-1'), font: v('--font-ui'),
    };
    return tokens;
  };

  /* ---------- URL state (everything lives in the hash) ---------- */
  /* The shared filters (spec: view-filters) are hash keys every view reads; 'all' is no filter.
     An address with none of them is "fresh": only then does Replay apply its default. */
  const ALL = 'all';
  const SHARED = ['season', 'robot', 'event', 'unit'];
  FP.ALL = ALL;
  FP.SHARED = SHARED;
  const kept = {}; // view id -> its own keys to restore from the nav (History's extra filters)
  let current = null; // the mounted view
  FP.state = () => {
    const params = new URLSearchParams(window.location.hash.replace(/^#/, ''));
    const out = {};
    for (const [k, v] of params) out[k] = v;
    return out;
  };
  FP.setState = (patch) => {
    const next = Object.assign(FP.state(), patch);
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(next)) if (v != null && v !== '') params.set(k, v);
    const hash = '#' + params.toString();
    if (hash !== window.location.hash) window.history.replaceState(null, '', hash);
    if (current && current.keep) kept[current.id] = Object.fromEntries(current.keep.map((k) => [k, next[k]]));
    updateNav(); // replaceState fires no hashchange, so the nav would keep stale filters
  };
  /* A link to `state`, carrying the current shared filters unless `state` sets the key (null
     removes it). */
  FP.link = (state) => {
    const now = FP.state();
    const out = { ...state };
    for (const k of SHARED) if (out[k] === undefined && now[k]) out[k] = now[k];
    return '#' + new URLSearchParams(Object.entries(out).filter(([, v]) => v != null && v !== '')).toString();
  };
  FP.filters = (state) => {
    const s = state || FP.state();
    return Object.fromEntries(SHARED.map((k) => [k, s[k] || ALL]));
  };
  FP.fresh = (state) => SHARED.every((k) => !(state || FP.state())[k]);
  FP.filtered = (f) => SHARED.some((k) => f[k] !== ALL);
  /* Write shared filters ('all' or a value); an 'all' unit leaves the address. */
  FP.setFilters = (patch) => FP.setState(Object.fromEntries(Object.entries(patch)
    .map(([k, v]) => [k, k === 'unit' && v === ALL ? null : v])));
  const navHref = (id) => FP.link({ ...(kept[id] || {}), view: id });
  function updateNav() {
    document.querySelectorAll('nav.tabs a[data-view]').forEach((a) => a.setAttribute('href', navHref(a.dataset.view)));
  }

  /* ---------- Data: script tags (work from file://, where fetch() does not; #59) ---------- */
  const matches = {};
  FP.register = (payload) => { matches[payload.match_key] = payload; };
  FP.index = (payload) => { FP.indexData = payload; };
  FP.loadScript = (src) => new Promise((resolve, reject) => {
    const el = document.createElement('script');
    el.src = src;
    el.addEventListener('load', () => { el.remove(); resolve(); });
    el.addEventListener('error', () => { el.remove(); reject(new Error('could not load ' + src)); });
    document.head.appendChild(el);
  });
  const bust = (src) => (MODE.static ? src : src + '?t=' + Date.now());
  FP.loadIndex = async () => {
    if (FP.indexData) return FP.indexData;
    try { await FP.loadScript(bust('data/matches.js')); } catch (_) { return null; }
    return FP.indexData || null;
  };
  FP.loadMatch = async (entry) => {
    if (matches[entry.key]) return matches[entry.key];
    await FP.loadScript(bust(entry.file));
    if (!matches[entry.key]) throw new Error('match data did not register: ' + entry.key);
    return matches[entry.key];
  };

  /* ---------- History API (served mode only) ---------- */
  FP.api = async (path, params) => {
    const query = new URLSearchParams();
    for (const [k, v] of Object.entries(params || {})) if (v != null && v !== '') query.set(k, v);
    const qs = query.toString();
    const response = await fetch('api/' + path + (qs ? '?' + qs : ''), { headers: { Accept: 'application/json' } });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error || response.statusText);
    return body;
  };
  /* ---------- Open in AdvantageScope (served on this machine only; ADR-0014) ---------- */
  let launchState = null;
  FP.launchAvailability = () => {
    if (!launchState) {
      launchState = fetch('api/launch', { headers: { Accept: 'application/json' } })
        .then((response) => response.json())
        .catch(() => ({ available: false, reason: 'the app could not be reached' }));
    }
    return launchState;
  };
  const NOT_HERE = new Set(['shared on the network', 'the request is not from this machine']);
  function launchResult(box, r) {
    if (!r.started) {
      FP.fill(box, h('p', { class: 'callout mt-2' }, 'AdvantageScope did not open: ', r.reason || 'unknown error', '. Use the downloads below.'));
      return;
    }
    FP.fill(box,
      h('p', { class: 'meta mt-2' }, 'Opened ', h('b', { class: 'hi' }, r.wpilog), ' in AdvantageScope.'),
      r.hoots.length ? [
        h('p', { class: 'meta mt-2' }, 'To add the hoots, use File › Insert log in AdvantageScope and pick them from:'),
        h('p', { class: 'mt-2' }, h('code', { class: 'code', id: 'as-folder' }, r.folder)),
        h('ul', { class: 'meta mt-2', id: 'as-hoots' }, r.hoots.map((x) => h('li', null, x.name,
          x.incomplete ? h('span', { class: 'badge badge--low' }, 'INCOMPLETE') : null))),
      ] : null);
  }
  async function launch(key, button, result) {
    button.disabled = true;
    FP.fill(result, h('p', { class: 'meta mt-2' }, 'Opening…'));
    try {
      const response = await fetch('api/launch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Flashpoint': 'launch', Accept: 'application/json' },
        body: JSON.stringify({ match_key: key }),
      });
      launchResult(result, await response.json());
    } catch (err) {
      launchResult(result, { started: false, reason: String(err.message) });
    } finally {
      button.disabled = false;
    }
  }
  /* The launch action for one match, or null in a static export (the downloads stay). */
  FP.launchPanel = (key) => {
    if (MODE.static) return null;
    const box = h('div', { class: 'mb-3', id: 'as-launch-panel' });
    FP.launchAvailability().then((a) => {
      if (!a.available) {
        const why = NOT_HERE.has(a.reason)
          ? 'Open in AdvantageScope is only available on the machine running Flashpoint'
          : (a.reason || 'Open in AdvantageScope is not available');
        FP.fill(box, h('p', { class: 'meta', id: 'as-unavailable' }, why, '. Use the downloads below.'));
        return;
      }
      const result = h('div', { id: 'as-result', 'aria-live': 'polite' });
      const button = h('button', { type: 'button', class: 'btn btn--primary', id: 'as-launch',
        on: { click: () => launch(key, button, result) } }, 'OPEN IN ADVANTAGESCOPE');
      FP.fill(box, button, result);
    });
    return box;
  };
  FP.rawHref = (source) => (MODE.static
    ? 'raw/' + encodeURIComponent(source.download)
    : 'raw/' + encodeURIComponent(source.sha256) + '/' + encodeURIComponent(source.download));

  /* ---------- Charts: track() wraps uPlot with the shell theme ---------- */
  /* opts: { height, x: [...], series: [{ label, values, stroke, width, dash, fill, points, paths,
             band: { lower, upper } }], refs: [{ value, label }], shade: [{ from, to, alpha }],
             marks: [{ x, label, line }]  labelled vertical lines (e.g. a competition boundary);
                                         line: false draws the label only,
             cursor: () => x | null, onPick: (x) => void, gapX: number, xLabel: (x) => string,
             highlight: () => ({ values, stroke, width }) | null  one series drawn over the rest,
             yLabel: (y) => string, ariaLabel: string,
             onSelect: (x0, x1) => void   shift-drag (a plain drag still picks),
             onZoom: (factor, x) => void  ctrl/meta+wheel or a Safari pinch; factor > 1 zooms in }
     Returns { u, redraw, destroy, setX(min, max), selecting() }. */
  FP.track = (el, opts) => {
    const t = FP.tokens();
    const data = [opts.x];
    const series = [{}];
    const bands = [];
    const gapFilter = (u, sidx, idx0, idx1, nullGaps) => {
      if (!opts.gapX || !nullGaps) return nullGaps;
      const x0 = u.scales.x.min;
      const px = Math.abs(u.valToPos(x0 + opts.gapX, 'x', true) - u.valToPos(x0, 'x', true));
      return nullGaps.filter(([a, b]) => b - a > px);
    };
    for (const s of opts.series) {
      if (s.band) {
        const upper = data.push(s.band.upper) - 1;
        const lower = data.push(s.band.lower) - 1;
        // Band edges need real paths (width > 0) for uPlot to fill between them.
        series.push({ label: s.label + ' max', stroke: 'rgba(0,0,0,0)', width: 1, points: { show: false }, gaps: gapFilter });
        series.push({ label: s.label + ' min', stroke: 'rgba(0,0,0,0)', width: 1, points: { show: false }, gaps: gapFilter });
        bands.push({ series: [upper, lower], fill: s.bandFill || 'rgba(255,176,0,0.14)' });
      }
      data.push(s.values);
      series.push({
        label: s.label,
        stroke: s.stroke || t.amber,
        width: s.width || 1.5,
        dash: s.dash,
        points: s.points || { show: false },
        paths: s.paths,
        gaps: s.paths ? undefined : gapFilter,
      });
    }
    let selection = null; // [x0, x1] while a shift-drag is in progress
    const drawShade = (u) => {
      const ctx = u.ctx;
      for (const band of opts.shade || []) {
        const a = u.valToPos(band.from, 'x', true);
        const b = u.valToPos(band.to, 'x', true);
        ctx.save();
        ctx.fillStyle = 'rgba(255,176,0,' + (band.alpha || 0.05) + ')';
        ctx.fillRect(Math.min(a, b), u.bbox.top, Math.abs(b - a), u.bbox.height);
        ctx.restore();
      }
    };
    const drawOver = (u) => {
      const ctx = u.ctx;
      const px = window.devicePixelRatio || 1;
      for (const mark of opts.marks || []) {
        const x = u.valToPos(mark.x, 'x', true);
        if (x < u.bbox.left - 1 || x > u.bbox.left + u.bbox.width + 1) continue;
        ctx.save();
        if (mark.line !== false) {
          ctx.strokeStyle = t.amberDim;
          ctx.lineWidth = px;
          ctx.setLineDash([2 * px, 3 * px]);
          ctx.beginPath();
          ctx.moveTo(x, u.bbox.top);
          ctx.lineTo(x, u.bbox.top + u.bbox.height);
          ctx.stroke();
        }
        if (mark.label) {
          ctx.fillStyle = t.amberDim;
          ctx.font = (10 * px) + 'px ' + t.font;
          ctx.textAlign = 'left';
          ctx.textBaseline = 'top';
          ctx.fillText(mark.label, x + 4 * px, u.bbox.top + 4 * px);
        }
        ctx.restore();
      }
      for (const ref of opts.refs || []) {
        if (ref.value == null) continue;
        const y = u.valToPos(ref.value, 'y', true);
        if (y < u.bbox.top || y > u.bbox.top + u.bbox.height) continue;
        ctx.save();
        ctx.strokeStyle = t.hot;
        ctx.lineWidth = px;
        ctx.setLineDash([4 * px, 3 * px]);
        ctx.beginPath();
        ctx.moveTo(u.bbox.left, y);
        ctx.lineTo(u.bbox.left + u.bbox.width, y);
        ctx.stroke();
        if (ref.label) {
          ctx.setLineDash([]);
          ctx.fillStyle = t.hot;
          ctx.font = (10 * px) + 'px ' + t.font;
          ctx.textAlign = 'right';
          ctx.fillText(ref.label, u.bbox.left + u.bbox.width - 6 * px, y - 4 * px);
        }
        ctx.restore();
      }
      const hl = opts.highlight ? opts.highlight() : null;
      if (hl) {
        ctx.save();
        ctx.strokeStyle = hl.stroke;
        ctx.fillStyle = hl.stroke;
        ctx.lineWidth = hl.width * px;
        ctx.beginPath();
        let on = false;
        hl.values.forEach((v, i) => {
          if (v == null) { on = false; return; }
          const x = u.valToPos(opts.x[i], 'x', true); const y = u.valToPos(v, 'y', true);
          if (on) ctx.lineTo(x, y); else ctx.moveTo(x, y);
          on = true;
        });
        ctx.stroke();
        hl.values.forEach((v, i) => {
          if (v == null) return;
          ctx.beginPath();
          ctx.arc(u.valToPos(opts.x[i], 'x', true), u.valToPos(v, 'y', true), 3 * px, 0, 2 * Math.PI);
          ctx.fill();
        });
        ctx.restore();
      }
      const at = opts.cursor ? opts.cursor() : null;
      if (at != null) {
        const x = u.valToPos(at, 'x', true);
        ctx.save();
        ctx.strokeStyle = t.amberHi;
        ctx.lineWidth = 2 * px;
        ctx.shadowColor = 'rgba(255,176,0,0.8)';
        ctx.shadowBlur = 8 * px;
        ctx.beginPath();
        ctx.moveTo(x, u.bbox.top);
        ctx.lineTo(x, u.bbox.top + u.bbox.height);
        ctx.stroke();
        ctx.restore();
      }
      if (selection) {
        const a = u.valToPos(selection[0], 'x', true);
        const b = u.valToPos(selection[1], 'x', true);
        ctx.save();
        ctx.fillStyle = 'rgba(255,176,0,0.18)';
        ctx.strokeStyle = t.amberMid;
        ctx.lineWidth = px;
        ctx.fillRect(Math.min(a, b), u.bbox.top, Math.abs(b - a), u.bbox.height);
        ctx.strokeRect(Math.min(a, b), u.bbox.top, Math.abs(b - a), u.bbox.height);
        ctx.restore();
      }
    };
    const axis = (label) => ({
      stroke: t.amberMuted,
      grid: { stroke: t.lineSoft, width: 1 },
      ticks: { stroke: t.line, width: 1 },
      font: '11px ' + t.font,
      values: label ? (u, vals) => vals.map(label) : undefined,
    });
    const width = Math.max(el.clientWidth, 200);
    const u = new window.uPlot({
      width,
      height: opts.height || 120,
      legend: { show: false },
      cursor: { show: Boolean(opts.hover), drag: { x: false, y: false }, points: { show: false }, x: false, y: false },
      select: { show: false },
      scales: {
        x: { time: false, range: opts.xRange },
        // Reference lines are always in view: the y range widens to include them.
        y: {
          range: opts.yRange || ((u2, min, max) => {
            const refs = (opts.refs || []).map((r) => r.value).filter((v) => v != null);
            let lo = Math.min(min == null ? Infinity : min, ...refs);
            let hi = Math.max(max == null ? -Infinity : max, ...refs);
            if (!Number.isFinite(lo) || !Number.isFinite(hi)) return [0, 1];
            if (lo === hi) { lo -= 1; hi += 1; }
            const pad = (hi - lo) * 0.08;
            return [lo - pad, hi + pad];
          }),
        },
      },
      axes: [Object.assign(axis(opts.xLabel), opts.xIncrs ? { incrs: opts.xIncrs } : {}), Object.assign(axis(opts.yLabel), { size: opts.ySize || 48 })],
      series,
      bands,
      hooks: {
        drawClear: [drawShade],
        draw: [drawOver],
        setCursor: opts.hover ? [(u2) => opts.hover(u2.cursor.idx)] : [],
      },
    }, data, el);
    if (opts.ariaLabel) {
      el.setAttribute('role', 'img');
      el.setAttribute('aria-label', opts.ariaLabel);
    }
    const xAt = (event) => {
      const rect = u.over.getBoundingClientRect();
      return u.posToVal(Math.max(0, Math.min(rect.width, event.clientX - rect.left)), 'x');
    };
    if (opts.onPick || opts.onSelect) {
      let dragging = null; // 'pick' | 'select'
      u.over.addEventListener('pointerdown', (event) => {
        dragging = event.shiftKey && opts.onSelect ? 'select' : (opts.onPick ? 'pick' : null);
        if (!dragging) return;
        u.over.setPointerCapture(event.pointerId);
        if (dragging === 'select') { const x = xAt(event); selection = [x, x]; u.redraw(false, false); } else opts.onPick(xAt(event), event);
      });
      u.over.addEventListener('pointermove', (event) => {
        if (dragging === 'pick') opts.onPick(xAt(event), event);
        else if (dragging === 'select') { selection[1] = xAt(event); u.redraw(false, false); }
      });
      const end = (commit) => {
        const sel = selection;
        selection = null;
        if (dragging === 'select') {
          u.redraw(false, false);
          if (commit && sel[0] !== sel[1]) opts.onSelect(Math.min(sel[0], sel[1]), Math.max(sel[0], sel[1]));
        }
        dragging = null;
      };
      u.over.addEventListener('pointerup', () => end(true));
      u.over.addEventListener('pointercancel', () => end(false));
    }
    if (opts.onZoom) {
      // Chromium and Firefox report a trackpad pinch as ctrl+wheel; plain wheel scrolls the page.
      u.over.addEventListener('wheel', (event) => {
        if (!event.ctrlKey && !event.metaKey) return;
        event.preventDefault();
        opts.onZoom(Math.exp(-event.deltaY * 0.002), xAt(event));
      }, { passive: false });
      let lastScale = 1;
      u.over.addEventListener('gesturestart', (event) => { event.preventDefault(); lastScale = 1; });
      u.over.addEventListener('gesturechange', (event) => {
        event.preventDefault();
        opts.onZoom(event.scale / lastScale, xAt(event));
        lastScale = event.scale;
      });
    }
    const observer = new ResizeObserver(() => {
      const w = Math.max(el.clientWidth, 200);
      if (w !== u.width) u.setSize({ width: w, height: opts.height || 120 });
    });
    observer.observe(el);
    return {
      u,
      redraw: () => u.redraw(false, false),
      destroy: () => { observer.disconnect(); u.destroy(); },
      setX: (min, max) => u.setScale('x', { min, max }),
      selecting: () => (selection ? selection.slice() : null),
    };
  };

  /* ---------- Sparkline (canvas: a tiny line in the row's own range, or in opts.range) ----------
     With range [lo, hi] (a scale shared by several lines), lo sits on the bottom edge and only
     the top is padded; opts.label names the line for a screen reader. */
  FP.spark = (values, opts = {}) => {
    const canvas = h('canvas', opts.label
      ? { width: 120, height: 24, role: 'img', 'aria-label': opts.label }
      : { width: 120, height: 24, 'aria-hidden': 'true' });
    if (opts.range) canvas.dataset.scale = opts.range.join(' ');
    const nums = values.filter((v) => v != null);
    const ctx = canvas.getContext && canvas.getContext('2d');
    if (!ctx || nums.length < 2) return canvas;
    const [lo, hi] = opts.range || [Math.min(...nums), Math.max(...nums)];
    const pad = (hi - lo) * 0.1 || 1;
    const below = opts.range ? 0 : pad;
    ctx.strokeStyle = opts.stroke || FP.tokens().amber;
    ctx.lineWidth = opts.width || 1.5;
    ctx.beginPath();
    let started = false;
    values.forEach((v, i) => {
      if (v == null) { started = false; return; }
      const x = (i / (values.length - 1)) * 118 + 1;
      const y = 23 - ((v - lo + below) / (hi - lo + below + pad)) * 22;
      if (started) ctx.lineTo(x, y); else ctx.moveTo(x, y);
      started = true;
    });
    ctx.stroke();
    return canvas;
  };

  /* ---------- Anomaly sensitivity: one setting shared by Anomalies and Replay ---------- */
  const SENS_KEY = 'flashpoint.sensitivity';
  FP.SENSITIVITIES = ['relaxed', 'normal', 'strict'];
  FP.sensitivity = (value) => {
    if (value && FP.SENSITIVITIES.includes(value)) {
      try { window.localStorage.setItem(SENS_KEY, value); } catch (_) { /* storage unavailable */ }
      return value;
    }
    let stored = null;
    try { stored = window.localStorage.getItem(SENS_KEY); } catch (_) { stored = null; }
    return FP.SENSITIVITIES.includes(stored) ? stored : 'normal';
  };
  /* An anomaly tagged `tag` (the lowest sensitivity that shows it) shows at `chosen`. */
  FP.shownAt = (tag, chosen) => FP.SENSITIVITIES.indexOf(tag) <= FP.SENSITIVITIES.indexOf(chosen);

  /* ---------- The filter bar (spec: view-filters / One filter bar) ---------- */
  const BAR = [['season', 'SEASON', 'All seasons'], ['robot', 'ROBOT', 'All robots'], ['event', 'EVENT', 'All events']];
  /* Distinct values of `key` in `combos` ({ season, robot, event, last }), newest first: seasons
     by value, the rest by their latest `last`. */
  function newest(combos, key) {
    const last = {};
    for (const c of combos) {
      const v = c[key];
      if (v != null && (last[v] == null || String(c.last || '') > last[v])) last[v] = String(c.last || '');
    }
    const values = Object.keys(last);
    return key === 'season' ? values.sort().reverse() : values.sort((a, b) => (last[b] > last[a] ? 1 : last[b] < last[a] ? -1 : 0));
  }
  /* The chosen shared values no combination holds, as "event 2019zzzz". */
  FP.unknownFilters = (f, combos) => BAR.filter(([k]) => f[k] !== ALL && !combos.some((c) => c[k] === f[k])).map(([k]) => k + ' ' + f[k]);
  /* Keep focus on the control with the same id when a re-render replaces it. */
  FP.swap = (old, next) => {
    const active = document.activeElement;
    const id = active && old.contains(active) ? active.id : null;
    old.replaceWith(next);
    const again = id ? document.getElementById(id) : null;
    if (again) again.focus();
    return next;
  };
  /* The band under the header: SEASON, ROBOT, EVENT, the UNIT chip when set, the view's own
     filters after a divider, and CLEAR FILTERS.
     opts: { filters, combos, onChange(patch), extras: [{ key, label, allLabel, options: [[value,
             text]], value (null: not set), fallback (shown when not set) }], only ('HISTORY ONLY'),
             unitNote, seasonNote (a node: season can't be filtered), unknownText }
     Clearing sends every shared key as 'all' and every extra as null. */
  FP.filterBar = (opts) => {
    const f = opts.filters;
    const combos = opts.combos || [];
    const extras = opts.extras || [];
    const inSeason = f.season === ALL ? combos : combos.filter((c) => c.season === f.season);
    const values = { season: newest(combos, 'season'), robot: newest(inSeason, 'robot'), event: newest(inSeason, 'event') };
    const unknown = FP.unknownFilters(f, combos);
    const change = (key, value) => {
      const patch = { [key]: value };
      if (key === 'season' && value !== ALL) {
        // The robot and event lists narrow to the season; a choice outside it goes back to all.
        for (const other of ['robot', 'event']) {
          if (f[other] !== ALL && !combos.some((c) => c.season === value && c[other] === f[other])) patch[other] = ALL;
        }
      }
      opts.onChange(patch);
    };
    const clear = () => {
      const patch = Object.fromEntries(SHARED.map((k) => [k, ALL]));
      for (const e of extras) patch[e.key] = null;
      opts.onChange(patch);
      const first = document.querySelector('#filter-bar select:not([disabled])');
      if (first) first.focus();
    };
    const shared = BAR.map(([key, label, allLabel]) => {
      const options = values[key].includes(f[key]) || f[key] === ALL ? values[key] : [f[key], ...values[key]];
      const disabled = key === 'season' && Boolean(opts.seasonNote);
      return h('div', { class: 'field' },
        h('label', { class: 'group__label', for: 'f-' + key }, label),
        h('select', { class: 'select', id: 'f-' + key, disabled, on: { change: (event) => change(key, event.target.value) } },
          h('option', { value: ALL, selected: f[key] === ALL }, allLabel),
          options.map((v) => h('option', { value: v, selected: f[key] === v }, v + (unknown.includes(key + ' ' + v) ? ' ' + (opts.unknownText || '(no data here)') : '')))),
        disabled ? h('p', { class: 'meta filter-bar__note' }, opts.seasonNote) : null);
    });
    const unit = f.unit !== ALL ? h('div', { class: 'field', id: 'unit-chip' },
      h('span', { class: 'group__label' }, 'UNIT'),
      h('span', { class: 'row' },
        h('button', { type: 'button', class: 'chip', id: 'unit-clear', 'aria-label': 'Remove the unit filter ' + f.unit, on: { click: () => opts.onChange({ unit: ALL }) } }, f.unit + ' ✕'),
        opts.unitNote ? h('span', { class: 'meta filter-bar__na', id: 'unit-note' }, opts.unitNote) : null)) : null;
    const own = extras.length ? [
      h('span', { class: 'filter-bar__divider', 'aria-hidden': 'true' }),
      h('span', { class: 'group__label filter-bar__only' }, opts.only || 'THIS VIEW ONLY'),
      extras.map((e) => h('div', { class: 'field' },
        h('label', { class: 'group__label', for: 'f-' + e.key }, e.label),
        h('select', { class: 'select', id: 'f-' + e.key, dataset: { only: '' }, on: { change: (event) => opts.onChange({ [e.key]: event.target.value || null }) } },
          e.allLabel ? h('option', { value: '' }, e.allLabel) : null,
          e.options.map(([v, text]) => h('option', { value: v, selected: (e.value == null ? e.fallback : e.value) === v }, text))))),
    ] : null;
    const nothing = !FP.filtered(f) && extras.every((e) => e.value == null);
    return h('section', { class: 'band filter-bar', id: 'filter-bar', role: 'group', 'aria-label': 'Filters' },
      shared, unit, own,
      h('button', { type: 'button', class: 'btn btn--sm filter-bar__clear', id: 'filters-clear', disabled: nothing, on: { click: clear } }, 'CLEAR FILTERS'));
  };

  /* ---------- Side rails: minimize to a strip, remembered per view and side (spec: match-reports) ---------- */
  /* Returns { el, body }: the rail element (tag + attrs) and the box its content goes in. */
  FP.rail = ({ view, side, label, tag, attrs }) => {
    const key = 'flashpoint.rail.' + view + '.' + side;
    const bodyId = 'rail-' + side + '-body';
    const body = h('div', { class: 'rail__body', id: bodyId });
    const name = label.toLowerCase();
    const toggle = h('button', { type: 'button', class: 'btn btn--sm btn--ghost rail__toggle', id: 'rail-' + side + '-min',
      'aria-expanded': 'true', 'aria-controls': bodyId, 'aria-label': 'Minimize ' + name, title: 'Minimize ' + name }, side === 'l' ? '◂' : '▸');
    const strip = h('button', { type: 'button', class: 'rail__strip', id: 'rail-' + side + '-strip',
      'aria-expanded': 'false', 'aria-controls': bodyId, 'aria-label': 'Restore ' + name, title: 'Restore ' + name },
    h('span', { class: 'rail__strip-label' }, label.toUpperCase()));
    const head = h('div', { class: 'rail__head' }, h('h2', { class: 'label rail__title' }, label), toggle);
    const el = h(tag || 'aside', { ...attrs, class: (attrs && attrs.class ? attrs.class + ' ' : '') + 'rail rail--' + side }, head, strip, body);
    const show = (collapsed) => {
      el.classList.toggle('is-collapsed', collapsed);
      head.hidden = collapsed;
      body.hidden = collapsed;
      strip.hidden = !collapsed;
    };
    const set = (collapsed) => {
      show(collapsed);
      try { window.localStorage.setItem(key, collapsed ? '1' : '0'); } catch (_) { /* storage unavailable */ }
      (collapsed ? strip : toggle).focus();
    };
    toggle.addEventListener('click', () => set(true));
    strip.addEventListener('click', () => set(false));
    let stored = null;
    try { stored = window.localStorage.getItem(key); } catch (_) { stored = null; }
    show(stored === '1');
    return { el, body };
  };

  /* ---------- Views and the router ---------- */
  const views = [];
  FP.view = (view) => { views.push(view); };
  const available = () => views.filter((v) => !(v.needsApi && MODE.static));

  /* A view's nav badge ({ text, hot } or null), filled after the header renders. */
  FP.refreshBadges = () => {
    for (const v of available()) {
      const slot = document.querySelector('[data-badge="' + v.id + '"]');
      if (!slot || !v.badge) continue;
      Promise.resolve(v.badge()).then((b) => {
        slot.hidden = !b;
        if (!b) return;
        slot.textContent = b.text;
        slot.classList.toggle('tab__badge--hot', Boolean(b.hot));
        slot.setAttribute('aria-label', b.label || b.text);
      }).catch(() => { slot.hidden = true; });
    }
  };

  function header(active) {
    const nav = h('nav', { class: 'tabs', 'aria-label': 'Views' },
      available().map((v) => h('a', { class: 'tab', href: navHref(v.id), dataset: { view: v.id }, 'aria-current': v.id === active ? 'page' : null },
        v.label.toUpperCase(),
        v.badge ? h('span', { class: 'tab__badge', dataset: { badge: v.id }, hidden: true }) : null)));
    const crt = h('button', { type: 'button', class: 'btn btn--sm btn--ghost', 'aria-pressed': 'true', title: 'Toggle scanline texture', on: { click: toggleCrt } }, 'CRT');
    crt.dataset.crt = '';
    return h('header', { class: 'app-header' },
      h('a', { class: 'brand', href: '#', 'aria-label': 'Flashpoint home' },
        h('span', { class: 'brand__mark' }, 'FLASHPOINT'),
        h('span', { class: 'brand__sub' }, 'telemetry // ignite robotics')),
      nav,
      h('div', { class: 'app-header__spacer' }),
      MODE.static
        ? h('span', { class: 'pill mode-pill' }, 'STATIC EXPORT')
        : h('span', { class: 'lake-path', id: 'lake-path', title: 'Lake' }),
      crt);
  }

  const CRT_KEY = 'ignite.scanlines';
  let crtOn = true;
  function setCrt(on) {
    crtOn = on;
    document.body.classList.toggle('no-scanlines', !on);
    document.querySelectorAll('[data-crt]').forEach((b) => b.setAttribute('aria-pressed', String(on)));
    try { window.localStorage.setItem(CRT_KEY, on ? '1' : '0'); } catch (_) { /* storage unavailable */ }
  }
  function toggleCrt() { setCrt(!crtOn); }

  function route() {
    const state = FP.state();
    const list = available();
    const view = list.find((v) => v.id === state.view) || list[0];
    if (!view) return;
    const root = document.getElementById('app');
    if (current && current.unmount) current.unmount();
    current = view;
    FP.fill(root, header(view.id), h('main', { class: 'view', id: 'view' }));
    setCrt(crtOn);
    if (FP.indexData && !MODE.static) {
      const path = document.getElementById('lake-path');
      if (path) { path.textContent = 'lake ' + FP.indexData.lake; path.title = FP.indexData.lake; }
    }
    document.title = view.label + ' · Flashpoint';
    FP.refreshBadges();
    view.mount(document.getElementById('view'), state);
  }
  FP.go = (state) => {
    const hash = FP.link(state);
    if (hash === window.location.hash) route();
    else window.location.hash = hash;
  };

  function start() {
    try { crtOn = window.localStorage.getItem(CRT_KEY) !== '0'; } catch (_) { crtOn = true; }
    window.addEventListener('hashchange', route);
    FP.loadIndex().finally(route);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
