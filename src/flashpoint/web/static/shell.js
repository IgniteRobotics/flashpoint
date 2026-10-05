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
  };
  FP.link = (state) => '#' + new URLSearchParams(Object.entries(state).filter(([, v]) => v != null && v !== '')).toString();

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
  FP.rawHref = (source) => (MODE.static
    ? 'raw/' + encodeURIComponent(source.download)
    : 'raw/' + encodeURIComponent(source.sha256) + '/' + encodeURIComponent(source.download));

  /* ---------- Charts: track() wraps uPlot with the shell theme ---------- */
  /* opts: { height, x: [...], series: [{ label, values, stroke, width, dash, fill, points, paths,
             band: { lower, upper } }], refs: [{ value, label }], shade: [{ from, to, alpha }],
             cursor: () => x | null, onPick: (x) => void, gapX: number, xLabel: (x) => string,
             yLabel: (y) => string, ariaLabel: string } */
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
    if (opts.onPick) {
      let dragging = false;
      const pick = (event) => {
        const rect = u.over.getBoundingClientRect();
        const x = u.posToVal(Math.max(0, Math.min(rect.width, event.clientX - rect.left)), 'x');
        opts.onPick(x, event);
      };
      u.over.addEventListener('pointerdown', (event) => {
        dragging = true;
        u.over.setPointerCapture(event.pointerId);
        pick(event);
      });
      u.over.addEventListener('pointermove', (event) => { if (dragging) pick(event); });
      u.over.addEventListener('pointerup', () => { dragging = false; });
      u.over.addEventListener('pointercancel', () => { dragging = false; });
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
    };
  };

  /* ---------- Sparkline (canvas: a tiny line in the row's own range) ---------- */
  FP.spark = (values, opts = {}) => {
    const canvas = h('canvas', { width: 120, height: 24, 'aria-hidden': 'true' });
    const nums = values.filter((v) => v != null);
    const ctx = canvas.getContext && canvas.getContext('2d');
    if (!ctx || nums.length < 2) return canvas;
    const lo = Math.min(...nums); const hi = Math.max(...nums); const pad = (hi - lo) * 0.1 || 1;
    ctx.strokeStyle = opts.stroke || FP.tokens().amber;
    ctx.lineWidth = opts.width || 1.5;
    ctx.beginPath();
    let started = false;
    values.forEach((v, i) => {
      if (v == null) { started = false; return; }
      const x = (i / (values.length - 1)) * 118 + 1;
      const y = 23 - ((v - lo + pad) / (hi - lo + 2 * pad)) * 22;
      if (started) ctx.lineTo(x, y); else ctx.moveTo(x, y);
      started = true;
    });
    ctx.stroke();
    return canvas;
  };

  /* ---------- Views and the router ---------- */
  const views = [];
  let current = null;
  FP.view = (view) => { views.push(view); };
  const available = () => views.filter((v) => !(v.needsApi && MODE.static));

  function header(active) {
    const nav = h('nav', { class: 'tabs', 'aria-label': 'Views' },
      available().map((v) => h('a', { class: 'tab', href: '#view=' + encodeURIComponent(v.id), 'aria-current': v.id === active ? 'page' : null }, v.label.toUpperCase())));
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
