/* ==========================================================================
   Live CAN: which CTRE device dropped out, rebooted or faulted while the pit
   crew shake-tests (disabled) or drive-tests (enabled) the robot, and whether
   it was one device or the whole bus (spec: live-can-health, ADR-0016).

   The server does the detecting; this view only renders its server-sent
   events: snapshots (connection, buses, devices) and events. Marks and the
   event log live in this page only: a reload drops them, other viewers never
   see them. Served mode only.
   URL: #view=live
   ========================================================================== */
(function () {
  'use strict';
  const FP = window.FP;
  const { h } = FP;

  const STREAM = 'api/live/can/stream';
  // The app references no remote host (offline in the pit), so the guide is named, not linked.
  const GUIDE = 'docs/guide/live-can-health.md';
  const MAX_LOG = 500;
  const MAX_MARK = 60;
  // Device states, worst first, with the status language the rest of the app uses.
  const STATES = {
    disconnected: ['fault', 'DISCONNECTED'],
    dropped: ['fault', 'DROPPED'],
    rebooted: ['fault', 'REBOOTED'],
    faulted: ['warn', 'FAULTED'],
    missing: ['retired', 'MISSING'],
    unmapped: ['retired', 'UNMAPPED'],
    ok: ['ok', 'OK'],
  };
  const CONNECTION = {
    closed: 'NOT CONNECTED',
    connecting: 'CONNECTING',
    connected: 'CONNECTED',
  };
  const BUS_COUNTS = [
    ['BusOffCount', 'bus off'], ['TxFullCount', 'tx full'],
    ['ReceiveErrorCount', 'rx errors'], ['TransmitErrorCount', 'tx errors'],
  ];

  let ui = null;

  /* ---------- formatting ---------- */
  const clock = (epoch) => {
    const d = new Date(epoch * 1000);
    const pad = (n, w = 2) => String(n).padStart(w, '0');
    return pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()) + '.' + pad(d.getMilliseconds(), 3);
  };
  const pct = (value) => (value == null ? '—' : Math.round(value * 100) + '%');
  const volts = (value) => (value == null ? '—' : FP.fmt(value, 2) + ' V');
  const who = (e) => (e.slot ? e.slot + ' (' + e.device + ')' : e.device);

  function describe(e) {
    switch (e.kind) {
      case 'dropout':
        return ['DROPOUT', who(e) + ' on ' + e.bus, '×' + e.count + (e.max_age_ms != null ? ' · ' + FP.fmt(e.max_age_ms, 0) + ' ms' : '')];
      case 'bus-dropout':
        return ['BUS DROP', e.bus + ': ' + e.devices.join(', '), e.devices.length + ' devices' + (e.max_age_ms != null ? ' · ' + FP.fmt(e.max_age_ms, 0) + ' ms' : '')];
      case 'reboot':
        return ['REBOOT', who(e) + ' on ' + e.bus, '×' + e.count];
      case 'fault':
        return ['FAULT', who(e) + ' on ' + e.bus, e.fault];
      case 'disconnected':
        return ['LOST', who(e) + ' on ' + e.bus, 'not heard'];
      case 'reconnected':
        return ['BACK', who(e) + ' on ' + e.bus, 'heard again'];
      case 'bus-errors':
        return ['BUS ERR', e.bus, Object.entries(e.counts || {}).map(([k, v]) => '+' + v + ' ' + ((BUS_COUNTS.find(([n]) => n === k) || [k, k])[1])).join(', ')];
      case 'restart':
        return ['RESTART', 'robot code restarted', 'totals start again'];
      default:
        return [String(e.kind).toUpperCase(), e.bus || '', ''];
    }
  }

  /* ---------- mount ---------- */
  function mount(el) {
    const mine = {
      el, source: null, mark: '', events: [], snapshot: null, seq: 0,
    };
    ui = mine;
    const markInput = h('input', {
      class: 'input', id: 'live-mark-input', type: 'text', maxlength: String(MAX_MARK),
      placeholder: 'now wiggling: FL module', 'aria-label': 'Mark', autocomplete: 'off',
      on: { keydown: (ev) => { if (ev.key === 'Enter') { ev.preventDefault(); setMark(markInput.value); } } },
    });
    const band = h('section', { class: 'band', 'aria-label': 'Live CAN' },
      h('div', { class: 'group' }, h('span', { class: 'group__label' }, 'LIVE CAN'),
        h('span', { class: 'pill', id: 'live-connection', role: 'status' }, 'CONNECTING'),
        h('span', { class: 'pill', id: 'live-enabled' }, '—')),
      h('span', { class: 'meta', id: 'live-robot' }),
      h('div', { class: 'band__spacer' }),
      h('div', { class: 'group', role: 'group', 'aria-label': 'Mark' },
        h('label', { class: 'group__label', htmlFor: 'live-mark-input' }, 'MARK'),
        markInput,
        h('button', { type: 'button', class: 'btn btn--sm', id: 'live-mark-set', on: { click: () => setMark(markInput.value) } }, 'SET'),
        h('button', { type: 'button', class: 'btn btn--sm btn--ghost', id: 'live-mark-clear', on: { click: () => { markInput.value = ''; setMark(''); } } }, 'CLEAR'),
        h('span', { class: 'meta', id: 'live-mark-current', 'aria-live': 'polite' })));
    const log = h('div', {
      class: 'log', id: 'live-log', role: 'log', tabIndex: 0, 'aria-label': 'Live CAN events, newest first', 'aria-live': 'polite',
    }, h('p', { class: 'muted', id: 'live-log-empty' }, 'No events yet. They appear here as the robot reports them.'));
    FP.fill(el, band,
      h('div', { class: 'pad', id: 'live-message', 'aria-live': 'polite' }),
      h('div', { class: 'pad live-layout' },
        h('div', { id: 'live-buses' }),
        h('section', { class: 'live-events', 'aria-labelledby': 'live-log-title' },
          h('h2', { class: 'h-section', id: 'live-log-title' }, 'Events'),
          log)));
    renderMark();
    const source = new EventSource(STREAM);
    mine.source = source;
    source.addEventListener('snapshot', (ev) => { if (ui === mine) { mine.snapshot = JSON.parse(ev.data); render(); } });
    source.addEventListener('event', (ev) => { if (ui === mine) addEvent(JSON.parse(ev.data)); });
    source.addEventListener('error', () => { if (ui === mine && source.readyState !== EventSource.OPEN) showConnection('connecting'); });
  }

  function unmount() {
    if (ui && ui.source) ui.source.close();
    ui = null;
  }

  /* ---------- marks ---------- */
  function setMark(text) {
    ui.mark = String(text || '').trim().slice(0, MAX_MARK);
    renderMark();
  }
  function renderMark() {
    const current = document.getElementById('live-mark-current');
    if (current) current.textContent = ui.mark ? 'marking: ' + ui.mark : 'no mark';
  }

  /* ---------- events ---------- */
  function addEvent(e) {
    ui.seq += 1;
    const entry = { ...e, mark: ui.mark, n: ui.seq };
    ui.events.unshift(entry);
    if (ui.events.length > MAX_LOG) ui.events.length = MAX_LOG;
    const log = document.getElementById('live-log');
    if (!log) return;
    const empty = document.getElementById('live-log-empty');
    if (empty) empty.remove();
    log.insertBefore(eventLine(entry), log.firstChild);
    while (log.childElementCount > MAX_LOG) log.removeChild(log.lastChild);
  }

  function eventLine(e) {
    const [verb, obj, result] = describe(e);
    const enabled = e.enabled === true ? 'ENABLED' : e.enabled === false ? 'DISABLED' : '?';
    const bad = e.kind !== 'reconnected';
    return h('div', { class: 'live-event', dataset: { event: e.kind, device: e.device || '', slot: e.slot || '', bus: e.bus || '' } },
      h('div', { class: 'live-event__head' },
        h('span', { class: 'log__n' }, clock(e.at)),
        h('span', { class: 'live-event__tag' + (e.enabled ? ' hot' : ''), dataset: { enabled: String(e.enabled) } }, enabled),
        h('span', { class: 'live-event__verb' + (bad ? ' hot' : '') }, verb),
        e.mark ? h('span', { class: 'chip', dataset: { mark: e.mark } }, e.mark) : null),
      h('div', { class: 'live-event__body' },
        h('span', { class: 'log__obj' }, obj),
        h('span', { class: 'log__res' }, result)));
  }

  /* ---------- snapshot ---------- */
  function showConnection(connection, status) {
    const pill = document.getElementById('live-connection');
    if (!pill) return;
    let text = CONNECTION[connection] || String(connection).toUpperCase();
    if (connection === 'connected' && status === 'not-publishing') text = 'NOT PUBLISHING';
    if (connection === 'connected' && status === 'unsupported') text = 'UNSUPPORTED VERSION';
    if (connection === 'connected' && status === 'waiting') text = 'WAITING FOR DATA';
    pill.textContent = text;
    pill.dataset.connection = connection;
    pill.dataset.status = status || '';
  }

  function render() {
    const s = ui.snapshot;
    showConnection(s.connection, s.status);
    const enabled = document.getElementById('live-enabled');
    enabled.textContent = s.enabled === true ? 'ENABLED' : s.enabled === false ? 'DISABLED' : '—';
    enabled.dataset.enabled = String(s.enabled);
    enabled.classList.toggle('pill--rec', s.enabled === true);
    const robot = document.getElementById('live-robot');
    robot.textContent = [
      s.robot_host ? 'robot ' + s.robot_host : (s.hosts && s.hosts.length ? 'trying ' + s.hosts.join(', ') : ''),
      s.project ? 'project ' + s.project : '',
      s.robot ? 'config ' + s.robot : '',
    ].filter(Boolean).join(' · ');
    const box = document.getElementById('live-message');
    const said = message(s);
    FP.fill(box, said);
    box.hidden = !box.childElementCount;
    const buses = document.getElementById('live-buses');
    FP.fill(buses, s.status === 'ok' ? s.buses.map(busPanel) : null);
  }

  function message(s) {
    if (s.connection !== 'connected' && s.hosts && s.hosts.length === 0) {
      return h('div', { class: 'callout', id: 'live-no-host' },
        'No robot address. Run ', h('code', { class: 'code' }, 'flashpoint init --team <number>'),
        ', or start the server with ', h('code', { class: 'code' }, 'flashpoint serve --robot-host <address>'), '.');
    }
    if (s.connection === 'connecting' || s.connection === 'closed') {
      return h('p', { class: 'muted', id: 'live-connecting' }, 'Looking for the robot. Turn it on and join its network; this keeps trying.');
    }
    if (s.status === 'not-publishing') {
      return h('div', { class: 'callout', id: 'live-not-publishing' },
        'The robot is connected, but its code is not publishing live CAN health under ',
        h('code', { class: 'code' }, '/Flashpoint/Live/CANHealth/'), '. ',
        'To add it to robot code, see ', h('code', { class: 'code', id: 'live-guide' }, GUIDE),
        ' in the Flashpoint repository.');
    }
    if (s.status === 'unsupported') {
      return h('div', { class: 'callout', id: 'live-unsupported' },
        'The robot publishes live CAN health version ' + s.version + '; this Flashpoint supports version ' + s.supported_version + '. ',
        'Update Flashpoint or the robot code so they match.');
    }
    const notes = [];
    if (s.project && s.project_found === false) {
      notes.push(h('div', { class: 'callout callout--quiet', id: 'live-unknown-project' },
        'No robot configuration has project ' + s.project + ', so every device shows unmapped. Add or fix a robot file in Setup.'));
    }
    if (s.notes && Object.keys(s.notes).length) {
      notes.push(h('p', { class: 'muted' }, 'Ignored: ' + Object.entries(s.notes).map(([k, v]) => k + ' (' + v + ')').join(', ')));
    }
    return notes;
  }

  function busPanel(bus) {
    const devices = bus.devices.slice().sort((a, b) => (a.slot == null) - (b.slot == null) || String(a.slot || a.name).localeCompare(String(b.slot || b.name)));
    const counts = BUS_COUNTS.map(([key, label]) => h('div', { class: 'fact' },
      h('div', { class: 'fact__k' }, label), h('div', { class: 'fact__v', dataset: { count: key } }, bus.counts[key] == null ? '—' : String(bus.counts[key]))));
    return h('section', { class: 'mb-3', dataset: { bus: bus.name }, 'aria-label': 'Bus ' + bus.name },
      h('div', { class: 'title-row' },
        h('h2', { class: 'h-section' }, bus.name),
        bus.dropped ? FP.status('fault', 'BUS DROPPED') : null,
        h('span', { class: 'meta' }, bus.devices.length + ' reporting' + (bus.missing.length ? ' · ' + bus.missing.length + ' missing' : ''))),
      h('div', { class: 'facts mb-3' },
        h('div', { class: 'fact' }, h('div', { class: 'fact__k' }, 'utilisation'), h('div', { class: 'fact__v', dataset: { utilization: '' } }, pct(bus.utilization))),
        counts,
        h('div', { class: 'fact' }, h('div', { class: 'fact__k' }, 'bus-wide drops'), h('div', { class: 'fact__v', dataset: { busDrops: '' } }, String(bus.bus_dropouts)))),
      Object.keys(bus.notes || {}).length
        ? h('p', { class: 'muted' }, 'Ignored: ' + Object.entries(bus.notes).map(([k, v]) => k + ' (' + v + ')').join(', ')) : null,
      h('div', { class: 'mechs' }, devices.map(deviceCard), bus.missing.map(missingCard)));
  }

  function deviceCard(d) {
    const [level, word] = STATES[d.state] || STATES.ok;
    const notes = Object.entries(d.notes || {});
    return h('div', { class: 'mech', dataset: { device: d.name, slot: d.slot || '', state: d.state } },
      h('div', { class: 'mech__top' },
        h('span', { class: 'mech__name' }, d.slot || 'unmapped'),
        FP.status(level, word)),
      h('div', { class: 'mech__top' },
        h('span', { class: 'meta' }, d.role || ''),
        h('span', { class: 'mech__can' }, d.name)),
      h('div', { class: 'mech__nums' },
        h('span', null, h('b', { dataset: { total: 'dropouts' } }, String(d.dropouts)), ' drops'),
        h('span', null, h('b', { dataset: { total: 'resets' } }, String(d.resets)), ' resets'),
        h('span', { dataset: { supply: '' } }, volts(d.min_supply_v))),
      d.faults && d.faults.length ? h('div', { class: 'small hot' }, d.faults.join(', ')) : null,
      notes.length ? h('div', { class: 'small hot', dataset: { notes: notes.map(([k]) => k).join(' ') } },
        'Ignored ' + notes.map(([k, v]) => k + ': ' + v).join('; ')) : null);
  }

  function missingCard(m) {
    const [level, word] = STATES.missing;
    return h('div', { class: 'mech', dataset: { device: m.model + '-' + m.can_id, slot: m.id, state: 'missing' } },
      h('div', { class: 'mech__top' }, h('span', { class: 'mech__name' }, m.id), FP.status(level, word)),
      h('div', { class: 'mech__top' }, h('span', { class: 'meta' }, m.role), h('span', { class: 'mech__can' }, m.model + '-' + m.can_id)),
      h('div', { class: 'mech__nums' }, 'configured, not reported'));
  }

  FP.view({ id: 'live', label: 'Live CAN', needsApi: true, mount, unmount });
})();
