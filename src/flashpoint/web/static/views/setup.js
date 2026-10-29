/* ==========================================================================
   Setup: the setup checks with their fixes, the configuration with inline
   editing, and drafting a robot from an imported log (spec: team-setup,
   config-editor). Served mode only. Fixes and saves run only when the server
   is bound to this machine and the request comes from it (ADR-0017); shared on
   the network, the view shows each fix's command and the configuration read-only.
   URL: #view=setup&tab=checks|config|draft
   ========================================================================== */
(function () {
  'use strict';
  const FP = window.FP;
  const { h } = FP;

  const TABS = [['checks', 'CHECKS'], ['config', 'CONFIGURATION'], ['draft', 'DRAFT ROBOT']];
  const GLYPH = { ok: '✓', warning: '!', problem: '✕', 'n/a': '–' };
  const STATE_CLASS = { warning: 'is-warn', problem: 'is-fault' };
  const SLOT_COLUMNS = [
    ['id', 'SLOT', 'text'], ['bus', 'BUS', 'text'], ['model', 'MODEL', 'text'], ['can_id', 'CAN ID', 'int'],
    ['subsystem', 'SUBSYSTEM', 'text'], ['role', 'ROLE', 'text'],
  ];
  const BUS_COLUMNS = [['name', 'NAME (ROBOT CODE)', 'text'], ['hoot', 'HOOT BUS', 'text']];
  const SIGNAL_COLUMNS = [
    ['id', 'SIGNAL', 'text'], ['root', 'ROOT', 'text'], ['entry', 'ENTRY', 'text'],
    ['subsystem', 'SUBSYSTEM', 'text'], ['component', 'COMPONENT', 'text'], ['metric', 'METRIC', 'text'],
  ];

  let ui = null;

  /* ---------- requests ---------- */
  async function write(method, path, body) {
    try {
      const response = await fetch('api/' + path, {
        method,
        headers: { 'Content-Type': 'application/json', 'X-Flashpoint': 'setup', Accept: 'application/json' },
        body: JSON.stringify(body),
      });
      const reply = await response.json();
      return { status: response.status, ...reply };
    } catch (err) {
      return { status: 0, ok: false, saved: false, reason: String(err.message) };
    }
  }
  const clone = (value) => JSON.parse(JSON.stringify(value));
  const slug = (text) => String(text).replace(/[^a-z0-9]+/gi, '-').toLowerCase();

  /* ---------- mount and tabs ---------- */
  async function mount(el, state) {
    ui = {
      el,
      tab: TABS.some(([t]) => t === state.tab) ? state.tab : 'checks',
      checks: null,
      config: null,
      writable: false,
      root: '',
      messages: {}, // section id -> { kind: 'ok' | 'error', text }
      sessions: null,
      draft: null,
    };
    const mine = ui;
    const tabs = h('span', { class: 'seg', id: 'setup-tabs', role: 'group', 'aria-label': 'Setup' });
    const band = h('section', { class: 'band', 'aria-label': 'Setup' },
      h('div', { class: 'group' }, h('span', { class: 'group__label' }, 'SETUP'), tabs),
      h('div', { class: 'band__spacer' }),
      h('span', { class: 'meta', id: 'setup-root' }));
    FP.fill(el, band, h('div', { class: 'pad', id: 'setup-note', 'aria-live': 'polite' }),
      h('div', { class: 'pad', id: 'setup-body' }));
    await loadChecks();
    if (ui !== mine) return;
    render();
  }

  function setTab(tab) {
    ui.tab = tab;
    FP.setState({ view: 'setup', tab });
    render();
  }

  function render() {
    const tabs = document.getElementById('setup-tabs');
    FP.fill(tabs, TABS.map(([id, text]) => h('button', {
      type: 'button', class: 'btn btn--sm', id: 'setup-tab-' + id, 'aria-pressed': String(ui.tab === id),
      on: { click: () => setTab(id) },
    }, text)));
    const root = document.getElementById('setup-root');
    root.textContent = ui.root ? 'configuration ' + ui.root : '';
    FP.fill(document.getElementById('setup-note'), ui.writable
      ? h('p', { class: 'meta', id: 'writable' }, 'Fixes and saves run on this computer.')
      : h('p', { class: 'callout callout--quiet', id: 'read-only' },
        'Read-only: this app is shared on the network (or not opened on the computer running it). Run the commands shown on that computer.'));
    if (ui.tab === 'checks') renderChecks();
    else if (ui.tab === 'config') renderConfig();
    else renderDraft();
  }

  /* ---------- checks ---------- */
  async function loadChecks() {
    try {
      const body = await FP.api('setup/checks');
      ui.checks = body.checks;
      ui.writable = Boolean(body.writable);
      ui.root = body.config_root;
    } catch (err) {
      ui.checks = [];
      ui.messages.checks = { kind: 'error', text: 'Could not run the checks: ' + err.message };
    }
  }

  function fixControls(c) {
    if (!c.fix_action || !ui.writable) {
      return c.command && c.status !== 'ok' ? h('code', { class: 'code', dataset: { command: c.id } }, c.command) : null;
    }
    const inputs = {};
    const fields = [];
    if (c.fix_action === 'init') {
      inputs.team = h('input', { class: 'input', id: 'init-team', type: 'number', min: '1', max: '99999', inputmode: 'numeric', 'aria-label': 'Team number' });
      inputs.lake = h('input', { class: 'input', id: 'init-lake', type: 'text', placeholder: '~/flashpoint-lake', 'aria-label': 'Lake folder' });
      fields.push(h('label', { class: 'field' }, h('span', { class: 'group__label' }, 'TEAM'), inputs.team),
        h('label', { class: 'field' }, h('span', { class: 'group__label' }, 'LAKE FOLDER'), inputs.lake));
    } else if (c.fix_action === 'advantagescope') {
      inputs.path = h('input', { class: 'input', id: 'as-path', type: 'text', 'aria-label': 'AdvantageScope path' });
      fields.push(h('label', { class: 'field' }, h('span', { class: 'group__label' }, 'ADVANTAGESCOPE PATH'), inputs.path));
    }
    const button = h('button', { type: 'button', class: 'btn btn--sm btn--primary', dataset: { fix: c.fix_action } }, c.fix_label || 'FIX');
    button.addEventListener('click', () => runFix(c.fix_action, inputs, button));
    return h('div', { class: 'row' }, fields, button);
  }

  async function runFix(action, inputs, button) {
    const body = {};
    if (inputs.team) body.team = inputs.team.value.trim();
    if (inputs.lake && inputs.lake.value.trim()) body.lake = inputs.lake.value.trim();
    if (inputs.path) body.path = inputs.path.value.trim();
    button.disabled = true;
    const mine = ui;
    const reply = await write('POST', 'setup/fix/' + encodeURIComponent(action), body);
    if (ui !== mine) return;
    if (reply.checks) ui.checks = reply.checks;
    if (reply.writable !== undefined) ui.writable = Boolean(reply.writable);
    ui.messages.checks = reply.ok
      ? { kind: 'ok', text: reply.message + (reply.changed && reply.changed.length ? ' (' + reply.changed.join(', ') + ')' : '') }
      : { kind: 'error', text: 'Not fixed: ' + (reply.reason || 'unknown error') + '.' };
    ui.config = null; // a fix can write configuration
    render();
  }

  function message(id) {
    const m = ui.messages[id];
    if (!m) return h('div', { id: 'msg-' + id, 'aria-live': 'polite' });
    return h('p', { class: m.kind === 'error' ? 'callout mt-2' : 'meta mt-2', id: 'msg-' + id, role: m.kind === 'error' ? 'alert' : 'status' }, m.text);
  }

  function renderChecks() {
    const checks = ui.checks || [];
    const problems = checks.filter((c) => c.status === 'problem').length;
    const warnings = checks.filter((c) => c.status === 'warning').length;
    FP.fill(document.getElementById('setup-body'),
      h('p', { class: 'meta', id: 'checks-summary' },
        problems ? problems + ' problem' + (problems === 1 ? '' : 's') : 'No problems',
        warnings ? ', ' + warnings + ' warning' + (warnings === 1 ? '' : 's') : '', '. ',
        'The same checks as ', h('code', { class: 'code' }, 'flashpoint doctor'), '.'),
      message('checks'),
      h('div', { id: 'checks' }, checks.map((c) => h('div', {
        class: 'check ' + (STATE_CLASS[c.status] || ''), dataset: { check: c.id, status: c.status },
      },
      h('span', { class: 'check__glyph', 'aria-hidden': 'true' }, GLYPH[c.status] || '?'),
      h('div', { class: 'check__body' },
        h('div', { class: 'check__name' }, c.title, h('span', { class: 'sr-only' }, ' (' + c.status + ')'),
          c.default ? h('span', { class: 'badge badge--low' }, 'DEFAULT') : null),
        h('div', { class: 'check__detail' }, c.message)),
      c.status === 'ok' || c.status === 'n/a' ? null : fixControls(c)))));
  }

  /* ---------- configuration ---------- */
  async function loadConfig() {
    try {
      const body = await FP.api('config');
      ui.config = body.sections;
      ui.writable = Boolean(body.writable);
      ui.root = body.config_root;
      ui.edits = {
        team: clone(ui.config.team.values || {}),
        robots: Object.fromEntries(ui.config.robots.filter((r) => r.data).map((r) => [r.id, clone(r.data)])),
        thresholds: Object.fromEntries(ui.config.thresholds.filter((t) => t.data).map((t) => [t.name, clone(t.data)])),
        changes: {}, // threshold file -> { table: { key: value } } not saved yet
        units: clone(ui.config.units.rows || []),
      };
    } catch (err) {
      ui.config = { error: String(err.message) };
    }
  }

  async function renderConfig() {
    const body = document.getElementById('setup-body');
    if (!ui.config) {
      FP.fill(body, h('p', { class: 'meta' }, 'Loading the configuration…'));
      const mine = ui;
      await loadConfig();
      if (ui !== mine || ui.tab !== 'config') return;
    }
    if (ui.config.error) {
      FP.fill(body, h('p', { class: 'callout', role: 'alert' }, 'Could not load the configuration: ' + ui.config.error));
      return;
    }
    FP.fill(body,
      teamSection(),
      h('h2', { class: 'label mt-6' }, 'Robots'),
      ui.config.robots.length ? ui.config.robots.map(robotSection)
        : h('p', { class: 'meta', id: 'no-robots' }, 'No robot files yet. Draft one from a log in the DRAFT ROBOT tab.'),
      h('h2', { class: 'label mt-6' }, 'Thresholds'),
      ui.config.thresholds.map(thresholdSection),
      unitsSection());
  }

  function source(entry) {
    return h('span', { class: 'meta' }, ' · ', h('code', { class: 'code' }, entry.source),
      entry.default ? h('span', { class: 'badge badge--low', title: 'Bundled with Flashpoint; saving writes your own copy' }, 'DEFAULT') : null);
  }

  function saveButton(id, onSave) {
    if (!ui.writable) return null;
    return h('button', { type: 'button', class: 'btn btn--sm btn--primary', id: 'save-' + id, on: { click: onSave } }, 'SAVE');
  }

  function sectionHead(title, entry, id, onSave) {
    return h('div', { class: 'row row--between mt-3' },
      h('h3', { class: 'label' }, title, source(entry)),
      entry.error ? null : saveButton(id, onSave));
  }

  async function save(id, path, payload) {
    const mine = ui;
    const button = document.getElementById('save-' + id);
    if (button) button.disabled = true;
    const reply = await write('PUT', 'config/' + path, payload);
    if (ui !== mine) return;
    if (reply.saved) {
      ui.messages[id] = { kind: 'ok', text: 'Saved ' + reply.path + '. Next: ' + reply.affects + '.' };
      ui.config = null; // reload: new hashes, and every section as the loader sees it
      ui.checks = null;
      await loadChecks();
    } else {
      ui.messages[id] = { kind: 'error', text: 'Not saved: ' + (reply.reason || 'unknown error') };
    }
    if (ui !== mine) return;
    if (ui.tab === 'config') renderConfig();
  }

  function input(value, onInput, attrs) {
    const el = h('input', { class: 'input', type: 'text', disabled: !ui.writable, ...attrs });
    el.value = value == null ? '' : String(value);
    el.addEventListener('input', () => onInput(el.value));
    return el;
  }

  function teamSection() {
    const team = ui.config.team;
    const edit = ui.edits.team;
    const field = (key, label, toValue, fromValue) => h('label', { class: 'field' },
      h('span', { class: 'group__label' }, label),
      input(fromValue ? fromValue(edit[key]) : edit[key], (v) => { edit[key] = toValue(v); }, { id: 'team-' + key }));
    const number = (v) => (v.trim() === '' ? null : (/^\d+$/.test(v.trim()) ? Number(v.trim()) : v.trim()));
    const text = (v) => (v.trim() === '' ? null : v.trim());
    const list = (v) => (v.trim() === '' ? null : v.split(',').map((x) => x.trim()).filter(Boolean));
    const onSave = () => save('team', 'team', { sha256: team.sha256, values: edit });
    return h('section', { id: 'cfg-team', dataset: { section: 'team' } },
      sectionHead('Team and connection', team, 'team', onSave),
      team.error ? h('p', { class: 'callout', role: 'alert' }, team.error) : null,
      h('div', { class: 'row gap-4 mt-2' },
        field('team', 'TEAM NUMBER', number),
        field('lake', 'LAKE FOLDER', text),
        field('hosts', 'ROBOT ADDRESSES (COMMA-SEPARATED; EMPTY: FROM THE TEAM)', list, (v) => (v || []).join(', '))),
      h('p', { class: 'meta mt-2', id: 'team-hosts-now' }, 'Acquire tries: ', (team.hosts || []).join(', ') || 'nothing (no team number)'),
      message('team'));
  }

  /* An editable table over `rows` (array of objects); columns: [key, label, kind]. */
  function editTable(id, columns, rows, opts) {
    const removable = ui.writable && opts.removable !== false;
    const cell = (row, [key, label, kind]) => {
      if (!ui.writable || (opts.readOnly || []).includes(key)) return h('td', null, row[key] == null ? '' : String(row[key]));
      const el = input(row[key], (v) => {
        if (kind === 'int') row[key] = /^-?\d+$/.test(v.trim()) ? Number(v.trim()) : v;
        else row[key] = v === '' && kind === 'optional' ? null : v;
      }, { class: 'input input--cell', 'aria-label': label + ' of ' + (row[opts.key] || 'new row'), dataset: { field: key } });
      return h('td', null, el);
    };
    const body = h('tbody', null, rows.map((row, i) => h('tr', { dataset: { row: String(row[opts.key] || i) } },
      columns.map((c) => cell(row, c)),
      removable ? h('td', null, h('button', {
        type: 'button', class: 'btn btn--sm btn--ghost', 'aria-label': 'Remove ' + (row[opts.key] || 'row ' + (i + 1)),
        on: { click: () => { rows.splice(i, 1); opts.rerender(); } },
      }, '✕')) : null)));
    return h('div', { class: 'table-wrap mt-2' },
      h('table', { class: 'table setup-table', id },
        h('thead', null, h('tr', null, columns.map(([, label]) => h('th', { scope: 'col' }, label)), removable ? h('th', null, h('span', { class: 'sr-only' }, 'Remove')) : null)),
        body),
      ui.writable && opts.blank ? h('button', {
        type: 'button', class: 'btn btn--sm mt-2', id: id + '-add',
        on: { click: () => { rows.push(opts.blank()); opts.rerender(); } },
      }, '+ ADD') : null);
  }

  function robotSection(entry) {
    const id = 'robot-' + slug(entry.id);
    const data = ui.edits.robots[entry.id];
    const rerender = () => renderConfig();
    const onSave = () => save(id, 'robots/' + encodeURIComponent(entry.id), { sha256: entry.sha256, robot: data });
    if (!data || entry.error) {
      return h('section', { class: 'mt-3', dataset: { robot: entry.id } },
        sectionHead(entry.id, entry, id, onSave),
        h('p', { class: 'callout', role: 'alert', id: 'error-' + id }, entry.error || 'This file does not load.'),
        message(id));
    }
    data.slot = data.slot || [];
    data.bus = data.bus || [];
    data.signal = data.signal || [];
    return h('section', { class: 'mt-3', dataset: { robot: entry.id } },
      sectionHead(entry.id, entry, id, onSave),
      h('p', { class: 'meta' }, 'Season ', String(data.season), ' · project ', String(data.project), ' · ',
        data.slot.length + ' slots, ' + data.bus.length + ' buses, ' + data.signal.length + ' signals'),
      message(id),
      h('h4', { class: 'group__label mt-3' }, 'SLOTS'),
      editTable(id + '-slots', SLOT_COLUMNS, data.slot, {
        key: 'id', rerender, blank: () => ({ id: '', bus: 'rio', model: 'TalonFX', can_id: 0, subsystem: '', role: '' }),
      }),
      h('h4', { class: 'group__label mt-3' }, 'BUSES'),
      editTable(id + '-buses', BUS_COLUMNS, data.bus, { key: 'name', rerender, blank: () => ({ name: '', hoot: 'canivore' }) }),
      h('details', { class: 'mt-3' },
        h('summary', { class: 'group__label' }, 'SIGNALS (' + data.signal.length + ')'),
        editTable(id + '-signals', SIGNAL_COLUMNS, data.signal, {
          key: 'id', rerender, blank: () => ({ id: '', root: 'robot', entry: '', subsystem: '', metric: '' }),
        })));
  }

  function thresholdSection(entry) {
    const id = 'thr-' + slug(entry.name.replace('.toml', ''));
    const data = ui.edits.thresholds[entry.name];
    const changed = ui.edits.changes[entry.name] || (ui.edits.changes[entry.name] = {});
    const shown = (table, key, value) => (changed[table] && key in changed[table] ? changed[table][key] : value);
    const onSave = () => save(id, 'thresholds/' + encodeURIComponent(entry.name), { sha256: entry.sha256, tables: changed });
    const field = (table, key, original) => {
      const value = shown(table, key, original);
      const set = (v) => { (changed[table] = changed[table] || {})[key] = v; };
      let el;
      if (typeof value === 'boolean') {
        el = h('input', { type: 'checkbox', checked: value, disabled: !ui.writable, dataset: { key: table + '.' + key } });
        el.addEventListener('change', () => set(el.checked));
      } else if (Array.isArray(original)) {
        el = input(value.join(', '), (v) => set(v.split(',').map((x) => x.trim()).filter(Boolean)), { dataset: { key: table + '.' + key } });
      } else {
        el = input(value, (v) => set(typeof original === 'number' && v.trim() !== '' && !Number.isNaN(Number(v)) ? Number(v) : v),
          { dataset: { key: table + '.' + key }, inputmode: typeof original === 'number' ? 'decimal' : null });
      }
      return h('label', { class: 'field' }, h('span', { class: 'group__label' }, key), el);
    };
    const tables = data ? Object.entries(data).filter(([, v]) => v && typeof v === 'object' && !Array.isArray(v)) : [];
    return h('section', { class: 'mt-3', dataset: { file: entry.name } },
      sectionHead(entry.name, entry, id, onSave),
      entry.error ? h('p', { class: 'callout', role: 'alert' }, entry.error) : null,
      message(id),
      tables.map(([table, values]) => h('fieldset', { class: 'mt-2 setup-fieldset' },
        h('legend', { class: 'group__label' }, '[' + table + ']'),
        h('div', { class: 'row gap-4' }, Object.entries(values)
          .filter(([, v]) => v === null || typeof v !== 'object' || Array.isArray(v))
          .map(([key, value]) => field(table, key, value))))));
  }

  function unitsSection() {
    const units = ui.config.units;
    const rows = ui.edits.units;
    const columns = units.columns.map((c) => [c, c.toUpperCase().replace('_', ' '), 'text']);
    const onSave = () => save('units', 'units', { sha256: units.sha256, rows });
    return h('section', { class: 'mt-6', id: 'cfg-units' },
      h('h2', { class: 'label' }, 'Units'),
      sectionHead('Every CTRE device the team owns, by serial', units, 'units', onSave),
      message('units'),
      editTable('units', columns, rows, {
        key: 'serial', rerender: () => renderConfig(), blank: () => Object.fromEntries(units.columns.map((c) => [c, ''])),
      }));
  }

  /* ---------- draft a robot ---------- */
  async function renderDraft() {
    const body = document.getElementById('setup-body');
    if (!ui.sessions) {
      FP.fill(body, h('p', { class: 'meta' }, 'Loading sessions…'));
      const mine = ui;
      try {
        ui.sessions = (await FP.api('setup/sessions')).sessions;
      } catch (err) {
        ui.sessions = [];
        ui.messages.draft = { kind: 'error', text: 'Could not list sessions: ' + err.message };
      }
      if (ui !== mine || ui.tab !== 'draft') return;
    }
    const select = h('select', { class: 'select', id: 'draft-session', 'aria-label': 'Session to draft from' },
      ui.sessions.map((s) => h('option', { value: s.session_id, selected: ui.draft && ui.draft.session_id === s.session_id },
        (s.match_key || s.filename || s.session_id) + ' · ' + (s.hoots ? s.hoots + ' hoot' + (s.hoots === 1 ? '' : 's') : 'no hoots'))));
    const go = h('button', { type: 'button', class: 'btn btn--sm', id: 'draft-go', disabled: !ui.sessions.length }, 'DRAFT FROM THIS LOG');
    go.addEventListener('click', () => startDraft(select.value));
    FP.fill(body,
      h('p', { class: 'meta' }, 'Start a robot file from an imported log: its CAN devices become slots, with serial numbers when the log has a CAN inventory. Name each slot, then save.'),
      ui.sessions.length ? h('div', { class: 'row mt-2' }, select, go)
        : h('p', { class: 'meta', id: 'draft-none' }, 'No imported logs yet. Run flashpoint ingest first.'),
      message('draft'),
      ui.draft ? draftForm() : null);
  }

  async function startDraft(sessionId) {
    const mine = ui;
    ui.messages.draft = null;
    try {
      ui.draft = await FP.api('setup/draft', { session: sessionId });
    } catch (err) {
      ui.draft = null;
      ui.messages.draft = { kind: 'error', text: err.message };
    }
    if (ui !== mine) return;
    renderDraft();
  }

  function draftForm() {
    const d = ui.draft;
    const robot = d.robot;
    const serial = Object.fromEntries(d.devices.map((x) => [x.slot, x.serial]));
    for (const s of robot.slot) if (s.serial === undefined) s.serial = serial[s.id] || '';
    const rows = robot.slot; // edited in place, so removing a row removes the slot
    const idInput = input(robot.robot, (v) => { robot.robot = v.trim(); }, { id: 'draft-id', 'aria-label': 'Robot id' });
    const saveDraft = async () => {
      const missing = robot.slot.filter((s) => !s.id || !String(s.subsystem).trim() || !String(s.role).trim());
      if (missing.length) {
        ui.messages.draft = { kind: 'error', text: 'Name every slot first: ' + missing.length + ' slot(s) still need an id, subsystem and role.' };
        renderDraft();
        return;
      }
      const clean = { ...robot, slot: robot.slot.map(({ serial: _s, ...slot }) => slot) };
      const mine = ui;
      const reply = await write('PUT', 'config/robots/' + encodeURIComponent(robot.robot), { robot: clean, new: true });
      if (ui !== mine) return;
      if (reply.saved) {
        ui.messages.draft = { kind: 'ok', text: 'Saved ' + reply.path + '. Next: ' + reply.affects + '.' };
        ui.draft = null;
        ui.config = null;
        await loadChecks();
      } else {
        ui.messages.draft = { kind: 'error', text: 'Not saved: ' + (reply.reason || 'unknown error') };
      }
      if (ui === mine) renderDraft();
    };
    return h('section', { class: 'mt-3', id: 'draft' },
      h('p', { class: 'meta', id: 'draft-summary' },
        rows.length + ' devices · season ' + robot.season + ' · project ' + (robot.project || '—') + ' · ',
        d.inventory ? 'serials from the CAN inventory' : 'no CAN inventory in this log (no serials)', ' · ',
        d.bus_status ? 'bus names from the robot code' : 'no bus names logged'),
      h('div', { class: 'row mt-2' },
        h('label', { class: 'field' }, h('span', { class: 'group__label' }, 'ROBOT ID'), idInput),
        ui.writable ? h('button', { type: 'button', class: 'btn btn--sm btn--primary', id: 'draft-save', on: { click: saveDraft } }, 'SAVE ROBOT') : null),
      editTable('draft-slots', [...SLOT_COLUMNS, ['serial', 'SERIAL', 'text']], rows, {
        key: 'id', readOnly: ['bus', 'model', 'can_id', 'serial'], removable: true, rerender: () => renderDraft(),
      }),
      robot.bus.length ? [h('h4', { class: 'group__label mt-3' }, 'BUSES'),
        editTable('draft-buses', BUS_COLUMNS, robot.bus, { key: 'name', rerender: () => renderDraft() })] : null);
  }

  FP.view({
    id: 'setup',
    label: 'Setup',
    needsApi: true,
    mount,
    unmount: () => { ui = null; },
    /* Shown on every view while the backup is overdue; the endpoint reads recorded state only. */
    badge: async () => {
      const b = await FP.api('backup/status');
      return b.level === 'problem' ? { text: '!', hot: true, label: 'Backup overdue: ' + b.message } : null;
    },
  });
})();
