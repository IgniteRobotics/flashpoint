// Shared logic for the Ignite agent guard hooks. Pure functions; no I/O except markerExists
// being computed by the caller. Node >= 20.19, no dependencies.
//
// Payload shapes (checked 2026-11-05; Copilot's toolArgs structure is NOT specified per tool in
// its docs, so the Copilot fixtures are shaped from the documented field names, not captured):
//   Claude Code  https://code.claude.com/docs/en/hooks
//     stdin: {cwd, hook_event_name, tool_name: "Edit"|"Write"|"MultiEdit"|"Bash", tool_input:
//     {file_path} | {command}}. Exit 2 blocks (stderr is the reason). On Windows file_path
//     arrives with backslashes, even under Git Bash.
//   Codex        https://learn.chatgpt.com/docs/hooks
//     stdin: {cwd, hook_event_name, tool_name: "Bash"|"apply_patch"|..., tool_input: {command}}.
//     File edits arrive as apply_patch with the patch text in tool_input.command. Exit 2 blocks.
//   Copilot CLI  https://docs.github.com/en/copilot/reference/hooks-configuration
//     camelCase: {cwd, toolName: "edit"|"create"|"bash"|"powershell", toolArgs}; the
//     PascalCase/VS Code form is Claude-shaped (tool_name, tool_input). Exit 2 denies; any
//     other non-zero exit is fail-CLOSED there. toolArgs may be an object or a JSON string.

import { posix } from "node:path";

const EDIT_TOOLS = new Set([
  "edit", "write", "multiedit", "create", "notebookedit", "str_replace_editor",
  "str_replace_based_edit_tool", "apply_patch",
]);
const SHELL_TOOLS = new Set(["bash", "shell", "powershell", "exec_command", "run_shell_command"]);

export function normalizePath(p, platform) {
  const s = String(p).replace(/\\/g, "/");
  return platform === "win32" ? s.toLowerCase() : s;
}

function asObject(v) {
  if (typeof v === "string") {
    try {
      v = JSON.parse(v);
    } catch {
      return null;
    }
  }
  return v && typeof v === "object" && !Array.isArray(v) ? v : null;
}

function patchPaths(patch) {
  const out = [];
  for (const m of patch.matchAll(/^\*\*\* (?:Add File|Update File|Delete File|Move to):\s*(.+?)\s*$/gm)) {
    out.push(m[1]);
  }
  return out;
}

/** Raw stdin -> {tool, path?, paths?, command?, cwd} or null when unrecognised. */
export function parsePayload(raw) {
  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!data || typeof data !== "object" || Array.isArray(data)) return null;
  const name = data.tool_name ?? data.toolName;
  if (typeof name !== "string" || name === "") return null;
  const input = asObject(data.tool_input ?? data.toolArgs);
  const cwd = typeof data.cwd === "string" && data.cwd !== "" ? data.cwd : process.cwd();
  const key = name.toLowerCase();

  if (EDIT_TOOLS.has(key)) {
    if (!input) return null;
    let paths = [];
    if (key === "apply_patch") {
      const text = Array.isArray(input.command) ? input.command.join("\n") : input.command;
      paths = typeof text === "string" ? patchPaths(text) : [];
    } else {
      const p = input.file_path ?? input.path ?? input.filePath ?? input.notebook_path;
      if (typeof p === "string" && p !== "") paths = [p];
    }
    if (paths.length === 0) return null;
    return { tool: "edit", path: paths[0], paths, cwd };
  }
  if (SHELL_TOOLS.has(key)) {
    if (!input) return null;
    const c = Array.isArray(input.command) ? input.command.join(" ") : input.command;
    if (typeof c !== "string" || c === "") return null;
    return { tool: "shell", command: c, cwd };
  }
  return { tool: "other", cwd };
}

function isAbsolute(p) {
  return p.startsWith("/") || /^[a-z]:\//i.test(p);
}

/** Does this edited path land in the frozen src/test tree? */
function isTestPath(rawPath, cwd, platform) {
  const p = normalizePath(rawPath, platform);
  const c = normalizePath(cwd, platform).replace(/\/+$/, "");
  const full = posix.normalize(isAbsolute(p) ? p : `${c}/${p}`);
  if (full.startsWith(`${c}/`)) return /^src\/test(\/|$)/.test(full.slice(c.length + 1));
  // Outside the reported cwd (or cwd disagrees with the path style): fail toward protection.
  return /(^|\/)src\/test\//.test(full);
}

// --- shell command checks: best effort, a guardrail against drift, not a security boundary ---

/** Split into command segments of tokens, respecting quotes. */
function segments(command) {
  const segs = [];
  let tokens = [];
  let cur = "";
  let has = false;
  let quote = null;
  const pushTok = () => {
    if (has) tokens.push(cur);
    cur = "";
    has = false;
  };
  const pushSeg = () => {
    pushTok();
    if (tokens.length) segs.push(tokens);
    tokens = [];
  };
  for (const ch of command) {
    if (quote) {
      if (ch === quote) quote = null;
      else cur += ch;
    } else if (ch === '"' || ch === "'") {
      quote = ch;
      has = true;
    } else if (/\s/.test(ch) && ch !== "\n") {
      pushTok();
    } else if (ch === ";" || ch === "|" || ch === "&" || ch === "\n") {
      pushSeg();
    } else {
      cur += ch;
      has = true;
    }
  }
  pushSeg();
  return segs;
}

const base = (t) => t.replace(/\\/g, "/").split("/").pop().toLowerCase().replace(/\.(exe|cmd)$/, "");
const WRAPPERS = new Set(["sudo", "command", "env", "time", "nohup", "exec"]);

function inspectSegment(tokens, out) {
  let i = 0;
  while (i < tokens.length && (/^\w+=/.test(tokens[i]) || WRAPPERS.has(base(tokens[i])))) i++;
  const rest = tokens.slice(i);
  if (rest.length === 0) return;
  const prog = base(rest[0]);

  if (["sh", "bash", "zsh", "pwsh", "powershell", "cmd"].includes(prog)) {
    const j = rest.findIndex((t) => /^(-c|-command|\/c)$/i.test(t));
    if (j >= 0 && rest[j + 1]) for (const s of segments(rest.slice(j + 1).join(" "))) inspectSegment(s, out);
    return;
  }
  if (prog === "git") {
    let k = 1;
    while (k < rest.length && rest[k].startsWith("-")) {
      k += /^-[Cc]$|^--(git-dir|work-tree|namespace)$/.test(rest[k]) ? 2 : 1;
    }
    const sub = rest[k];
    if (sub === "push") out.push("push");
    if ((sub === "commit" || sub === "push") && rest.includes("--no-verify")) out.push("no-verify");
    return;
  }
  const isSpec = (t) => /^(@fission-ai\/)?openspec(@\S+)?$/.test(t.replace(/\\/g, "/").split("/").pop());
  const at = rest.findIndex(isSpec);
  if (at >= 0 && at <= 3) {
    const sub = rest.slice(at + 1).find((t) => !t.startsWith("-"));
    if (sub === "init" || sub === "update") out.push("openspec");
  }
}

const WRITE_HINT = new RegExp(
  [
    ">", "(^|[\\s;&|(])(tee|rm|mv|cp|truncate|dd|install)\\s", "\\bsed\\b.*\\s-(\\w*i|-in-place)",
    "\\b(set-content|add-content|out-file|remove-item|move-item|copy-item|new-item)\\b",
  ].join("|"),
  "i",
);
const MENTIONS_TEST = /(^|[\s"'=:(>/])src\/test(\/|[\s"')]|$)/;

function shellWritesTests(command, platform) {
  const text = normalizePath(command, platform)
    .replace(/\d?>&\d/g, " ") // 2>&1
    .replace(/\d?>>?\s*\/dev\/null/g, " ");
  return MENTIONS_TEST.test(text) && WRITE_HINT.test(text);
}

function shown(p, cwd) {
  const s = String(p).replace(/\\/g, "/");
  const c = String(cwd).replace(/\\/g, "/").replace(/\/+$/, "");
  return s.toLowerCase().startsWith(`${c.toLowerCase()}/`) ? s.slice(c.length + 1) : s;
}

/** Decide whether to block. markerExists = `.ignite/implementing` exists under event.cwd. */
export function decide(event, { markerExists, platform }) {
  const allow = { block: false, reason: "" };
  if (!event) return allow;

  if (event.tool === "edit" && markerExists) {
    for (const p of event.paths ?? [event.path]) {
      if (isTestPath(p, event.cwd, platform)) {
        return {
          block: true,
          reason: `Blocked: editing ${shown(p, event.cwd)} while .ignite/implementing exists. Tests are frozen during implementation — if a test looks wrong, stop and ask your mentor (see the SDD loop).`,
        };
      }
    }
  }
  if (event.tool === "shell") {
    const found = [];
    for (const seg of segments(event.command)) inspectSegment(seg, found);
    if (found.includes("push")) {
      return { block: true, reason: "Blocked: git push. Agents don't push — review the diff yourself, then push from your own terminal." };
    }
    if (found.includes("no-verify")) {
      return { block: true, reason: "Blocked: --no-verify skips the repo's checks. Fix what the check reports instead of skipping it." };
    }
    if (found.includes("openspec")) {
      return { block: true, reason: "Blocked: openspec init/update rewrites the committed OpenSpec tool files. Never run it here — if those files look wrong, ask a mentor." };
    }
    if (markerExists && shellWritesTests(event.command, platform)) {
      return { block: true, reason: "Blocked: this command writes into src/test/ while .ignite/implementing exists. Tests are frozen during implementation — ask your mentor if a test looks wrong." };
    }
  }
  return allow;
}
