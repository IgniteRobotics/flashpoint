// Ignite PreToolUse guard. Exit 2 + reason on stderr blocks the call; exit 0 allows it.
// Any crash or unreadable payload ALLOWS the call and says so on stderr — a crashing
// PreToolUse hook would otherwise block every call. Guardrail against the agent, not a
// security boundary; CI and review are the enforcement layer.
//
// Enable LOCALLY only — never commit these files (they would fire for mentors and in Copilot).
//
// Claude Code — .claude/settings.local.json (Copilot CLI also reads this file, so it covers both):
//   {"hooks": {
//     "PreToolUse": [{"matcher": "Edit|Write|MultiEdit|Bash",
//       "hooks": [{"type": "command", "command": "node .ignite/hooks/guard.mjs"}]}],
//     "SessionStart": [{"hooks": [{"type": "command", "command": "node .ignite/hooks/session-start.mjs"}]}]
//   }}
//
// Codex — .codex/hooks.json (loads only after you trust the project):
//   {"hooks": {
//     "PreToolUse": [{"matcher": "Bash|apply_patch|Edit|Write",
//       "hooks": [{"type": "command", "command": "node .ignite/hooks/guard.mjs"}]}],
//     "SessionStart": [{"matcher": "startup|resume",
//       "hooks": [{"type": "command", "command": "node .ignite/hooks/session-start.mjs"}]}]
//   }}
//
// Copilot CLI — use the .claude/settings.local.json snippet above. Its own hook files
// (.github/hooks/*.json) are committed, and a hook that fails to run is fail-closed there.

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { decide, parsePayload } from "./lib.mjs";

const WARN = "ignite guard: could not read hook input — not checking this call";

try {
  const event = parsePayload(readFileSync(0, "utf8"));
  if (!event || event.tool === "other") {
    console.error(WARN);
    process.exit(0);
  }
  const verdict = decide(event, {
    markerExists: existsSync(join(event.cwd, ".ignite", "implementing")),
    platform: process.platform,
  });
  if (verdict.block) {
    console.error(verdict.reason);
    process.exit(2);
  }
} catch {
  console.error(WARN);
}
process.exit(0);
