// Regenerates the OpenSpec tool files from .ignite/openspec-recipe.json, with no user or
// runner config leaking in. `--check` also fails when the result differs from what is committed.
import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const TOOL_PATHS = [
  ".claude/skills",
  ".claude/commands",
  ".agents/skills",
  ".github/skills",
  ".github/prompts/opsx-*",
  "openspec/config.yaml",
];

// Only what a child process needs to find node/npx, reach the registry through a proxy, and use a
// temp dir. HOME and every XDG/APPDATA variable are deliberately absent so the caller's OpenSpec
// profile cannot reach the run.
export const PASSTHROUGH = [
  "PATH", "Path", "PATHEXT", "SystemRoot", "SYSTEMROOT", "windir", "ProgramFiles", "ProgramData", "ComSpec",
  "TEMP", "TMP", "TMPDIR", "LANG", "CI",
  "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy",
  "NODE_EXTRA_CA_CERTS", "SSL_CERT_FILE",
  "npm_config_proxy", "npm_config_https_proxy", "npm_config_registry", "npm_config_cafile",
];

export function buildEnv(home, base = process.env) {
  const env = {};
  for (const key of PASSTHROUGH) {
    if (base[key] !== undefined) env[key] = base[key];
  }
  return {
    ...env,
    HOME: home,
    USERPROFILE: home,
    XDG_CONFIG_HOME: join(home, ".config"),
    XDG_DATA_HOME: join(home, ".local", "share"),
    APPDATA: join(home, "AppData", "Roaming"),
    LOCALAPPDATA: join(home, "AppData", "Local"),
    OPENSPEC_TELEMETRY: "0",
  };
}

export function buildCommand(recipe) {
  const cmd = ["npx", "-y", `@fission-ai/openspec@${recipe.openspecVersion}`, "init", "--tools", recipe.tools.join(",")];
  if (!recipe.copilotCloud) cmd.push("--no-copilot-cloud");
  cmd.push("--no-animation", ".");
  return cmd;
}

export function globalConfig(recipe) {
  return { profile: recipe.profile, delivery: recipe.delivery, workflows: recipe.workflows };
}

export function driftMessage(paths) {
  return [
    "OpenSpec tool files changed:",
    ...paths.map((p) => `  ${p}`),
    "Students: put these files back the way development has them — `git fetch origin`, then `git checkout origin/development -- <changed paths>`, `git rm` any new files listed, commit, and push. Never run `openspec init` or `openspec update` in a team repo.",
    "Mentors: regenerate with `node .ignite/openspec-regen.mjs` (recipe: .ignite/openspec-recipe.json) and commit the result.",
  ].join("\n");
}

export function parsePorcelain(output) {
  return output
    .split("\n")
    .filter((line) => line.length > 3)
    .map((line) => line.slice(3).replace(/^"|"$/g, ""));
}

// `init` only writes. Delete the generated set first so files the recipe no longer produces (or
// that someone added by hand under an OpenSpec name) show up as git changes. openspec/config.yaml
// carries hand-written context and is never deleted; non-OpenSpec files are never touched.
export function generatedPaths(root) {
  const found = [];
  const collect = (dir, prefix) => {
    const abs = join(root, dir);
    if (!existsSync(abs)) return;
    for (const entry of readdirSync(abs)) {
      if (entry.startsWith(prefix)) found.push(join(dir, entry));
    }
  };
  for (const dir of [".claude/skills", ".agents/skills", ".github/skills"]) collect(dir, "openspec-");
  collect(".github/prompts", "opsx-");
  if (existsSync(join(root, ".claude", "commands", "opsx"))) found.push(join(".claude", "commands", "opsx"));
  return found;
}

export function removeGenerated(root) {
  for (const rel of generatedPaths(root)) rmSync(join(root, rel), { recursive: true, force: true });
}

// Copies the generated set (and config.yaml, which init edits in place) aside so a failed run can
// put everything back exactly as it was.
export function snapshot(root, dest) {
  const rels = [...generatedPaths(root), ...(existsSync(join(root, "openspec", "config.yaml")) ? ["openspec/config.yaml"] : [])];
  for (const rel of rels) cpSync(join(root, rel), join(dest, rel), { recursive: true });
  return rels;
}

export function restore(root, dest, rels) {
  removeGenerated(root);
  for (const rel of rels) {
    mkdirSync(dirname(join(root, rel)), { recursive: true });
    cpSync(join(dest, rel), join(root, rel), { recursive: true });
  }
}

function regenerate(root, recipe) {
  const home = mkdtempSync(join(tmpdir(), "openspec-regen-"));
  try {
    const env = buildEnv(home);
    const body = JSON.stringify(globalConfig(recipe));
    // OpenSpec reads $XDG_CONFIG_HOME/openspec on macOS/Linux and %APPDATA%/openspec on Windows.
    for (const dir of [env.XDG_CONFIG_HOME, env.APPDATA]) {
      mkdirSync(join(dir, "openspec"), { recursive: true });
      writeFileSync(join(dir, "openspec", "config.json"), body);
    }
    const backup = join(home, "backup");
    const saved = snapshot(root, backup);
    removeGenerated(root);
    const [bin, ...args] = buildCommand(recipe);
    let status = 1;
    try {
      const result = spawnSync(bin, args, { cwd: root, env, stdio: "inherit", shell: process.platform === "win32" });
      status = result.status ?? 1;
    } finally {
      if (status !== 0) restore(root, backup, saved);
    }
    return status;
  } finally {
    rmSync(home, { recursive: true, force: true });
  }
}

function drift(root) {
  const result = spawnSync("git", ["status", "--porcelain", "--untracked-files=all", "--", ...TOOL_PATHS], {
    cwd: root,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    console.error(`git status failed: ${result.stderr}`);
    return null;
  }
  return parsePorcelain(result.stdout);
}

function main() {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const recipe = JSON.parse(readFileSync(join(root, ".ignite", "openspec-recipe.json"), "utf8"));
  const status = regenerate(root, recipe);
  if (status !== 0) {
    console.error("The OpenSpec download or run failed. Check your network and proxy settings, then try again.");
    console.error(`openspec exited with status ${status}. Your tool files were restored exactly as they were.`);
    process.exit(2);
  }
  if (!process.argv.includes("--check")) return;
  const changed = drift(root);
  if (changed === null) process.exit(2);
  if (changed.length > 0) {
    console.error(driftMessage(changed));
    process.exit(1);
  }
}

export function isEntry(argv1, metaUrl, platform = process.platform) {
  if (!argv1) return false;
  const norm = (p) => (platform === "win32" ? realpathSync(p).toLowerCase() : realpathSync(p));
  try {
    return norm(argv1) === norm(fileURLToPath(metaUrl));
  } catch {
    return resolve(argv1) === fileURLToPath(metaUrl);
  }
}

if (isEntry(process.argv[1], import.meta.url)) main();
