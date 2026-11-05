import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { buildCommand, buildEnv, driftMessage, removeGenerated } from "./openspec-regen.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const recipe = JSON.parse(readFileSync(join(here, "openspec-recipe.json"), "utf8"));

test("regen: builds an isolated env", () => {
  const saved = { HOME: process.env.HOME, XDG: process.env.XDG_CONFIG_HOME };
  process.env.HOME = "/home/student-with-core-profile";
  process.env.XDG_CONFIG_HOME = "/home/student-with-core-profile/.config";
  process.env.OPENSPEC_TELEMETRY = "1";
  try {
    const home = join(tmpdir(), "regen-home");
    const env = buildEnv(home);
    assert.equal(env.HOME, home);
    assert.equal(env.USERPROFILE, home);
    assert.equal(env.XDG_CONFIG_HOME, join(home, ".config"));
    assert.equal(env.APPDATA, join(home, "AppData", "Roaming"));
    assert.equal(env.OPENSPEC_TELEMETRY, "0");
    for (const value of Object.values(env)) {
      assert.ok(!String(value).includes("student-with-core-profile"), `leaked: ${value}`);
    }
  } finally {
    process.env.HOME = saved.HOME;
    if (saved.XDG === undefined) delete process.env.XDG_CONFIG_HOME;
    else process.env.XDG_CONFIG_HOME = saved.XDG;
    delete process.env.OPENSPEC_TELEMETRY;
  }
});

test("regen: command uses pinned version and recipe tools", () => {
  assert.deepEqual(buildCommand(recipe), [
    "npx",
    "-y",
    "@fission-ai/openspec@1.10.0",
    "init",
    "--tools",
    "claude,codex,github-copilot",
    "--no-copilot-cloud",
    "--no-animation",
    ".",
  ]);
  const withCloud = buildCommand({ ...recipe, copilotCloud: true });
  assert.ok(!withCloud.includes("--no-copilot-cloud"));
  assert.ok(buildCommand({ ...recipe, openspecVersion: "2.0.0" }).includes("@fission-ai/openspec@2.0.0"));
});

test("regen: drift message names files and both audiences", () => {
  const msg = driftMessage([".claude/skills/openspec-explore/SKILL.md", "openspec/config.yaml"]);
  assert.equal(
    msg,
    [
      "OpenSpec tool files changed:",
      "  .claude/skills/openspec-explore/SKILL.md",
      "  openspec/config.yaml",
      "Students: undo these changes — `git checkout -- <changed paths>` and delete any new files listed. Never run `openspec init` or `openspec update` in a team repo.",
      "Mentors: regenerate with `node .ignite/openspec-regen.mjs` (recipe: .ignite/openspec-recipe.json) and commit the result.",
    ].join("\n"),
  );
});

test("regen: env keeps proxy vars and replaces the HOME family", () => {
  const home = join(tmpdir(), "regen-home");
  const env = buildEnv(home, {
    PATH: "/usr/bin",
    HOME: "/home/hostile",
    USERPROFILE: "C:\\Users\\hostile",
    APPDATA: "/home/hostile/AppData",
    XDG_CONFIG_HOME: "/home/hostile/.config",
    HTTP_PROXY: "http://proxy.school:3128",
    NODE_EXTRA_CA_CERTS: "/etc/ca.pem",
    npm_config_registry: "https://registry.example",
  });
  assert.equal(env.HTTP_PROXY, "http://proxy.school:3128");
  assert.equal(env.NODE_EXTRA_CA_CERTS, "/etc/ca.pem");
  assert.equal(env.npm_config_registry, "https://registry.example");
  assert.equal(env.PATH, "/usr/bin");
  assert.equal(env.HOME, home);
  assert.equal(env.USERPROFILE, home);
  assert.equal(env.APPDATA, join(home, "AppData", "Roaming"));
  assert.equal(env.XDG_CONFIG_HOME, join(home, ".config"));
});

test("regen: removeGenerated deletes only OpenSpec-named files", () => {
  const root = mkdtempSync(join(tmpdir(), "regen-rm-"));
  const put = (rel) => {
    mkdirSync(dirname(join(root, rel)), { recursive: true });
    writeFileSync(join(root, rel), "x");
  };
  const gone = [
    ".claude/skills/openspec-foo/SKILL.md",
    ".agents/skills/openspec-explore/SKILL.md",
    ".github/skills/openspec-sync-specs/SKILL.md",
    ".claude/commands/opsx/sync.md",
    ".github/prompts/opsx-sync.prompt.md",
  ];
  const kept = [
    ".claude/skills/my-skill/SKILL.md",
    ".claude/commands/other/cmd.md",
    ".github/prompts/code-review-prompt.md",
    "openspec/config.yaml",
  ];
  [...gone, ...kept].forEach(put);
  removeGenerated(root);
  for (const rel of gone) assert.ok(!existsSync(join(root, rel)), `${rel} should be removed`);
  for (const rel of kept) assert.ok(existsSync(join(root, rel)), `${rel} should be kept`);
});

test("regen: runs main when invoked through a symlinked path", { skip: process.platform === "win32" }, () => {
  const real = mkdtempSync(join(tmpdir(), "regen-real-"));
  cpSync(here, join(real, ".ignite"), { recursive: true, filter: (src) => !src.includes("hooks") });
  const bin = join(real, "bin");
  mkdirSync(bin);
  // A fake npx stands in for OpenSpec: it writes one tool file, so the run must show up as drift.
  writeFileSync(join(bin, "npx"), '#!/bin/sh\nmkdir -p .claude/skills/openspec-fake && echo hi > .claude/skills/openspec-fake/SKILL.md\n');
  chmodSync(join(bin, "npx"), 0o755);
  const git = (...a) => spawnSync("git", ["-c", "user.email=t@t", "-c", "user.name=t", ...a], { cwd: real });
  git("init", "-q");
  git("add", ".ignite");
  git("commit", "-qm", "base");
  const linkParent = mkdtempSync(join(tmpdir(), "regen-link-"));
  const link = join(linkParent, "link");
  symlinkSync(real, link);
  const r = spawnSync(process.execPath, [join(link, ".ignite", "openspec-regen.mjs"), "--check"], {
    cwd: linkParent,
    encoding: "utf8",
    env: { ...process.env, PATH: `${bin}:${process.env.PATH}` },
  });
  assert.equal(r.status, 1, r.stdout + r.stderr);
  assert.match(r.stderr, /\.claude\/skills\/openspec-fake\/SKILL\.md/);
});

test("regen: restores generated files when openspec fails", { skip: process.platform === "win32" }, () => {
  const real = mkdtempSync(join(tmpdir(), "regen-fail-"));
  cpSync(here, join(real, ".ignite"), { recursive: true, filter: (src) => !src.includes("hooks") });
  const files = {
    ".claude/skills/openspec-explore/SKILL.md": "skill body\n",
    ".claude/commands/opsx/explore.md": "cmd\n",
    ".github/prompts/opsx-explore.prompt.md": "prompt\n",
    ".github/prompts/code-review-prompt.md": "mine\n",
    "openspec/config.yaml": "context: |\n  hand written\n",
  };
  for (const [rel, body] of Object.entries(files)) {
    mkdirSync(dirname(join(real, rel)), { recursive: true });
    writeFileSync(join(real, rel), body);
  }
  const bin = join(real, "bin");
  mkdirSync(bin);
  // Fake npx that half-works then fails: clobbers config.yaml, writes a stray file, exits 1.
  writeFileSync(join(bin, "npx"), '#!/bin/sh\necho junk > openspec/config.yaml\nmkdir -p .claude/skills/openspec-partial && echo p > .claude/skills/openspec-partial/SKILL.md\nexit 1\n');
  chmodSync(join(bin, "npx"), 0o755);
  const r = spawnSync(process.execPath, [join(real, ".ignite", "openspec-regen.mjs"), "--check"], {
    cwd: real,
    encoding: "utf8",
    env: { ...process.env, PATH: `${bin}:${process.env.PATH}` },
  });
  assert.equal(r.status, 2, r.stdout + r.stderr);
  assert.match(r.stderr, /network and proxy/);
  assert.match(r.stderr, /restored/);
  for (const [rel, body] of Object.entries(files)) {
    assert.equal(readFileSync(join(real, rel), "utf8"), body, `${rel} not restored`);
  }
  assert.ok(!existsSync(join(real, ".claude/skills/openspec-partial")), "partial output left behind");
});
