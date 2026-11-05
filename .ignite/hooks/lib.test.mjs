import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { decide, normalizePath, parsePayload } from "./lib.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const guard = join(here, "guard.mjs");
const sessionStart = join(here, "session-start.mjs");

const edit = (path, cwd = "/work/robot") => ({ tool: "edit", path, paths: [path], cwd });
const shell = (command, cwd = "/work/robot") => ({ tool: "shell", command, cwd });
const withMarker = { markerExists: true, platform: "linux" };
const noMarker = { markerExists: false, platform: "linux" };

function tmpRepo(marker) {
  const repo = mkdtempSync(join(tmpdir(), "ignite-guard-"));
  if (marker) {
    mkdirSync(join(repo, ".ignite"));
    writeFileSync(join(repo, ".ignite", "implementing"), "");
  }
  return repo;
}

function runGuard(input, repo) {
  return spawnSync(process.execPath, [guard], { input, cwd: repo, encoding: "utf8" });
}

const claudeEdit = (repo, file) =>
  JSON.stringify({ cwd: repo, tool_name: "Edit", tool_input: { file_path: file } });
const claudeBash = (repo, command) =>
  JSON.stringify({ cwd: repo, tool_name: "Bash", tool_input: { command } });

test("normalizePath: backslashes become slashes, lower-case only on win32", () => {
  assert.equal(normalizePath("C:\\Robot\\SRC", "win32"), "c:/robot/src");
  assert.equal(normalizePath("C:\\Robot\\SRC", "linux"), "C:/Robot/SRC");
});

test("guard: blocks test edit when marker present", () => {
  assert.equal(decide(edit("/work/robot/src/test/java/X.java"), withMarker).block, true);
  assert.equal(decide(edit("src/test/java/X.java"), withMarker).block, true);
  const repo = tmpRepo(true);
  const r = runGuard(claudeEdit(repo, join(repo, "src", "test", "java", "X.java")), repo);
  assert.equal(r.status, 2);
  assert.match(r.stderr, /^Blocked: editing .*src\/test\/java\/X\.java/);
});

test("guard: blocks backslash and mixed-case test paths", () => {
  const win = { markerExists: true, platform: "win32" };
  for (const p of ["C:\\robot\\src\\test\\java\\X.java", "C:\\robot\\SRC\\Test\\X.java"]) {
    assert.equal(decide(edit(p, "C:\\robot"), win).block, true, p);
  }
  // Backslash path with a cwd that does not match: still protected.
  assert.equal(decide(edit("D:\\other\\src\\test\\X.java", "C:\\robot"), win).block, true);
});

test("guard: allows test edit when marker absent", () => {
  assert.equal(decide(edit("/work/robot/src/test/java/X.java"), noMarker).block, false);
  const repo = tmpRepo(false);
  assert.equal(runGuard(claudeEdit(repo, join(repo, "src", "test", "X.java")), repo).status, 0);
});

test("guard: allows src/main edit with marker present", () => {
  assert.equal(decide(edit("/work/robot/src/main/java/X.java"), withMarker).block, false);
  assert.equal(decide(edit("/work/robot/src/main/java/test/src/test.java"), withMarker).block, false);
  assert.equal(decide(edit("/work/robot/notes/src/testing.md"), withMarker).block, false);
});

test("guard: blocks git push and --no-verify", () => {
  for (const c of [
    "git push origin x",
    "git -C . push",
    "git -c core.x=1 push --force",
    "cd a && git push",
    "FOO=1 sudo git push",
  ]) {
    assert.equal(decide(shell(c), noMarker).block, true, c);
  }
  assert.equal(decide(shell("git commit --no-verify -m y"), noMarker).block, true);
  assert.match(decide(shell("git commit --no-verify -m y"), noMarker).reason, /no-verify/);
  for (const c of ['echo "git push"', "git commit -m 'push it'", "git status", "git log --oneline"]) {
    assert.equal(decide(shell(c), noMarker).block, false, c);
  }
  const repo = tmpRepo(false);
  assert.equal(runGuard(claudeBash(repo, "git push origin x"), repo).status, 2);
});

test("guard: blocks openspec init and update", () => {
  for (const c of [
    "openspec init",
    "openspec update --force",
    "npx @fission-ai/openspec update",
    "npx -y @fission-ai/openspec@1.10.0 init --tools claude",
  ]) {
    assert.equal(decide(shell(c), noMarker).block, true, c);
  }
});

test("guard: allows openspec list, status, validate, archive", () => {
  for (const c of [
    "openspec list",
    "openspec status --change x",
    "openspec validate x",
    "openspec archive x -y",
    "npx @fission-ai/openspec list",
  ]) {
    assert.equal(decide(shell(c), withMarker).block, false, c);
  }
});

test("guard: blocks shell writes into src/test with marker", () => {
  for (const c of [
    "echo x > src/test/A.java",
    "echo x>>src/test/A.java",
    "sed -i s/a/b/ src/test/A.java",
    "rm src/test/A.java",
    "cp a src/test/",
    "mv a src\\test\\A.java",
    "cat a | tee ./src/test/A.java",
  ]) {
    assert.equal(decide(shell(c), withMarker).block, true, c);
    assert.equal(decide(shell(c), noMarker).block, false, `${c} (no marker)`);
  }
  for (const c of ["cat src/test/A.java", "./gradlew test", "cat src/test/A.java 2>&1", "grep -rn foo src/test"]) {
    assert.equal(decide(shell(c), withMarker).block, false, c);
  }
});

test("guard: unknown payload allows with warning", () => {
  const repo = tmpRepo(true);
  const odd = JSON.stringify({ cwd: repo, tool_name: "SomethingNew", tool_input: {} });
  const noPath = JSON.stringify({ cwd: repo, tool_name: "Edit", tool_input: {} });
  for (const input of ["", "{}", "not json", "[]", "null", odd, noPath]) {
    const r = runGuard(input, repo);
    assert.equal(r.status, 0, `exit for ${JSON.stringify(input)}`);
    assert.match(r.stderr, /not checking/);
  }
  assert.equal(parsePayload(""), null);
  assert.equal(parsePayload("{}"), null);
  assert.equal(parsePayload("not json"), null);
  assert.equal(parsePayload(odd).tool, "other");
});

test("guard: every fixture parses", () => {
  const dir = join(here, "fixtures");
  const files = readdirSync(dir).filter((f) => f.endsWith(".json"));
  assert.ok(files.length >= 6, "expected fixtures for claude, codex and copilot");
  for (const f of files) {
    const event = parsePayload(readFileSync(join(dir, f), "utf8"));
    assert.ok(event, f);
    assert.notEqual(event.tool, "other", f);
    // edit fixtures sit under src/test/ (block cases); shell fixtures are git push.
    assert.equal(decide(event, withMarker).block, true, f);
    assert.equal(decide(event, noMarker).block, f.includes("shell"), `${f} without marker`);
  }
});

test("session-start: prints tier or unknown", () => {
  const run = (home) =>
    spawnSync(process.execPath, [sessionStart], {
      encoding: "utf8",
      env: { ...process.env, HOME: home, USERPROFILE: home },
    });
  const withTier = mkdtempSync(join(tmpdir(), "ignite-home-"));
  writeFileSync(join(withTier, ".ignite-tier"), "Yellow — M5 signed off\nsecond line\n");
  let r = run(withTier);
  assert.equal(r.status, 0);
  assert.equal(r.stdout.trim(), "Ignite tier: Yellow — M5 signed off");
  r = run(mkdtempSync(join(tmpdir(), "ignite-home-")));
  assert.equal(r.status, 0);
  assert.equal(r.stdout.trim(), "Ignite tier: unknown — ask a mentor");
});

test("guard: blocks commands wrapped by cmd /c and pwsh -Command", () => {
  for (const c of [
    "cmd /c git push origin x",
    "cmd.exe /C git push",
    "pwsh -Command git push",
    "powershell -Command openspec update",
    'bash -c "git push"',
  ]) {
    assert.equal(decide(shell(c), noMarker).block, true, c);
  }
  for (const c of ["cmd /c dir", "pwsh -Command Get-ChildItem", 'bash -c "echo hi"']) {
    assert.equal(decide(shell(c), withMarker).block, false, c);
  }
});
