// Ignite SessionStart hook: prints the student's tier from ~/.ignite-tier. Always exits 0.
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

let tier = "";
try {
  tier = readFileSync(join(homedir(), ".ignite-tier"), "utf8").split(/\r?\n/)[0].trim();
} catch {
  // missing or unreadable: fall through to unknown
}
console.log(tier ? `Ignite tier: ${tier}` : "Ignite tier: unknown — ask a mentor");
process.exit(0);
