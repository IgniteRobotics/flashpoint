# 0004. owlet: fetched, checksummed registry selected by hoot compliancy

- Status: Proposed
- Date: 2026-10-04
- Refs: 04 §1, D4; pitfalls #5, #65; P0 findings

## Context
- Converting hoot needs CTRE's `owlet`, and a given owlet only converts hoots at its own format version.
- Today the binary is chosen by host OS and file path. The binaries are committed to git (about 20 MB), and the Linux builds are x86-64 only.
- **P0 finding (2026-10-04):** `owlet --compliancy` prints both versions. For example, owlet 26.1.0 reports `owlet: 19`, and for a 2025 hoot it reports `hoot log: 13`.
- owlet 26.1.0 *reads* the compliancy of older hoots, but refuses to convert them ("Input file is an unsupported version").
- owlet 25.4.1 handles compliancy 13.

## Decision
- **Registry:** a manifest maps `compliancy → {owlet version, os, arch, url, sha256}`. Binaries are fetched on first use into `~/.cache/flashpoint/owlet/` and verified. They are never committed.
- **Selection:** probe each hoot with the newest owlet (`--compliancy`), then convert with the owlet whose compliancy matches. If there is no match, quarantine the file with reason `owlet-unsupported-compliancy:<n>`.
- **Pro detection:** record `--check-pro` per log.

## Consequences
- Version mismatches become explicit and testable (the corpus has compliancy 13 and 19).
- The repo stops growing every season.
- A first run needs network access, or a pre-seeded cache for offline pit laptops (`flashpoint doctor --seed-owlet`).
- ARM Linux depends on CTRE publishing aarch64 builds.

## Alternatives considered
- **Commit binaries** (status quo): repo bloat.
- **Git LFS:** quota limits on a public repo.
- **Select by year in the filename:** fragile, and wrong for mid-season Phoenix updates.
