## 1. Foundations (config, lake paths, pull ledger)

- [x] 1.1 Tests first: `AcquireConfig` defaults with no file, full file load, unknown key rejected naming the key, CLI overrides win
- [x] 1.2 Implement `acquire/config.py` (pydantic, `extra="forbid"`); `LakePaths.inbox` and `LakePaths.status`
- [x] 1.3 Tests first: `pulls` table upsert by (source, path, size, mtime); dedupe lookup; attempts → `failed` after 3; Parquet snapshot export; `busy_timeout` 30 s on both connections
- [x] 1.4 Implement `acquire/pulls.py` and `Ledger.add_warning`; check that the tests pass

## 2. Fake robot server (test infrastructure)

- [x] 2.1 An in-process paramiko SFTP/exec server fixture on a random localhost port, serving a temp directory; exec handles `sha256sum` and `df -Pk`
- [x] 2.2 Fault switches: drop after N bytes, wrong hash, no `sha256sum` (exit 127), low `df`, auth reject, rotated host key; a self-test proves each switch works

## 3. Robot client (spec: log-acquisition)

- [x] 3.1 Tests first: candidate order and tether fallback, no robot is not an error, `auth_none` then password, host-key record/change warning, auth error logged once per hour
- [x] 3.2 Implement `RobotClient.connect` with timeouts and the known-robots store
- [x] 3.3 Tests first: recursive listing under both roots, a missing root skipped silently, relpaths kept, `remote_sha256`, `free_bytes` with the `statvfs` fallback
- [x] 3.4 Implement listing, hashing, and free space; the low-space warning sets and clears

## 4. Selection and verified transfer (spec: log-acquisition)

- [x] 4.1 Tests first: active rule (newest wpilog per directory, every hoot in the newest session directory), settle re-stat, include-active bypass and flag
- [x] 4.2 Implement selection in `acquire/transfer.py`
- [x] 4.3 Tests first: clean pull is `verified`; a drop mid-transfer leaves no final file or `.part`; hash mismatch retries then `failed`; no `sha256sum` gives `size-verified`; a second cycle transfers 0 bytes; the robot listing is identical before and after
- [x] 4.4 Implement the streaming hash, prefetch read, `.part` and atomic rename, stop-flag checks between 1 MiB chunks

## 5. Removable volumes (spec: removable-media-acquisition)

- [x] 5.1 Capture fixtures: `diskutil info -plist` for a USB stick, an internal SSD, a .dmg, and SMB (captured on Josh's Mac); Linux mountinfo and a fake `/sys` tree (stick and `removable=0` USB SSD); PowerShell JSON (USB and NVMe)
- [x] 5.2 Tests first, then the macOS parser and runner
- [x] 5.3 Tests first, then the Linux parser and runner
- [x] 5.4 Tests first, then the Windows parser and runner (PowerShell JSON, single call, 10 s timeout)
- [x] 5.5 Tests first: depth-4 scan, hidden and OS-metadata directories skipped, verified copy with source re-hash, label sanitising, stick removed mid-copy, reinsert copies 0 bytes, volume untouched, safe-to-eject only when nothing is pending or failed
- [x] 5.6 Implement `volumes.detect()` dispatch, scan, and copy; `--no-usb`

## 6. Cycle, watch, lock, status (spec: acquisition-service)

- [x] 6.1 Tests first: one cycle runs robot → volumes → ingest subprocess → inbox clear (`success`, `skipped`, `quarantined` cleared; a crash keeps files); manual drop ingested once stable; `incomplete-read` added for include-active pulls
- [x] 6.2 Implement `acquire/cycle.py`
- [ ] 6.3 Tests first: second watcher exits non-zero naming the holder; a killed holder's lock is taken over; interrupt exits in ≤ 5 s with no partial final file; dry run changes nothing and lists files with sizes
- [ ] 6.4 Implement `acquire/watch.py` (POSIX/Windows file lock, signal handling, poll loop) and atomic `acquire-status.json`
- [ ] 6.5 CLI: `flashpoint acquire [--watch] [--host] [--include-active] [--dry-run] [--no-usb]`; `doctor` shows the status (last cycle, sources, `low-space`, failed files, last backup)
- [ ] 6.6 E2E test: fake robot → lake gold on a synthetic log; corpus-marked variant serving `2026-gacmp-q7` asserts time to lake < 5 min; perf tests for idle cycle < 3 s and watch RSS ≤ 150 MB

## 7. Backup and restore (spec: lake-backup)

- [ ] 7.1 Tests first (fake `rclone` on `PATH`): raw copied with `--immutable`, never deleted; meta snapshot opens while a writer holds a transaction; no remote is skipped with a message; missing rclone is a status error and the cycle continues; nothing changed means no backup; 15 min interval respected
- [ ] 7.2 Implement `acquire/backup.py` and `flashpoint backup`; wire it into the cycle
- [ ] 7.3 Tests first: restore refuses an existing ledger without `--force`; restore resets `pipeline_version`
- [ ] 7.4 Implement `flashpoint restore`; corpus restore drill with real rclone and a `:local:` remote (backup → restore → `rebuild` → equal hashes and gold features); add rclone to the CI corpus job

## 8. Packaging

- [ ] 8.1 `deploy/systemd/flashpoint-acquire.service` (user unit, single absolute `ExecStart`, restart on failure); CI step runs `systemd-analyze verify --user`
- [ ] 8.2 `deploy/windows/flashpoint-acquire.xml` (logon trigger, restart ×3); a test parses the XML and checks the command
- [ ] 8.3 `deploy/README.md`: pipx install, `acquire.toml`, Linux user unit with `enable-linger` and udisks/udiskie automount, Windows `schtasks /Create /XML`, macOS foreground run, rclone remote setup, restore drill

## 9. Windows CI

- [ ] 9.1 Add a `windows-latest` job (py3.12: ruff, mypy, `pytest -m "not corpus"`) to `.github/workflows/ci.yml`
- [ ] 9.2 Fix the P1/P2 Windows failures it finds, one task per root cause (add sub-tasks as found); any test that can't be fixed in 2 hours gets `skipif(win32)` plus an issue and is listed in the PR

## 10. Docs

- [ ] 10.1 Amend ADR-0009: container deferred, Linux user unit plus Windows scheduled task, Windows supported
- [ ] 10.2 Update `docs/rewrite/05-target-architecture.md` (acquire layout, USB source, argparse not typer) and `06-roadmap.md` P3 (status, USB, Windows, container moved out)
- [ ] 10.3 README: the acquire quick start

## 11. Human verification

- [ ] 11.1 [HUMAN] Live rio: confirm the log roots, `sha256sum` and `df` presence, auth as `lvuser`, transfer MB/s over the radio and the tether, `sha256sum` time per 100 MB; record the results in design.md
- [ ] 11.2 [HUMAN] Robot USB stick on macOS, Linux, and Windows: detected, copied, safe-to-eject notice, stick unchanged
- [ ] 11.3 [HUMAN] Unattended exit check: watch running as a service, plug in the robot after a practice match, match in gold within 5 min
- [ ] 11.4 [HUMAN] Configure the team's rclone remote (Drive), run one backup, run the restore drill into a scratch lake
