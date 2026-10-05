## Context

P1 and P2 built the lake and the derive pipeline, but files still reach it only through a hand-run `flashpoint ingest`. The legacy puller (`docker-services/ingest/src/main.py`) hardcodes `10.68.29.2`, shells out to `sshpass scp`, and deletes robot logs after an unverified copy (#50). Its systemd unit has an invalid `ExecStart` (#7). Backup (`drive-backup.py`) copies into a mounted Drive folder using a broken competition-ID regex (#55).

Decisions: **D9** (deploy) is amended below. No other D1–D11 decision is touched. Robot code in Robot-2025, Robot-2026, and Phoenix-2026 calls `DataLogManager.start()` and `SignalLogger.start()` with default paths, so logs land in `/home/lvuser/logs`, or in `/u/logs` when a USB stick is in the rio.

Settled with Josh during brainstorming (2026-10-04):
- One `acquire --watch` code path runs on a pit laptop (foreground) or an always-on box (service). No container in P3.
- The robot, removable USB volumes (macOS, Linux, Windows), and manual inbox drops are the three sources.
- Logs still being written are skipped by default; `--include-active` pulls them anyway and tags them `incomplete-read`.
- Backup goes to any rclone remote.

## Goals / Non-Goals

**Goals:**
- Plug in a robot (or a stick) and its matches reach gold within 5 minutes, unattended.
- No code path can modify a robot or a stick.
- Every inbox file is hash-verified against its source.
- Runs on macOS, Linux, and Windows.

**Non-Goals:** see the proposal. Notably, no container, no concurrent multi-robot pulls, and no dedupe of partial include-active pulls.

## Decisions

### Layout
```
src/flashpoint/acquire/
  config.py      AcquireConfig (pydantic) from <config_root>/acquire.toml + CLI overrides
  pulls.py       pull ledger table, in the existing SQLite ledger
  robot.py       RobotClient: connect(candidates), list_logs, stat, remote_sha256, free_bytes
  volumes/       detect() -> list[Volume]; macos.py, linux.py, windows.py (pure parsers + thin runners)
  transfer.py    stable-file selection, verified copy to .part, atomic rename into inbox
  cycle.py       one cycle: robot -> volumes -> ingest/derive subprocess -> clear inbox -> backup
  watch.py       loop, lock, signal handling, status.json
  backup.py      rclone copy of raw, sqlite snapshot of meta, restore
deploy/systemd/flashpoint-acquire.service
deploy/windows/flashpoint-acquire.xml
```
`LakePaths` gains `inbox` (`<lake>/inbox`) and `status` (`<lake>/meta/acquire-status.json`).

### SSH and SFTP: paramiko
- The roadmap offered paramiko or asyncssh. paramiko wins, because the code base is sync end to end and only one robot is reachable at a time. Shelling out to `ssh` and `scp` repeats the legacy mistake (no rsync on the rio, no `sshpass` on Windows).
- **Auth:** `lvuser` with an empty password by default. Try `auth_none`, then a password (configurable), with `look_for_keys=False` and `allow_agent=False`, so a laptop's SSH agent never prompts.
- **Host keys:** a custom policy. Fingerprints are stored in `<cache_root>/known-robots.json`, a new one is recorded, and a changed one is logged as a warning and replaced. A reimaged rio changes its key, and this is a LAN-only threat model. It's documented in the operations notes.
- **Throughput:** read with a bounded reader (32 parallel 32 KiB requests per 1 MiB chunk) instead of whole-file `prefetch()`, which would leave the file queued and make a stop wait on `close()`. The plain paramiko `get` is known to run 3–10× slower without pipelining.
- **Remote checks** use exec with `shlex.quote` paths: `sha256sum -- <path>` and `df -Pk -- <root>`. If `sha256sum` is missing (exit 127), the file is `size-verified`. If `df` fails, fall back to SFTP `statvfs`.
- **Connect:** go through the candidates in order with a 2 s TCP timeout each, then a 10 s banner and auth timeout. Defaults are `10.68.29.2`, `roborio-6829-frc.local`, and `172.22.11.2`.

### Stable and active selection
- **Active (robot):** the newest `.wpilog` per directory by mtime, and every `.hoot` in the newest hoot session directory (by directory mtime). These are skipped unless `--include-active` is set.
- **Stable (all sources):** stat, wait for the settle delay (5 s, one wait per cycle per source, not per file), stat again, and keep files whose size and mtime are unchanged.
- The rio clock is often wrong (it has no RTC until the DS connects), so mtime is used only for comparison, never as a date.

### Verified transfer
1. Write to `inbox/<source>/<relpath>.part`, hashing as the bytes stream in.
2. Compare the size, then the hash: the robot's `sha256sum`, or for a volume, the source re-hashed after the copy.
3. `os.replace` to the final name. Since `.part` sits in the same directory, the rename is atomic on all three operating systems.

On any failure, unlink the `.part`, increment `attempts`, and after 3 attempts set the status to `failed`. Volume sources use `usb-<label>`; labels are sanitised to `[A-Za-z0-9_-]`, and an empty label becomes the volume ID's first 8 characters.

### Pull ledger
A new `pulls` table goes in `meta/flashpoint.sqlite`:
```
pulls(source_kind, source_id, remote_path, size, mtime_ns, sha256, status, attempts, reason,
      include_active, first_seen, pulled_at, PRIMARY KEY (source_id, remote_path, size, mtime_ns))
```
`source_id` is the host for robots and the volume UUID for sticks. It's exported to `meta/pulls.parquet` alongside the other snapshots. Concurrency: the watch process writes `pulls` while the ingest subprocess writes the rest, so both connections set `busy_timeout = 30 s` (the ledger is already in WAL mode; Python's default 5 s timeout is too short behind a derive transaction).

### Removable volume backends
Each backend is a pure parser (unit-tested on captured output) plus a runner that calls the OS. It returns `Volume(id, label, mount)`.
- **macOS:** list `/Volumes/*` (skip the symlink to `/`). For each entry, `diskutil info -plist <mount>` is parsed with `plistlib`. Accept when `(Ejectable or RemovableMedia or not Internal)`, `VirtualOrPhysical != "Virtual"`, and `BusProtocol != "Disk Image"` (a real mounted .dmg has no `VirtualOrPhysical` and reports Ejectable and External, so the first rule alone would accept it), and the filesystem is not `smbfs`, `afpfs`, `nfs`, or `webdav`. The ID is `VolumeUUID`.
- **Linux:** parse `/proc/self/mountinfo` and keep block devices under `/media`, `/run/media`, or `/mnt`. Resolve the partition to its disk through `/sys/class/block/<dev>`. Accept when `/sys/block/<disk>/removable == 1`, or when the resolved device path contains `/usb`. The ID is the filesystem UUID from `/dev/disk/by-uuid`.
- **Windows:** one `powershell -NoProfile -Command` call that emits JSON with `DriveLetter`, `BusType`, `UniqueId`, and `FileSystemLabel` from `Get-Partition`, `Get-Disk`, and `Get-Volume`. Accept `BusType` values `USB`, `SD`, or `MMC`. The ID is the volume `UniqueId` GUID.
- **Scan:** walk to depth 4 with `os.scandir`, skipping dot-directories, `System Volume Information`, `$RECYCLE.BIN`, `.Spotlight-V100`, `.Trashes`, and `.fseventsd`.
- **Safe-to-eject** is logged once per insertion, keyed by volume ID plus mount time, when no file from that volume is pending or failed.
- **Timeout:** every OS call has a 10 s timeout. A failed detection logs a warning and skips USB for that cycle.

### The cycle and ingest handoff
1. Robot pulls and volume copies land in the inbox. A failure in one source never stops the others.
2. If the inbox has any non-`.part` stable files, run `[sys.executable, "-m", "flashpoint", "ingest", <settled paths>, "--no-derive", "--lake", L]` as a subprocess, in batches of 200 paths. It gets the explicit settled inbox paths, never the inbox directory, so a manual drop landing mid-cycle can't be ingested half-written. Then one `derive` subprocess runs and its exit is checked. A derive failure is recorded in status, and derive re-runs on later cycles until it succeeds. Ingest and derive each run in their own process group, which is ended on stop. The watch process itself stays small.
3. Read each inbox file's SHA (from `pulls`, or hash it for manual drops). Remove the file if the ledger stage is `success` or `quarantined`, or if it was skipped because it's already in the ledger. On a non-zero exit with no ledger change, keep everything.
4. An include-active file that grows during the pull is copied to its listed size and recorded `size-verified`. The same relpath under two roots waits as `inbox-occupied` until the inbox clears.
5. `incomplete-read` for include-active pulls: the cycle calls a new `Ledger.add_warning(sha, "incomplete-read")` after ingest. Hoots truncated by power loss already get this from owlet.
6. Backup runs if raw or the ledger changed and 15 minutes have passed since the last one.
7. Status is written atomically (temp file plus `os.replace`).

### Locking
`<lake>/meta/acquire.lock` is held with `fcntl.flock(LOCK_EX | LOCK_NB)` on POSIX and `msvcrt.locking(LK_NBLCK)` on Windows. The holder's PID, host, and start time are written into the file so the error message can name it. The OS releases the lock when the process dies, which is the stale-lock takeover. There are no PID liveness checks (`os.kill(pid, 0)` terminates the process on Windows).

### Signals
`acquire` exit codes: 0 ok (also when a watch is stopped), 1 source or step errors, 2 config error, 3 lock held, 130 a one-shot cycle stopped.

SIGINT and SIGTERM (and Ctrl-C/Ctrl-Break on Windows) set a stop flag. The transfer loop checks it between 1 MiB chunks, unlinks the `.part`, and exits. Worst case is 5 s or less, since one chunk takes well under a second.

### Backup (rclone)
- `rclone copy --immutable <lake>/raw <remote>/raw`. `--immutable` makes rclone refuse to overwrite a remote file that differs, so raw on the remote is append-only.
- Raw backup excludes `*.part`. `flashpoint backup` and `flashpoint restore` take the acquire lock, and a manual backup updates the status backup slot.
- **Meta:** use `sqlite3.Connection.backup` to `<lake>/tmp/meta-snapshot/flashpoint.sqlite`, alongside the existing `meta/*.parquet` snapshots copied as files (not re-exported, to keep pyarrow out of the watch and inside the RSS budget) and the status, then `rclone sync` that folder to `<remote>/meta/latest`. Snapshots are consistent because the SQLite online backup API copies a transactionally consistent image.
- **Restore:** refuse if the target ledger exists, unless `--force`. Copy remote raw into `lake/raw` and `meta/latest` into `lake/meta`. Don't import the source's `acquire-status.json`. Set `pipeline_version = NULL` on every file, so `needs_processing` is true. Then print `run: flashpoint rebuild`. `rebuild` reprocesses from raw because the version differs, then runs `derive --all` itself.
- Settings are `backup.remote` (for example `gdrive:flashpoint`) and `backup.interval_min` (default 15). Flashpoint never reads rclone credentials. If `rclone` isn't on `PATH`, that's a status error, not a crash.

### Configuration (`<config_root>/acquire.toml`)
```toml
hosts = ["10.68.29.2", "roborio-6829-frc.local", "172.22.11.2"]
user = "lvuser"
password = ""                          # optional
roots = ["/home/lvuser/logs", "/u/logs"]
poll_s = 30
settle_s = 5
low_space_mb = 100
removable_media = true
[backup]
remote = "gdrive:flashpoint"           # absent = no backup
interval_min = 15
```
The model uses pydantic with `extra="forbid"`. CLI flags: `flashpoint acquire [--watch] [--host H ...] [--include-active] [--dry-run] [--no-usb] [--lake L]`, `flashpoint backup [--lake L]`, and `flashpoint restore [--force] [--lake L]`. The CLI keeps argparse; P1 already uses it, and the roadmap's "typer" is not worth a dependency.

### Packaging (amends ADR-0009)
- **Linux** (`deploy/systemd/flashpoint-acquire.service`), a user unit:
  - `ExecStart=%h/.local/bin/flashpoint acquire --watch`, `Restart=on-failure`, `RestartSec=10`, `WantedBy=default.target`
  - Environment comes from `EnvironmentFile=-%h/.config/flashpoint/env`
  - Install with `systemctl --user enable --now`, plus `loginctl enable-linger` on headless boxes. The README covers `udisks2` and `udiskie` automount for headless USB.
  - Verified in CI with `systemd-analyze verify --user`.
- **Windows** (`deploy/windows/flashpoint-acquire.xml`): a Task Scheduler logon trigger that runs `flashpoint.exe acquire --watch` with "restart on failure" ×3. Import it with `schtasks /Create /XML`.
- **macOS:** the README shows a foreground run. A launchd plist is out of scope until someone runs an always-on Mac.
- **ADR-0009** is amended: the container is deferred out of P3, and the Windows scheduled task and Linux user unit are added. Status stays Proposed.

### Windows CI
A `windows-latest` job runs `ruff`, `mypy`, and `pytest -m "not corpus"` on Python 3.12. Expected P1/P2 fallout is path separators in tests, owlet `.exe` resolution, and open-file renames (Windows can't replace a file that's open). Each failure becomes a fix task under group 9. If a P1/P2 test can't be made portable within the 2-hour task limit, it gets a `skipif(sys.platform == "win32")` with an issue link, and that is reported in the PR.

### Budgets
| Path | Budget | Basis |
|---|---|---|
| Idle cycle (robot reachable, nothing new) | < 3 s wall, excluding the 5 s settle | one connect plus a listing of under 200 files |
| Idle cycle (no robot) | < 7 s | 3 candidates × 2 s timeout, plus volume detection |
| Watch process RSS | ≤ 150 MB | ingest and derive run in subprocesses (their 1 GB budgets are unchanged) |
| Transfer | ≥ 3 MB/s over the radio, ≥ 10 MB/s over the tether | paramiko with prefetch; measured in the `[HUMAN]` live-rio task |
| Time to lake, one match (~100 MB of logs) | ≤ 5 min | pull ~35 s + ingest ~6 s + derive ~30 s, well inside |
| Remote `sha256sum` on the rio | ~1–2 s per 100 MB | rio ARM; measured in the live task |

Heavy imports (polars, duckdb, pyarrow) are deferred off the acquire path, and a guard test (`tests/acquire/test_acquire_imports.py`) enforces it. Measured: watch RSS ~55 MB on macOS; time to lake for corpus q7 ~16 s.

The idle-cycle and watch-RSS budgets get perf tests against the fake SFTP server. Transfer and remote-hash numbers come from the live task.

### Testing
- **Fake robot:** an in-process paramiko `ServerInterface` plus `SFTPServerInterface` serving a temp directory. It implements exec for `sha256sum` and `df` with switches for drop-after-N-bytes, wrong-hash, no-sha256sum, low-df, auth-reject, and a key change. It runs on a random localhost port and works on all three operating systems.
- **Volume backends:** parsers are tested against captured fixtures in `tests/fixtures/volumes/`: diskutil plists for a USB stick, an internal SSD, a .dmg, and an SMB share; mountinfo and a fake `/sys` tree for a removable stick and a `removable=0` USB SSD; PowerShell JSON for USB and NVMe.
- **Cycle end-to-end:** fake robot → inbox → real ingest on a tiny synthetic wpilog → a fake `rclone` on `PATH` → assert the ledger, pulls, status, and inbox. A corpus-marked variant serves `2026-gacmp-q7` and checks time to lake under 5 min.
- **Restore drill** (corpus): a real `rclone` with a local-directory remote (`:local:` backend), installed in the CI corpus job.

## Risks / Trade-offs

- **`sha256sum` may be missing on the NI Linux RT image.** BusyBox normally provides it. If it's absent, everything is `size-verified`. The live-rio task checks this first.
- **paramiko SFTP throughput** may still miss 3 MB/s over the radio. Mitigation: tune prefetch and window size. The 5-minute budget has about 4× headroom.
- **Active-file rule:** the last log of a session sits on the robot until the code restarts. That's accepted, because events power-cycle between matches. `--include-active` covers the bench.
- **The robot's clock jumps when the DS connects**, which changes nothing here. mtime is compared, never interpreted.
- **Windows-only P1/P2 bugs** could balloon. The 2-hour-per-task rule and `skipif` fallback cap the damage, and any skips are listed in the PR.
- **Headless Linux USB** depends on an automounter outside Flashpoint. It's documented, not automated.
- **Host-key auto-accept** means a spoofed robot on the pit LAN could feed us logs. Impact is limited to bad data in the lake: content is quarantined if it's malformed, and we never send credentials that matter.
