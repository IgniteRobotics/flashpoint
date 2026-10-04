## Purpose

Copy logs from a robot USB stick (or any removable drive) plugged into the pit machine into the inbox, on macOS, Linux, and Windows, without touching internal or network disks and without modifying the drive.

## ADDED Requirements

### Requirement: Removable volume detection
The system SHALL detect currently mounted removable volumes on macOS, Linux, and Windows. A volume is removable when the operating system reports it as ejectable, removable, or attached over USB, SD, or MMC. Internal disks, disk images, and network filesystems SHALL NOT be treated as removable. Detection SHALL be on by default and SHALL be possible to disable.

#### Scenario: USB stick on macOS
- **WHEN** the OS reports a volume at `/Volumes/ROBOTLOGS` as ejectable, external, and physical
- **THEN** it is detected as removable

#### Scenario: Mounted disk image on macOS
- **WHEN** a `.dmg` is mounted under `/Volumes`
- **THEN** it is not detected as removable

#### Scenario: USB SSD on Linux reporting non-removable
- **WHEN** a block device reports `removable = 0` but is attached through the USB bus and mounted under `/media/<user>/`
- **THEN** it is detected as removable

#### Scenario: USB drive on Windows
- **WHEN** drive `E:` is on a disk whose bus type is USB
- **THEN** it is detected as removable, and the internal `C:` drive is not

#### Scenario: Network share
- **WHEN** an SMB or NFS share is mounted
- **THEN** it is not detected as removable on any OS

#### Scenario: Detection disabled
- **WHEN** removable-media acquisition is turned off
- **THEN** no volume is scanned, even if a robot stick is mounted

### Requirement: Log discovery on a volume
The system SHALL find `.wpilog` and `.hoot` files on each removable volume, up to 4 directory levels deep. Hidden directories and operating-system metadata directories SHALL be skipped.

#### Scenario: Robot stick layout
- **WHEN** a volume holds `logs/FRC_20260314_102233_GACMP_Q7.wpilog` and `logs/2026-03-14_10-22-33/rio_2026-03-14_10-22-33.hoot`
- **THEN** both files are found

#### Scenario: Too deep
- **WHEN** a `.wpilog` sits 6 directories deep
- **THEN** it is not found

#### Scenario: OS metadata
- **WHEN** a volume has a `.Spotlight-V100` or `System Volume Information` directory containing `.wpilog`-named files
- **THEN** those files are not found

### Requirement: Verified copy from a volume
Each file SHALL be copied to a temporary name, then its SHA-256 compared with a hash of the source computed in the same cycle. On a match, the copy is atomically moved into `inbox/usb-<volume label>/<path relative to volume root>`. Stability, deduplication, retry, and failure rules SHALL be the same as for robot pulls. The source is identified by the volume's unique identifier, or by its label when no identifier exists.

#### Scenario: Stick removed mid-copy
- **WHEN** the volume disappears while a file is being copied
- **THEN** no file appears in the inbox under its final name, the temporary file is removed, and the cycle continues with other sources

#### Scenario: Stick plugged in twice
- **WHEN** the same stick is inserted, copied, ejected, and reinserted with no new files
- **THEN** the second insertion copies zero bytes

### Requirement: Never modify the volume
The system SHALL NOT delete, rename, or write any file on a removable volume.

#### Scenario: After a copy
- **WHEN** all logs on a stick have been copied and ingested
- **THEN** the stick's listing and file contents are unchanged

### Requirement: Safe-to-eject notice
After a cycle has copied every eligible file from a volume, the system SHALL log a notice naming the volume as safe to remove. A volume with any file still pending or failed SHALL NOT be reported as safe.

#### Scenario: All copied
- **WHEN** every log on `ROBOTLOGS` is copied and verified
- **THEN** the log contains a safe-to-remove notice for `ROBOTLOGS`

#### Scenario: One file failed
- **WHEN** one file on the volume has failed verification
- **THEN** no safe-to-remove notice is logged, and the failed file is named in the status
