# Volume detection fixtures

| File | Origin |
| --- | --- |
| `macos/internal-ssd.plist` | Captured: `diskutil info -plist /` on macOS 26 (Apple silicon). Volume and group UUIDs replaced with placeholders. |
| `macos/dmg.plist` | Captured: `diskutil info -plist /Volumes/FPTEST` for a throwaway `hdiutil create -fs HFS+` image. Note it reports Ejectable, RemovableMedia and not Internal, and has no `VirtualOrPhysical` key; only `BusProtocol` = `Disk Image` identifies it. |
| `macos/usb-stick.plist` | Synthesized from the captured dmg key schema, with USB stick values (BusProtocol USB, exfat, VolumeUUID set). |
| `macos/smb-share.plist` | Synthesized from the same schema with SMB values (`FilesystemType` smbfs, no UUID). |
| `linux/mountinfo-pit.txt` | Synthesized from the documented `/proc/self/mountinfo` format (`proc(5)`), including an octal-escaped space. |
| `windows/*.json` | Synthesized from the documented `Get-Partition`/`Get-Disk`/`Get-Volume` properties, as `ConvertTo-Json` emits them (single object and array forms). |

The fake `/sys` and `/dev/disk/by-uuid` trees are built in `tmp_path` by `tests/acquire/test_volumes_linux.py` (synthesized).

The synthesized macOS stick and SMB plists get recaptured from real hardware during human task 11.2. Captured plists contain no usernames or serial numbers.
