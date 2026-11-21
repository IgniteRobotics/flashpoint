# Troubleshooting

Fixes for the problems people hit most, by the message or symptom you see.

## The sample's checksum doesn't match

**What you see:** the Quickstart's checksum line for `flashpoint-sample-1.zip` prints `FAILED` (macOS and Linux) or `False` (Windows).

**Why:** the download was cut short or damaged.

**Fix:** delete `flashpoint-sample-1.zip` and download it again with the [Quickstart's](quickstart.md#try-it-with-sample-logs) command, then check it again. If it still fails, report it with the [bug form](https://github.com/IgniteRobotics/flashpoint/issues/new/choose).
