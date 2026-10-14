# Service files

Files for running `flashpoint acquire --watch` as a per-user background service. Setup, `acquire.toml`, backup, status, and troubleshooting are in the [automatic import guide](../docs/guide/auto-import.md).

| File | For | Runs |
|---|---|---|
| [`systemd/flashpoint-acquire.service`](systemd/flashpoint-acquire.service) | Linux, systemd user unit | `%h/.local/bin/flashpoint acquire --watch`, restarted on failure; environment from `~/.config/flashpoint/env` |
| [`windows/flashpoint-acquire.xml`](windows/flashpoint-acquire.xml) | Windows, Task Scheduler | `%USERPROFILE%\.local\bin\flashpoint.exe acquire --watch` at logon, restarted up to 3 times |

macOS has no service file yet; see [Automatic import: macOS](../docs/guide/auto-import.md#macos).

Both files expect the pipx install location. Install Flashpoint from a checkout (the repository is private, so there's no package index install):

```sh
gh repo clone IgniteRobotics/flashpoint && cd flashpoint
git switch rewrite
pipx install --python python3.12 .
```

If `pipx environment --value PIPX_BIN_DIR` isn't `~/.local/bin`, edit the path in the file before installing it.

## Linux

```sh
mkdir -p ~/.config/systemd/user
cp deploy/systemd/flashpoint-acquire.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now flashpoint-acquire
```

## Windows

```bat
schtasks /Create /XML deploy\windows\flashpoint-acquire.xml /TN flashpoint-acquire
schtasks /Run /TN flashpoint-acquire
```

CI checks both files (`tests/test_deploy.py`, and the systemd unit with `systemd-analyze verify`).
