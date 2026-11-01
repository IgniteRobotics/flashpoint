# Service files

Files for running `flashpoint acquire --watch` as a per-user background service. Setup, `acquire.toml`, backup, status, and troubleshooting are in the [automatic import guide](../docs/guide/auto-import.md).

| File | For | Runs |
|---|---|---|
| [`src/flashpoint/deploy/systemd/flashpoint-acquire.service`](../src/flashpoint/deploy/systemd/flashpoint-acquire.service) | Linux, systemd user unit | `%h/.local/bin/flashpoint acquire --watch`, restarted on failure; environment from `~/.config/flashpoint/env` |
| [`src/flashpoint/deploy/windows/flashpoint-acquire.xml`](../src/flashpoint/deploy/windows/flashpoint-acquire.xml) | Windows, Task Scheduler | `%USERPROFILE%\.local\bin\flashpoint.exe acquire --watch` at logon, restarted up to 3 times |

macOS has no service file yet; see [Automatic import: macOS](../docs/guide/auto-import.md#macos).

Both files expect the pipx install location. Install Flashpoint with pipx, then clone the repository to get the service files (the commands below use paths inside it):

```sh
pipx install --python python3.12 flashpoint
git clone https://github.com/IgniteRobotics/flashpoint.git && cd flashpoint
```

To run unreleased changes instead, install from the checkout: `pipx install --python python3.12 .`

If `pipx environment --value PIPX_BIN_DIR` isn't `~/.local/bin`, edit the path in the file before installing it.

## Linux

```sh
mkdir -p ~/.config/systemd/user
cp src/flashpoint/deploy/systemd/flashpoint-acquire.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now flashpoint-acquire
```

## Windows

```bat
schtasks /Create /XML src\flashpoint\deploy\windows\flashpoint-acquire.xml /TN flashpoint-acquire
schtasks /Run /TN flashpoint-acquire
```

CI checks both files (`tests/test_deploy.py`, and the systemd unit with `systemd-analyze verify`).
