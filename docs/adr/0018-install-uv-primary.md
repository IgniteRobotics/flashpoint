# 0018. Install: `uv tool install` primary, pipx the alternative (amends 0009)

- Status: Proposed
- Date: 2026-11-19
- Refs: D9, ADR-0009; change `mvp-install-and-first-run` (design D-a); issue #70

## Context
ADR-0009 made `pipx install flashpoint` the install. pipx needs a Python that fits the package's range already on the laptop. For an outside team that often fails: python.org and Homebrew ship the newest Python first (3.14, then 3.15), Windows has no `python3.12` command, and Ubuntu 24.04's apt pipx is old. The package also capped Python below 3.14, although every runtime dependency ships cp314 wheels.

pyntcore (Live CAN) sets the platform floor: its wheels stop at macOS 13, manylinux_2_35 and win_amd64, with no cp315 wheel.

## Decision
- The documented install on macOS, Linux and Windows is `uv tool install --python 3.13 flashpoint`. uv downloads a managed CPython when none fits.
- pipx stays the alternative: `pipx install --python python3.13 flashpoint` on macOS and Linux, and the `py -3.13 -m pipx` form on Windows. Every install, upgrade or reinstall hint the app shows names the uv command first and pipx second.
- The package supports Python 3.11 to 3.14 (`requires-python = ">=3.11,<3.15"`), and CI tests each declared version. 3.13 is the documented default: it's the newest version every dependency has shipped wheels for over several releases.
- The install docs state the platform floor: macOS 13 or later, Linux with glibc 2.35 or later, x86-64 Windows, and no Python 3.15 yet.
- Per-user services are unchanged. uv and pipx both put the command in `~/.local/bin` (`%USERPROFILE%\.local\bin` on Windows), which the systemd unit and the Windows task already use.

## Consequences
- A fresh laptop needs one tool (uv) and no hand-picked Python.
- Existing pipx installs keep working (`pipx upgrade flashpoint`); switching to uv is optional (`pipx uninstall flashpoint`, then the uv command). The configuration and lake are untouched.
- A cross-OS first-run smoke installs the wheel with `uv tool install`, so the documented path is what CI tests.
- pyntcore still limits platforms. Making it an optional extra would lower the floor, but is deferred.

## Alternatives considered
- **`pipx install --fetch-missing-python`:** Ubuntu 24.04's apt pipx predates the flag, and pipx itself still needs a Python.
- **pyntcore as a `[live]` extra:** changes the install story late in the MVP; deferred.
