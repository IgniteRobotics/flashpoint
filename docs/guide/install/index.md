# Install

Flashpoint installs with one command on macOS, Linux and Windows. It brings its own Python, so you don't need to install one first.

## What you need

A computer running one of:
- macOS 13 or later;
- Linux with glibc 2.35 or later (Ubuntu 22.04 or later);
- Windows on an x86-64 PC (not Windows on ARM).

Flashpoint runs on Python 3.11 to 3.14 (not 3.15 yet). [uv](https://docs.astral.sh/uv/) installs Flashpoint and, if your computer has no Python in that range, downloads one just for Flashpoint.

You need internet for the install, and once more before your first event to fetch the hoot converter (each OS page says how). To install on a laptop that never goes online, see [Install without internet](../how-to/install-offline.md).

## Install

Follow the page for your computer:
- [macOS](macos.md)
- [Linux](linux.md)
- [Windows](windows.md)

Each one is the same three steps: install uv once, install Flashpoint, and create your configuration:

```sh
uv tool install --python 3.13 flashpoint
flashpoint init --team <your team number>
```

If you already use [pipx](https://pipx.pypa.io/), each page also gives the pipx command. pipx needs a Python 3.11 to 3.14 already installed.

## Next

Import a match and open the app: [Quickstart](../quickstart.md). New to Flashpoint as a team? The [team setup tutorial](../tutorials/team-setup.md) goes through everything in order.
